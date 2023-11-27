import boto3
import fiona
import geopandas as gpd
from io import BytesIO
import logging
from datetime import datetime, timezone
import json
import os
import pathlib as pl
from pystac import Asset, Item, Collection, Extent, SpatialExtent, TemporalExtent
from shapely.geometry import mapping
import s3fs
from typing import Tuple
import uuid
import zipfile

from .utils import (
    vsi_path,
    footprint_from_bbox,
    collection_bounding_boxes,
    texas_bbox,
    us_bbox,
    bbox_to_4326,
    key_last_updated
)
from .vectors import (
    vector_item_properties,
    get_vector_meta,
    approx_vector_size,
    add_vector_thumbnail_asset_to_item,
    to_hull,
    STAC_VECTOR_EXTENSIONS,
)
from .rasters import (
    get_raster_meta,
    add_raster_thumbnail_asset_to_item,
    raster_item_properties,
    STAC_RASTER_EXTENSIONS,
)

from.ras_model import (
    get_ras_model_meta,
    ras_model_item_properties,
    STAC_RAS_MODEL_EXTENSIONS
)


class ZipReaderError(Exception):
    def __init__(self, message="Error extracting data from zip"):
        self.message = message
        super().__init__(self.message)


class ZippedFGDB:
    def __init__(self, bucket: str, key: str, session: fiona.session.AWSSession):
        self.bucket = bucket
        self.key = key
        self.vsi_path = vsi_path(bucket, key)
        try:
            with fiona.Env(session=session):
                self._contents = fiona.listlayers(self.vsi_path)
        except Exception as e:
            raise ZipReaderError(
                f"Cannot read or list contents of {self.vsi_path}: {e}"
            )

    @property
    def contents(self):
        return self._contents

    def process_fgdb(
        self,
        project: str,
        collection_id: str,
        sess: fiona.session.AWSSession,
        mem_limit: float = 0.001,
    ) -> Tuple[Item, str]:
        items, bboxes, extensions = [], [], []

        for layer in self.contents:
            zv = ZippedVector(self.bucket, self.key, layer, sess)
            approx_size, nrows = approx_vector_size(zv.bucket, zv.key, zv.vector_name)
            if approx_size < mem_limit:
                logging.info(
                    f"process_fgdb | {collection_id}:{zv.vector_name} initializeing processing: {nrows} rows ~ {approx_size} GB"
                )
                item, output_file = zipped_vector_to_item(project, zv, collection_id)
                if isinstance(item, Item):
                    try:
                        items.append(item)
                    except Exception as e:
                        logging.warning(
                            f"process_fgdb | {collection_id}:{zv.vector_name} cannot be added to collection items list"
                        )

                    try:
                        bboxes.append(item.bbox)
                    except Exception as e:
                        logging.warning(
                            f"process_fgdb | {collection_id}:{zv.vector_name} bbox cannot be added to collection bbox list"
                        )

                    try:
                        extensions.extend(item.stac_extensions)
                    except Exception as e:
                        logging.warning(
                            f"process_fgdb | {collection_id}:{zv.vector_name} extensions cannot be added to collection extension list"
                        )
                else:
                    logging.warning(
                        f"process_fgdb | {collection_id}:{zv.vector_name} | unable to process vector (skipping!)"
                    )
            else:
                logging.warning(
                    f"process_fgdb | {collection_id}:{zv.vector_name} too large for processing: {nrows} rows ~ {approx_size}GB"
                )

        collection_bbox = collection_bounding_boxes(bboxes)

        collection = Collection(
            id=collection_id,
            description="REMOVE HARDCODING",
            stac_extensions=extensions,
            extent=Extent(
                spatial=SpatialExtent(collection_bbox),
                temporal=TemporalExtent(
                    intervals=[datetime.now(tz=timezone.utc), None]
                ),
            ),
        )
        return collection

    def __repr__(self):
        return json.dumps(
            {
                "ZippedFGDB": {
                    "bucket": self.bucket,
                    "key": self.key,
                    "layers": len(self._contents),
                }
            }
        )


def scan_s3_zip(fs: s3fs.S3FileSystem, s3_zip_file: str):
    contents = []
    ras_models = []
    with fs.open(s3_zip_file, "rb") as zip_file:
        with zipfile.ZipFile(BytesIO(zip_file.read())) as zip_ref:
            info = zip_ref.infolist()
            for i, file in enumerate(info):
                logging.info(f"scan_s3_zip | {i} {file.filename}")
                contents.append(file.filename)
                if pl.Path(file.filename).suffix == ".prj":
                    file_bytes = zip_ref.read(file.filename)
                    logging.debug(
                        f"scan_s3_zip | prj data for {file.filename}: {file_bytes.decode()}"
                    )
                    if "Proj Title" in file_bytes.decode():
                        ras_models.append(file.filename)

    return contents, ras_models


class S3Zip:
    def __init__(self, bucket: str, key: str, fs: s3fs.S3FileSystem):
        self.bucket = bucket
        self.key = key
        self.vsi_path = (vsi_path(self.bucket, self.key),)
        self.fs = fs

        try:
            contents, ras_models = scan_s3_zip(self.fs, f"{self.bucket}/{self.key}")
            self._contents = contents
            self._ras_models = ras_models
        except Exception as e:
            raise ZipReaderError(
                f"Cannot read or list contents of {self.bucket}/{self.key}: {e}"
            )

    @property
    def contents(self):
        return self._contents

    @property
    def unique_file_extensions(self):
        """
        Return inventory of file extensions
        """
        suffixes = [pl.Path(f).suffix for f in self.contents]
        return list(set(suffixes))

    @property
    def ras_models(self):
        """
        Return list of ras models identified
        """
        return self._ras_models

    @property
    def contains_ras_models(self):
        if len(self.ras_models) >= 1:
            return True
        return False

    @property
    def shapefiles(self):
        """
        Return list of shapefiles identified
        """
        return [f for f in self.contents if pl.Path(f).suffix in [".shp"]]

    @property
    def contains_shapefiles(self):
        if len(self.shapefiles) >= 1:
            return True
        return False

    @property
    def rasters(self):
        return [f for f in self.contents if pl.Path(f).suffix in [".tif"]]

    @property
    def contains_rasters(self):
        if len(self.rasters) >= 1:
            return True
        return False

    @property
    def non_spatial_data(self):
        """
        Return list of shapefiles identified
        Intentionally not including other known vector tyes, as these will need to be included in the search befor excluding here
        """
        non_spatial_datasets = [
            f
            for f in self.contents
            if pl.Path(f).suffix.lower()
            not in [".shp", ".shx", ".sbx", ".sbn", ".cpg", ".dbf", ".prj", ".tif"]
        ]
        # Remove directories
        return [f for f in non_spatial_datasets if pl.Path(f).suffix != ""]

    def shapefile_parts(self, filename: str):
        """
        Return list of auxilary files (assumed) to be parts of a shapefile
        """
        if pl.Path(filename).suffix != ".shp":
            raise ValueError(
                f"filename ext must be `.shp` not {pl.Path(filename).suffix}"
            )
        return [
            f
            for f in self.contents
            if f[:-4] == filename[:-4] and pl.Path(f).suffix != ".shp"
        ]

    def zipped_ras_model(self, ras_prj_file: str, collection_id: str, session: any):
        return ZippedRASModel(
            bucket=self.bucket,
            key=self.key,
            ras_prj_file=ras_prj_file,
            collection_id=collection_id,
            contents=self.contents,
            fs=self.fs,
            session=session,
        )

    def zipped_vector(self, vector_name: str, collection_id: str, session: any):
        return ZippedVector(
            bucket=self.bucket,
            key=self.key,
            vector_name=vector_name,
            contents=self.contents,
            collection_id=collection_id,
            fs=self.fs,
            session=session,
        )

    def __repr__(self):
        return json.dumps(
            {
                "S3ZIP": {
                    "bucket": self.bucket,
                    "key": self.key,
                    "shapefiles": len(self.shapefiles),
                    "rasters": len(self.rasters),
                    "ras_models": len(self.ras_models),
                }
            }
        )


class ZippedVector:
    """
    file_name represents the name of a *.shp file or the name of a layer in a gdb
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        vector_name: str,
        contents: list,
        collection_id: str,
        fs: s3fs.S3FileSystem,
        session: fiona.session.AWSSession,
    ):
        self.bucket = bucket
        self.key = key
        self.vector_name = vector_name
        self.contents = contents
        self.collection_id = collection_id
        self.fs = fs
        self._fiona_session = session

        if vector_name.endswith(".shp"):
            self.store = "shapefile"
            self.vsi_path = vsi_path(self.bucket, self.key, self.vector_name)
            with fiona.Env(session=self._fiona_session):
                try:
                    self.meta_data = get_vector_meta(self.vsi_path)
                    self.meta_data.shapefile_parts = self.shapefile_parts
                except Exception as e:
                    logging.error(
                        f"ZippedVector | failed reading metadata shapefile {self.vector_name}: {e}"
                    )
                    raise LookupError(e)
        else:
            self.vsi_path = vsi_path(self.bucket, self.key)
            self.store = "fgdb"
            try:
                self.meta_data = get_vector_meta(self.vsi_path, layer=self.vector_name)
            except Exception as e:
                logging.error(
                    f"ZippedVector | failed reading metadata from gdb layer {self.vector_name}: {e}"
                )
                raise LookupError(e)

    @property
    def s3_client(self):
        return boto3.client("s3")

    @property
    def bbox(self):
        return self.meta_data.bbox

    @property
    def projection(self):
        return self.meta_data.projection

    def footprint(self):
        logging.debug(
            f"footprint | {self.vector_name}: {footprint_from_bbox(self.bbox, self.projection)}"
        )
        if footprint_from_bbox(self.bbox, self.projection).within(texas_bbox()):
            try:
                return to_hull(self.as_gdf())
            except:
                logging.warning(
                    f"footprint | {self.vector_name}: unable to simplfy geometry: defaulting to state bbox"
                )
                return texas_bbox()
        else:
            logging.warning(
                f"footprint | {self.vector_name}: data not properly constrained to state: need to clip"
            )
            try:
                return footprint_from_bbox(self.bbox, self.projection)
            except:
                return us_bbox()

    @property
    def bbox_4326(self):
        return bbox_to_4326(self.meta_data.bbox, self.projection)

    @property
    def fields(self):
        return self.meta_data.fields

    @property
    def geom_type(self):
        return self.meta_data.geom_type

    def as_gdf(self):
        if self.store == "fgdb":
            with fiona.open(
                f"zip+s3://{self.bucket}/{self.key}", layer=self.vector_name
            ) as src:
                gdf = gpd.GeoDataFrame.from_features(
                    [feature for feature in src], crs=self.projection
                )
        else:
            gdf = gpd.read_file(f"zip+s3://{self.bucket}/{self.key}/{self.vector_name}")
            gdf.crs = self.projection
        return gdf

    def shapefile_parts(self):
        """
        This function compares items in the zip at the same level and returns
        items that share file name with a shapefile, in an attempt to capture
        the many files associated with a shapefile
        """
        return [
            f
            for f in self.contents
            if f[:-4] == self.vector_name[:-4] and pl.Path(f).suffix != ".shp"
        ]

    def to_stac_item(
        self,
        item_id: str,
        dtm: str,
        bbox_4326: str,
        footprint: str,
        properties: list,
        stac_extensions: list = STAC_VECTOR_EXTENSIONS,
        href: str = None,
    ):
        return Item(
            href=href,
            id=item_id,
            collection=self.collection_id,
            geometry=footprint,
            bbox=bbox_4326,
            datetime=dtm,
            stac_extensions=stac_extensions,
            properties=properties,
        )

    def __repr__(self):
        return json.dumps(
            {
                "ZippedVector": {
                    "bucket": self.bucket,
                    "key": self.key,
                    "vector": self.vector_name,
                }
            }
        )


def zipped_vector_to_item(
    project: str,
    zv: ZippedVector,
    collection_id: str,
) -> Item:
    approx_size, nrows = approx_vector_size(zv.bucket, zv.key, zv.vector_name)

    try:
        properties = vector_item_properties(project, fields=zv.meta_data.fields)

        if not properties:
            return None, None
        properties["approx_gb_in_memory"] = approx_size
        properties["feature_count"] = nrows
        logging.info(
            f"zipped_vector_to_item | `{zv.vector_name}`: retrieved vector properties"
        )
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to retrieve vector properties"
        )
        return None, None

    try:
        gdf = zv.as_gdf()
        gdf = gdf.to_crs("epsg:4326")
        logging.info(
            f"zipped_vector_to_item | `{zv.vector_name}`: converted to geodataframe (4326)"
        )
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to convert to geodataframe (4326)"
        )
        raise ZipReaderError(e)

    try:
        footprint = zv.footprint()
        logging.info(f"zipped_vector_to_item | `{zv.vector_name}`: created footprint")
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to create footprint"
        )
        raise ZipReaderError(e)

    try:
        bbox = zv.bbox_4326
        logging.info(f"zipped_vector_to_item | `{zv.vector_name}`: created bbox (4326)")
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to create bbox (4326)"
        )
        raise ZipReaderError(e)

    try:
        item_id = pl.Path(zv.vector_name).name.replace(".shp", "")
        logging.info(
            f"zipped_vector_to_item | `{zv.vector_name}`: item_id assigned `{item_id}`"
        )
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to assign item_id `{item_id}`"
        )
        raise ZipReaderError(e)

    try:
        dtm = key_last_updated(zv.bucket, zv.key)
        logging.info(
            f"zipped_vector_to_item | `{zv.vector_name}`: last update time accessed: `{dtm}`"
        )
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to retrieve last update time"
        )
        raise ZipReaderError(e)

    if isinstance(zv, S3Zip):
        properties["shapefile_parts"] = zv.shapefile_parts(zv.vector_name)

    try:
        item = zv.to_stac_item(item_id, dtm, bbox, mapping(footprint), properties)
        logging.info(f"zipped_vector_to_item | `{zv.vector_name}`: created pystac.Item")
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: failed to create pystac.Item"
        )
        raise ZipReaderError(e)

    try:
        item.add_asset(
            str(uuid.uuid4()),
            Asset(
                href=f"s3://{zv.bucket}/{zv.key}/{zv.vector_name}",
                title=zv.vector_name,
                description="zipped vector file",
            ),
        )
        logging.info(
            f"zipped_vector_to_item | `{zv.vector_name}`: added vector asset to item"
        )
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: failed adding vector asset to item"
        )
        raise ZipReaderError(e)

    try:
        thumbnail_key = f"stac/collections/{collection_id}/{item.id}-thumbnail.png"
        item_with_thumbnail = add_vector_thumbnail_asset_to_item(
            gdf, footprint, zv.bucket, thumbnail_key, item
        )
        item, png = item_with_thumbnail
        zv.s3_client.put_object(Body=png, Bucket=zv.bucket, Key=thumbnail_key)
        logging.info(
            f"zipped_vector_to_item | `{zv.vector_name}`: added thumbnail asset: {thumbnail_key}"
        )
    except Exception as e:
        logging.error(
            f"zipped_vector_to_item | `{zv.vector_name}`: unable to add thumbnail asset: {thumbnail_key} error: {e}"
        )
        raise ZipReaderError(e)

    # try:
    #     item.validate()
    # except Exception as e:
    #     logging.error(f"{e}")
    #     raise

    logging.info(f"zipped_vector_to_item | `{zv.vector_name}`: processing complete!")
    return item


class ZippedRaster:
    def __init__(self, bucket: str, key: str, file_name: str, collection_id: str):
        self.bucket = bucket
        self.key = key
        self.file_name = file_name
        self.collection_id = collection_id
        self.vsi_path = vsi_path(self.bucket, self.key, self.file_name)
        self.meta_data = get_raster_meta(self.vsi_path)

    @property
    def s3_client(self):
        return boto3.client("s3")

    @property
    def bbox(self):
        return self.meta_data.bbox

    @property
    def projection(self):
        return self.meta_data.projection

    @property
    def footprint(self):
        return footprint_from_bbox(self.meta_data.bbox, self.projection)

    @property
    def bbox_4326(self):
        return bbox_to_4326(self.meta_data.bbox, self.projection)

    @property
    def resoultion(self):
        return self.meta_data.resoultion

    def to_stac_item(
        self,
        item_id: str,
        dtm: str,
        properties: dict,
        stac_extensions: list = STAC_RASTER_EXTENSIONS,
        href: str = None,
    ):
        return Item(
            href=href,
            id=item_id,
            geometry=mapping(self.footprint),
            bbox=self.bbox_4326,
            datetime=dtm,
            stac_extensions=stac_extensions,
            properties=properties,
        )

    def __repr__(self):
        return json.dumps(
            {
                "ZippedRaster": {
                    "bucket": self.bucket,
                    "key": self.key,
                    "raster": self.file_name,
                }
            }
        )


def zipped_raster_to_item(
    project: str,
    zr: ZippedRaster,
    collection_id: str,
) -> Item:
    try:
        properties = raster_item_properties(project, zr.projection, zr.resoultion)
        logging.info(
            f"zipped_raster_to_item | `{zr.file_name}`: retrieved raster properties"
        )
    except Exception as e:
        logging.error(
            f"zipped_raster_to_item | `{zr.file_name}`: unable to retrieve raster properties"
        )
        raise ZipReaderError(e)

    try:
        item_id = pl.Path(zr.file_name).stem
    except Exception as e:
        logging.error(f"{e}")
        raise ZipReaderError(e)

    try:
        dtm = key_last_updated(zr.bucket, zr.key)
        logging.info(
            f"zipped_raster_to_item | `{zr.file_name}`: last update time accessed: `{dtm}`"
        )
    except Exception as e:
        logging.error(
            f"zipped_raster_to_item | `{zr.file_name}`: unable to retrieve last update time"
        )
        raise ZipReaderError(e)

    try:
        item = zr.to_stac_item(item_id, dtm, properties)
        logging.info(f"zipped_raster_to_item | `{zr.file_name}`: created pystac.Item")
    except Exception as e:
        logging.error(f"zipped_raster_to_item | `{zr.file_name}`: created pystac.Item")
        raise ZipReaderError(e)

    try:
        item.add_asset(
            str(uuid.uuid4()),
            Asset(
                href=f"s3://{zr.bucket}/{zr.key}/{zr.file_name}",
                title=zr.file_name,
                description="zipped raster file",
            ),
        )
        logging.info(
            f"zipped_raster_to_item | `{zr.file_name}`: added raster asset to item"
        )
    except Exception as e:
        logging.error(
            f"zipped_raster_to_item | `{zr.file_name}`: failed adding raster asset to item"
        )
        raise ZipReaderError(e)

    try:
        thumbnail_key = f"stac/collections/{collection_id}/{item.id}-thumbnail.png"
        item_with_thumbnail = add_raster_thumbnail_asset_to_item(
            zr.vsi_path, zr.bucket, thumbnail_key, item
        )
        item, png = item_with_thumbnail
        zr.s3_client.put_object(Body=png, Bucket=zr.bucket, Key=thumbnail_key)
        logging.info(
            f"zipped_raster_to_item | `{zr.file_name}`: added thumbnail asset: {thumbnail_key}"
        )
    except Exception as e:
        logging.error(
            f"zipped_raster_to_item | `{zr.file_name}`: unable to add thumbnail asset: {thumbnail_key} error: {e}"
        )
        raise ZipReaderError(e)

    # try:
    #     item.validate()
    # except Exception as e:
    #     logging.error(f"{e}")
    #     raise

    logging.info(f"zipped_raster_to_item | `{zr.file_name}`: processing complete!")
    return item

class ZippedRASModel:
    """
    ras_prj_file represents the name of a *.prj file within a zip file
    """
    def __init__(
        self,
        bucket: str,
        key: str,
        ras_prj_file: str,
        contents: list,
        collection_id: str,
        fs: s3fs.S3FileSystem,
        session: fiona.session.AWSSession,
    ):
        self.bucket = bucket
        self.key = key
        self.ras_prj_file = ras_prj_file
        self.contents = contents
        self.collection_id = collection_id
        self.fs = fs
        self._fiona_session = session

        try:
            self.vsi_path = f"{self.bucket}/{self.key}"
        except Exception as e:
            logging.error(
                f"ZippedRASModel | failed reading metadata {self.ras_prj_file}: {e}"
            )
            raise LookupError(e)
        
    # @property
    # def fields(self):
    #     return self.meta_data.fields
    
    @property
    def ras_model_files(self) -> dict:
        """
        Return dictionary of auxilary files (assumed) to be parts of a ras model
        """
        model_files = {"geometry_files": [], "other_files": []}
        for f in self.contents:
            if self.ras_prj_file.strip(".prj") in f:
                if ".g" in pl.Path(f).suffix:
                    model_files["geometry_files"].append(f)
                else:
                    model_files["other_files"].append(f)
        return model_files
    
    @property
    def geometry_files(self):
        return self.ras_model_files["geometry_files"]

    @property
    def non_geometry_files(self):
        return self.ras_model_files["other_files"]
    
    def geometry_meta(self, filename:str):
        return get_ras_model_meta(self.fs, self.vsi_path, filename)
    
    def bbox_4326(self, bbox:list, projection: str):
        return bbox_to_4326(bbox, projection)
    
    def footprint(self, bbox:list, projection: str):
        return mapping(footprint_from_bbox(self.bbox_to_4326(bbox, projection)))

    def to_stac_item(
        self,
        item_id: str,
        dtm: datetime,
        bbox_4326: str,
        footprint:dict,
        properties: dict,
        stac_extensions: list = STAC_RAS_MODEL_EXTENSIONS,
        href: str = None,
    ):
        return Item(
            id=item_id,
            collection=self.collection_id,
            geometry=footprint,
            bbox=bbox_4326,
            datetime=dtm,
            stac_extensions=stac_extensions,
            properties=properties,
            href=href,
        )

    def __repr__(self):
        return json.dumps(
            {
                "ZippedRASModel": {
                    "bucket": self.bucket,
                    "key": self.key,
                    "prj": self.ras_prj_file,
                }
            }
        )


def zipped_ras_model_to_item(project:str, zrm:ZippedRASModel, collection_id:str, projection:str):
    try:
        g = zrm.geometry_files[0]
        meta = zrm.geometry_meta(g)
        logging.info(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: retrieved ras geometry file"
        )
    except Exception as e:
        logging.error(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: unable to retrieve ras geometry file {g}"
        )
        
    try:
        bbox4326 = zrm.bbox_4326(meta.bbox, projection)
        footprint = mapping(footprint_from_bbox(bbox4326, projection))
        logging.info(f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: created first guess at raster bbox: {bbox4326}"
        )
    except Exception as e:
        logging.error(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: unable to create raster bbox"
        )

    try:
        item_id = pl.Path(zrm.ras_prj_file).name.replace(".prj", "") + "-ras-model"
        logging.info(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: item_id assigned `{item_id}`"
        )
    except Exception as e:
        logging.error(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: unable to assign item_id `{item_id}`"
        )
        raise ZipReaderError(e)

    try:
        dtm = key_last_updated(zrm.bucket, zrm.key)
        logging.info(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: last update time accessed: `{dtm}`"
        )
    except Exception as e:
        logging.error(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: unable to retrieve last update time"
        )
        raise ZipReaderError(e)
    
    try:
        dtm =  datetime.now(tz=timezone.utc)
        properties = ras_model_item_properties(project)
        item = zrm.to_stac_item(
                item_id,
                dtm,
                bbox4326,
                footprint,
                properties=properties,
                stac_extensions=STAC_RAS_MODEL_EXTENSIONS
            )
        logging.info(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: created pystac.Item"
        )
    except Exception as e:
        logging.error(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: failed to create pystac.Item"
        )
        raise ZipReaderError(e)
    

    for part in zrm.ras_model_files["other_files"]:
        item.add_asset(
            str(uuid.uuid4()),
            Asset(
                href=f"s3://{zrm.bucket}/{zrm.key}/{part}",
                title=part,
                description="hec-ras file",
            ),
        )
        logging.info(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: added ras asset {part} to {item.id}"
        )

    for part in zrm.ras_model_files["geometry_files"]:
        item.add_asset(
            str(uuid.uuid4()),
            Asset(
                href=f"s3://{zrm.bucket}/{zrm.key}/{part}",
                title=part,
                description="hec-ras file",
                roles=["hec-ras"]
            ),
        )
        logging.info(
            f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: added ras asset {part} to {item.id}"
        )

    # try:
    #     item.validate()
    # except Exception as e:
    #     logging.error(
    #         f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: item invalid"
    #     )
    #     raise ZipReaderError(e)
    

    logging.info(f"zipped_ras_model_to_item | `{zrm.ras_prj_file}`: processing complete!")
    return item
        

def new_collection_from_zip(
    project: str,
    z: S3Zip,
    collection_id: str,
    collection_title: str,
    sess: fiona.session.AWSSession,
) -> Tuple[Item, str]:
    items, bboxes, extensions, projections = [], [], [], []
    all_ras_model_files = []
    for shapefile in z.shapefiles:
        try:
            zv = z.zipped_vector(shapefile, collection_id, sess)
            projections.append(zv.meta_data.projection)
        except LookupError:
            continue

        item = zipped_vector_to_item(project, zv, collection_id)

        if isinstance(item, Item):
            try:
                items.append(item)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{zv.vector_name} cannot be added to collection items list"
                )

            try:
                bboxes.append(item.bbox)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{zv.vector_name} bbox cannot be added to collection bbox list"
                )

            try:
                extensions.extend(item.stac_extensions)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{zv.vector_name} extensions cannot be added to collection extension list"
                )

        else:
            logging.warning(
                f"process_collection | {collection_title}:{zv.vector_name} | unable to process vector (skipping!)"
            )

    for raster in z.rasters:
        try:
            zr = ZippedRaster(z.bucket, z.key, raster, collection_id)
        except LookupError:
            continue

        item = zipped_raster_to_item(project, zr, collection_id)

        if isinstance(item, Item):
            try:
                items.append(item)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{zr.file_name} cannot be added to collection items list"
                )

            try:
                bboxes.append(item.bbox)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{zr.file_name} bbox cannot be added to collection bbox list"
                )

            try:
                extensions.extend(item.stac_extensions)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{zr.file_name} extensions cannot be added to collection extension list"
                )

        else:
            logging.warning(
                f"process_collection | {collection_title}:{zr.file_name} | unable to process vector (skipping!)"
            )

    for model in z.ras_models:
        try:
            zrm = z.zipped_ras_model(model, collection_id, sess)
        except LookupError:
            continue

        use_first_projection = projections[0]
        item = zipped_ras_model_to_item(project, zrm, collection_id, use_first_projection)

        if isinstance(item, Item):
            try:
                items.append(item)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{model} cannot be added to collection items list"
                )

            try:
                bboxes.append(item.bbox)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{model} bbox cannot be added to collection bbox list"
                )

            try:
                extensions.extend(item.stac_extensions)
            except Exception as e:
                logging.warning(
                    f"process_collection | {collection_title}:{model} extensions cannot be added to collection extension list"
                )

            all_ras_model_files.extend(zrm.ras_model_files["other_files"])
            all_ras_model_files.extend(zrm.ras_model_files["geometry_files"])

        else:
            logging.warning(
                f"process_collection | {collection_title}:{model} | unable to process ras model (skipping!)"
            )

    collection_bbox = collection_bounding_boxes(bboxes)

    collection = Collection(
        id=collection_id,
        title=collection_title,
        href=f"https://{z.bucket}.s3.amazonaws.com/stac/collections/{collection_id}/collection.json",
        description="Zip archive",
        stac_extensions=extensions,
        extent=Extent(
            spatial=SpatialExtent(collection_bbox),
            temporal=TemporalExtent(intervals=[datetime.now(tz=timezone.utc), None]),
        ),
    )

    collection.add_items(items)

    for f in z.non_spatial_data:
        if f not in all_ras_model_files:
            logging.info(f"process_collection | adding asset {f} to {collection_title}")
            collection.add_asset(
                str(uuid.uuid4()),
                Asset(
                    href=f"s3://{z.bucket}/{z.key}/{f}",
                    title=pl.Path(f).name,
                    roles=["data"],
                    description="internal file",
                ),
            )

    return collection