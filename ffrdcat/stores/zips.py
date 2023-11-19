import fiona
from dataclasses import dataclass
from datetime import datetime, timezone
import pathlib as pl
import json
from pystac import Item, Asset
from shapely.geometry import mapping
import logging    
from common.s3_utils import vsi_path
from common.geo_utils import bbox_to_4326, footprint_from_bbox

from stores.vectors import get_vector_meta, simplified_footprint, STAC_VECTOR_EXTENSIONS
from stores.rasters import get_raster_meta, STAC_RASTER_EXTENSIONS


def read_zip(bucket: str, key: str, session: fiona.session.AWSSession):
    """
    Uses fiona to search the root directory of zip files and list contents,
    In the case of nested contents, this funcion will miss any files in
    subdirectories.

    TODO: Address the possiblity of silent failures (i.e., not identifying all files) ASAP.
    """
    try:
        zfile = vsi_path(bucket, key)
        with fiona.Env(session=session):
            contents = fiona.listdir(zfile)
    except:
        raise KeyError(f"Cannot read or list contents of {zfile}")
    return contents


def read_zipped_gdb(bucket: str, key: str, session: fiona.session.AWSSession):
    try:
        zfile = vsi_path(bucket, key)
        with fiona.Env(session=session):
            contents = fiona.listlayers(zfile)
    except:
        raise KeyError(f"Cannot read or list contents of {zfile}")
    return contents


class S3Zip:
    def __init__(self, bucket: str, key: str, session: fiona.session.AWSSession):
        self.bucket = bucket
        self.key = key
        self.session = session
        self._contents = read_zip(bucket, key, session)

    @property
    def contents(self):
        return self._contents

    @property
    def unique_file_extensions(self):
        """
        Return inventory f file extensions
        """
        suffixes = [pl.Path(f).suffix for f in self.contents]
        return list(set(suffixes))

    @property
    def shapefiles(self):
        """
        Return list of shapefiles identified at the root level
        """
        return [f for f in self.contents if pl.Path(f).suffix in [".shp"]]

    @property
    def contains_shapefiles(self):
        if len(self.shapefiles) >= 1:
            return True
        return False

    @property
    def contains_rasters(self):
        if len(self.rasters) >= 1:
            return True
        return False

    @property
    def rasters(self):
        return [f for f in self.contents if pl.Path(f).suffix in [".tif"]]

    @property
    def stac_type(self):
        """
        Placeholder, looking for a clean way to label a zip archive based on
        the content type and quantity.
        TODO: Improve / move to funciton / somehting else to capture proper desgination in stac context.
        """
        if len(self.shapefiles) + len(self.rasters) < 1:
            return "asset"
        if len(self.shapefiles) + len(self.rasters) == 1:
            return "item"
        if len(self.shapefiles) + len(self.rasters) > 1:
            return "collection"

    def shapefile_parts(self, filename: str):
        """
        Return list of auxilary files (assumed) to be parts of a shapefile
        """
        return [f for f in self.contents if f[:-4] == filename[:-4]]

    def __repr__(self):
        return json.dumps(
            {
                "S3ZIP": {
                    "bucket": self.bucket,
                    "key": self.key,
                    "shapefiles": len(self.shapefiles),
                    "rasters": len(self.rasters),
                }
            }
        )


class ZippedVector(S3Zip):
    def __init__(
        self, bucket: str, key: str, file_name: str, session: fiona.session.AWSSession
    ):
        super().__init__(bucket, key, session)
        self.file_name = file_name
        self.vsi_path = vsi_path(self.bucket, self.key, self.file_name)
        with fiona.Env(session=session):
            try:
                self.meta_data = get_vector_meta(self.vsi_path)
            except Exception as e:
                logging.error(f"unable to read data from file {self.file_name}:{e}")

    @property
    def bbox(self):
        return self.meta_data.bbox

    @property
    def projection(self):
        return self.meta_data.projection

    @property
    def footprint(self):
        return simplified_footprint(self.vsi_path)

    @property
    def bbox_4326(self):
        return bbox_to_4326(self.meta_data.bbox, self.projection)

    @property
    def fields(self):
        return self.meta_data.fields

    @property
    def geom_type(self):
        return self.meta_data.geom_type

    def shapefile_parts(self, filename: str):
        """
        This function compares items in the zip at the same level and returns
        items that share file name with a shapefile, in an attempt to capture
        the many files associated with a shapefile
        """
        return [f for f in self.contents if f[:-4] == filename[:-4]]

    def to_stac_item(
        self,
        item_id: str,
        dtm: str,
        properties: list,
        stac_extensions: list = STAC_VECTOR_EXTENSIONS,
        href: str = None,
    ):
        return Item(
            href=href,
            id=item_id,
            geometry=self.footprint,
            bbox=self.bbox_4326,
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
                    "vector": self.file_name,
                }
            }
        )


class ZippedFGDB:
    def __init__(self, bucket: str, key: str, session: fiona.session.AWSSession):
        self.bucket = bucket
        self.key = key
        self.vsi_path = vsi_path(self.bucket, self.key)
        self._contents = read_zipped_gdb(bucket, key, session)

    @property
    def contents(self):
        return self._contents

    def layer_to_stac_item(self, item_id, dtm, layer: str, properties: dict):
        meta_data = get_vector_meta(self.vsi_path, layer=layer)
        return Item(
            href=layer,
            id=item_id,
            geometry=simplified_footprint(self.vsi_path, layer=layer),
            bbox=bbox_to_4326(meta_data.bbox, meta_data.projection),
            datetime=dtm,
            stac_extensions=STAC_VECTOR_EXTENSIONS,
            properties=properties,
        )

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


class ZippedRaster:
    def __init__(self, bucket: str, key: str, file_name: str):
        self.bucket = bucket
        self.key = key
        self.file_name = file_name
        self.vsi_path = vsi_path(self.bucket, self.key, self.file_name)
        self.meta_data = get_raster_meta(self.vsi_path)

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
        properties: list,
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


def add_shapefile_assets_to_item(
    zv: ZippedVector, shapefile_name: str, item: Item
) -> Item:
    for part in zv.shapefile_parts(shapefile_name):
        item.add_asset(
            key=pl.Path(part).suffix.replace(".", ""),
            asset=Asset(
                href=f"{zv.key}/{part}", media_type="Shapefile-part", title=part
            ),
        )
    return item
