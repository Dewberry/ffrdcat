import fiona
from dataclasses import dataclass
from datetime import datetime, timezone
import pathlib as pl
from pystac import Item

from common.s3_utils import vsi_path
from common.geo_utils import simplified_footprint, bbox_to_4326

from stores.vectors import get_vector_meta, STAC_VECTOR_EXTENSIONS


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
        if len(self.shapefiles) > 1:
            return True
        return False

    @property
    def contains_rasters(self):
        if len(self.rasters) > 1:
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
        return (
            f"S3ZIP ("
            f"\n    bucket:      {self.bucket}"
            f"\n    key:         {self.key}"
            f"\n    shapefiles:  {len(self.shapefiles)}"
            f"\n    rasters:     {len(self.rasters)}"
            # f"\n    ras_model_count={len(self.not_implemented)},"
            f"\n)"
        )


class ZippedVector(S3Zip):
    def __init__(
        self, bucket: str, key: str, file_name: str, session: fiona.session.AWSSession
    ):
        super().__init__(bucket, key, session)
        self.file_name = file_name
        self.vsi_path = vsi_path(self.bucket, self.key, self.file_name)
        self.meta_data = get_vector_meta(self.vsi_path)

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
    def propertes(self):
        return self.meta_data.properties

    @property
    def geom_type(self):
        return self.meta_data.geom_type

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
