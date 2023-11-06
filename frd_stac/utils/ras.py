from datetime import datetime, timezone
import json

# from kerchunk import hdf
import fsspec
import h5py
import pyproj
import pystac
from shapely.geometry import Polygon, mapping

from utils.s3_utils import get_object_datetime


class PlanHDF:
    """
    HEC RAS Plan HDF class for conversion into a stac item or asset

    """

    def __init__(self, bucket: str, key: str):
        """
        Initialize RasPlan object.

        Args:
            uri (str): The URI of the COG.
            bbox (list): Bounding box coordinates [minx, miny, maxx, maxy].
            temporal (list): Temporal information.
            resolution (str): Resolution details.
            projection (str): Projection details.
            tree (str): output from kerchunk
        """
        self.uri = f"s3://{bucket}/{key}"
        try:
            s3f = fsspec.open(self.uri, mode="rb", default_fill_cache=False)
            self.h5f = h5py.File(s3f.open(), mode="r")
        except:
            print("unable to open file")
        # self._tree = tree

    # @property
    # def tree(self):
    #     return self._tree

    # @property
    # def attrs(self):
    #     return json.loads(self._tree["refs"][".zattrs"])

    @property
    def version(self):
        return self.h5f.attrs["File Version"].decode("UTF-8")

    @property
    def units(self):
        return self.h5f.attrs["Units System"].decode("UTF-8")

    @property
    def projection(self):
        return self.h5f.attrs["Projection"].decode("UTF-8")

    @property
    def volume_error(self):
        return self.h5f.get("Results/Unsteady/Summary/Volume Accounting/").attrs[
            "Error Percent"
        ]

    # @property
    # def geometry(self):
    #     return json.loads(self._tree["refs"]["Geometry/.zattrs"])

    @property
    def success(self):
        return (
            self.h5f.get("Event Conditions")
            .attrs["Completed Successfully"]
            .decode("UTF-8")
        )

    @property
    def simulation_date(self):
        return self.h5f.get("Event Conditions").attrs["Date Processed"].decode("UTF-8")

    @property
    def bbox(self):
        return self.h5f.get("Geometry").attrs["Extents"].tolist()

    @property
    def bbox_4326(self):
        transformer = pyproj.Transformer.from_crs(
            self.projection, "epsg:4326", always_xy=True
        )

        return list(transformer.transform(self.bbox[0], self.bbox[1])) + list(
            transformer.transform(self.bbox[2], self.bbox[3])
        )

    @property
    def footprint_4326(self):
        """
        Only provide footprint option in 4326
        """
        return Polygon(
            [
                [self.bbox_4326[0], self.bbox_4326[1]],
                [self.bbox_4326[0], self.bbox_4326[3]],
                [self.bbox_4326[2], self.bbox_4326[3]],
                [self.bbox_4326[2], self.bbox_4326[1]],
            ]
        )

    def create_thumbnail(self, thumbnail_path, factor=8, cmap="Blues"):
        raise NotImplementedError("create_thumbnail method is not implemented")

    def __repr__(self):
        return (
            f"PlanHDF("
            f"\n    uri='{self.uri}',"
            f"\n    bbox={self.bbox},"
            f"\n    footprint_4326={self.footprint_4326},"
            f"\n    projection='{self.projection}'"
            f"\n)"
        )


class FRDRasPlan(PlanHDF):
    def __init__(self, bucket, key):
        super().__init__(bucket, key)

    def to_pystac_item(self, item_id, log):
        item = pystac.Item(
            id=item_id,
            geometry=mapping(self.footprint_4326),
            bbox=self.bbox,
            #  TODO: update datetime
            datetime=datetime.now(tz=timezone.utc),
            stac_extensions=[
                "https://stac-extensions.github.io/projection/v1.0.0/schema.json"
            ],
            properties={
                "frd:project": "kanawha",
                "frd:project_status": "FFRD pilot",
                "frd:model_version": self.version,
                "frd:units": self.units,
                "frd:volume_error": str(self.volume_error),
                "frd:simulaton_date": self.simulation_date,
                "proj:bbox": self.bbox,
                "proj:wkt2": self.projection,
                "storage:platform": "AWS",
                "storage:region": "us-east-1",
                "processing:software": {"frd-to-stac": "2023.11.04"},
                "created": pystac.utils.datetime_to_str(datetime.now(tz=timezone.utc)),
                "updated": pystac.utils.datetime_to_str(datetime.now(tz=timezone.utc)),
            },
        )

        item.add_asset(
            key="hdf",
            asset=pystac.Asset(
                href=self.uri,
                media_type=pystac.MediaType.HDF,
                title=f"{item_id}",
            ),
        )

        item.add_asset(
            key="log",
            asset=pystac.Asset(
                href="rasoutput.log",
                media_type=pystac.MediaType.TEXT,
                title="rasoutput.log",
            ),
        )

        return item
