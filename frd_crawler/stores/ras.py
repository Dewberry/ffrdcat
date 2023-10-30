from datetime import datetime, timezone
import json
from kerchunk import hdf
import pyproj
import pystac
from shapely.geometry import Polygon, mapping

from s3utils.s3utils import get_object_datetime


AORC_DATERANGE = [
    datetime(1979, 2, 1, tzinfo=timezone.utc),
    datetime(2022, 10, 31, tzinfo=timezone.utc),
]


class PlanHDF:
    """
    HEC RAS Plan HDF class for conversion into a stac item or asset

    """

    def __init__(self, uri: str, bbox: list, temporal: list, resolution: str, projection: str, tree: dict):
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
        self.uri = uri
        self._bbox = bbox
        self.temporal = AORC_DATERANGE[0]
        self.resolution = resolution
        self.projection = projection
        self._footprint_4326 = mapping(self._create_footprint())
        self._tree = tree

    @property
    def tree(self):
        return self._tree

    @property
    def attrs(self):
        return json.loads(self._tree["refs"][".zattrs"])

    @property
    def version(self):
        return self.attrs["File Version"]

    @property
    def srs_projection(self):
        return self.attrs["Projection"]

    @property
    def geometry(self):
        return json.loads(self._tree["refs"]["Geometry/.zattrs"])

    @property
    def bbox(self):
        return self.geometry["Extents"]

    @property
    def bbox_4326(self):
        transformer = pyproj.Transformer.from_crs(self.projection, "epsg:4326", always_xy=True)

        return list(transformer.transform(self._bbox[0], self._bbox[1])) + list(
            transformer.transform(self._bbox[2], self._bbox[3])
        )

    def _create_footprint(self):
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

    @classmethod
    def from_s3(cls, bucket_name, file_key):
        try:
            uri = f"s3://{bucket_name}/{file_key}"
            kerchunk_response = hdf.SingleHdf5ToZarr(uri)
            tree = kerchunk_response.translate()

            return cls(
                uri=uri,
                bbox=json.loads(tree["refs"]["Geometry/.zattrs"])["Extents"],
                temporal=AORC_DATERANGE[0],
                resolution=None,
                projection=json.loads(tree["refs"][".zattrs"])["Projection"],
                tree=tree,
            )
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def create_thumbnail(self, thumbnail_path, factor=8, cmap="Blues"):
        raise NotImplementedError("create_thumbnail method is not implemented")

    def __repr__(self):
        return (
            f"PlanHDF("
            f"\n    uri='{self.uri}',"
            f"\n    bbox={self._bbox},"
            f"\n    footprint_4326={self._footprint_4326},"
            f"\n    temporal={self.temporal},"
            f"\n    resolution='{self.resolution}',"
            f"\n    projection='{self.projection}'"
            f"\n)"
        )


class FRDRasPlan(PlanHDF):
    def __init__(self, uri, bbox, temporal, resolution, projection, tree):
        super().__init__(uri, bbox, temporal, resolution, projection, tree)

    def to_pystac_item(self, item_id):
        item = pystac.Item(
            id=item_id,
            geometry=self._footprint_4326,
            bbox=self.bbox,
            # datetime=self.temporal,
            datetime=AORC_DATERANGE[0],
            stac_extensions=["https://stac-extensions.github.io/projection/v1.0.0/schema.json"],
            properties=dict(tile="tile"),
        )

        item.add_asset(
            key="hdf",
            asset=pystac.Asset(
                href=f"{item_id}",
                media_type=pystac.MediaType.HDF,
                title=f"{item_id}",
            ),
        )

        return item
