from dataclasses import dataclass
import pystac
import rasterio


@dataclass
class RasterMeta:
    bbox: tuple
    projection: str
    resoultion: float


STAC_RASTER_EXTENSIONS = [
    # "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
    "https://stac-extensions.github.io/storage/v1.0.0/schema.json",
    "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
]


def get_raster_meta(filename: str) -> RasterMeta:
    with rasterio.Env(AWS_S3_ENDPOINT_URL="https://s3.amazonaws.com"):
        with rasterio.open(filename) as src:
            return RasterMeta(
                bbox=src.bounds, projection=src.crs.to_string(), resoultion=src.res
            )


def raster_item_properties(
    ffrd_proj_name: str,
    proj_level: str = "pilot",
    processing: str = {"ffrd-to-stac": "2023.11.17"},
    data_type: str = pystac.MediaType.GEOTIFF,
):
    """
    TODO: Remove hardcoded values
    """
    return {
        "FFRD:project_name": ffrd_proj_name,
        "FFRD:project_type": proj_level,
        "storage:platform": "AWS",
        "storage:region": "us-east-1",
        "processing:software": processing,
        "data_type": data_type,
    }
