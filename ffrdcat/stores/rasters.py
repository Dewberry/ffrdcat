from dataclasses import dataclass
import numpy as np
from pystac import Item, Asset, MediaType
import rasterio
from rasterio.warp import reproject, Resampling
from io import BytesIO
from matplotlib import pyplot as plt
import uuid


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
    projection: str,
    resolution: float,
    proj_level: str = "pilot",
    processing: str = {"ffrd-to-stac": "2023.11.17"},
    data_type: str = MediaType.GEOTIFF,
) -> dict:
    """
    TODO: Remove hardcoded values
    """
    return {
        "FFRD:project_name": ffrd_proj_name,
        "FFRD:project_type": proj_level,
        "FFRD:resolution": resolution,
        "proj:wkt2": projection,
        "storage:platform": "AWS",
        "storage:region": "us-east-1",
        "processing:software": processing,
        "data_type": data_type,
    }


def make_raster_thumbnail(vsi_path: str, factor: int = 50, cmap: str = "inferno"):
    with rasterio.open(vsi_path) as dataset:
        # Full extent of the image --warning....
        window = rasterio.windows.Window(0, 0, dataset.width, dataset.height)
        data = dataset.read(window=window)

        # Calculate the lower resolution dimensions
        new_width = dataset.width // factor
        new_height = dataset.height // factor

        transform = dataset.transform * dataset.transform.scale(
            (dataset.width / new_width), (dataset.height / new_height)
        )
        resampled = np.empty(
            (dataset.count, new_height, new_width), dataset.profile["dtype"]
        )

        for i in range(1, dataset.count + 1):
            reproject(
                source=data[i - 1],
                destination=resampled[i - 1],
                src_transform=dataset.transform,
                src_crs=dataset.crs,
                dst_transform=transform,
                dst_crs=dataset.crs,
                resampling=Resampling.nearest,
            )

        nodata_mask = resampled == dataset.nodata
        resampled[nodata_mask] = -10

        image_stream = BytesIO()
        plt.imsave(image_stream, np.squeeze(resampled, axis=0), cmap=cmap, format="png")
        plt.savefig(image_stream, format="png")
        plt.close()
        image_bytes = image_stream.getvalue()
        return image_bytes


def add_raster_thumbnail_asset_to_item(
    vsi_path: str,
    bucket: str,
    thumbnail_key: str,
    item: Item,
) -> Item:
    image_bytes = make_raster_thumbnail(vsi_path)

    item.add_asset(
        key=str(uuid.uuid4()),
        asset=Asset(
            href=f"https://{bucket}.s3.amazonaws.com/{thumbnail_key}",
            media_type=MediaType.PNG,
            title="thubmnail",
            roles=["thumbnail"],
        ),
    )

    return item, image_bytes
