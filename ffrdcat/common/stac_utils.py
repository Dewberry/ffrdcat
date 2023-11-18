import pathlib as pl
from datetime import datetime, timezone
import pystac

from s3_utils import verify_key 

def item_from_zip():
    

def main(s3_zip, bucket, prefix, s3_key, item_id):
    zvf = s3_zip.get_metadata(
        filename=f"/vsizip/vsis3/{bucket}/{prefix}/{s3_key}/{item_id}"
    )
    href = f"s3://{bucket}/{s3_key}/{prefix}/{item_id}"
    item = pystac.Item(
        href=f"s3://{bucket}/{s3_key}/{prefix}/{item_id}",
        id=item_id.replace(".shp", ""),
        geometry=zvf.footprint_4326,
        bbox=zvf.bbox_4326,
        datetime=datetime.now(tz=timezone.utc),
        stac_extensions=[
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
            "https://stac-extensions.github.io/storage/v1.0.0/schema.json",
            "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
        ],
        properties={
            "access:virtual": href.replace("s3://", "/vsiszip/vsis3/"),
            "FFRD:project_name": "trinity",
            "FFRD:project_type": "pilot",
            "FFRD:bounding_box": str(zvf.bbox),
            "proj:wkt2": zvf.projection,
            "proj:bbox": zvf.bbox, 
            "storage:platform": "AWS",
            "storage:region": "us-east-1",
            "processing:software": {"ffrd-to-stac": "2023.11.17"},
            "data_type": "ESRI Shapefile",
            "fields": zvf.properties,
        },
    )

    for part in s3_zip.shapefile_parts(item_id):
        item.add_asset(
            key=pl.Path(part).suffix,
            asset=pystac.Asset(
                href=part,
                media_type="Shapefile-part",
                title=part,
            ),
        )
    return item
