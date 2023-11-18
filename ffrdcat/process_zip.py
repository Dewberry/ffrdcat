from dotenv import load_dotenv, find_dotenv
import fiona
import os
import pathlib as pl

from stores.zips import S3Zip, ZippedVector, ZippedRaster
from stores.vectors import vector_item_properties
from stores.rasters import raster_item_properties
from common.s3_utils import verify_key, key_last_updated

load_dotenv(find_dotenv())

project = "trinity"
bucket = "trinity-pilot"
key = "staging/area1.zip"

# TODO: Verify key exists and is accessible
if verify_key(bucket, key):
    raise

sess = fiona.session.AWSSession(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

z = S3Zip(bucket, key, sess)

for s in z.shapefiles:
    # parts = z.shapefile_parts(s)
    zv = ZippedVector(bucket, key, s, sess)
    item_id = pl.Path(key).name
    dtm = key_last_updated(bucket, key)
    properties = vector_item_properties(project, fields=zv.meta_data.fields)
    item = zv.to_stac_item(item_id, dtm, properties)
    item.validate()


for r in z.rasters:
    zr = ZippedRaster(bucket, key, r)
    item_id = pl.Path(key).name
    dtm = key_last_updated(bucket, key)
    properties = raster_item_properties(project)
    item = zr.to_stac_item(item_id, dtm, properties)
    item.validate()
