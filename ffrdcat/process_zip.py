from dotenv import load_dotenv, find_dotenv
import fiona
import os
import pathlib as pl
from common.s3_utils import verify_key, key_last_updated
from stores.zips import S3Zip, ZippedVector
from stores.vectors import vector_item_properties

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
print(z.stac_type)

for s in z.shapefiles:
    # parts = z.shapefile_parts(s)
    v = ZippedVector(bucket, key, s, sess)
    item_id = pl.Path(key).name
    dtm = key_last_updated(bucket, key)
    properties = vector_item_properties(project, fields=v.meta_data.fields)
    item = v.to_stac_item(item_id, dtm, properties)
    item.validate()

