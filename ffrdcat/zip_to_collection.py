import boto3
from dotenv import load_dotenv, find_dotenv
import fiona
import json
import logging
import os
import pathlib as pl
from papipyplug import plugin_logger
import s3fs
import uuid
import warnings

from stores.utils import verify_key
from stores.zips import S3Zip, new_collection_from_zip


# warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings(action="ignore")
logging.getLogger("s3fs").setLevel(logging.CRITICAL)
logging.getLogger("rasterio").setLevel(logging.CRITICAL)
logging.getLogger("fiona").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)
boto3.set_stream_logger(name="botocore.credentials", level=logging.WARNING)
boto3.set_stream_logger(name="urllib3.connectionpool", level=logging.WARNING)

plugin_params = {
    "required": ["project", "bucket", "key", "collection_title"],
    "optional": [],
}


def main(params: dict) -> dict:
    try:
        load_dotenv(find_dotenv())
    except:
        pass

    plugin_logger()

    item_results = []
    results = {}
    (project, bucket, key, collection_title) = (
        params["project"],
        params["bucket"],
        params["key"],
        params["collection_title"],
    )

    sess = fiona.session.AWSSession(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    fs = s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    s3_resource = boto3.resource("s3")

    # TODO: Verify key exists and is accessible
    if verify_key(bucket, key):
        raise

    # Case 1: zipped gdb
    if ".gdb.zip" in key:
        # zfile = ZippedFGDB(bucket, key, sess)
        logging.info(f"zip_reader | {zfile.key}: unpacking collection (gdb)")

        # collection_dict, outputs = process_fgdb(project, zfile, collection_id, sess)
        # item_results.extend(outputs)

        # if not os.path.exists(collection_id):
        #     os.makedirs(collection_id)

        # collection_file = f"{collection_id}/collection.json"
        # results["collection"] = collection_file

        # with open(collection_file, "w") as f:
        #     json.dump(collection_dict, f)

    # Case 2: zipfile (unknown contents)
    else:
        zfile = S3Zip(bucket, key, fs)
        logging.info(f"zip_reader | {zfile.key}: creating collection")
        collection_id = str(uuid.uuid4())

        collection = new_collection_from_zip(
            project, zfile, collection_id, collection_title, sess
        )

        for item in collection.get_items():
            item_json = f"stac/collections/{collection_id}/{item.id}/{item.id}.json"
            logging.info(f"zip_reader | {zfile.key}: writing  to {item_json}")
            item_results.append(item_json)
            logging.info(f"{item.id}:{item.datetime}")
            s3_resource.Object(bucket, item_json).put(Body=json.dumps(item.to_dict()))

        collection_file = f"stac/collections/{collection_id}/collection.json"
        results["collection"] = collection_file

        logging.info(f"zip_reader | {zfile.key}: wrting  to {collection_file}")
        s3_resource.Object(bucket, collection_file).put(
            Body=json.dumps(collection.to_dict())
        )

    results["item_results"] = item_results
    logging.info(f"zip_reader | {zfile.key}: processing complete!")
    return results
