from dotenv import load_dotenv, find_dotenv
import fiona
import json
import logging
import os
import pathlib as pl
from papipyplug import plugin_logger

from common.zip_utils import (
    process_zipped_shapefile,
    process_zipped_raster,
    process_fgdb,
    process_collection,
)

from stores.zips import S3Zip, ZippedFGDB
from common.s3_utils import verify_key

logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)

plugin_params = {
    "required": ["project", "bucket", "key", "collection_id"],
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
    (project, bucket, key, collection_id) = (
        params["project"],
        params["bucket"],
        params["key"],
        params["collection_id"],
    )

    sess = fiona.session.AWSSession(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # TODO: Verify key exists and is accessible
    if verify_key(bucket, key):
        raise

    # Case 1: zipped gdb
    if ".gdb.zip" in key:
        zfile = ZippedFGDB(bucket, key, sess)
        logging.info(zfile)

        collection_dict, outputs = process_fgdb(project, zfile, collection_id)
        item_results.extend(outputs)

        if not os.path.exists(collection_id):
            os.makedirs(collection_id)

        collection_file = f"{collection_id}/collection.json"
        results["collection"] = collection_file

        with open(collection_file, "w") as f:
            json.dump(collection_dict, f)

    # Case 2: zipfile (unknown contents)
    else:
        zfile = S3Zip(bucket, key, sess)
        logging.info(zfile)

        if zfile.stac_type == "collection":
            collection_dict, outputs = process_collection(
                project, zfile, collection_id, sess
            )
            item_results.extend(outputs)

            if not os.path.exists(collection_id):
                os.makedirs(collection_id)

            collection_file = f"{collection_id}/collection.json"
            results["collection"] = collection_file

            with open(collection_file, "w") as f:
                json.dump(collection_dict, f)

        # single shapefile case
        if zfile.contains_shapefiles:
            shapefile_name = zfile.shapefiles[0]
            _, item_json = process_zipped_shapefile(
                project, zfile, collection_id, shapefile_name, sess
            )
            item_results.append(item_json)

        # single tiff case
        if zfile.contains_rasters:
            raster_name = zfile.rasters[0]
            _, item_json = process_zipped_raster(
                project, zfile, collection_id, raster_name
            )
            item_results.append(item_json)

    results["item_results"] = item_results
    logging.info("Process complete")
    return results

raster_example = {
    "project": "trinity",
    "bucket": "ffrd-trinity",
    "key": "from-USACE/Trinity2021LandCover.zip",
    "collection_id": "top-level",
}

shapefile_example = {
    "project": "trinity",
    "bucket": "ffrd-trinity",
    "key": "from-USACE/Subbasin265.zip",
    "collection_id": "top-level",
}

gdb_example = {
    "project": "trinity",
    "bucket": "ffrd-trinity",
    "key": "from-USACE/FEMA_FFRD_Initiative_TrinityRecBasin.gdb.zip",
    "collection_id": "top-level",
}

zip_collection_example = {
    "project": "trinity",
    "bucket": "ffrd-trinity",
    "key": "from-USACE/ModelingUnitLayers_v1.zip",
    "collection_id": "top-level",
}

# Uncomment for local testing
if __name__ == "__main__":
    main(zip_collection_example)