import fiona
import os
import pathlib as pl

from stores.zips import S3Zip, ZippedVector, ZippedRaster
from stores.vectors import vector_item_properties
from stores.rasters import raster_item_properties
from common.s3_utils import verify_key, key_last_updated
from common.stac_utils import collection_from_zip


def process_item():
    pass

def process_collection(project:str, z, sess:fiona.session.AWSSession):
    items, bboxes, extensions = [], [], []
    for s in z.shapefiles:
        # parts = z.shapefile_parts(s)
        zv = ZippedVector(z.bucket, z.key, s, sess)
        item_id = pl.Path(s).stem
        dtm = key_last_updated(z.bucket, z.key)
        properties = vector_item_properties(project, fields=zv.meta_data.fields)
        item = zv.to_stac_item(item_id, dtm, properties)
        item.validate()
        items.append(item)
        bboxes.append(item.bbox)
        extensions.extend(item.stac_extensions)

    for r in z.rasters:
        zr = ZippedRaster(z.bucket, z.key, r)
        item_id = pl.Path(r).stem
        dtm = key_last_updated(z.bucket, z.key)
        properties = raster_item_properties(project)
        item = zr.to_stac_item(item_id, dtm, properties)
        item.validate()

        items.append(item)
        bboxes.append(item.bbox)
        extensions.extend(item.stac_extensions)

    return collection_from_zip(
        pl.Path(z.key).stem, "zip archive", extensions, items, bboxes
    )


def main(project:str, bucket:str, key:str, sess: fiona.session.AWSSession):

    # TODO: Verify key exists and is accessible
    if verify_key(bucket, key):
        raise

    z = S3Zip(bucket, key, sess)

    if z.stac_type == "collection":
        return process_collection(project, z, sess)
    else:
        print("nothing happening here....")


