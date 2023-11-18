import fiona
import os
import pathlib as pl
import logging

from stores.zips import S3Zip, ZippedVector, ZippedRaster
from stores.vectors import vector_item_properties
from stores.rasters import raster_item_properties
from common.s3_utils import verify_key, key_last_updated
from common.stac_utils import collection_from_zip


def process_item():
    pass


def process_zipped_shapefile(project: str, z: S3Zip, sess: fiona.session.AWSSession):
    # parts = z.shapefile_parts(s)
    shapefile_name = z.shapefiles[0]
    logging.info(f"processing: {shapefile_name}")
    zv = ZippedVector(z.bucket, z.key, shapefile_name, sess)
    item_id = pl.Path(shapefile_name).stem
    dtm = key_last_updated(z.bucket, z.key)
    properties = vector_item_properties(project, fields=zv.meta_data.fields)
    item = zv.to_stac_item(item_id, dtm, properties, href=z.key)
    # item.validate()
    return item


def process_zipped_raster(project: str, z: S3Zip, sess: fiona.session.AWSSession):
    rasterfile_name = z.rasters[0]
    logging.info(f"processing: {rasterfile_name}")
    zr = ZippedRaster(z.bucket, z.key, rasterfile_name)
    item_id = pl.Path(rasterfile_name).stem
    dtm = key_last_updated(z.bucket, z.key)
    properties = raster_item_properties(project)
    item = zr.to_stac_item(item_id, dtm, properties, href=z.key)
    item.validate()
    return item


def process_collection(project: str, z: S3Zip, sess: fiona.session.AWSSession):
    items, bboxes, extensions = [], [], []
    for _ in z.shapefiles:
        item = process_zipped_shapefile(project, z, sess)
        items.append(item)
        bboxes.append(item.bbox)
        extensions.extend(item.stac_extensions)

    for _ in z.rasters:
        item = process_zipped_raster(project, z, sess)
        items.append(item)
        bboxes.append(item.bbox)
        extensions.extend(item.stac_extensions)

    return collection_from_zip(
        pl.Path(z.key).stem, "zip archive", extensions, items, bboxes
    )


def main(project: str, bucket: str, key: str, sess: fiona.session.AWSSession):
    # TODO: Verify key exists and is accessible
    if verify_key(bucket, key):
        raise

    z = S3Zip(bucket, key, sess)
    logging.info(z)

    if z.stac_type == "collection":
        return process_collection(project, z, sess)

    if z.contains_rasters:
        return process_zipped_raster(project, z, sess)

    if z.contains_shapefiles:
        logging.warning("YEP")
        return process_zipped_shapefile(project, z, sess)
