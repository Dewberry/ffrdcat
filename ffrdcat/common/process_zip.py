import fiona
import os
import pathlib as pl
import logging

from stores.zips import (
    S3Zip,
    ZippedVector,
    ZippedRaster,
    ZippedFGDB,
    add_shapefile_assets_to_item,
)
from stores.vectors import vector_item_properties, get_vector_meta
from stores.rasters import raster_item_properties
from common.s3_utils import verify_key, key_last_updated, vsi_path
from common.stac_utils import collection_from_zip


def process_item():
    pass


def process_zipped_shapefile(
    project: str, z: S3Zip, shapefile_name: str, sess: fiona.session.AWSSession
):
    logging.info(f"processing: {shapefile_name}")
    zv = ZippedVector(z.bucket, z.key, shapefile_name, sess)
    item_id = pl.Path(shapefile_name).stem
    dtm = key_last_updated(z.bucket, z.key)
    properties = vector_item_properties(project, fields=zv.meta_data.fields)
    item = zv.to_stac_item(item_id, dtm, properties, href=z.key)
    item = add_shapefile_assets_to_item(z, shapefile_name, item)
    # TODO: add validation for this process and update how extension properties are injected.
    # item.validate()
    return item


def process_zipped_raster(
    project: str, z: S3Zip, rasterfile_name: str, sess: fiona.session.AWSSession
):
    logging.info(f"processing: {rasterfile_name}")
    zr = ZippedRaster(z.bucket, z.key, rasterfile_name)
    item_id = pl.Path(rasterfile_name).stem
    dtm = key_last_updated(z.bucket, z.key)
    properties = raster_item_properties(project)
    item = zr.to_stac_item(item_id, dtm, properties, href=z.key)
    item.validate()
    return item


def process_fgdb(project: str, z: ZippedFGDB, sess: fiona.session.AWSSession):
    items, bboxes, extensions = [], [], []
    for layer in z.contents:
        try:
            if  "FEMA"  in layer  or "NHD"  in layer or "NSI" in layer:
                continue
            else:
                logging.info(f"processing {layer}")
                meta = get_vector_meta(vsi_path(z.bucket, z.key), layer=layer)
                dtm = key_last_updated(z.bucket, z.key)
                properties = vector_item_properties(
                    project, fields=meta.fields, data_type="ESRI FGDB"
                )
                item = z.layer_to_stac_item(layer, dtm, layer, properties)
                items.append(item)
                try:
                    bboxes.append(item.bbox)
                except:
                    logging.warning(f"Unable to calculate bounds for {layer}")
                extensions.extend(item.stac_extensions)
        except Exception as e:
            logging.warning(f"unable to process {layer}")

    return collection_from_zip(
        pl.Path(z.key).stem, "zip archive", extensions, items, bboxes
    )


def process_collection(project: str, z: S3Zip, sess: fiona.session.AWSSession):
    items, bboxes, extensions = [], [], []
    for shapefile in z.shapefiles:
        item = process_zipped_shapefile(project, z, shapefile, sess)
        items.append(item)
        bboxes.append(item.bbox)
        extensions.extend(item.stac_extensions)

    for raster in z.rasters:
        item = process_zipped_raster(project, z, raster, sess)
        items.append(item)
        bboxes.append(item.bbox)
        extensions.extend(item.stac_extensions)

    # TODO: Add assets for any other files that are in the zip file?
    # If there are assets, does this mean it should be a catalog and not
    # a collection???

    return collection_from_zip(
        pl.Path(z.key).stem, "zip archive", extensions, items, bboxes
    )


def main(project: str, bucket: str, key: str, sess: fiona.session.AWSSession):
    # TODO: Verify key exists and is accessible
    if verify_key(bucket, key):
        raise

    try:

        if ".gdb.zip" in key:
            z = ZippedFGDB(bucket, key, sess)
            logging.info(z)
            return process_fgdb(project, z, sess)

        else:
            z = S3Zip(bucket, key, sess)
            logging.info(z)

            if z.stac_type == "collection":
                return process_collection(project, z, sess) 

            if z.contains_shapefiles:
                shapefile_name = z.shapefiles[0]
                return process_zipped_shapefile(project, z, shapefile_name, sess)

            if z.contains_rasters:
                raster_name = z.rasters[0]
                return process_zipped_raster(project, z, raster_name, sess)
            
    except Exception as e:
        logging.error(f"{key} failed: {e}")
