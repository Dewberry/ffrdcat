import fiona
import os
import pathlib as pl
import logging
from datetime import datetime, timezone
import json
from pystac import Item, Collection, Extent, SpatialExtent, TemporalExtent

from stores.zips import (
    S3Zip,
    ZippedVector,
    ZippedRaster,
    ZippedFGDB,
    add_shapefile_assets_to_item,
)
from stores.vectors import vector_item_properties, get_vector_meta
from stores.rasters import raster_item_properties
from common.s3_utils import key_last_updated, vsi_path


def write_local_item(item: Item, collection_id: str = None):
    if collection_id is None:
        collection_id = "top-level"

    if not os.path.exists(collection_id):
        os.makedirs(collection_id)
    output_file = f"{collection_id}/{item.id}.json"

    with open(output_file, "w") as f:
        json.dump(item.to_dict(), f)

    return output_file


def process_zipped_shapefile(
    project: str,
    z: S3Zip,
    collection_id: str,
    shapefile_name: str,
    sess: fiona.session.AWSSession,
):
    logging.info(f"processing: {shapefile_name}")

    zv = ZippedVector(z.bucket, z.key, shapefile_name, sess)
    item_id = pl.Path(shapefile_name).stem
    dtm = key_last_updated(z.bucket, z.key)
    properties = vector_item_properties(project, fields=zv.meta_data.fields)
    item = zv.to_stac_item(item_id, dtm, properties, href=z.key)
    item = add_shapefile_assets_to_item(z, shapefile_name, item)
    output_file = write_local_item(item, collection_id=collection_id)

    # TODO: add validation for this process and update how extension properties are injected.
    # item.validate()
    return item, output_file


def process_zipped_raster(
    project: str, z: S3Zip, collection_id: str, rasterfile_name: str
):
    logging.info(f"processing: {rasterfile_name}")

    zr = ZippedRaster(z.bucket, z.key, rasterfile_name)
    item_id = pl.Path(rasterfile_name).stem
    dtm = key_last_updated(z.bucket, z.key)
    properties = raster_item_properties(project)
    item = zr.to_stac_item(item_id, dtm, properties, href=z.key)
    item.validate()
    output_file = write_local_item(item, collection_id=collection_id)

    return item, output_file


def process_fgdb(project: str, z: ZippedFGDB, collection_id: str):
    items, bboxes, extensions, outputs = [], [], [], []
    for layer in z.contents:
        try:
            # Removing long running processes
            # Would be good to handle these in some pre-screening
            if "FEMA" in layer or "NHD" in layer or "NSI" in layer:
                continue
            else:
                logging.info(f"processing {layer}")
                meta = get_vector_meta(vsi_path(z.bucket, z.key), layer=layer)
                dtm = key_last_updated(z.bucket, z.key)
                properties = vector_item_properties(
                    project, fields=meta.fields, data_type="ESRI FGDB"
                )
                item = z.layer_to_stac_item(layer, dtm, layer, properties)
                output_file = write_local_item(item, collection_id=collection_id)
                outputs.append(output_file)
                items.append(item)
                try:
                    bboxes.append(item.bbox)
                except:
                    logging.warning(f"Unable to calculate bounds for {layer}")
                for extension in item.stac_extensions:
                    if extension not in extensions:
                        extensions.extend(item.stac_extensions)
        except Exception as e:
            logging.warning(f"unable to process {layer}")

    collection = Collection(
        id=collection_id,
        description="REMOVE HARDCODING",
        stac_extensions=extensions,
        extent=Extent(
            spatial=SpatialExtent(bboxes),
            temporal=TemporalExtent(
                intervals=[
                    datetime.now(tz=timezone.utc),
                    datetime.now(tz=timezone.utc),
                    datetime.now(tz=timezone.utc),
                ]
            ),
        ),
    )
    collection.add_items(items)
    return collection.to_dict(), outputs


def process_collection(
    project: str, z: S3Zip, collection_id: str, sess: fiona.session.AWSSession
):
    items, bboxes, extensions, outputs = [], [], [], []
    for shapefile in z.shapefiles:
        try:
            item, output_file = process_zipped_shapefile(
                project, z, collection_id, shapefile, sess
            ) 
            logging.info(item.dict(), output_file)         
            items.append(item)
            bboxes.append(item.bbox)
            extensions.extend(item.stac_extensions)
            outputs.append(output_file)
        except:
            logging.error(f"unable to process {shapefile}")

    for raster in z.rasters:
        try:
            item, output_file = process_zipped_raster(
                project, z, collection_id, raster, sess
            )
            items.append(item)
            bboxes.append(item.bbox)
            extensions.extend(item.stac_extensions)
            outputs.append(output_file)
        except:
            logging.error(f"unable to process {raster}")

    collection = Collection(
        id=collection_id,
        description="REMOVE HARDCODING",
        stac_extensions=extensions,
        extent=Extent(
            spatial=SpatialExtent(bboxes),
            temporal=TemporalExtent(
                intervals=[
                    datetime.now(tz=timezone.utc),
                    datetime.now(tz=timezone.utc),
                    datetime.now(tz=timezone.utc),
                ]
            ),
        ),
    )
    return collection.to_dict(), outputs
