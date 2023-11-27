import pathlib as pl
from datetime import datetime, timezone
from io import BytesIO
from shapely.geometry import Polygon
import pyproj
import s3fs
from typing import List
import zipfile
import logging


def transformer_4326(projection: str, always_xy: bool = True) -> pyproj.Transformer:
    return pyproj.Transformer.from_crs(projection, "epsg:4326", always_xy=always_xy)


def bbox_to_4326(bbox: tuple, projection: str) -> List[float]:
    transformer = transformer_4326(projection)
    return list(transformer.transform(bbox[0], bbox[1])) + list(
        transformer.transform(bbox[2], bbox[3])
    )


def footprint_from_bbox(bbox: tuple, projection: str) -> Polygon:
    """
    Only provide footprint option in 4326
    """
    bbox_4326 = bbox_to_4326(bbox, projection)
    return Polygon(
        [
            [bbox_4326[0], bbox_4326[1]],
            [bbox_4326[0], bbox_4326[3]],
            [bbox_4326[2], bbox_4326[3]],
            [bbox_4326[2], bbox_4326[1]],
        ]
    )


def texas_bbox() -> Polygon:
    return footprint_from_bbox(
        (-108.442783, 25.610107, -93.061924, 36.992270), "epsg:4326"
    )


def us_bbox() -> Polygon:
    return footprint_from_bbox(
        (-129.740295, 20.941240, -61.888733, 50.106708), "epsg:4326"
    )


def collection_bounding_boxes(bboxes: list) -> List[float]:
    min_x, min_y, max_x, max_y = bboxes[0]

    for bbox in bboxes[1:]:
        logging.info(f"bbox -> {bbox}")
        min_x = min(min_x, bbox[0])
        min_y = min(min_y, bbox[1])
        max_x = max(max_x, bbox[2])
        max_y = max(max_y, bbox[3])

    return [min_x, min_y, max_x, max_y]


def verify_key(bucket: str, key: str):
    """
    TODO
    """
    pass


def key_last_updated(bucket: str, key: str):
    """
    TODO
    """
    return datetime.now(tz=timezone.utc)


def vsi_path(bucket: str, key: str, filename: str = None) -> str:
    if pl.Path(key).suffix == ".zip":
        vsi_prefix = "/vsizip/vsis3"
    else:
        vsi_prefix = "/vsis3"

    if filename is not None:
        return f"{vsi_prefix}/{bucket}/{key}/{filename}"
    else:
        return f"{vsi_prefix}/{bucket}/{key}"


def read_file_from_zip(fs: s3fs.S3FileSystem, s3_zip_file: str, internal_file:str):
    with fs.open(s3_zip_file, "rb") as zip_file:
        with zipfile.ZipFile(BytesIO(zip_file.read())) as zip_ref:
            info = zip_ref.infolist()
            for i in info:
                logging.info(f"read_file_from_zip | {i.filename}")
                if i.filename == internal_file:
                    logging.debug(f"geometry file identified | {i.filename}")
                    file_bytes = zip_ref.read(i.filename)
                    data = file_bytes.decode()
                    return data.splitlines()