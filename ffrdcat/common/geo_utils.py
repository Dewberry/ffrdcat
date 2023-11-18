import fiona
from shapely.geometry import Polygon, mapping
import geopandas as gpd
import pyproj


def transformer_4326(projection: str, always_xy: bool = True):
    return pyproj.Transformer.from_crs(projection, "epsg:4326", always_xy=always_xy)


def bbox_to_4326(bbox: tuple, projection: str):
    transformer = transformer_4326(projection)
    return list(transformer.transform(bbox[0], bbox[1])) + list(
        transformer.transform(bbox[2], bbox[3])
    )


def footprint_from_bbox(bbox: tuple, projection: str):
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


def simplified_footprint(path: str):
    """
    path should be formatted: (TODO) - add examples
    TODO: Add error handling, crs handling, and generally review and improve
    """
    gdf = gpd.read_file(path)
    gdf["dissolver"] = 1
    gdf = gdf.dissolve(by="dissolver")
    gdf = gdf.to_crs("epsg:4326")
    gdf.geometry = gdf.geometry.simplify(1)
    mapped_geometry = gdf["geometry"].apply(mapping)
    return mapped_geometry.iloc[0]
