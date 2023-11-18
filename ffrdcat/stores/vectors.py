from dataclasses import dataclass
import geopandas as gpd
from shapely.geometry import mapping
import fiona


@dataclass
class VectorMeta:
    bbox: tuple
    projection: str
    geom_type: str
    fields: list


STAC_VECTOR_EXTENSIONS = [
    # "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
    "https://stac-extensions.github.io/storage/v1.0.0/schema.json",
    "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
]


def get_vector_meta(filename: str) -> VectorMeta:
    with fiona.open(filename) as src:
        return VectorMeta(
            bbox=src.bounds,
            projection=src.crs_wkt,
            geom_type=src.meta["schema"]["geometry"],
            fields=list(src.schema["properties"].keys()),
        )


def vector_item_properties(
    ffrd_proj_name: str,
    proj_level: str = "pilot",
    fields: str = [],
    data_type: str = "ESRI Shapefile",
):
    """
    TODO: Remove hardcoded values
    """
    return {
        "FFRD:project_name": ffrd_proj_name,
        "FFRD:project_type": proj_level,
        "storage:platform": "AWS",
        "storage:region": "us-east-1",
        "processing:software": {"ffrd-to-stac": "2023.11.17"},
        "data_type": data_type,
        "fields": fields,
    }


def simplified_footprint(path: str):
    """
    path should be formatted: (TODO) - add examples
    TODO: Add error handling, crs handling, and generally review and improve
    """
    gdf = gpd.read_file(path)
    gdf["dissolver"] = 1
    gdf = gdf.dissolve(by="dissolver")
    gdf = gdf.to_crs("epsg:4326")
    gdf.geometry = gdf.geometry.simplify(0.001)
    mapped_geometry = gdf["geometry"].apply(mapping)
    return mapped_geometry.iloc[0]
