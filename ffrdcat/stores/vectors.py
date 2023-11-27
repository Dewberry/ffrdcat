from alphashape import alphashape
from dataclasses import dataclass
import fiona
import geopandas as gpd
from io import BytesIO
import logging
import matplotlib as mpl
from matplotlib import pyplot as plt
import numpy as np
from pystac import Item, Asset, MediaType
from shapely.geometry import mapping, Point
from shapely import Geometry
import uuid


@dataclass
class VectorMeta:
    bbox: tuple
    projection: str
    geom_type: str
    fields: list
    shapefile_parts: list = None


STAC_VECTOR_EXTENSIONS = [
    # "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
    "https://stac-extensions.github.io/storage/v1.0.0/schema.json",
    "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
]


def get_vector_meta(filename: str, layer: str = None) -> VectorMeta:
    with fiona.open(filename, layer=layer) as src:
        projection = src.crs_wkt
        if projection == "":
            return None

        return VectorMeta(
            bbox=src.bounds,
            projection=projection,
            geom_type=src.meta["schema"]["geometry"],
            fields=list(src.schema["properties"].keys()),
        )


def vector_item_properties(
    ffrd_proj_name: str,
    proj_level: str = "pilot",
    fields: str = [],
    data_type: str = "ESRI Shapefile",
    status: str = "provisional",
) -> dict:
    """
    TODO: Remove hardcoded values
    """
    return {
        "FFRD:project_name": ffrd_proj_name,
        "FFRD:project_type": proj_level,
        "FFRD:satus": status,
        "storage:platform": "AWS",
        "storage:region": "us-east-1",
        "processing:software": {"ffrd-to-stac": "2023.11.17"},
        "data_type": data_type,
        "fields": fields,
    }


def concave_hull_from_points(gdf: gpd.GeoDataFrame) -> Geometry:
    gdf = gdf.to_crs("epsg:4326")
    gdf = gdf.explode(index_parts=False)
    gdf.replace([np.inf, -np.inf], np.nan, inplace=True)
    gdf.dropna(inplace=True)
    gdf.reset_index(drop=True, inplace=True)
    geometry = gdf.geometry.apply(lambda geom: Point(geom.x, geom.y))
    hull = alphashape(geometry, alpha=1)
    logging.debug(mapping(hull))
    return hull


def poly_to_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.to_crs("epsg:4326")
    points_list = []
    for row in gdf.geometry:
        if row.geom_type == "Polygon":
            exterior_coords = list(
                zip(
                    row.exterior.coords.xy[0].tolist(),
                    row.exterior.coords.xy[1].tolist(),
                )
            )
            points_list.extend(exterior_coords)

            for interior in row.interiors:
                interior_coords = list(
                    zip(interior.coords.xy[0].tolist(), interior.coords.xy[1].tolist())
                )
                points_list.extend(interior_coords)

        elif row.geom_type == "MultiPolygon":
            for polygon in row.geoms:
                exterior_coords = list(
                    zip(
                        polygon.exterior.coords.xy[0].tolist(),
                        polygon.exterior.coords.xy[1].tolist(),
                    )
                )
                points_list.extend(exterior_coords)

                for interior in polygon.interiors:
                    interior_coords = list(
                        zip(
                            interior.coords.xy[0].tolist(),
                            interior.coords.xy[1].tolist(),
                        )
                    )
                    points_list.extend(interior_coords)

    points_geometry = [Point(x, y) for x, y in points_list]
    return gpd.GeoDataFrame(geometry=points_geometry, crs="epsg:4326")


def line_to_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.to_crs("epsg:4326")
    points_list = []
    for row in gdf.geometry:
        if row.geom_type == "LineString":
            coords = list(
                zip(
                    row.coords.xy[0].tolist(),
                    row.coords.xy[1].tolist(),
                )
            )
            points_list.extend(coords)

        elif row.geom_type == "MultiLineString":
            for line in row.geoms:
                coords = list(
                    zip(
                        line.coords.xy[0].tolist(),
                        line.coords.xy[1].tolist(),
                    )
                )
                points_list.extend(coords)

    points_geometry = [Point(x, y) for x, y in points_list]
    return gpd.GeoDataFrame(geometry=points_geometry, crs="epsg:4326")


def to_hull(gdf: gpd.GeoDataFrame) -> Geometry:
    geometry_type = gdf.geometry.type.unique()[0].lower()

    if "polygon" in geometry_type or "multipolygon" in geometry_type:
        logging.debug("starting polygon simplification")
        gdf = poly_to_points(gdf)
        return concave_hull_from_points(gdf.simplify(0.05))

    elif "line" in geometry_type:
        logging.debug("starting line simplification")
        gdf = line_to_points(gdf)
        return concave_hull_from_points(gdf)

    elif "point" in geometry_type:
        logging.debug("starting point simplification")
        return concave_hull_from_points(gdf)


def make_vector_thumbnail(gdf, footprint) -> None:
    # Plot the original data and the concave hull
    mpl.rcParams["savefig.pad_inches"] = 0.2
    ax = plt.axes([0, 0, 1, 1], frameon=False)
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    plt.autoscale(tight=True)
    gdf = gdf.to_crs("epsg:4326")
    geometry_type = gdf.geometry.type.unique()[0].lower()
    if "point" in geometry_type:
        gdf.replace([np.inf, -np.inf], np.nan, inplace=True)
        gdf.dropna(inplace=True)
    gdf = gdf.simplify(0.1)
    gdf.plot(ax=ax, color="black")
    gpd.GeoSeries(footprint).plot(ax=ax, alpha=0.3)
    ax.set_xticks([])
    ax.set_yticks([])

    image_stream = BytesIO()
    plt.savefig(image_stream, format="png")
    plt.close()
    image_bytes = image_stream.getvalue()
    return image_bytes


def add_vector_thumbnail_asset_to_item(
    gdf,
    footprint,
    bucket: str,
    thumbnail_key: str,
    item: Item,
) -> Item:
    image_bytes = make_vector_thumbnail(gdf, footprint)

    item.add_asset(
        key=str(uuid.uuid4()),
        asset=Asset(
            href=f"https://{bucket}.s3.amazonaws.com/{thumbnail_key}",
            media_type=MediaType.PNG,
            title="thubmnail",
            roles=["thumbnail"],
        ),
    )

    return item, image_bytes


def approx_vector_size(bucket: str, key: str, vector_file: str) -> tuple:
    # if isinstance(zv, ZippedFGDB):
    #     with fiona.open(f"zip+s3://{zv.bucket}/{zv.key}", layer=zv.vector_name) as src:
    #         features = []
    #         for i, feature in enumerate(src):
    #             if i < 1:
    #                 features.append(feature)
    #             else:
    #                 break
    # else:
    with fiona.open(f"zip+s3://{bucket}/{key}/{vector_file}") as src:
        features = []
        nfeatures = len(src)
        for i, feature in enumerate(src):
            if i < 1:
                features.append(feature)
            else:
                break
    gdf = gpd.GeoDataFrame.from_features(features)
    mem_reqs = gdf.memory_usage(deep=True).sum() * nfeatures / (1024**3)
    return mem_reqs, nfeatures
