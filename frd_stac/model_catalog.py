from datetime import datetime, timezone
from models import hydraulic, hydrologic, reservoirs, consequences
import os
import pystac

# ----------CONFIG----------#
cat_map = {
    "id": "models",
    "description": "This catalog provides access to the (source) model input files used in the  \
        development of flood hazards and flood risk products.",
    "extensions": [
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        "https://stac-extensions.github.io/osc/v1.0.0-rc.3/schema.json",
        "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
    ],
}


def main():
    child_collections = [
        hydraulic.col_map,
        hydrologic.col_map,
        reservoirs.col_map,
        consequences.col_map,
    ]

    catalog = pystac.Catalog(
        id=cat_map["id"],
        description=cat_map["description"],
        stac_extensions=[],
    )

    for collection in child_collections:
        collection = pystac.Collection(
            id=collection["id"],
            description=collection["description"],
            stac_extensions=collection["extensions"],
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([0, 0, 0, 0]),
                temporal=pystac.TemporalExtent(
                    intervals=[
                        datetime.now(tz=timezone.utc),
                        datetime.now(tz=timezone.utc),
                    ]
                ),
            ),
        )
        catalog.add_child(collection)

    return catalog
