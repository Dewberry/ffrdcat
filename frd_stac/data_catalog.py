from datetime import datetime, timezone
from data import nature, built, observations, events
import os
import pystac

# ----------CONFIG----------#
cat_map = {
    "id": "data",
    "description": "This catalog provides access to the supporting data used in the \
       development of flood risk models including datasets describing the natural \
        and built environments, observations used for validation and calibration, \
        and data used to parametrize and force the models (i.e. storms, temperature)",
    "extensions": [
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        "https://stac-extensions.github.io/osc/v1.0.0-rc.3/schema.json",
        "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
    ],
}


def main():
    child_catalogs = [
        nature.nature_map,
        built.built_map,
        observations.observations_map,
        events.events_map,
    ]

    catalog = pystac.Catalog(
        id=cat_map["id"],
        description=cat_map["description"],
        stac_extensions=[],
    )

    # Step 2. Create the child catalogs
    for i, cat in enumerate(child_catalogs):
        child_cat = pystac.Catalog(
            id=cat["id"],
            description=cat["description"],
            stac_extensions=cat["extensions"],
        )

        catalog.add_child(child_cat)
        catalog.stac_extensions.extend(child_cat.stac_extensions)

    return catalog
