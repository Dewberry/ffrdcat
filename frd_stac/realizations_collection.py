from datetime import datetime, timezone
from realizations import r1, r1_sims
from realizations import ras, ressim, hms, consequences, depth_grids, notebooks
import os
import pystac

# ----------CONFIG----------#
col_map = {
    "id": "realizations",
    "description": "This collection provides access to realizations of the monte carlo analysis. Each \
        realization contains a catalog of model input and outputs for each simulation included in the \
        realzation",
    "extensions": [
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        "https://stac-extensions.github.io/osc/v1.0.0-rc.3/schema.json",
        "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
    ],
}


def main():
    # Realizations top level is a collection with each realization representing
    # a realization catalog
    realizations_collection = pystac.Collection(
        id=col_map["id"],
        description=col_map["description"],
        stac_extensions=col_map["extensions"],
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

    # Realization 1 is a collection of simulations
    r1_collection = pystac.Collection(
        id=r1.col_map["id"],
        description=r1.col_map["description"],
        stac_extensions=r1.col_map["extensions"],
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
    realizations_collection.add_child(r1_collection)

    # A realization is a collection of simulation catalogs.
    sim_cols = [
        ras.col_map,
        ressim.col_map,
        hms.col_map,
        consequences.col_map,
        depth_grids.col_map,
        notebooks.col_map,
    ]

    for sim in range(250, 255):
        catalog = pystac.Catalog(
            id=f"r1-s{sim}",
            description="Simulation Catalog",
            stac_extensions=[],
        )

        for col in sim_cols:
            collection = pystac.Collection(
                id=col["id"],
                description=col["description"],
                stac_extensions=col["extensions"],
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

        # add each simulation member collection to the catalog
        r1_collection.add_child(catalog)

    return realizations_collection
