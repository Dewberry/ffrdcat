from datetime import datetime, timezone
import os
import pystac
from stores.cogs import FRDCog
from stores.ras import FRDRasPlan

project = "kanawha"
realization = 1


AORC_DATERANGE = [
    datetime(1979, 2, 1, tzinfo=timezone.utc),
    datetime(2022, 10, 31, tzinfo=timezone.utc),
]


# start project catalog
catalog = pystac.Catalog(
    id=project,
    description="Kanawha Pilot Study catalog",
    stac_extensions=["https://stac-extensions.github.io/projection/v1.0.0/schema.json"],
)


# Add runs to realization catalog
bucket = "ffrd-pilot"
ras_models = [
    "New_Little_River",
    "Stochastic_Greenbri",
    "MiddleNew",
    "ElkMiddle",
]


# Add realization catalog
r_catalog_idx = f"{project}-R{realization}"
r_catalog = pystac.Catalog(
    id=r_catalog_idx,
    description=f"Realization {realization} of the Kanawha Pilot Study",
    stac_extensions=["https://stac-extensions.github.io/projection/v1.0.0/schema.json"],
)

catalog.add_child(r_catalog)

runs = range(250, 260)
grids = range(90, 120)

print(r_catalog_idx)
for model in ras_models:
    for run in runs:
        try:
            hdf_key = f"model-library/FFRD_Kanawha_Compute/runs/{run}/ras/{model}/{model}.p01.hdf"

            collection_idx = f"ras-R{realization}-r{run}"
            print("\t", collection_idx)
            root = f"s3://ffrd-pilot/model-library/FFRD_Kanawha_Compute/runs/{run}"

            grid_bboxes = []
            grid_items = []

            for grid in grids:
                grid_idx = f"R{realization}-r{run}-{model}-dg-{grid}"
                print("\t\t", grid_idx)
                grid_key = f"model-library/FFRD_Kanawha_Compute/runs/{run}/depth-grids/{model}/grid_{grid}.tif"

                png_dir = f"stac-tiffs/{r_catalog_idx}/{collection_idx}/{grid_idx}"
                if not os.path.exists(png_dir):
                    os.makedirs(png_dir)

                cog = FRDCog.from_s3(bucket, grid_key)
                grid_item = cog.to_pystac_item(grid_idx, thumbnail_path=f"{png_dir}/thumbnail.png")

                grid_bboxes.append(grid_item.bbox)
                grid_items.append(grid_item)

            plan_data = FRDRasPlan.from_s3(bucket, hdf_key)
            ras_idx = f"R{realization}-r{run}-{model}-p1"
            print("\t\t", ras_idx)
            ras_item = plan_data.to_pystac_item(ras_idx)

            run_collection = pystac.Collection(
                id=collection_idx,
                description=f"HEC-RAS data from Realization {realization} of the {project} FFRD project",
                stac_extensions=["https://stac-extensions.github.io/projection/v1.0.0/schema.json"],
                extent=pystac.Extent(
                    spatial=pystac.SpatialExtent(grid_bboxes),
                    temporal=pystac.TemporalExtent(intervals=[AORC_DATERANGE[0], AORC_DATERANGE[1]]),
                ),
            )
            run_collection.add_item(ras_item)
            run_collection.add_items(pystac.ItemCollection(grid_items))
            r_catalog.add_child(run_collection)
        except:
            print(f"skipping {model}-{run}")
            continue

print(len(list(catalog.get_all_items())))
catalog.describe()

catalog.normalize_hrefs("./stac-tiffs")


# # Save the catalog
catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
catalog.save()
