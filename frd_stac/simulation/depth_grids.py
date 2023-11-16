from datetime import datetime, timezone
import os
from utils import ras, cogs
from utils.s3_utils import s3_key_exists, download_from_s3
import pystac
import json

realization = "r1"
sample_models = ["New_Little_River", "ElkMiddle", "GSummersville"]
sample_simulations = range(250, 255)
sample_grids = range(120, 130)
parent_collection = f"kanawha/realizations/r1/collection.json"
# bucket = "ffrd-pilot"


def main(bucket: str = "ffrd-pilot"):
    # Inventory HEC-RAS simulation output data
    catalog_bboxes = []
    for sim in sample_simulations:
        items, bboxes = [], []
        col_file: str = f"kanawha/realizations/r1/r1-s{sim}/depth-grids/collection.json"
        collection = pystac.Collection.from_file(col_file)
        for model in sample_models:
            for grid in sample_grids:
                grid_id = f"{model}-{sim}-grid_{grid}"
                print(grid_id)
                s3_key = f"model-library/FFRD_Kanawha_Compute/runs/{sim}/depth-grids/{model}/grid_{grid}.tif"
                local_dir = f"kanawha/realizations/{realization}/{realization}-s{sim}/depth-grids/{grid_id}"

                # print("\t\t", grid_id)

                if s3_key_exists(bucket, s3_key):
                    cog = cogs.FRDCog.from_s3(bucket, s3_key, cmap="GnBu")

                    # Do not process tiles that have all no data values
                    if not cog.all_cells_dry:
                        if not os.path.exists(local_dir):
                            os.makedirs(local_dir)

                        download_from_s3(bucket, s3_key, f"{local_dir}/grid.tif")
                        grid_item = cog.to_pystac_item(
                            grid_id, thumbnail_path=f"{local_dir}/thumbnail.png"
                        )
                        bboxes.append(grid_item.bbox)
                        items.append(grid_item)

                        if cog.bbox_4326 not in catalog_bboxes:
                            catalog_bboxes.append(cog.bbox_4326)
                else:
                    print(f"error for {sim}-{model}-{grid}")

        collection.add_items(pystac.ItemCollection(items))
        collection.extent.spatial = pystac.SpatialExtent(bboxes)
        collection.save()