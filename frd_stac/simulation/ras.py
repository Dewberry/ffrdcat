from datetime import datetime, timezone
import os
from utils import ras
import pystac
import json

# sample_models = ["New_Little_River", "ElkMiddle"]
# sample_simulations = range(250, 252)


sample_models = ["New_Little_River"]
sample_simulations = range(250, 251)


def main(bucket: str = "ffrd-pilot"):
    bucket="ffrd-pilot"
    # catalog_bboxes = []
    for sim in sample_simulations:
        col_file = f"kanawha/realizations/r1/r1-s{sim}/hec-ras/collection.json"
        collection = pystac.Collection.from_file(col_file)

        # Inventory HEC-RAS simulation output data
        items, bboxes = [], []
        for model in sample_models:
            hdf_key = f"model-library/FFRD_Kanawha_Compute/runs/{sim}/ras/{model}/{model}.p01.hdf"
            ras_log = f"model-library/FFRD_Kanawha_Compute/runs/{sim}/ras/{model}/{model}.rasoutput.log"
            plan_data = ras.FRDRasPlan(bucket, hdf_key)
            ras_id = f"r1-s{sim}-{model}"
            print("\t\t", ras_id)
            ras_item = plan_data.to_pystac_item(ras_id, log=ras_log)
            items.append(ras_item)
            bboxes.append(plan_data.bbox_4326)
            # if plan_data.bbox_4326 not in catalog_bboxes:
            #     catalog_bboxes.append(plan_data.bbox_4326)

        ras_item
        collection.add_items(pystac.ItemCollection(items))
        collection.extent.spatial = pystac.SpatialExtent(bboxes)
        collection.save()
        
        # #  TODO: Update collections in a separate script
        # parent = pystac.Collection.from_file(parent_collection)
        # if [0, 0, 0, 0] in parent.extent.spatial.bboxes:
        #     parent.extent.spatial.bboxes.remove([0, 0, 0, 0])
        # parent.extent.spatial.bboxes = parent.extent.spatial.bboxes.extend(catalog_bboxes)
        # parent.save()
