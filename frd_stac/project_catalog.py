from datetime import datetime, timezone
import data_catalog
import model_catalog
import realizations_collection
import os
import pystac

# ----------CONFIG----------#
project_name = "kanawha"
project_description = (
    f"Project catalog for the {project_name} flood risk pilot project."
)
catalogs = [data_catalog, model_catalog]
collections = [realizations_collection]
project_extensions = []


# Step 1. Create the top level catalog
catalog = pystac.Catalog(
    id=project_name,
    description=project_description,
    stac_extensions=project_extensions,
)

# Step 2. Create the child catalogs
for cat in catalogs:
    child_catalog = cat.main()
    project_extensions.extend(child_catalog.stac_extensions)
    catalog.add_child(child_catalog)

for col in collections:
    child_collection = col.main()
    project_extensions.extend(child_collection.stac_extensions)
    catalog.add_child(child_collection)

# Step 4. Save
catalog.normalize_hrefs(f"./{project_name}")
catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
