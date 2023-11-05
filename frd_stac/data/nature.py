from utils.cogs import FRDCog
from utils.s3_utils import download_from_s3, s3_key_exists
from datetime import datetime, timezone
import pystac
import os
import pathlib as pl

nature_map = {
    "id": "nature",
    "description": "This catalog contains datasets describing the natural environment. \
          Examples include topographic datasets of varying resolution and projection, land-use /land-cover \
            datasets, and derivative products including infiltration grids and friction surfaces",
    "extensions": [
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        "https://stac-extensions.github.io/osc/v1.0.0-rc.3/schema.json",
        "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
    ],
}
