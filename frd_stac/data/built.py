from utils.cogs import FRDCog
from utils.s3_utils import download_from_s3, s3_key_exists
from datetime import datetime, timezone
import pystac
import os
import pathlib as pl

built_map = {
    "id": "built",
    "description": "This catalog contains datasets describing the built environment. \
          Examples include hydraulic structures such as levees, bridges, culverts, and other control \
            structures as well as point data reprsenting structures. Some of these datasets are available \
            directly via API endpoints.",
    "extensions": [
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        "https://stac-extensions.github.io/osc/v1.0.0-rc.3/schema.json",
        "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
    ],
}
