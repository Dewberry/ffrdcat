from dataclasses import dataclass
from .utils import read_file_from_zip


@dataclass
class RasMeta:
    bbox: tuple
    projection: str = None
    geometry_files: list = None
    fields: list = None


STAC_RAS_MODEL_EXTENSIONS = [
    "https://stac-extensions.github.io/storage/v1.0.0/schema.json",
    "https://stac-extensions.github.io/processing/v1.1.0/schema.json",
]


def get_ras_model_meta(fs, zip_filename: str, internal_filename: str = None) -> RasMeta:        
    flines = read_file_from_zip(fs, zip_filename, internal_filename)
    view_window = flines[2]
    data = view_window.split()
    bbox= [float(data[2]),float(data[4]),float(data[6]),float(data[8])]

    return RasMeta(
        bbox=bbox,
    )


def ras_model_item_properties(
    ffrd_proj_name: str,
    proj_level: str = "pilot",
    fields: str = [],
    data_type: str = "HEC-RAS Model",
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

