import pathlib as pl
from datetime import datetime, timezone


def verify_key(bucket: str, key: str):
    """
    TODO
    """
    pass


def key_last_updated(bucket: str, key: str):
    """
    TODO
    """
    return datetime.now(tz=timezone.utc)


def vsi_path(bucket: str, key: str, filename: str = None):
    if pl.Path(key).suffix == ".zip":
        vsi_prefix = "/vsizip/vsis3"
    else:
        vsi_prefix = "/vsis3"

    if filename is not None:
        return f"{vsi_prefix}/{bucket}/{key}/{filename}"
    else:
        return f"{vsi_prefix}/{bucket}/{key}"
