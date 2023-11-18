from datetime import datetime, timezone
import pystac


def collection_from_zip(
    col_id: str,
    description: str,
    extensions: list,
    items: list,
    bboxes: list,
    start_date: datetime = datetime.now(tz=timezone.utc),
    end_date: datetime = datetime.now(tz=timezone.utc),
):
    """
    TODO: Update the temporal intervals
    """
    collection = pystac.Collection(
        id=col_id,
        description=description,
        stac_extensions=extensions,
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes),
            temporal=pystac.TemporalExtent(
                intervals=[
                    start_date,
                    end_date,
                    datetime.now(tz=timezone.utc),
                ]
            ),
        ),
    )
    collection.add_items(items)
    return collection
