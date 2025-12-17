from services.cdc_constants import EVENT_CONTENT_COMPLETED, STATUS_COMPLETED
from services.cdc_events_repo import insert_event


VALID_RESOLVED_BY = {"crawler", "override"}


def record_content_completed_event(
    conn,
    *,
    content_id,
    source,
    final_completed_at,
    resolved_by,
):
    """
    Record a CONTENT_COMPLETED CDC event idempotently.

    Returns True if a new event was inserted, False otherwise.
    """
    if resolved_by not in VALID_RESOLVED_BY:
        raise ValueError("resolved_by must be 'crawler' or 'override'")

    return insert_event(
        conn,
        content_id=content_id,
        source=source,
        event_type=EVENT_CONTENT_COMPLETED,
        final_status=STATUS_COMPLETED,
        final_completed_at=final_completed_at,
        resolved_by=resolved_by,
    )
