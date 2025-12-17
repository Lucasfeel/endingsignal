from services.cdc_constants import EVENT_CONTENT_COMPLETED, STATUS_COMPLETED
from repositories.cdc_events_repo import insert_event


def record_content_completed_event(conn, *, content_id, source, final_completed_at, resolved_by) -> bool:
    """
    Record a CONTENT_COMPLETED CDC event idempotently.
    """
    return insert_event(
        conn,
        content_id=content_id,
        source=source,
        event_type=EVENT_CONTENT_COMPLETED,
        final_status=STATUS_COMPLETED,
        final_completed_at=final_completed_at,
        resolved_by=resolved_by,
    )
