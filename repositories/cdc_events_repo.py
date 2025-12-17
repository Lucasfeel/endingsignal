"""Repository for CDC event persistence."""

from database import get_cursor


def insert_event(conn, *, content_id, source, event_type, final_status, final_completed_at, resolved_by) -> bool:
    """
    Insert a CDC event idempotently.

    Returns True if a new row was inserted, False if it already existed.
    """
    cursor = get_cursor(conn)
    cursor.execute(
        """
        INSERT INTO cdc_events (
            content_id,
            source,
            event_type,
            final_status,
            final_completed_at,
            resolved_by
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (content_id, source, event_type) DO NOTHING
        RETURNING id
        """,
        (content_id, source, event_type, final_status, final_completed_at, resolved_by),
    )
    inserted = cursor.fetchone() is not None
    cursor.close()
    return inserted
