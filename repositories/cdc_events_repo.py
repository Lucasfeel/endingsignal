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
        SELECT
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        WHERE NOT EXISTS (
            SELECT 1
            FROM cdc_event_tombstones
            WHERE content_id = %s
              AND source = %s
              AND event_type = %s
        )
        ON CONFLICT (content_id, source, event_type) DO NOTHING
        RETURNING id
        """,
        (
            content_id,
            source,
            event_type,
            final_status,
            final_completed_at,
            resolved_by,
            content_id,
            source,
            event_type,
        ),
    )
    inserted = cursor.fetchone() is not None
    cursor.close()
    return inserted
