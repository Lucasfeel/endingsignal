"""Repository for CDC event consumption persistence."""

from database import get_cursor


def mark_consumed(conn, *, consumer, event_id, status, reason=None) -> bool:
    """
    Insert a CDC event consumption idempotently.

    Returns True if a new row was inserted, False if it already existed.
    """
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            INSERT INTO cdc_event_consumptions (
                consumer,
                event_id,
                status,
                reason
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (consumer, event_id) DO NOTHING
            RETURNING id
            """,
            (consumer, event_id, status, reason),
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def get_consumption(conn, *, consumer, event_id):
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT consumer, event_id, status, reason, created_at
            FROM cdc_event_consumptions
            WHERE consumer = %s AND event_id = %s
            """,
            (consumer, event_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "consumer": row["consumer"],
            "event_id": row["event_id"],
            "status": row["status"],
            "reason": row["reason"],
            "created_at": row["created_at"],
        }
    finally:
        cursor.close()
