from services.cdc_constants import EVENT_CONTENT_COMPLETED, STATUS_COMPLETED
from repositories.cdc_events_repo import insert_event
from utils.record import read_field


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


def record_due_scheduled_completions(conn, cursor, now):
    """
    Insert CONTENT_COMPLETED events for scheduled override completions that
    became effective as of ``now``.
    """

    cursor.execute(
        """
        SELECT content_id, source, override_completed_at
        FROM admin_content_overrides
        WHERE override_status = %s
          AND override_completed_at IS NOT NULL
          AND override_completed_at <= %s
        """,
        (STATUS_COMPLETED, now),
    )
    due_rows = cursor.fetchall()

    inserted_count = 0
    for row in due_rows:
        content_id = read_field(row, "content_id")
        source = read_field(row, "source")
        override_completed_at = read_field(row, "override_completed_at")

        if content_id is None or source is None:
            continue

        cursor.execute(
            "SELECT 1 FROM contents WHERE content_id = %s AND source = %s",
            (content_id, source),
        )
        if cursor.fetchone() is None:
            continue

        inserted = record_content_completed_event(
            conn,
            content_id=content_id,
            source=source,
            final_completed_at=override_completed_at,
            resolved_by="override",
        )
        if inserted:
            inserted_count += 1

    return {
        "due_count": len(due_rows),
        "inserted_count": inserted_count,
    }
