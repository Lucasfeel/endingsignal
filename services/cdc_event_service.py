from services.cdc_constants import (
    EVENT_CONTENT_COMPLETED,
    EVENT_CONTENT_PUBLISHED,
    STATUS_COMPLETED,
    STATUS_PUBLISHED,
)
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


def record_content_published_event(conn, *, content_id, source, public_published_at, resolved_by) -> bool:
    """
    Record a CONTENT_PUBLISHED CDC event idempotently.
    """
    return insert_event(
        conn,
        content_id=content_id,
        source=source,
        event_type=EVENT_CONTENT_PUBLISHED,
        final_status=STATUS_PUBLISHED,
        final_completed_at=public_published_at,
        resolved_by=resolved_by,
    )


def record_due_scheduled_completions(conn, cursor, now):
    """
    Insert CONTENT_COMPLETED events for scheduled override completions that
    became effective as of ``now``.
    """
    cursor.execute(
        """
        WITH due_rows AS (
            SELECT DISTINCT o.content_id, o.source, o.override_completed_at
            FROM admin_content_overrides o
            JOIN contents c
              ON c.content_id = o.content_id
             AND c.source = o.source
            LEFT JOIN cdc_event_tombstones tombstone
              ON tombstone.content_id = o.content_id
             AND tombstone.source = o.source
             AND tombstone.event_type = %s
            WHERE o.override_status = %s
              AND o.override_completed_at IS NOT NULL
              AND o.override_completed_at <= %s
              AND tombstone.id IS NULL
        ),
        inserted AS (
            INSERT INTO cdc_events (
                content_id,
                source,
                event_type,
                final_status,
                final_completed_at,
                resolved_by
            )
            SELECT
                d.content_id,
                d.source,
                %s,
                %s,
                d.override_completed_at,
                'override'
            FROM due_rows d
            ON CONFLICT (content_id, source, event_type) DO NOTHING
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM due_rows) AS due_count,
            (SELECT COUNT(*) FROM inserted) AS inserted_count
        """,
        (
            EVENT_CONTENT_COMPLETED,
            STATUS_COMPLETED,
            now,
            EVENT_CONTENT_COMPLETED,
            STATUS_COMPLETED,
        ),
    )
    row = cursor.fetchone() or {}
    due_count = row.get("due_count", 0)
    inserted_count = row.get("inserted_count", 0)

    return {
        "due_count": due_count,
        "inserted_count": inserted_count,
    }


def record_due_scheduled_publications(conn, cursor, now):
    """
    Insert CONTENT_PUBLISHED events for scheduled publications that
    became effective as of ``now``.
    """
    cursor.execute(
        """
        WITH due_rows AS (
            SELECT DISTINCT m.content_id, m.source, m.public_at
            FROM admin_content_metadata m
            JOIN contents c
              ON c.content_id = m.content_id
             AND c.source = m.source
            LEFT JOIN cdc_event_tombstones tombstone
              ON tombstone.content_id = m.content_id
             AND tombstone.source = m.source
             AND tombstone.event_type = %s
            WHERE m.public_at IS NOT NULL
              AND m.public_at <= %s
              AND COALESCE(c.is_deleted, FALSE) = FALSE
              AND tombstone.id IS NULL
        ),
        inserted AS (
            INSERT INTO cdc_events (
                content_id,
                source,
                event_type,
                final_status,
                final_completed_at,
                resolved_by
            )
            SELECT
                d.content_id,
                d.source,
                %s,
                %s,
                d.public_at,
                'publication'
            FROM due_rows d
            ON CONFLICT (content_id, source, event_type) DO NOTHING
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM due_rows) AS due_count,
            (SELECT COUNT(*) FROM inserted) AS inserted_count
        """,
        (
            EVENT_CONTENT_PUBLISHED,
            now,
            EVENT_CONTENT_PUBLISHED,
            STATUS_PUBLISHED,
        ),
    )
    row = cursor.fetchone() or {}
    due_count = row.get("due_count", 0)
    inserted_count = row.get("inserted_count", 0)

    return {
        "scheduled_publication_due_count": due_count,
        "scheduled_publication_events_inserted_count": inserted_count,
        "cdc_events_inserted_count": inserted_count,
    }
