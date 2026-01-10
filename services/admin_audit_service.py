from psycopg2.extras import Json

from database import get_cursor


def insert_admin_action_log(
    conn,
    *,
    admin_id,
    action_type,
    content_id,
    source,
    reason=None,
    payload=None,
):
    cursor = get_cursor(conn)
    cursor.execute(
        """
        INSERT INTO admin_action_logs (
            admin_id,
            action_type,
            content_id,
            source,
            reason,
            payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            admin_id,
            action_type,
            content_id,
            source,
            reason,
            Json(payload) if payload is not None else None,
        ),
    )
    cursor.close()
