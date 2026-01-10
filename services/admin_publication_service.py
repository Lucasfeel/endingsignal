from database import get_cursor


_DEF_NOT_FOUND = {"error": "CONTENT_NOT_FOUND"}


def _get_row_value(row, key):
    try:
        return row[key]
    except Exception:
        return None


def _serialize_publication_row(row):
    if not row:
        return None
    return {
        "id": row["id"],
        "content_id": row["content_id"],
        "source": row["source"],
        "public_at": row["public_at"],
        "reason": row["reason"],
        "admin_id": row["admin_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "title": _get_row_value(row, "title"),
        "content_type": _get_row_value(row, "content_type"),
        "status": _get_row_value(row, "status"),
        "meta": _get_row_value(row, "meta"),
        "is_deleted": _get_row_value(row, "is_deleted"),
    }


def upsert_publication(
    conn,
    *,
    admin_id,
    content_id,
    source,
    public_at,
    reason,
):
    cursor = get_cursor(conn)

    cursor.execute(
        "SELECT 1 FROM contents WHERE content_id = %s AND source = %s",
        (content_id, source),
    )
    if cursor.fetchone() is None:
        cursor.close()
        return _DEF_NOT_FOUND

    cursor.execute(
        """
        INSERT INTO admin_content_metadata (
            content_id,
            source,
            public_at,
            reason,
            admin_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (content_id, source) DO UPDATE SET
            public_at = EXCLUDED.public_at,
            reason = EXCLUDED.reason,
            admin_id = EXCLUDED.admin_id,
            updated_at = NOW()
        RETURNING id, content_id, source, public_at, reason, admin_id, created_at, updated_at
        """,
        (content_id, source, public_at, reason, admin_id),
    )
    publication_row = cursor.fetchone()
    cursor.close()

    return {"publication": _serialize_publication_row(publication_row)}


def delete_publication(conn, *, content_id, source):
    cursor = get_cursor(conn)
    cursor.execute(
        "DELETE FROM admin_content_metadata WHERE content_id = %s AND source = %s",
        (content_id, source),
    )
    cursor.close()


def list_publications(conn, *, limit, offset):
    cursor = get_cursor(conn)
    cursor.execute(
        """
        SELECT
            m.id,
            m.content_id,
            m.source,
            m.public_at,
            m.reason,
            m.admin_id,
            m.created_at,
            m.updated_at,
            c.title,
            c.content_type,
            c.status,
            c.meta,
            COALESCE(c.is_deleted, FALSE) AS is_deleted
        FROM admin_content_metadata m
        JOIN contents c
          ON c.content_id = m.content_id AND c.source = m.source
        ORDER BY m.updated_at DESC NULLS LAST, m.created_at DESC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )
    rows = cursor.fetchall()
    cursor.close()
    return [_serialize_publication_row(row) for row in rows]
