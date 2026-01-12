from database import get_cursor


_DEF_NOT_FOUND = {"error": "CONTENT_NOT_FOUND"}


def _serialize_deleted_content_row(row):
    if not row:
        return None
    return {
        "content_id": row["content_id"],
        "source": row["source"],
        "content_type": row["content_type"],
        "title": row["title"],
        "status": row["status"],
        "is_deleted": row["is_deleted"],
        "meta": row["meta"],
        "deleted_at": row["deleted_at"],
        "deleted_reason": row["deleted_reason"],
        "deleted_by": row["deleted_by"],
    }


def soft_delete_content(conn, *, admin_id, content_id, source, reason):
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT content_id, source, content_type, title, status,
                   is_deleted, meta, deleted_at, deleted_reason, deleted_by
            FROM contents
            WHERE content_id = %s AND source = %s
            """,
            (content_id, source),
        )
        row = cursor.fetchone()
        if row is None:
            return _DEF_NOT_FOUND

        if not row["is_deleted"]:
            cursor.execute(
                """
                UPDATE contents
                SET is_deleted = TRUE,
                    deleted_at = NOW(),
                    deleted_reason = %s,
                    deleted_by = %s
                WHERE content_id = %s AND source = %s
                """,
                (reason, admin_id, content_id, source),
            )
            cursor.execute(
                """
                SELECT content_id, source, content_type, title, status,
                       is_deleted, meta, deleted_at, deleted_reason, deleted_by
                FROM contents
                WHERE content_id = %s AND source = %s
                """,
                (content_id, source),
            )
            row = cursor.fetchone()

        result = {
            "content": _serialize_deleted_content_row(row),
            "subscriptions_retained": True,
        }
        return result
    finally:
        cursor.close()


def restore_content(conn, *, content_id, source):
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT content_id, source, content_type, title, status,
                   is_deleted, meta, deleted_at, deleted_reason, deleted_by
            FROM contents
            WHERE content_id = %s AND source = %s
            """,
            (content_id, source),
        )
        row = cursor.fetchone()
        if row is None:
            return _DEF_NOT_FOUND

        if row["is_deleted"]:
            cursor.execute(
                """
                UPDATE contents
                SET is_deleted = FALSE,
                    deleted_at = NULL,
                    deleted_reason = NULL,
                    deleted_by = NULL
                WHERE content_id = %s AND source = %s
                """,
                (content_id, source),
            )
            cursor.execute(
                """
                SELECT content_id, source, content_type, title, status,
                       is_deleted, meta, deleted_at, deleted_reason, deleted_by
                FROM contents
                WHERE content_id = %s AND source = %s
                """,
                (content_id, source),
            )
            row = cursor.fetchone()

        return {"content": _serialize_deleted_content_row(row)}
    finally:
        cursor.close()


def list_deleted_contents(conn, *, limit, offset, q=None):
    cursor = get_cursor(conn)
    params = []
    query = """
        SELECT content_id, source, content_type, title, status,
               is_deleted, meta, deleted_at, deleted_reason, deleted_by
        FROM contents
        WHERE COALESCE(is_deleted, FALSE) = TRUE
    """
    if q:
        query += " AND (title ILIKE %s OR normalized_title ILIKE %s)"
        like_value = f"%{q}%"
        params.extend([like_value, like_value])
    query += """
        ORDER BY deleted_at DESC NULLS LAST, title ASC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    cursor.close()
    return [_serialize_deleted_content_row(row) for row in rows]
