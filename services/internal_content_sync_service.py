from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2.extras

from database import get_cursor
from utils.novel_genres import resolve_novel_genre_columns

UPSERT_PAGE_SIZE = 500
MAX_BATCH_SIZE = 1000

UPSERT_SQL = """
INSERT INTO contents (
    content_id,
    source,
    content_type,
    title,
    normalized_title,
    normalized_authors,
    status,
    meta,
    novel_genre_group,
    novel_genre_groups,
    is_deleted,
    deleted_at,
    deleted_reason,
    deleted_by,
    created_at,
    updated_at,
    search_document
)
VALUES %s
ON CONFLICT (content_id, source)
DO UPDATE SET
    content_type = EXCLUDED.content_type,
    title = EXCLUDED.title,
    normalized_title = EXCLUDED.normalized_title,
    normalized_authors = EXCLUDED.normalized_authors,
    status = EXCLUDED.status,
    meta = EXCLUDED.meta,
    novel_genre_group = EXCLUDED.novel_genre_group,
    novel_genre_groups = EXCLUDED.novel_genre_groups,
    is_deleted = EXCLUDED.is_deleted,
    deleted_at = EXCLUDED.deleted_at,
    deleted_reason = EXCLUDED.deleted_reason,
    deleted_by = EXCLUDED.deleted_by,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    search_document = EXCLUDED.search_document
RETURNING (xmax = 0) AS inserted
"""

SUMMARY_SQL = """
SELECT
    source,
    content_type,
    COUNT(*)::int AS total_count,
    COUNT(*) FILTER (WHERE COALESCE(is_deleted, FALSE) = FALSE)::int AS active_count,
    COUNT(*) FILTER (WHERE COALESCE(is_deleted, FALSE) = TRUE)::int AS deleted_count
FROM contents
GROUP BY source, content_type
ORDER BY source, content_type
"""


def _parse_optional_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise ValueError("timestamp fields must be ISO 8601 strings or null")

    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    try:
        return datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp value: {value}") from exc


def _coerce_required_text(raw_row: Dict[str, Any], field_name: str) -> str:
    value = str(raw_row.get(field_name) or "").strip()
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def _coerce_optional_text(raw_row: Dict[str, Any], field_name: str) -> Optional[str]:
    value = raw_row.get(field_name)
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_int(raw_row: Dict[str, Any], field_name: str) -> Optional[int]:
    value = raw_row.get(field_name)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer or null") from exc


def _normalize_row(raw_row: Dict[str, Any]) -> Tuple[Any, ...]:
    if not isinstance(raw_row, dict):
        raise ValueError("each row must be an object")

    meta = raw_row.get("meta")
    if meta is not None and not isinstance(meta, dict):
        raise ValueError("meta must be an object or null")
    content_type = _coerce_required_text(raw_row, "content_type")
    novel_genre_group = None
    novel_genre_groups = []
    if content_type == "novel":
        novel_genre_group, novel_genre_groups = resolve_novel_genre_columns(meta or {})

    created_at = _parse_optional_timestamp(raw_row.get("created_at"))
    updated_at = _parse_optional_timestamp(raw_row.get("updated_at"))
    if created_at is None or updated_at is None:
        raise ValueError("created_at and updated_at are required")

    return (
        _coerce_required_text(raw_row, "content_id"),
        _coerce_required_text(raw_row, "source"),
        content_type,
        _coerce_required_text(raw_row, "title"),
        _coerce_optional_text(raw_row, "normalized_title"),
        _coerce_optional_text(raw_row, "normalized_authors"),
        _coerce_required_text(raw_row, "status"),
        psycopg2.extras.Json(meta) if meta is not None else None,
        novel_genre_group,
        novel_genre_groups,
        bool(raw_row.get("is_deleted", False)),
        _parse_optional_timestamp(raw_row.get("deleted_at")),
        _coerce_optional_text(raw_row, "deleted_reason"),
        _coerce_optional_int(raw_row, "deleted_by"),
        created_at,
        updated_at,
        _coerce_optional_text(raw_row, "search_document"),
    )


def summarize_contents(conn) -> Dict[str, Any]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(SUMMARY_SQL)
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()

    total_count = sum(int(row.get("total_count") or 0) for row in rows)
    active_count = sum(int(row.get("active_count") or 0) for row in rows)
    deleted_count = sum(int(row.get("deleted_count") or 0) for row in rows)
    return {
        "rows": rows,
        "total_count": total_count,
        "active_count": active_count,
        "deleted_count": deleted_count,
    }


def upsert_contents_batch(conn, rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(rows, Sequence):
        raise ValueError("rows must be an array")

    received_count = len(rows)
    if received_count == 0:
        return {
            "received_count": 0,
            "inserted_count": 0,
            "updated_count": 0,
        }
    if received_count > MAX_BATCH_SIZE:
        raise ValueError(f"rows batch size exceeds limit ({MAX_BATCH_SIZE})")

    normalized_rows = [_normalize_row(raw_row) for raw_row in rows]

    cursor = get_cursor(conn)
    try:
        results = psycopg2.extras.execute_values(
            cursor,
            UPSERT_SQL,
            normalized_rows,
            page_size=min(received_count, UPSERT_PAGE_SIZE),
            fetch=True,
        )
        inserted_count = sum(1 for row in results if row and bool(row[0]))
        updated_count = len(results) - inserted_count
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

    return {
        "received_count": received_count,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
    }
