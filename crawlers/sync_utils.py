"""Shared changed-row-only sync helpers for crawler writers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import psycopg2.extras

from database import get_cursor
from utils.novel_genres import resolve_novel_genre_columns
from utils.content_indexing import build_search_document, canonicalize_json


@dataclass
class ContentSyncStats:
    inserted_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    write_skipped_count: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "inserted_count": int(self.inserted_count),
            "updated_count": int(self.updated_count),
            "unchanged_count": int(self.unchanged_count),
            "write_skipped_count": int(self.write_skipped_count),
        }


def load_existing_content_snapshot(
    conn,
    source_name: str,
    *,
    cursor_getter: Callable = get_cursor,
) -> Dict[str, Dict[str, Any]]:
    cursor = cursor_getter(conn)
    try:
        cursor.execute(
            """
            SELECT
                content_id,
                content_type,
                title,
                normalized_title,
                normalized_authors,
                status,
                meta,
                search_document,
                novel_genre_group,
                novel_genre_groups
            FROM contents
            WHERE source = %s
            """,
            (source_name,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        return {
            str(row["content_id"]): {
                "content_type": row.get("content_type"),
                "title": row.get("title"),
                "normalized_title": row.get("normalized_title") or "",
                "normalized_authors": row.get("normalized_authors") or "",
                "status": row.get("status"),
                "meta_json": canonicalize_json(row.get("meta") or {}),
                "search_document": row.get("search_document") or "",
                "novel_genre_group": row.get("novel_genre_group"),
                "novel_genre_groups_json": canonicalize_json(row.get("novel_genre_groups") or []),
            }
            for row in rows
        }
    finally:
        cursor.close()


def build_sync_row(
    *,
    content_id: str,
    source: str,
    content_type: str,
    title: str,
    normalized_title: str,
    normalized_authors: str,
    status: str,
    meta: Mapping[str, Any],
    novel_genre_group: Optional[str] = None,
    novel_genre_groups: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    safe_meta = dict(meta) if isinstance(meta, Mapping) else {}
    resolved_genre_group = novel_genre_group
    resolved_genre_groups = list(novel_genre_groups or [])
    if str(content_type).strip().lower() == "novel":
        if resolved_genre_group is None and not resolved_genre_groups:
            resolved_genre_group, resolved_genre_groups = resolve_novel_genre_columns(safe_meta)
    else:
        resolved_genre_group = None
        resolved_genre_groups = []
    return {
        "content_id": str(content_id),
        "source": str(source),
        "content_type": str(content_type),
        "title": str(title),
        "normalized_title": str(normalized_title or ""),
        "normalized_authors": str(normalized_authors or ""),
        "status": str(status),
        "meta": safe_meta,
        "meta_json": canonicalize_json(safe_meta),
        "search_document": build_search_document(
            title=title,
            normalized_title=normalized_title,
            normalized_authors=normalized_authors,
            meta=safe_meta,
        ),
        "novel_genre_group": resolved_genre_group,
        "novel_genre_groups": resolved_genre_groups,
        "novel_genre_groups_json": canonicalize_json(resolved_genre_groups),
    }


def sync_prepared_content_rows(
    conn,
    *,
    source_name: str,
    prepared_rows: Iterable[Mapping[str, Any]],
    existing_snapshot: Optional[Mapping[str, Mapping[str, Any]]] = None,
    write_skipped_count: int = 0,
    cursor_getter: Callable = get_cursor,
) -> Dict[str, int]:
    snapshot = (
        {
            str(content_id): dict(row)
            for content_id, row in (existing_snapshot or {}).items()
        }
        if existing_snapshot is not None
        else load_existing_content_snapshot(conn, source_name, cursor_getter=cursor_getter)
    )

    stats = ContentSyncStats(write_skipped_count=max(0, int(write_skipped_count)))
    inserts: List[tuple] = []
    updates: List[tuple] = []

    for raw_row in prepared_rows:
        row = dict(raw_row)
        content_id = str(row["content_id"])
        comparable = {
            "content_type": row["content_type"],
            "title": row["title"],
            "normalized_title": row["normalized_title"],
            "normalized_authors": row["normalized_authors"],
            "status": row["status"],
            "meta_json": row["meta_json"],
            "search_document": row["search_document"],
            "novel_genre_group": row.get("novel_genre_group"),
            "novel_genre_groups_json": row.get("novel_genre_groups_json") or canonicalize_json([]),
        }

        existing = snapshot.get(content_id)
        if existing is None:
            inserts.append(
                (
                    content_id,
                    source_name,
                    row["content_type"],
                    row["title"],
                    row["normalized_title"],
                    row["normalized_authors"],
                    row["status"],
                    psycopg2.extras.Json(row["meta"]),
                    row["search_document"],
                    row.get("novel_genre_group"),
                    row.get("novel_genre_groups") or [],
                )
            )
            stats.inserted_count += 1
            continue

        existing_comparable = dict(existing)
        existing_comparable.setdefault("novel_genre_group", None)
        existing_comparable.setdefault("novel_genre_groups_json", canonicalize_json([]))
        if all(existing_comparable.get(key) == value for key, value in comparable.items()):
            stats.unchanged_count += 1
            continue

        updates.append(
            (
                row["content_type"],
                row["title"],
                row["normalized_title"],
                row["normalized_authors"],
                row["status"],
                psycopg2.extras.Json(row["meta"]),
                row["search_document"],
                row.get("novel_genre_group"),
                row.get("novel_genre_groups") or [],
                content_id,
                source_name,
            )
        )
        stats.updated_count += 1

    cursor = cursor_getter(conn)
    try:
        if updates:
            cursor.executemany(
                """
                UPDATE contents
                SET content_type=%s,
                    title=%s,
                    normalized_title=%s,
                    normalized_authors=%s,
                    status=%s,
                    meta=%s,
                    search_document=%s,
                    novel_genre_group=%s,
                    novel_genre_groups=%s,
                    updated_at=NOW()
                WHERE content_id=%s
                  AND source=%s
                """,
                updates,
            )

        if inserts:
            cursor.executemany(
                """
                INSERT INTO contents (
                    content_id,
                    source,
                    content_type,
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    meta,
                    search_document,
                    novel_genre_group,
                    novel_genre_groups
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_id, source) DO NOTHING
                """,
                inserts,
            )
    finally:
        cursor.close()

    return stats.to_dict()
