"""Shared database sync helpers for novel crawlers."""

from __future__ import annotations

from typing import Any, Dict, List

from .sync_utils import build_sync_row, sync_prepared_content_rows
from utils.backfill import STATUS_COMPLETED, STATUS_ONGOING, dedupe_strings
from utils.text import normalize_search_text


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _merge_unique_strings(*values: object) -> List[str]:
    merged: List[str] = []
    for value in values:
        if isinstance(value, list):
            merged.extend(str(item) for item in value)
    return dedupe_strings(merged)


def synchronize_novel_contents(
    conn,
    *,
    source_name: str,
    all_content_today: Dict[str, Dict[str, Any]],
    ongoing_today: Dict[str, Dict[str, Any]],
    finished_today: Dict[str, Dict[str, Any]],
    existing_snapshot: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, int]:
    prepared_rows = []
    write_skipped_count = 0
    for content_id, entry in all_content_today.items():
        cid = _clean_text(content_id)
        if not cid:
            write_skipped_count += 1
            continue

        if cid in finished_today:
            status = STATUS_COMPLETED
        elif cid in ongoing_today:
            status = STATUS_ONGOING
        else:
            status = _clean_text(entry.get("status")) or STATUS_ONGOING

        title = _clean_text(entry.get("title"))
        authors = _merge_unique_strings(entry.get("authors"))
        content_url = _clean_text(entry.get("content_url"))
        if not title or not authors or not content_url:
            write_skipped_count += 1
            continue

        normalized_title = normalize_search_text(title)
        normalized_authors = normalize_search_text(" ".join(authors))
        thumbnail_url = _clean_text(entry.get("thumbnail_url")) or None
        genres = _merge_unique_strings(entry.get("genres"))
        crawl_roots = _merge_unique_strings(entry.get("crawl_roots"))

        attributes = {
            "weekdays": ["daily"],
            "genres": genres,
            "source_genres": genres,
            "is_completed": status == STATUS_COMPLETED,
        }
        if crawl_roots:
            attributes["crawl_roots"] = crawl_roots
        genre_value = _clean_text(entry.get("genre") or entry.get("genre_group"))
        if genre_value:
            attributes["genre"] = genre_value
        elif genres:
            attributes["genre"] = genres[0]

        meta = {
            "common": {
                "authors": authors,
                "content_url": content_url,
            },
            "attributes": attributes,
        }
        if thumbnail_url:
            meta["common"]["thumbnail_url"] = thumbnail_url

        prepared_rows.append(
            build_sync_row(
                content_id=cid,
                source=source_name,
                content_type="novel",
                title=title,
                normalized_title=normalized_title,
                normalized_authors=normalized_authors,
                status=status,
                meta=meta,
            )
        )

    return sync_prepared_content_rows(
        conn,
        source_name=source_name,
        prepared_rows=prepared_rows,
        existing_snapshot=existing_snapshot,
        write_skipped_count=write_skipped_count,
    )
