"""Shared helpers for one-time backfill scripts."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import psycopg2.extras

from database import get_cursor
from utils.text import normalize_search_text

EXECUTE_VALUES_PAGE_SIZE = 1000
LOGGER = logging.getLogger(__name__)

STATUS_COMPLETED = "완결"
STATUS_ONGOING = "연재중"


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def dedupe_strings(values: Iterable[str]) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for raw in values:
        text = _clean_text(raw)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(text)
    return deduped


def coerce_status(value: str) -> str:
    if _clean_text(value) == STATUS_COMPLETED:
        return STATUS_COMPLETED
    return STATUS_ONGOING


def merge_genres(*genre_lists: Optional[Sequence[str]]) -> List[str]:
    merged: List[str] = []
    for item in genre_lists:
        if not item:
            continue
        merged.extend(item)
    return dedupe_strings(merged)


@dataclass
class BackfillRecord:
    content_id: str
    source: str
    title: str
    authors: List[str]
    status: str
    content_url: str
    genres: List[str] = field(default_factory=list)
    thumbnail_url: Optional[str] = None

    @property
    def is_completed(self) -> bool:
        return self.status == STATUS_COMPLETED


def normalize_record(raw_record: Dict[str, Any]) -> Optional[BackfillRecord]:
    content_id = _clean_text(raw_record.get("content_id"))
    source = _clean_text(raw_record.get("source"))
    title = _clean_text(raw_record.get("title"))
    content_url = _clean_text(raw_record.get("content_url"))
    status = coerce_status(_clean_text(raw_record.get("status")))

    raw_authors = raw_record.get("authors")
    if isinstance(raw_authors, list):
        authors = dedupe_strings(str(item) for item in raw_authors)
    else:
        authors = []

    raw_genres = raw_record.get("genres")
    if isinstance(raw_genres, list):
        genres = dedupe_strings(str(item) for item in raw_genres)
    else:
        genres = []

    thumbnail_url = _clean_text(raw_record.get("thumbnail_url"))
    if not thumbnail_url:
        thumbnail_url = None

    if not content_id or not source or not title or not authors or not content_url:
        return None

    return BackfillRecord(
        content_id=content_id,
        source=source,
        title=title,
        authors=authors,
        status=status,
        content_url=content_url,
        genres=genres,
        thumbnail_url=thumbnail_url,
    )


def _build_meta(record: BackfillRecord) -> Dict[str, Any]:
    common_meta: Dict[str, Any] = {
        "authors": list(record.authors),
        "content_url": record.content_url,
    }
    if record.thumbnail_url:
        common_meta["thumbnail_url"] = record.thumbnail_url

    attributes_meta: Dict[str, Any] = {
        "genres": list(record.genres),
        "source_genres": list(record.genres),
        "is_completed": bool(record.is_completed),
        "backfill": True,
    }

    return {
        "common": common_meta,
        "attributes": attributes_meta,
    }


def _build_upsert_rows(records: Sequence[BackfillRecord]) -> List[tuple]:
    rows = []
    for record in records:
        rows.append(
            (
                record.content_id,
                record.source,
                "novel",
                record.title,
                normalize_search_text(record.title),
                normalize_search_text(" ".join(record.authors)),
                record.status,
                psycopg2.extras.Json(_build_meta(record)),
            )
        )
    return rows


def _dedupe_records_last_wins(records: Sequence[BackfillRecord]) -> Tuple[List[BackfillRecord], List[str]]:
    deduped_reversed: List[BackfillRecord] = []
    duplicate_content_ids: List[str] = []
    seen_duplicate_ids: Set[str] = set()
    seen_keys: Set[Tuple[str, str]] = set()

    for record in reversed(records):
        key = (record.content_id, record.source)
        if key in seen_keys:
            if record.content_id not in seen_duplicate_ids:
                seen_duplicate_ids.add(record.content_id)
                duplicate_content_ids.append(record.content_id)
            continue
        seen_keys.add(key)
        deduped_reversed.append(record)

    return list(reversed(deduped_reversed)), duplicate_content_ids


UPSERT_SQL = """
INSERT INTO contents (
    content_id,
    source,
    content_type,
    title,
    normalized_title,
    normalized_authors,
    status,
    meta
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
    updated_at = NOW()
RETURNING (xmax = 0) AS inserted
"""


@dataclass
class UpsertStats:
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0


class BackfillUpserter:
    """Batching upsert writer for one-time backfill rows."""

    def __init__(
        self,
        conn,
        *,
        batch_size: int = 500,
        dry_run: bool = False,
    ) -> None:
        self.conn = conn
        self.batch_size = max(1, int(batch_size))
        self.dry_run = bool(dry_run)
        self._buffer: List[BackfillRecord] = []
        self.stats = UpsertStats()

    def add_raw(self, raw_record: Dict[str, Any]) -> bool:
        record = normalize_record(raw_record)
        if record is None:
            self.stats.skipped_count += 1
            return False
        self._buffer.append(record)
        if len(self._buffer) >= self.batch_size:
            self.flush()
        return True

    def flush(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer
        self._buffer = []
        deduped_batch, duplicate_content_ids = _dedupe_records_last_wins(batch)
        duplicates_dropped = len(batch) - len(deduped_batch)
        if duplicates_dropped > 0:
            LOGGER.warning(
                "Dropped duplicate keys in backfill batch before upsert: "
                "duplicates_dropped=%s original_batch_size=%s deduped_batch_size=%s duplicate_content_ids=%s",
                duplicates_dropped,
                len(batch),
                len(deduped_batch),
                duplicate_content_ids[:5],
            )
        if self.dry_run:
            return

        rows = _build_upsert_rows(deduped_batch)
        if not rows:
            return

        cursor = get_cursor(self.conn)
        try:
            results = psycopg2.extras.execute_values(
                cursor,
                UPSERT_SQL,
                rows,
                template=None,
                page_size=min(len(rows), EXECUTE_VALUES_PAGE_SIZE),
                fetch=True,
            )
            inserted = sum(1 for row in results if row and bool(row[0]))
            updated = len(results) - inserted
            self.stats.inserted_count += inserted
            self.stats.updated_count += updated
            self.conn.commit()
        except Exception:
            self._buffer = batch + self._buffer
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        self.flush()
