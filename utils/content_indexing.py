"""Helpers for searchable content documents and canonical row comparisons."""

from __future__ import annotations

import json
from typing import Any, Iterable, List

from utils.text import normalize_search_text


def _clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _iter_text_values(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        text = _clean_text(value)
        if text:
            yield text
        return

    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _iter_text_values(item)


def _dedupe_preserve_order(values: Iterable[str]) -> List[str]:
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


def canonicalize_json(value: Any) -> str:
    if value is None:
        safe_value = {}
    else:
        safe_value = value
    return json.dumps(
        safe_value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def build_search_document(
    *,
    title: Any,
    normalized_title: Any = "",
    normalized_authors: Any = "",
    meta: Any = None,
) -> str:
    parts: List[str] = []
    title_text = _clean_text(title)
    if title_text:
        parts.append(title_text)

    normalized_title_text = _clean_text(normalized_title)
    if normalized_title_text:
        parts.append(normalized_title_text)

    normalized_authors_text = _clean_text(normalized_authors)
    if normalized_authors_text:
        parts.append(normalized_authors_text)

    common = {}
    if isinstance(meta, dict):
        common = meta.get("common") if isinstance(meta.get("common"), dict) else {}

    alias_tokens = []
    alias_tokens.extend(_iter_text_values(common.get("alt_title")))
    alias_tokens.extend(_iter_text_values(common.get("title_alias")))

    for alias in _dedupe_preserve_order(alias_tokens):
        parts.append(alias)
        normalized_alias = normalize_search_text(alias)
        if normalized_alias:
            parts.append(normalized_alias)

    deduped_parts = _dedupe_preserve_order(parts)
    return " ".join(deduped_parts)
