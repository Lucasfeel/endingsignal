from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from services.novel_seed_catalog import (
    KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID,
    NAVER_SERIES_GENRE_SEED_DEFINITIONS,
)

CANONICAL_NOVEL_GENRE_GROUPS: Tuple[str, ...] = (
    "FANTASY",
    "HYEONPAN",
    "ROMANCE",
    "ROMANCE_FANTASY",
    "MYSTERY",
    "LIGHT_NOVEL",
    "WUXIA",
    "BL",
)

GENRE_GROUP_MAPPING: Dict[str, List[str]] = {
    "ALL": [],
    "FANTASY": [
        "\uD310\uD0C0\uC9C0",
        "\uD604\uD310",
        "\uD604\uB300\uD310\uD0C0\uC9C0",
        "fantasy",
        "modern fantasy",
        "urban fantasy",
    ],
    "HYEONPAN": [
        "\uD604\uD310",
        "hyeonpan",
    ],
    "ROMANCE": [
        "\uB85C\uB9E8\uC2A4",
        "romance",
    ],
    "ROMANCE_FANTASY": [
        "\uB85C\uD310",
        "\uB85C\uB9E8\uC2A4\uD310\uD0C0\uC9C0",
        "romance fantasy",
        "romance_fantasy",
    ],
    "MYSTERY": [
        "\uBBF8\uC2A4\uD130\uB9AC",
        "mystery",
    ],
    "LIGHT_NOVEL": [
        "\uB77C\uC774\uD2B8\uB178\uBCA8",
        "\uB77C\uB178\uBCA8",
        "light novel",
        "light_novel",
        "lightnovel",
    ],
    "WUXIA": [
        "\uBB34\uD611",
        "wuxia",
        "martial arts",
    ],
    "BL": [
        "bl",
        "\uBE44\uC5D8",
        "boys love",
        "boys' love",
    ],
}

GENRE_GROUP_ALIASES: Dict[str, Tuple[str, ...]] = {
    "ALL": ("all", "\uC804\uCCB4"),
    "FANTASY": ("fantasy", "\uD310\uD0C0\uC9C0", "\uD604\uD310", "\uD604\uB300\uD310\uD0C0\uC9C0"),
    "HYEONPAN": ("hyeonpan", "\uD604\uD310"),
    "ROMANCE": ("romance", "\uB85C\uB9E8\uC2A4"),
    "ROMANCE_FANTASY": (
        "romancefantasy",
        "romance_fantasy",
        "\uB85C\uD310",
        "\uB85C\uB9E8\uC2A4\uD310\uD0C0\uC9C0",
    ),
    "MYSTERY": ("mystery", "\uBBF8\uC2A4\uD130\uB9AC"),
    "LIGHT_NOVEL": (
        "lightnovel",
        "light_novel",
        "\uB77C\uC774\uD2B8\uB178\uBCA8",
        "\uB77C\uB178\uBCA8",
    ),
    "WUXIA": ("wuxia", "\uBB34\uD611"),
    "BL": ("bl", "\uBE44\uC5D8", "boyslove", "boys'love"),
}


def normalize_genre_token(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"[\s_\-/]+", "", value.strip().lower())


GENRE_GROUP_ALIAS_MAP: Dict[str, str] = {}
for _group, _aliases in GENRE_GROUP_ALIASES.items():
    for _alias in _aliases:
        _normalized = normalize_genre_token(_alias)
        if _normalized:
            GENRE_GROUP_ALIAS_MAP[_normalized] = _group


_NAVER_GENRE_CODE_TO_GROUP = {
    str(definition["genre_code"]): str(definition["key"]).upper()
    for definition in NAVER_SERIES_GENRE_SEED_DEFINITIONS
}
_KAKAOPAGE_GENRE_ID_TO_GROUP = {
    str(genre_id): GENRE_GROUP_ALIAS_MAP[normalize_genre_token(genre_value)]
    for genre_id, genre_value in KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID.items()
}
_ROOT_KEY_PREFIX_TO_GROUP = {
    "romancefantasy": "ROMANCE_FANTASY",
    "lightnovel": "LIGHT_NOVEL",
    "hyeonpan": "HYEONPAN",
    "mystery": "MYSTERY",
    "romance": "ROMANCE",
    "fantasy": "FANTASY",
    "wuxia": "WUXIA",
    "bl": "BL",
}


def _coerce_text_values(raw_value: Any) -> List[str]:
    if raw_value is None:
        return []

    if isinstance(raw_value, (list, tuple, set)):
        merged: List[str] = []
        for entry in raw_value:
            merged.extend(_coerce_text_values(entry))
        return merged

    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return []
        if "://" in stripped:
            return [stripped]
        try:
            parsed = json.loads(stripped)
        except Exception:
            parsed = None
        if parsed is not None and parsed is not raw_value:
            parsed_values = _coerce_text_values(parsed)
            if parsed_values:
                return parsed_values
        split_values = [part.strip() for part in re.split(r"[,/|>]+", stripped) if part.strip()]
        return split_values if split_values else [stripped]

    return []


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _resolve_source_specific_genre_group(raw_value: Any) -> str | None:
    if not isinstance(raw_value, str):
        return None

    stripped = raw_value.strip()
    if not stripped:
        return None

    lowered = stripped.lower()
    normalized = normalize_genre_token(stripped)

    for genre_code, group in _NAVER_GENRE_CODE_TO_GROUP.items():
        if f"genrecode={genre_code}" in lowered or f"genrecode{genre_code}" in normalized:
            return group

    kakao_match = re.search(r"/landing/genre/11/(\d+)", lowered)
    if kakao_match:
        return _KAKAOPAGE_GENRE_ID_TO_GROUP.get(kakao_match.group(1))

    kakao_normalized_match = re.search(r"landinggenre11(\d+)", normalized)
    if kakao_normalized_match:
        return _KAKAOPAGE_GENRE_ID_TO_GROUP.get(kakao_normalized_match.group(1))

    for prefix, group in _ROOT_KEY_PREFIX_TO_GROUP.items():
        if normalized == prefix or normalized.startswith(prefix):
            return group

    return None


def resolve_genre_group(raw_value: Any) -> str:
    source_specific = _resolve_source_specific_genre_group(raw_value)
    if source_specific:
        return source_specific

    normalized = normalize_genre_token(raw_value)
    if not normalized:
        return "ALL"
    return GENRE_GROUP_ALIAS_MAP.get(normalized, "ALL")


def resolve_genre_groups(raw_values: Any = None) -> List[str]:
    entries = _coerce_text_values(raw_values)
    if not entries:
        return ["ALL"]

    resolved: List[str] = []
    seen = set()
    for entry in entries:
        group = resolve_genre_group(entry)
        if group == "ALL":
            normalized = normalize_genre_token(entry)
            if normalized in {"all", normalize_genre_token("\uC804\uCCB4")}:
                return ["ALL"]
            continue
        if group in seen:
            continue
        seen.add(group)
        resolved.append(group)

    return resolved if resolved else ["ALL"]


def select_compat_genre_group(genre_groups: Sequence[str]) -> str:
    groups = list(genre_groups or [])
    if not groups:
        return "ALL"
    return groups[0] if len(groups) == 1 else "ALL"


def _iter_novel_genre_candidates(meta: Mapping[str, Any]) -> Iterable[str]:
    safe_meta = dict(meta) if isinstance(meta, Mapping) else {}
    attributes = _safe_dict(safe_meta.get("attributes"))
    common = _safe_dict(safe_meta.get("common"))

    candidates = [
        attributes.get("genre"),
        attributes.get("genres"),
        attributes.get("crawl_roots"),
        attributes.get("subgenres"),
        attributes.get("sub_genres"),
        common.get("genres"),
        common.get("genre"),
        safe_meta.get("genres"),
        safe_meta.get("genre"),
    ]

    for candidate in candidates:
        for value in _coerce_text_values(candidate):
            if value:
                yield value


def extract_novel_genre_groups_from_meta(meta: Mapping[str, Any] | Any) -> List[str]:
    resolved: List[str] = []
    seen = set()
    for candidate in _iter_novel_genre_candidates(meta if isinstance(meta, Mapping) else {}):
        group = resolve_genre_group(candidate)
        if group == "ALL" or group in seen:
            continue
        seen.add(group)
        resolved.append(group)
    return resolved


def resolve_novel_genre_columns(meta: Mapping[str, Any] | Any) -> Tuple[str | None, List[str]]:
    groups = extract_novel_genre_groups_from_meta(meta)
    if not groups:
        return None, []
    return groups[0], groups


def expand_query_genre_groups(genre_groups: Sequence[str]) -> List[str]:
    requested = [
        str(group).strip()
        for group in (genre_groups or [])
        if str(group).strip() and str(group).strip() != "ALL"
    ]
    return requested
