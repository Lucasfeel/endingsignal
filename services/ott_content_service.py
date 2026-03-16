from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import psycopg2.extras

from database import get_cursor
from utils.content_indexing import build_search_document, canonicalize_json
from utils.text import normalize_search_text
from utils.time import now_kst_naive, parse_iso_naive_kst

OTT_CANONICAL_SOURCE = "ott_canonical"
OTT_PLATFORM_SOURCES = (
    "coupangplay",
    "disney_plus",
    "netflix",
    "tving",
    "wavve",
)
OTT_PLATFORM_SOURCE_SET = set(OTT_PLATFORM_SOURCES)

OTT_GENRE_DRAMA = "drama"
OTT_GENRE_ANIME = "anime"
OTT_GENRE_VARIETY = "variety"
OTT_GENRE_DOCU = "docu"
OTT_GENRE_ETC = "etc"
OTT_MAX_CAST_MEMBERS = 4
OTT_ALLOWED_GENRES = {
    OTT_GENRE_DRAMA,
    OTT_GENRE_ANIME,
    OTT_GENRE_VARIETY,
    OTT_GENRE_DOCU,
    OTT_GENRE_ETC,
}
OTT_GENRE_PRIORITY = (
    OTT_GENRE_ANIME,
    OTT_GENRE_DOCU,
    OTT_GENRE_VARIETY,
    OTT_GENRE_DRAMA,
    OTT_GENRE_ETC,
)

_OTT_GENRE_TOKEN_RE = re.compile(r"[\s_\-/]+")
_OTT_GENRE_PATTERNS = {
    OTT_GENRE_ANIME: re.compile(
        "|".join(
            [
                r"\b(?:anime|animation|animated)\b",
                r"애니(?:메이션)?",
                r"만화",
            ]
        ),
        re.I,
    ),
    OTT_GENRE_DOCU: re.compile(
        "|".join(
            [
                r"\b(?:documentary|docuseries|docu-series|docu)\b",
                r"다큐(?:멘터리)?",
                r"휴먼다큐",
                r"실화다큐",
            ]
        ),
        re.I,
    ),
    OTT_GENRE_VARIETY: re.compile(
        "|".join(
            [
                r"\b(?:variety|reality|competition|survival|dating|talk\s*show|game\s*show|observational)\b",
                r"예능",
                r"버라이어티",
                r"리얼리티",
                r"토크쇼",
                r"게임쇼",
                r"서바이벌",
                r"연애\s*리얼리티",
                r"관찰",
                r"여행\s*예능",
            ]
        ),
        re.I,
    ),
    OTT_GENRE_DRAMA: re.compile(
        "|".join(
            [
                r"\b(?:drama|scripted|series|thriller|crime|mystery|romance|legal|medical|fantasy|comedy|family|action|suspense|sitcom)\b",
                r"드라마",
                r"시리즈",
                r"스릴러",
                r"범죄",
                r"미스터리",
                r"로맨스",
                r"법정",
                r"의학",
                r"판타지",
                r"코미디",
                r"블랙\s*코미디",
                r"가족",
                r"액션",
                r"서스펜스",
            ]
        ),
        re.I,
    ),
}

STATUS_ONGOING = "연재중"
STATUS_COMPLETED = "완결"

_OTT_GENRE_PATTERNS[OTT_GENRE_ANIME] = re.compile(
    "|".join(
        [
            r"\b(?:anime|animation|animated)\b",
            r"\uC560\uB2C8\uBA54\uC774\uC158",
            r"\uB9CC\uD654",
        ]
    ),
    re.I,
)
_OTT_GENRE_PATTERNS[OTT_GENRE_DOCU] = re.compile(
    "|".join(
        [
            r"\b(?:documentary|docuseries|docu-series|docu)\b",
            r"\uB2E4\uD050(?:\uBA58\uD130\uB9AC)?",
            r"\uD734\uBA3C\uB2E4\uD050",
            r"\uC2E4\uD654\uB2E4\uD050",
        ]
    ),
    re.I,
)
_OTT_GENRE_PATTERNS[OTT_GENRE_VARIETY] = re.compile(
    "|".join(
        [
            r"\b(?:variety|reality|competition|survival|dating|talk\s*show|game\s*show|observational)\b",
            r"\uC608\uB2A5",
            r"\uBC84\uB77C\uC774\uC5B4\uD2F0",
            r"\uB9AC\uC5BC\uB9AC\uD2F0",
            r"\uD1A0\uD06C\uC1FC",
            r"\uAC8C\uC784\uC1FC",
            r"\uC11C\uBC14\uC774\uBC8C",
            r"\uC5F0\uC560\s*\uB9AC\uC5BC\uB9AC\uD2F0",
            r"\uAD00\uCC30",
            r"\uC5EC\uD589\s*\uC608\uB2A5",
        ]
    ),
    re.I,
)
_OTT_GENRE_PATTERNS[OTT_GENRE_DRAMA] = re.compile(
    "|".join(
        [
            r"\b(?:drama|scripted|series|thriller|crime|mystery|romance|legal|medical|fantasy|comedy|family|action|suspense|sitcom)\b",
            r"\uB4DC\uB77C\uB9C8",
            r"\uC2DC\uB9AC\uC988",
            r"\uC2A4\uB9B4\uB7EC",
            r"\uBC94\uC8C4",
            r"\uBBF8\uC2A4\uD130\uB9AC",
            r"\uB85C\uB9E8\uC2A4",
            r"\uBC95\uC815",
            r"\uC758\uD559",
            r"\uD310\uD0C0\uC9C0",
            r"\uCF54\uBBF8\uB514",
            r"\uBE14\uB799\s*\uCF54\uBBF8\uB514",
            r"\uAC00\uC871",
            r"\uC561\uC158",
            r"\uC11C\uC2A4\uD39C\uC2A4",
        ]
    ),
    re.I,
)

RELEASE_END_STATUS_UNKNOWN = "unknown"
RELEASE_END_STATUS_SCHEDULED = "scheduled"
RELEASE_END_STATUS_CONFIRMED = "confirmed"
RELEASE_END_STATUS_VALUES = {
    RELEASE_END_STATUS_UNKNOWN,
    RELEASE_END_STATUS_SCHEDULED,
    RELEASE_END_STATUS_CONFIRMED,
}

RESOLUTION_TRACKING = "tracking"
RESOLUTION_CONFLICT = "conflict"
RESOLUTION_RESOLVED = "resolved"

UPSERT_CANONICAL_SQL = """
INSERT INTO contents (
    content_id,
    source,
    content_type,
    title,
    normalized_title,
    normalized_authors,
    status,
    meta,
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
    search_document = EXCLUDED.search_document,
    updated_at = NOW()
RETURNING content_id, (xmax = 0) AS inserted
"""

UPSERT_LINK_SQL = """
INSERT INTO content_platform_links (
    canonical_content_id,
    platform_source,
    platform_content_id,
    platform_url,
    availability_status,
    verified_at,
    is_primary
)
VALUES %s
ON CONFLICT (canonical_content_id, platform_source)
DO UPDATE SET
    platform_content_id = EXCLUDED.platform_content_id,
    platform_url = EXCLUDED.platform_url,
    availability_status = EXCLUDED.availability_status,
    verified_at = EXCLUDED.verified_at,
    is_primary = EXCLUDED.is_primary,
    updated_at = NOW()
"""

UPSERT_WATCHLIST_SQL = """
INSERT INTO ott_schedule_watchlist (
    canonical_content_id,
    platform_source,
    release_start_at,
    release_end_at,
    release_end_status,
    last_checked_at,
    next_check_at,
    check_fail_count,
    resolution_state
)
VALUES %s
ON CONFLICT (canonical_content_id, platform_source)
DO UPDATE SET
    release_start_at = EXCLUDED.release_start_at,
    release_end_at = EXCLUDED.release_end_at,
    release_end_status = EXCLUDED.release_end_status,
    last_checked_at = EXCLUDED.last_checked_at,
    next_check_at = EXCLUDED.next_check_at,
    check_fail_count = EXCLUDED.check_fail_count,
    resolution_state = EXCLUDED.resolution_state,
    updated_at = NOW()
"""


def is_ott_platform_source(source_name: Optional[str]) -> bool:
    return str(source_name or "").strip() in OTT_PLATFORM_SOURCE_SET


def _clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _coerce_text_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = _clean_text(value)
        return [cleaned] if cleaned else []
    if isinstance(value, (list, tuple, set)):
        merged: List[str] = []
        for item in value:
            merged.extend(_coerce_text_list(item))
        return merged
    return []


def _dedupe_texts(values: Iterable[str]) -> List[str]:
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


def _limit_ott_people(values: Iterable[str], max_items: int = OTT_MAX_CAST_MEMBERS) -> List[str]:
    if max_items <= 0:
        return []
    return _dedupe_texts(values)[:max_items]


def _normalize_ott_genre_token(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return _OTT_GENRE_TOKEN_RE.sub("", value.strip().lower())


def infer_ott_genre_bucket(
    *values: Any,
    platform_source: Optional[str] = None,
    default: str = OTT_GENRE_ETC,
) -> str:
    safe_source = str(platform_source or "").strip().lower()
    if safe_source == "laftel":
        return OTT_GENRE_ANIME

    merged = " ".join(_dedupe_texts(_coerce_text_list(values)))
    if not merged:
        return default

    for genre_name in OTT_GENRE_PRIORITY:
        if genre_name == OTT_GENRE_ETC:
            continue
        pattern = _OTT_GENRE_PATTERNS.get(genre_name)
        if pattern is not None and pattern.search(merged):
            return genre_name
    return default


def normalize_ott_genres(
    *values: Any,
    platform_source: Optional[str] = None,
    default: str = OTT_GENRE_ETC,
) -> List[str]:
    safe_source = str(platform_source or "").strip().lower()
    if safe_source == "laftel":
        return [OTT_GENRE_ANIME]

    buckets: List[str] = []
    seen = set()
    flattened = _dedupe_texts(_coerce_text_list(values))
    for token in flattened:
        normalized = _normalize_ott_genre_token(token)
        if not normalized:
            continue
        direct_bucket = normalized if normalized in OTT_ALLOWED_GENRES else ""
        if not direct_bucket:
            direct_bucket = infer_ott_genre_bucket(token, platform_source=safe_source, default="")
        if not direct_bucket or direct_bucket in seen:
            continue
        seen.add(direct_bucket)
        buckets.append(direct_bucket)

    if not buckets:
        inferred = infer_ott_genre_bucket(flattened, platform_source=safe_source, default=default)
        if inferred:
            buckets = [inferred]

    for genre_name in OTT_GENRE_PRIORITY:
        if genre_name in buckets:
            return [genre_name]
    return [default] if default else []


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return parse_iso_naive_kst(value)
    return None


def _isoformat(value: Any) -> Optional[str]:
    resolved = _coerce_datetime(value)
    if resolved is None:
        return None
    return resolved.isoformat()


def _canonical_year(value: Any) -> str:
    resolved = _coerce_datetime(value)
    if resolved is not None:
        return str(resolved.year)
    if isinstance(value, int):
        return str(value)
    text = _clean_text(value)
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    return "unknown"


def build_canonical_content_id(
    *,
    title: str,
    release_start_at: Any = None,
    representative_year: Any = None,
) -> str:
    normalized_title = normalize_search_text(title)
    year = _canonical_year(representative_year or release_start_at)
    digest = hashlib.sha1(
        f"{normalized_title}|{year}|series".encode("utf-8")
    ).hexdigest()[:12]
    safe_title = normalized_title[:48] or "untitled"
    return f"ott_series:{year}:{safe_title}:{digest}"


def build_canonical_ott_entry(
    *,
    platform_source: str,
    title: str,
    platform_content_id: Any,
    platform_url: str,
    release_start_at: Any = None,
    release_end_at: Any = None,
    release_end_status: str = RELEASE_END_STATUS_UNKNOWN,
    cast: Optional[Sequence[str]] = None,
    genres: Optional[Sequence[str]] = None,
    thumbnail_url: Optional[str] = None,
    alt_title: Optional[str] = None,
    title_alias: Optional[Sequence[str]] = None,
    upcoming: Optional[bool] = None,
    availability_status: str = "available",
    description: Optional[str] = None,
    representative_year: Any = None,
    resolution_state: str = RESOLUTION_TRACKING,
    raw_schedule_note: Optional[str] = None,
    episode_hint: Optional[str] = None,
) -> Dict[str, Any]:
    safe_title = _clean_text(title)
    if not safe_title:
        raise ValueError("title is required")

    safe_platform_source = str(platform_source or "").strip()
    if safe_platform_source not in OTT_PLATFORM_SOURCE_SET:
        raise ValueError(f"unsupported OTT platform source: {platform_source}")

    canonical_content_id = build_canonical_content_id(
        title=safe_title,
        release_start_at=release_start_at,
        representative_year=representative_year,
    )
    normalized_end_status = str(release_end_status or RELEASE_END_STATUS_UNKNOWN).strip().lower()
    if normalized_end_status not in RELEASE_END_STATUS_VALUES:
        normalized_end_status = RELEASE_END_STATUS_UNKNOWN

    cast_list = _dedupe_texts(_coerce_text_list(cast))
    alias_list = _dedupe_texts(_coerce_text_list(title_alias))
    genres_list = _dedupe_texts(_coerce_text_list(genres))

    start_dt = _coerce_datetime(release_start_at)
    end_dt = _coerce_datetime(release_end_at)
    if upcoming is None:
        upcoming = bool(start_dt and start_dt > now_kst_naive())

    return {
        "canonical_content_id": canonical_content_id,
        "title": safe_title,
        "platform_source": safe_platform_source,
        "platform_content_id": str(platform_content_id or "").strip() or canonical_content_id,
        "platform_url": _clean_text(platform_url),
        "thumbnail_url": _clean_text(thumbnail_url),
        "alt_title": _clean_text(alt_title),
        "title_alias": alias_list,
        "cast": cast_list,
        "genres": genres_list,
        "release_start_at": start_dt,
        "release_end_at": end_dt,
        "release_end_status": normalized_end_status,
        "upcoming": bool(upcoming),
        "availability_status": _clean_text(availability_status) or "available",
        "description": _clean_text(description),
        "resolution_state": _clean_text(resolution_state) or RESOLUTION_TRACKING,
        "raw_schedule_note": _clean_text(raw_schedule_note),
        "episode_hint": _clean_text(episode_hint),
        "status": STATUS_COMPLETED if normalized_end_status == RELEASE_END_STATUS_CONFIRMED else STATUS_ONGOING,
    }


def _normalize_platforms_meta(meta: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    safe_meta = dict(meta) if isinstance(meta, Mapping) else {}
    ott_meta = safe_meta.get("ott")
    ott_meta = ott_meta if isinstance(ott_meta, dict) else {}
    raw_platforms = ott_meta.get("platforms")
    platforms: Dict[str, Dict[str, Any]] = {}
    if not isinstance(raw_platforms, list):
        return platforms
    for raw_item in raw_platforms:
        if not isinstance(raw_item, dict):
            continue
        source_name = str(raw_item.get("source") or "").strip()
        if not source_name:
            continue
        platforms[source_name] = dict(raw_item)
    return platforms


def choose_display_source(
    meta: Mapping[str, Any],
    *,
    requested_sources: Optional[Sequence[str]] = None,
) -> str:
    safe_meta = dict(meta) if isinstance(meta, Mapping) else {}
    common = safe_meta.get("common")
    common = common if isinstance(common, dict) else {}
    requested = [str(item).strip() for item in (requested_sources or []) if str(item).strip()]
    platforms = _normalize_platforms_meta(safe_meta)
    if requested:
        for source_name in requested:
            if source_name in platforms:
                return source_name
    primary = str(common.get("primary_source") or "").strip()
    if primary and primary in platforms:
        return primary
    if platforms:
        return sorted(platforms.keys())[0]
    return str(common.get("primary_source") or OTT_CANONICAL_SOURCE).strip() or OTT_CANONICAL_SOURCE


def resolve_display_meta(
    meta: Mapping[str, Any],
    *,
    requested_sources: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, Any], str]:
    safe_meta = deepcopy(dict(meta) if isinstance(meta, Mapping) else {})
    common = safe_meta.get("common")
    common = dict(common) if isinstance(common, Mapping) else {}
    safe_meta["common"] = common
    ott_meta = safe_meta.get("ott")
    ott_meta = dict(ott_meta) if isinstance(ott_meta, Mapping) else {}
    safe_meta["ott"] = ott_meta

    ott_meta["cast"] = _limit_ott_people(_coerce_text_list(ott_meta.get("cast")))
    common["authors"] = _limit_ott_people(ott_meta["cast"] or _coerce_text_list(common.get("authors")))

    chosen_source = choose_display_source(safe_meta, requested_sources=requested_sources)
    platforms = _normalize_platforms_meta(safe_meta)
    chosen_platform = dict(platforms.get(chosen_source) or {})

    if chosen_platform.get("content_url"):
        common["content_url"] = chosen_platform["content_url"]
        common["url"] = chosen_platform["content_url"]
    if chosen_platform.get("thumbnail_url"):
        common["thumbnail_url"] = chosen_platform["thumbnail_url"]
    if chosen_platform.get("source"):
        common["primary_source"] = chosen_platform["source"]

    genre_platform_source = chosen_source
    if genre_platform_source == OTT_CANONICAL_SOURCE:
        for requested_source in requested_sources or []:
            cleaned_requested_source = str(requested_source or "").strip()
            if cleaned_requested_source:
                genre_platform_source = cleaned_requested_source
                break
    if genre_platform_source == OTT_CANONICAL_SOURCE:
        genre_platform_source = str(common.get("primary_source") or "").strip() or OTT_CANONICAL_SOURCE

    normalized_genres = normalize_ott_genres(
        safe_meta.get("genres"),
        safe_meta.get("genre"),
        common.get("genres"),
        common.get("genre"),
        ott_meta.get("genres"),
        ott_meta.get("genre"),
        ott_meta.get("description"),
        ott_meta.get("raw_schedule_note"),
        ott_meta.get("episode_hint"),
        platform_source=genre_platform_source,
    )
    resolved_genre = normalized_genres[0] if normalized_genres else OTT_GENRE_ETC
    common["genres"] = normalized_genres
    common["genre"] = resolved_genre
    safe_meta["genres"] = normalized_genres
    safe_meta["genre"] = resolved_genre
    ott_meta["genres"] = normalized_genres
    ott_meta["genre"] = resolved_genre

    attributes = safe_meta.get("attributes")
    attributes = dict(attributes) if isinstance(attributes, Mapping) else {}
    attributes["genres"] = normalized_genres
    attributes["genre"] = resolved_genre
    safe_meta["attributes"] = attributes

    ott_meta["display_source"] = chosen_source
    ott_meta["platforms"] = sorted(platforms.values(), key=lambda item: str(item.get("source") or ""))
    return safe_meta, chosen_source


def _merge_text_field(existing: Any, incoming: Any) -> Optional[str]:
    incoming_text = _clean_text(incoming)
    if incoming_text:
        return incoming_text
    existing_text = _clean_text(existing)
    return existing_text or None


def _merge_datetime_field(*values: Any, choose_min: bool = False) -> Optional[datetime]:
    resolved = [_coerce_datetime(value) for value in values]
    filtered = [value for value in resolved if value is not None]
    if not filtered:
        return None
    return min(filtered) if choose_min else max(filtered)


def _distinct_datetimes(*values: Any) -> List[datetime]:
    distinct: Dict[str, datetime] = {}
    for value in values:
        resolved = _coerce_datetime(value)
        if resolved is None:
            continue
        distinct[resolved.isoformat()] = resolved
    return [distinct[key] for key in sorted(distinct.keys())]


def _compute_schedule_state(
    *,
    existing_meta: Mapping[str, Any],
    entry: Mapping[str, Any],
    now_value: datetime,
) -> Tuple[Optional[datetime], str, str]:
    safe_existing = dict(existing_meta) if isinstance(existing_meta, Mapping) else {}
    existing_ott = safe_existing.get("ott")
    existing_ott = existing_ott if isinstance(existing_ott, dict) else {}

    entry_end_at = entry.get("release_end_at")
    entry_status = str(entry.get("release_end_status") or "").strip().lower()
    entry_end_dt = _coerce_datetime(entry_end_at)
    if entry_status in {RELEASE_END_STATUS_SCHEDULED, RELEASE_END_STATUS_CONFIRMED} and entry_end_dt is not None:
        candidate_dates = _distinct_datetimes(entry_end_dt)
    elif (
        entry_status == RELEASE_END_STATUS_UNKNOWN
        and entry_end_dt is None
    ):
        candidate_dates = _distinct_datetimes(entry_end_at)
    else:
        candidate_dates = _distinct_datetimes(
            existing_ott.get("release_end_at"),
            existing_ott.get("completed_at"),
            entry_end_at,
        )
    resolution_state = str(
        entry.get("resolution_state")
        or existing_ott.get("resolution_state")
        or RESOLUTION_TRACKING
    ).strip() or RESOLUTION_TRACKING
    if len(candidate_dates) > 1:
        resolution_state = RESOLUTION_CONFLICT

    existing_status = str(existing_ott.get("release_end_status") or "").strip().lower()

    if resolution_state == RESOLUTION_CONFLICT:
        release_end_status = RELEASE_END_STATUS_UNKNOWN
    elif entry_status == RELEASE_END_STATUS_CONFIRMED:
        release_end_status = RELEASE_END_STATUS_CONFIRMED
    elif entry_status == RELEASE_END_STATUS_SCHEDULED:
        release_end_status = RELEASE_END_STATUS_SCHEDULED
    elif candidate_dates:
        release_end_status = RELEASE_END_STATUS_SCHEDULED
    elif entry_status == RELEASE_END_STATUS_UNKNOWN:
        release_end_status = RELEASE_END_STATUS_UNKNOWN
    elif existing_status == RELEASE_END_STATUS_CONFIRMED:
        release_end_status = RELEASE_END_STATUS_CONFIRMED
    elif existing_status == RELEASE_END_STATUS_SCHEDULED:
        release_end_status = RELEASE_END_STATUS_SCHEDULED
    else:
        release_end_status = RELEASE_END_STATUS_UNKNOWN

    release_end_at = candidate_dates[0] if candidate_dates else None
    if release_end_status == RELEASE_END_STATUS_CONFIRMED and release_end_at is None:
        release_end_at = _coerce_datetime(existing_ott.get("completed_at")) or now_value

    return release_end_at, release_end_status, resolution_state


def _compute_watchlist_state(
    *,
    existing_row: Optional[Mapping[str, Any]],
    release_start_at: Optional[datetime],
    release_end_at: Optional[datetime],
    release_end_status: str,
    resolution_state: str,
    status: str,
    now_value: datetime,
) -> Tuple[Optional[datetime], int, str]:
    existing = dict(existing_row) if isinstance(existing_row, Mapping) else {}
    previous_fail_count = int(existing.get("check_fail_count") or 0)
    previous_end_status = str(existing.get("release_end_status") or "").strip().lower()
    previous_end_at = _coerce_datetime(existing.get("release_end_at"))

    if resolution_state == RESOLUTION_CONFLICT:
        next_check_at = now_value + timedelta(days=1)
        if previous_end_status == release_end_status and previous_end_at == release_end_at:
            return next_check_at, previous_fail_count + 1, RESOLUTION_CONFLICT
        return next_check_at, 0, RESOLUTION_CONFLICT

    if status == STATUS_COMPLETED and release_end_status == RELEASE_END_STATUS_CONFIRMED:
        return None, 0, RESOLUTION_RESOLVED

    start_dt = _coerce_datetime(release_start_at)
    if (
        release_end_status != RELEASE_END_STATUS_CONFIRMED
        and start_dt is not None
        and now_value < start_dt + timedelta(days=1)
    ):
        next_check_at = start_dt + timedelta(days=1)
        fail_count = previous_fail_count if previous_end_status == release_end_status and previous_end_at == release_end_at else 0
        return next_check_at, fail_count, RESOLUTION_TRACKING

    if release_end_status == RELEASE_END_STATUS_SCHEDULED and release_end_at is not None:
        if release_end_at - now_value > timedelta(days=7):
            next_check_at = release_end_at - timedelta(days=7)
        else:
            next_check_at = now_value + timedelta(days=1)
    else:
        next_check_at = now_value + timedelta(days=1)

    if previous_end_status == release_end_status and previous_end_at == release_end_at:
        fail_count = previous_fail_count + 1 if release_end_status != RELEASE_END_STATUS_CONFIRMED else 0
    else:
        fail_count = 0

    return next_check_at, fail_count, RESOLUTION_TRACKING


def _build_canonical_meta(
    *,
    existing_meta: Optional[Mapping[str, Any]],
    entry: Mapping[str, Any],
    platform_source: str,
    now_value: datetime,
) -> Tuple[Dict[str, Any], str]:
    safe_existing_meta = deepcopy(dict(existing_meta) if isinstance(existing_meta, Mapping) else {})
    existing_common = safe_existing_meta.get("common")
    existing_common = dict(existing_common) if isinstance(existing_common, Mapping) else {}
    existing_ott = safe_existing_meta.get("ott")
    existing_ott = dict(existing_ott) if isinstance(existing_ott, Mapping) else {}
    existing_attrs = safe_existing_meta.get("attributes")
    existing_attrs = dict(existing_attrs) if isinstance(existing_attrs, Mapping) else {}

    incoming_cast = _limit_ott_people(_coerce_text_list(entry.get("cast")))
    clear_cast = bool(entry.get("_clear_cast"))
    existing_cast = _limit_ott_people(
        _coerce_text_list(existing_common.get("authors"))
        + _coerce_text_list(existing_ott.get("cast"))
    )
    cast = incoming_cast if incoming_cast or clear_cast else existing_cast

    incoming_genre_inputs = _dedupe_texts(
        _coerce_text_list(entry.get("genres"))
        + _coerce_text_list(entry.get("genre"))
    )
    incoming_genres = normalize_ott_genres(incoming_genre_inputs, platform_source=platform_source)
    if incoming_genres and incoming_genres[0] != OTT_GENRE_ETC:
        genres = incoming_genres
    else:
        raw_genres = _dedupe_texts(
            incoming_genre_inputs
            + _coerce_text_list(entry.get("description"))
            + _coerce_text_list(entry.get("raw_schedule_note"))
            + _coerce_text_list(entry.get("episode_hint"))
            + _coerce_text_list(existing_attrs.get("genres"))
            + _coerce_text_list(existing_attrs.get("genre"))
            + _coerce_text_list(existing_common.get("genres"))
            + _coerce_text_list(existing_common.get("genre"))
            + _coerce_text_list(existing_ott.get("genres"))
            + _coerce_text_list(existing_ott.get("genre"))
        )
        genres = normalize_ott_genres(raw_genres, platform_source=platform_source)
    resolved_genre = genres[0] if genres else OTT_GENRE_ETC
    aliases = _dedupe_texts(
        _coerce_text_list(existing_common.get("title_alias"))
        + _coerce_text_list(existing_common.get("alt_title"))
        + _coerce_text_list(entry.get("title_alias"))
        + _coerce_text_list(entry.get("alt_title"))
    )

    existing_platforms = _normalize_platforms_meta(safe_existing_meta)
    current_platform = {
        "source": platform_source,
        "platform_content_id": str(entry.get("platform_content_id") or "").strip(),
        "content_url": _clean_text(entry.get("platform_url") or entry.get("content_url")),
        "thumbnail_url": _clean_text(entry.get("thumbnail_url")),
        "availability_status": _clean_text(entry.get("availability_status")) or "available",
        "release_start_at": _isoformat(entry.get("release_start_at")),
        "release_end_at": _isoformat(entry.get("release_end_at")),
        "release_end_status": str(entry.get("release_end_status") or RELEASE_END_STATUS_UNKNOWN),
        "upcoming": bool(entry.get("upcoming")),
    }
    existing_platforms[platform_source] = {
        **existing_platforms.get(platform_source, {}),
        **{key: value for key, value in current_platform.items() if value not in (None, "", [])},
    }

    platform_release_starts = [
        _coerce_datetime(platform.get("release_start_at"))
        for platform in existing_platforms.values()
    ]
    platform_release_starts = [value for value in platform_release_starts if value is not None]
    release_start_at = (
        min(platform_release_starts)
        if platform_release_starts
        else _merge_datetime_field(
            existing_ott.get("release_start_at"),
            entry.get("release_start_at"),
            choose_min=True,
        )
    )
    release_end_at, release_end_status, resolution_state = _compute_schedule_state(
        existing_meta=safe_existing_meta,
        entry=entry,
        now_value=now_value,
    )

    primary_source = str(existing_common.get("primary_source") or "").strip()
    if not primary_source or primary_source not in existing_platforms:
        primary_source = platform_source

    status = STATUS_COMPLETED if release_end_status == RELEASE_END_STATUS_CONFIRMED else STATUS_ONGOING
    upcoming = bool(entry.get("upcoming"))
    if release_start_at is not None and release_start_at > now_value:
        upcoming = True

    common = {
        "authors": cast,
        "content_url": _merge_text_field(
            existing_platforms.get(primary_source, {}).get("content_url"),
            current_platform.get("content_url") if primary_source == platform_source else None,
        )
        or _merge_text_field(existing_common.get("content_url"), current_platform.get("content_url")),
        "url": _merge_text_field(
            existing_platforms.get(primary_source, {}).get("content_url"),
            current_platform.get("content_url") if primary_source == platform_source else None,
        )
        or _merge_text_field(existing_common.get("url"), current_platform.get("content_url")),
        "thumbnail_url": _merge_text_field(
            existing_platforms.get(primary_source, {}).get("thumbnail_url"),
            current_platform.get("thumbnail_url") if primary_source == platform_source else None,
        )
        or _merge_text_field(existing_common.get("thumbnail_url"), current_platform.get("thumbnail_url")),
        "alt_title": aliases[0] if aliases else "",
        "title_alias": aliases,
        "genres": genres,
        "genre": resolved_genre,
        "primary_source": primary_source,
    }

    attributes = {
        "source": "ott",
        "genres": genres,
        "genre": resolved_genre,
        "platforms": sorted(existing_platforms.keys()),
    }

    ott_meta = {
        "platforms": sorted(existing_platforms.values(), key=lambda item: str(item.get("source") or "")),
        "cast": cast,
        "genres": genres,
        "genre": resolved_genre,
        "release_start_at": _isoformat(release_start_at),
        "release_end_at": _isoformat(release_end_at),
        "release_end_status": release_end_status,
        "needs_end_date_verification": status != STATUS_COMPLETED,
        "upcoming": upcoming,
        "resolution_state": resolution_state,
        "completed_at": _isoformat(release_end_at) if status == STATUS_COMPLETED else None,
        "description": _merge_text_field(existing_ott.get("description"), entry.get("description")),
        "raw_schedule_note": _merge_text_field(existing_ott.get("raw_schedule_note"), entry.get("raw_schedule_note")),
        "episode_hint": _merge_text_field(existing_ott.get("episode_hint"), entry.get("episode_hint")),
    }

    return {
        "common": common,
        "attributes": attributes,
        "genres": genres,
        "genre": resolved_genre,
        "ott": ott_meta,
    }, status


def load_ott_source_snapshot(conn, platform_source: str) -> Dict[str, Any]:
    safe_source = str(platform_source or "").strip()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT
                c.content_id,
                c.content_type,
                c.title,
                c.normalized_title,
                c.normalized_authors,
                c.status,
                c.meta,
                c.search_document
            FROM contents c
            JOIN content_platform_links l
              ON l.canonical_content_id = c.content_id
            WHERE c.source = %s
              AND c.content_type = 'ott'
              AND l.platform_source = %s
            ORDER BY c.content_id ASC
            """,
            (OTT_CANONICAL_SOURCE, safe_source),
        )
        existing_rows = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                canonical_content_id,
                platform_source,
                platform_content_id,
                platform_url,
                availability_status,
                verified_at,
                is_primary
            FROM content_platform_links
            WHERE platform_source = %s
            ORDER BY canonical_content_id ASC
            """,
            (safe_source,),
        )
        platform_links = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
                canonical_content_id,
                platform_source,
                release_start_at,
                release_end_at,
                release_end_status,
                last_checked_at,
                next_check_at,
                check_fail_count,
                resolution_state
            FROM ott_schedule_watchlist
            WHERE platform_source = %s
            ORDER BY canonical_content_id ASC
            """,
            (safe_source,),
        )
        watchlist_rows = [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()

    return {
        "source_name": safe_source,
        "existing_rows": existing_rows,
        "override_rows": [],
        "platform_links": platform_links,
        "watchlist_rows": watchlist_rows,
    }


def _resolve_entry_platform_content_id(fallback_key: str, entry: Mapping[str, Any]) -> str:
    return str(entry.get("platform_content_id") or fallback_key or "").strip()


def _resolve_entry_canonical_identity(entry: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    normalized_entry = dict(entry or {})
    resolved_title = _clean_text(normalized_entry.get("title"))
    if not resolved_title:
        raise ValueError("OTT entry title is required")
    release_start_at = _coerce_datetime(normalized_entry.get("release_start_at"))
    representative_year = normalized_entry.get("representative_year") or release_start_at
    season_label = _clean_text(resolved_title)
    if re.search(r"(?i)(?:시즌|season)\s*\d+", season_label) and release_start_at is not None:
        representative_year = release_start_at
    canonical_content_id = build_canonical_content_id(
        title=resolved_title,
        release_start_at=release_start_at,
        representative_year=representative_year,
    )
    normalized_entry["title"] = resolved_title
    normalized_entry["canonical_content_id"] = canonical_content_id
    normalized_entry["release_start_at"] = release_start_at
    normalized_entry["release_end_at"] = _coerce_datetime(normalized_entry.get("release_end_at"))
    return canonical_content_id, normalized_entry


def upsert_ott_source_entries(
    conn,
    *,
    platform_source: str,
    all_content_today: Mapping[str, Mapping[str, Any]],
) -> Dict[str, int]:
    safe_source = str(platform_source or "").strip()
    if safe_source not in OTT_PLATFORM_SOURCE_SET:
        raise ValueError(f"unsupported OTT platform source: {platform_source}")

    now_value = now_kst_naive()
    raw_entries = {
        str(content_id): dict(item)
        for content_id, item in (all_content_today or {}).items()
        if str(content_id).strip() and isinstance(item, Mapping)
    }
    normalized_entries: Dict[str, Dict[str, Any]] = {}
    expected_platform_ids: Dict[str, str] = {}
    write_skipped_count = 0
    excluded_platform_ids = {
        _resolve_entry_platform_content_id(raw_content_id, raw_entry)
        for raw_content_id, raw_entry in raw_entries.items()
        if raw_entry.get("exclude_from_sync")
        and _resolve_entry_platform_content_id(raw_content_id, raw_entry)
    }
    for raw_content_id, raw_entry in raw_entries.items():
        platform_content_id = _resolve_entry_platform_content_id(raw_content_id, raw_entry)
        if not platform_content_id:
            continue
        if platform_content_id in excluded_platform_ids:
            continue
        canonical_content_id, normalized_entry = _resolve_entry_canonical_identity(raw_entry)
        normalized_entry["platform_content_id"] = platform_content_id
        normalized_entries[canonical_content_id] = normalized_entry
        expected_platform_ids[platform_content_id] = canonical_content_id

    entries = normalized_entries
    canonical_ids = sorted(entries.keys())
    read_cursor = get_cursor(conn)
    try:
        if canonical_ids:
            read_cursor.execute(
                """
                SELECT
                    content_id,
                    content_type,
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    meta,
                    search_document
                FROM contents
                WHERE source = %s
                  AND content_id = ANY(%s)
                """,
                (OTT_CANONICAL_SOURCE, canonical_ids),
            )
            existing_rows = {str(row["content_id"]): dict(row) for row in read_cursor.fetchall()}
        else:
            existing_rows = {}

        if canonical_ids:
            read_cursor.execute(
                """
                SELECT
                    canonical_content_id,
                    platform_source,
                    release_end_at,
                    release_end_status,
                    check_fail_count
                FROM ott_schedule_watchlist
                WHERE canonical_content_id = ANY(%s)
                  AND platform_source = %s
                """,
                (canonical_ids, safe_source),
            )
            existing_watchlist = {
                (str(row["canonical_content_id"]), str(row["platform_source"])): dict(row)
                for row in read_cursor.fetchall()
            }
        else:
            existing_watchlist = {}

        read_cursor.execute(
            """
            SELECT
                canonical_content_id,
                platform_source,
                platform_content_id
            FROM content_platform_links
            WHERE platform_source = %s
            """,
            (safe_source,),
        )
        existing_platform_rows = [dict(row) for row in read_cursor.fetchall()]
    finally:
        read_cursor.close()

    seen_platform_ids = {
        str(row.get("platform_content_id") or "").strip()
        for row in existing_platform_rows
        if str(row.get("platform_content_id") or "").strip()
    }
    filtered_entries: Dict[str, Dict[str, Any]] = {}
    filtered_expected_platform_ids: Dict[str, str] = {}
    preserved_platform_ids = set()
    for canonical_content_id, entry in entries.items():
        platform_content_id = str(entry.get("platform_content_id") or "").strip()
        platform_url = _clean_text(entry.get("platform_url") or entry.get("content_url"))
        if not platform_url:
            write_skipped_count += 1
            continue
        if platform_content_id and platform_content_id in seen_platform_ids:
            write_skipped_count += 1
            preserved_platform_ids.add(platform_content_id)
            continue
        filtered_entries[canonical_content_id] = entry
        if platform_content_id:
            filtered_expected_platform_ids[platform_content_id] = canonical_content_id

    entries = filtered_entries
    expected_platform_ids = filtered_expected_platform_ids

    if not entries and not excluded_platform_ids:
        return {
            "inserted_count": 0,
            "updated_count": 0,
            "unchanged_count": 0,
            "write_skipped_count": write_skipped_count,
        }

    canonical_rows = []
    link_rows = []
    watchlist_rows = []

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    for canonical_content_id, entry in entries.items():
        existing_row = existing_rows.get(canonical_content_id)
        existing_meta = existing_row.get("meta") if existing_row else {}
        canonical_meta, status = _build_canonical_meta(
            existing_meta=existing_meta,
            entry=entry,
            platform_source=safe_source,
            now_value=now_value,
        )
        title = _clean_text(entry.get("title")) or _clean_text((existing_row or {}).get("title"))
        authors = _coerce_text_list(canonical_meta.get("common", {}).get("authors"))
        normalized_title = normalize_search_text(title)
        normalized_authors = normalize_search_text(" ".join(authors))
        search_document = build_search_document(
            title=title,
            normalized_title=normalized_title,
            normalized_authors=normalized_authors,
            meta=canonical_meta,
        )
        canonical_row = {
            "content_type": "ott",
            "title": title,
            "normalized_title": normalized_title,
            "normalized_authors": normalized_authors,
            "status": status,
            "meta_json": canonicalize_json(canonical_meta),
            "search_document": search_document,
        }
        existing_comparable = None
        if existing_row:
            existing_comparable = {
                "content_type": existing_row.get("content_type"),
                "title": existing_row.get("title"),
                "normalized_title": existing_row.get("normalized_title") or "",
                "normalized_authors": existing_row.get("normalized_authors") or "",
                "status": existing_row.get("status"),
                "meta_json": canonicalize_json(existing_row.get("meta") or {}),
                "search_document": existing_row.get("search_document") or "",
            }

        if existing_comparable is None:
            inserted_count += 1
        elif existing_comparable == canonical_row:
            unchanged_count += 1
        else:
            updated_count += 1

        canonical_rows.append(
            (
                canonical_content_id,
                OTT_CANONICAL_SOURCE,
                "ott",
                title,
                normalized_title,
                normalized_authors,
                status,
                psycopg2.extras.Json(canonical_meta),
                search_document,
            )
        )

        primary_source = str(canonical_meta.get("common", {}).get("primary_source") or safe_source).strip() or safe_source
        link_rows.append(
            (
                canonical_content_id,
                safe_source,
                str(entry.get("platform_content_id") or canonical_content_id),
                _clean_text(entry.get("platform_url") or entry.get("content_url")),
                _clean_text(entry.get("availability_status")) or "available",
                now_value,
                primary_source == safe_source,
            )
        )

        release_end_at = _coerce_datetime(canonical_meta.get("ott", {}).get("release_end_at"))
        release_end_status = str(
            canonical_meta.get("ott", {}).get("release_end_status") or RELEASE_END_STATUS_UNKNOWN
        ).strip().lower()
        resolution_state = str(
            canonical_meta.get("ott", {}).get("resolution_state") or RESOLUTION_TRACKING
        ).strip() or RESOLUTION_TRACKING
        next_check_at, check_fail_count, final_resolution_state = _compute_watchlist_state(
            existing_row=existing_watchlist.get((canonical_content_id, safe_source)),
            release_start_at=_coerce_datetime(canonical_meta.get("ott", {}).get("release_start_at")),
            release_end_at=release_end_at,
            release_end_status=release_end_status,
            resolution_state=resolution_state,
            status=status,
            now_value=now_value,
        )
        watchlist_rows.append(
            (
                canonical_content_id,
                safe_source,
                _coerce_datetime(canonical_meta.get("ott", {}).get("release_start_at")),
                release_end_at,
                release_end_status,
                now_value,
                next_check_at,
                check_fail_count,
                final_resolution_state,
            )
        )

    stale_pairs = sorted(
        {
            (str(row.get("canonical_content_id") or "").strip(), safe_source)
            for row in existing_platform_rows
            if str(row.get("canonical_content_id") or "").strip()
            and (
                str(row.get("platform_content_id") or "").strip() in excluded_platform_ids
                or expected_platform_ids.get(str(row.get("platform_content_id") or "").strip())
                != str(row.get("canonical_content_id") or "").strip()
            )
            and str(row.get("platform_content_id") or "").strip() not in preserved_platform_ids
        }
    )

    write_cursor = get_cursor(conn)
    try:
        if stale_pairs:
            write_cursor.executemany(
                """
                DELETE FROM ott_schedule_watchlist
                WHERE canonical_content_id = %s
                  AND platform_source = %s
                """,
                stale_pairs,
            )
            write_cursor.executemany(
                """
                DELETE FROM content_platform_links
                WHERE canonical_content_id = %s
                  AND platform_source = %s
                """,
                stale_pairs,
            )
            stale_canonical_ids = sorted({canonical_id for canonical_id, _ in stale_pairs if canonical_id})
            if stale_canonical_ids:
                write_cursor.execute(
                    """
                    DELETE FROM contents c
                    WHERE c.source = %s
                      AND c.content_type = 'ott'
                      AND c.content_id = ANY(%s)
                      AND NOT EXISTS (
                          SELECT 1
                          FROM content_platform_links l
                          WHERE l.canonical_content_id = c.content_id
                      )
                    """,
                    (OTT_CANONICAL_SOURCE, stale_canonical_ids),
                )

        if not canonical_rows:
            return {
                "inserted_count": 0,
                "updated_count": 0,
                "unchanged_count": 0,
                "write_skipped_count": write_skipped_count,
            }

        psycopg2.extras.execute_values(
            write_cursor,
            UPSERT_CANONICAL_SQL,
            canonical_rows,
            page_size=min(len(canonical_rows), 200),
            fetch=True,
        )
        psycopg2.extras.execute_values(
            write_cursor,
            UPSERT_LINK_SQL,
            link_rows,
            page_size=min(len(link_rows), 200),
        )
        psycopg2.extras.execute_values(
            write_cursor,
            UPSERT_WATCHLIST_SQL,
            watchlist_rows,
            page_size=min(len(watchlist_rows), 200),
        )
    finally:
        write_cursor.close()

    return {
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "unchanged_count": unchanged_count,
        "write_skipped_count": write_skipped_count,
    }
