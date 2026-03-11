# views/contents.py

from flask import Blueprint, jsonify, request, current_app
from database import get_db, get_cursor
from utils.perf import jsonify_timed
from utils.text import normalize_search_text
from utils.ttl_cache import TTLCache
import base64
import json
import os
import re
from datetime import datetime

contents_bp = Blueprint('contents', __name__)

STATUS_ONGOING = "\uC5F0\uC7AC\uC911"
STATUS_HIATUS = "\uD734\uC7AC"
STATUS_COMPLETED = "\uC644\uACB0"
ALLOWED_CONTENT_TYPES = {"webtoon", "novel", "ott", "series"}
ALLOWED_BROWSE_DAYS = {"mon", "tue", "wed", "thu", "fri", "sat", "sun", "daily", "all"}
ALLOWED_BROWSE_DAY_TOKENS = ALLOWED_BROWSE_DAYS - {"all"}
META_MODE_FULL = "full"
META_MODE_LIST = "list"
DEFAULT_API_CACHE_MAX_ENTRIES = 500
DEFAULT_API_CACHE_TTL_SECONDS = 30.0
_API_CACHE = None
_API_CACHE_MAX_ENTRIES = None
_META_LIST_PROJECTION_SQL = """
jsonb_strip_nulls(jsonb_build_object(
  'common', jsonb_strip_nulls(jsonb_build_object(
    'authors', meta->'common'->'authors',
    'content_url', meta->'common'->'content_url',
    'url', meta->'common'->'url',
    'thumbnail_url', meta->'common'->'thumbnail_url',
    'alt_title', meta->'common'->'alt_title',
    'title_alias', meta->'common'->'title_alias',
    'genres', meta->'common'->'genres',
    'genre', meta->'common'->'genre'
  )),
  'attributes', jsonb_strip_nulls(jsonb_build_object(
    'weekdays', meta->'attributes'->'weekdays',
    'genres', meta->'attributes'->'genres',
    'genre', meta->'attributes'->'genre',
    'category', meta->'attributes'->'category',
    'subgenres', meta->'attributes'->'subgenres',
    'sub_genres', meta->'attributes'->'sub_genres'
  )),
  'genres', meta->'genres',
  'genre', meta->'genre'
))
""".strip()


def _read_bool_env(name, default=False):
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "t", "yes", "y", "on"}


def _read_int_env(name, default, minimum=1):
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    if parsed < minimum:
        return default
    return parsed


def _read_float_env_non_negative(name, default):
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    if parsed < 0:
        return default
    return parsed


def _get_meta_mode():
    mode = (os.getenv("ES_META_MODE") or META_MODE_FULL).strip().lower()
    if mode == META_MODE_LIST:
        return META_MODE_LIST
    return META_MODE_FULL


def _meta_select_expr():
    if _get_meta_mode() == META_MODE_LIST:
        return _META_LIST_PROJECTION_SQL
    return "meta"


def _meta_select_expr_for(alias):
    expr = _meta_select_expr()
    if not alias:
        return expr
    return re.sub(r"(?<![\w.])meta(?![\w])", f"{alias}.meta", expr)


def _is_api_cache_enabled():
    return _read_bool_env("ES_API_CACHE_ENABLED", default=False)


def _api_cache_ttl_seconds():
    return _read_float_env_non_negative(
        "ES_API_CACHE_TTL_SECONDS",
        DEFAULT_API_CACHE_TTL_SECONDS,
    )


def _get_api_cache():
    global _API_CACHE, _API_CACHE_MAX_ENTRIES
    max_entries = _read_int_env(
        "ES_API_CACHE_MAX_ENTRIES",
        DEFAULT_API_CACHE_MAX_ENTRIES,
        minimum=1,
    )
    if _API_CACHE is None or _API_CACHE_MAX_ENTRIES != max_entries:
        _API_CACHE = TTLCache(max_entries=max_entries)
        _API_CACHE_MAX_ENTRIES = max_entries
    return _API_CACHE


def _build_api_cache_key():
    query_items = []
    for key in sorted(request.args.keys()):
        values = [str(value) for value in request.args.getlist(key)]
        query_items.append((key, sorted(values)))
    return json.dumps(
        {
            "path": request.path,
            "query": query_items,
            "meta_mode": _get_meta_mode(),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _cache_lookup(cacheable):
    cache_enabled = _is_api_cache_enabled()
    if not cache_enabled or not cacheable or request.method != "GET":
        return cache_enabled, None, None
    cache_key = _build_api_cache_key()
    cached_payload = _get_api_cache().get(cache_key)
    return cache_enabled, cache_key, cached_payload


def _cache_store(cache_key, payload):
    if not cache_key:
        return
    _get_api_cache().set(cache_key, payload, _api_cache_ttl_seconds())


def _json_response(payload, *, cache_enabled=False, cache_hit=False, status_code=None):
    response = jsonify_timed(payload, status_code=status_code)
    if cache_enabled:
        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
    return response


def _normalize_genre_token(value):
    if not isinstance(value, str):
        return ""
    return re.sub(r"[\s_\-/]+", "", value.strip().lower())


GENRE_GROUP_MAPPING = {
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

GENRE_GROUP_ALIASES = {
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

GENRE_GROUP_ALIAS_MAP = {}
for _group, _aliases in GENRE_GROUP_ALIASES.items():
    for _alias in _aliases:
        _normalized = _normalize_genre_token(_alias)
        if _normalized:
            GENRE_GROUP_ALIAS_MAP[_normalized] = _group


def _parse_bool_arg(raw_value, default=False):
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value
    normalized = str(raw_value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _resolve_genre_group(raw_value):
    normalized = _normalize_genre_token(raw_value)
    if not normalized:
        return "ALL"
    return GENRE_GROUP_ALIAS_MAP.get(normalized, "ALL")


def _resolve_genre_groups(raw_values=None):
    entries = _coerce_genre_values(raw_values)
    if not entries:
        return ["ALL"]

    resolved = []
    seen = set()
    for entry in entries:
        group = _resolve_genre_group(entry)
        if group == "ALL":
            normalized = _normalize_genre_token(entry)
            if normalized in {"all", _normalize_genre_token("\uC804\uCCB4")}:
                return ["ALL"]
            continue
        if group in seen:
            continue
        seen.add(group)
        resolved.append(group)

    return resolved if resolved else ["ALL"]


def _select_compat_genre_group(genre_groups):
    groups = list(genre_groups or [])
    if not groups:
        return "ALL"
    return groups[0] if len(groups) == 1 else "ALL"


def _coerce_genre_values(raw_value):
    if raw_value is None:
        return []

    if isinstance(raw_value, (list, tuple, set)):
        merged = []
        for entry in raw_value:
            merged.extend(_coerce_genre_values(entry))
        return merged

    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
            if parsed is not raw_value:
                parsed_values = _coerce_genre_values(parsed)
                if parsed_values:
                    return parsed_values
        except Exception:
            pass
        split_values = [part.strip() for part in re.split(r"[,/|>]+", stripped) if part.strip()]
        return split_values if split_values else [stripped]

    return []


def _extract_internal_genres(meta):
    safe_meta = normalize_meta(meta)
    attrs = safe_get_dict(safe_meta.get("attributes"))
    common = safe_get_dict(safe_meta.get("common"))

    candidates = [
        attrs.get("genres"),
        attrs.get("genre"),
        attrs.get("subgenres"),
        attrs.get("sub_genres"),
        common.get("genres"),
        common.get("genre"),
        safe_meta.get("genres"),
        safe_meta.get("genre"),
    ]

    merged = []
    seen = set()
    for candidate in candidates:
        for token in _coerce_genre_values(candidate):
            normalized = _normalize_genre_token(token)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(token)
    return merged


def _filter_novel_rows_by_genre_groups(rows, genre_groups):
    groups = list(genre_groups or [])
    if not groups or "ALL" in groups:
        return rows

    target_token_set = set()
    for genre_group in groups:
        for token in GENRE_GROUP_MAPPING.get(genre_group, []):
            normalized_token = _normalize_genre_token(token)
            if normalized_token:
                target_token_set.add(normalized_token)
    target_tokens = list(target_token_set)
    if not target_tokens:
        return rows

    saw_genre_metadata = False
    filtered = []
    for row in rows:
        genres = _extract_internal_genres(row.get("meta"))
        normalized_genres = [_normalize_genre_token(token) for token in genres if _normalize_genre_token(token)]
        if normalized_genres:
            saw_genre_metadata = True

        matched = any(
            target in genre or genre in target
            for genre in normalized_genres
            for target in target_tokens
        )
        if matched:
            filtered.append(row)

    # If upstream rows do not have genre metadata yet, keep current visibility.
    return filtered if saw_genre_metadata else rows


def _filter_novel_rows_by_genre_group(rows, genre_group):
    return _filter_novel_rows_by_genre_groups(rows, [genre_group])


def normalize_meta(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except Exception:
        return {}


def safe_get_dict(v):
    return v if isinstance(v, dict) else {}


def normalize_weekdays(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [x for x in v if isinstance(x, str)]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, str)]
        except Exception:
            pass
        return [v]
    return []


def coerce_row_dict(row):
    """
    psycopg2.extras.DictCursor rows may be DictRow; do NOT rely on row.get().
    Coerce to a plain dict safely.
    """
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        try:
            return {k: row[k] for k in row.keys()}
        except Exception:
            pass
    try:
        return dict(row)
    except Exception:
        return {}


def _normalize_string_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(entry).strip() for entry in value if str(entry).strip()]
    if isinstance(value, tuple):
        return [str(entry).strip() for entry in value if str(entry).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except Exception:
            return [stripped]
        if isinstance(parsed, list):
            return [str(entry).strip() for entry in parsed if str(entry).strip()]
        return [stripped]
    return []


def _extract_display_meta(meta):
    safe_meta = normalize_meta(meta)
    attrs = safe_get_dict(safe_meta.get("attributes"))
    common = safe_get_dict(safe_meta.get("common"))
    content_url = (common.get("content_url") or common.get("url") or "") if isinstance(common, dict) else ""
    genres = _normalize_string_list(
        attrs.get("genres")
        or attrs.get("genre")
        or common.get("genres")
        or common.get("genre")
        or safe_meta.get("genres")
        or safe_meta.get("genre")
    )
    return {
        "authors": _normalize_string_list(common.get("authors")),
        "content_url": content_url or "",
        "url": common.get("url") or "",
        "thumbnail_url": common.get("thumbnail_url") or "",
        "alt_title": common.get("alt_title") or "",
        "title_alias": common.get("title_alias") or "",
        "weekdays": normalize_weekdays(attrs.get("weekdays")),
        "genres": genres,
    }


def _serialize_card_payload(row):
    coerced = coerce_row_dict(row)
    display_meta = _extract_display_meta(coerced.get("meta"))
    status = coerced.get("status")
    return {
        "content_id": coerced.get("content_id"),
        "title": coerced.get("title"),
        "status": status,
        "source": coerced.get("source"),
        "content_type": coerced.get("content_type"),
        "thumbnail_url": display_meta.get("thumbnail_url") or None,
        "content_url": display_meta.get("content_url") or display_meta.get("url") or None,
        "display_meta": display_meta,
        "final_state_badge": status if status in {STATUS_COMPLETED, STATUS_HIATUS} else None,
        "cursor": encode_cursor(
            coerced.get("title"),
            coerced.get("content_id"),
            source=coerced.get("source"),
        ),
    }


def encode_cursor(title, content_id, source=None):
    try:
        payload = {
            "t": "" if title is None else title,
            "s": source,
            "id": "" if content_id is None else content_id,
        }
        raw = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        return base64.urlsafe_b64encode(raw).decode('utf-8').rstrip('=')
    except Exception:
        return None


def decode_cursor(cursor):
    if not cursor:
        return None, None, None
    try:
        padded = cursor + '=' * (-len(cursor) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode('utf-8')).decode('utf-8')
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return None, None, None
        title = payload.get("t")
        content_id = payload.get("id")
        source = payload.get("s") if "s" in payload else None
        return title, source, content_id
    except Exception:
        return None, None, None


def _parse_per_page_arg(raw_value, *, default=80, min_value=1, max_value=200):
    try:
        parsed = int(raw_value) if raw_value is not None else default
    except (TypeError, ValueError):
        parsed = default
    return max(min_value, min(parsed, max_value))


def _parse_sources_args():
    raw_sources = request.args.get("sources")
    if raw_sources is not None:
        seen = set()
        parsed_sources = []
        for entry in str(raw_sources).split(","):
            source_id = entry.strip()
            if not source_id or source_id in seen:
                continue
            seen.add(source_id)
            parsed_sources.append(source_id)
        if parsed_sources:
            return {
                "mode": "multi",
                "source": "all",
                "sources": parsed_sources,
            }

    source = request.args.get("source", "all")
    safe_source = str(source).strip() if source is not None else "all"
    if safe_source and safe_source != "all":
        return {
            "mode": "single",
            "source": safe_source,
            "sources": [safe_source],
        }

    return {
        "mode": "all",
        "source": "all",
        "sources": [],
    }


def _parse_browse_days_args():
    raw_values = []
    raw_values.extend(request.args.getlist("day"))
    raw_values.extend(request.args.getlist("days"))

    resolved_days = []
    seen = set()

    for raw_value in raw_values:
        for token in str(raw_value).split(","):
            day = token.strip().lower()
            if not day:
                continue
            if day == "all":
                return ["all"]
            if day not in ALLOWED_BROWSE_DAY_TOKENS or day in seen:
                continue
            seen.add(day)
            resolved_days.append(day)

    return resolved_days if resolved_days else ["all"]


def _append_source_filter(where_parts, params, source_filter):
    mode = source_filter.get("mode")
    sources = source_filter.get("sources") or []
    if mode == "multi" and sources:
        placeholders = ", ".join(["%s"] * len(sources))
        where_parts.append(f"source IN ({placeholders})")
        params.extend(sources)
        return
    if mode == "single" and sources:
        where_parts.append("source = %s")
        params.append(sources[0])


def _append_cursor_filter(where_parts, params, cursor_title, cursor_source, cursor_content_id):
    if cursor_title is None or cursor_content_id is None:
        return False
    if cursor_source is not None:
        where_parts.append("(title, source, content_id) > (%s, %s, %s)")
        params.extend([cursor_title, cursor_source, cursor_content_id])
    else:
        where_parts.append("(title, content_id) > (%s, %s)")
        params.extend([cursor_title, cursor_content_id])
    return True


def _append_browse_cursor_filter(where_parts, params, cursor_title, cursor_source, cursor_content_id):
    if cursor_title is None or cursor_content_id is None:
        return False
    title_group_expr = _browse_title_group_expr("title")
    cursor_title_group_expr = _browse_title_group_expr("%s")
    if cursor_source is not None:
        where_parts.append(
            f'({title_group_expr}, char_length(title), title COLLATE "ko-KR-x-icu", source COLLATE "ko-KR-x-icu", content_id) '
            f'> ({cursor_title_group_expr}, char_length(%s), %s COLLATE "ko-KR-x-icu", %s COLLATE "ko-KR-x-icu", %s)'
        )
        params.extend([cursor_title, cursor_title, cursor_title, cursor_source, cursor_content_id])
    else:
        where_parts.append(
            f'({title_group_expr}, char_length(title), title COLLATE "ko-KR-x-icu", content_id) '
            f'> ({cursor_title_group_expr}, char_length(%s), %s COLLATE "ko-KR-x-icu", %s)'
        )
        params.extend([cursor_title, cursor_title, cursor_title, cursor_content_id])
    return True


def _browse_title_group_expr(value_expr):
    return (
        f"CASE "
        f"WHEN left({value_expr}, 1) >= U&'\\AC00' AND left({value_expr}, 1) <= U&'\\D7A3' THEN 0 "
        f"WHEN left({value_expr}, 1) >= 'A' AND left({value_expr}, 1) <= 'Z' THEN 1 "
        f"WHEN left({value_expr}, 1) >= 'a' AND left({value_expr}, 1) <= 'z' THEN 1 "
        f"WHEN left({value_expr}, 1) >= '0' AND left({value_expr}, 1) <= '9' THEN 2 "
        f"ELSE 3 END"
    )


def _browse_order_by_clause():
    return (
        f"{_browse_title_group_expr('title')} ASC, "
        'char_length(title) ASC, '
        'title COLLATE "ko-KR-x-icu" ASC, '
        'source COLLATE "ko-KR-x-icu" ASC, '
        'content_id ASC'
    )


def _parse_status_filter(raw_value, *, default="ongoing"):
    normalized = str(raw_value or default).strip().lower()
    if normalized in {"completed", "hiatus"}:
        return normalized
    return "ongoing"


def _execute_recommendations_query(cursor, *, limit, meta_expr):
    statuses = (STATUS_ONGOING, STATUS_HIATUS)
    per_type = ((limit + 2) // 3) * 2
    query = f"""
        WITH ranked_by_type AS (
            SELECT
                content_id,
                title,
                status,
                {meta_expr} AS meta,
                source,
                content_type,
                updated_at,
                CASE content_type
                    WHEN 'webtoon' THEN 0
                    WHEN 'novel' THEN 1
                    WHEN 'ott' THEN 2
                    ELSE 99
                END AS type_priority,
                ROW_NUMBER() OVER (
                    PARTITION BY content_type
                    ORDER BY updated_at DESC, content_id ASC
                ) AS type_rank
            FROM contents
            WHERE content_type IN ('webtoon', 'novel', 'ott')
              AND COALESCE(is_deleted, FALSE) = FALSE
              AND status IN (%s, %s)
        ),
        limited_by_type AS (
            SELECT
                content_id,
                title,
                status,
                meta,
                source,
                content_type,
                updated_at,
                type_priority
            FROM ranked_by_type
            WHERE type_rank <= %s
        ),
        deduped AS (
            SELECT
                content_id,
                title,
                status,
                meta,
                source,
                content_type,
                updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY content_id, source
                    ORDER BY updated_at DESC, type_priority ASC, content_id ASC
                ) AS dedupe_rank,
                type_priority
            FROM limited_by_type
        )
        SELECT content_id, title, status, meta, source, content_type
        FROM deduped
        WHERE dedupe_rank = 1
        ORDER BY updated_at DESC, type_priority ASC, content_id ASC
        LIMIT %s
    """
    cursor.execute(query, (statuses[0], statuses[1], per_type, limit))
    return cursor.fetchall()


def _parse_float_env(name):
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        parsed = float(stripped)
    except ValueError:
        return None
    if parsed < 0 or parsed > 1:
        return None
    return parsed


def _get_search_limit():
    default_limit = 100
    raw = os.getenv("SEARCH_MAX_RESULTS")
    if raw is None:
        return default_limit
    stripped = raw.strip()
    if not stripped:
        return default_limit
    try:
        parsed = int(stripped)
    except ValueError:
        return default_limit
    return parsed if parsed > 0 else default_limit


def _parse_recommendation_limit(raw_value):
    default_limit = 12
    if raw_value is None:
        return default_limit
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return default_limit
    return max(1, min(parsed, 50))


def _updated_at_sort_value(value):
    if value is None:
        return float('-inf')
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
        except ValueError:
            return float('-inf')
    return float('-inf')


def _apply_threshold_overrides(defaults, _q_len):
    if defaults is None:
        return None
    title_threshold, author_threshold = defaults
    title_override = _parse_float_env("SEARCH_SIMILARITY_TITLE_THRESHOLD")
    author_override = _parse_float_env("SEARCH_SIMILARITY_AUTHOR_THRESHOLD")
    if title_override is not None:
        title_threshold = title_override
    if author_override is not None:
        author_threshold = author_override
    return title_threshold, author_threshold


def _build_search_query(*, normalized_query, content_type, source, search_limit):
    base_filters = ["COALESCE(is_deleted, FALSE) = FALSE"]
    base_params = []

    if content_type:
        base_filters.append("content_type = %s")
        base_params.append(content_type)

    if source != "all":
        base_filters.append("source = %s")
        base_params.append(source)

    filter_sql = " AND ".join(base_filters)
    substring_param = f"%{normalized_query}%"
    candidate_limit = max(50, search_limit * 4)

    def _candidate_branch(
        predicate_sql,
        predicate_params,
        *,
        title_exact=0,
        title_prefix=0,
        title_substring=0,
        author_exact=0,
        author_prefix=0,
        author_substring=0,
    ):
        branch_sql = f"""
            (
                SELECT
                    content_id,
                    source,
                    {title_exact} AS title_exact,
                    {title_prefix} AS title_prefix,
                    {title_substring} AS title_substring,
                    {author_exact} AS author_exact,
                    {author_prefix} AS author_prefix,
                    {author_substring} AS author_substring
                FROM contents
                WHERE {filter_sql}
                  AND {predicate_sql}
                LIMIT {candidate_limit}
            )
        """
        branch_params = list(base_params)
        branch_params.extend(predicate_params)
        return branch_sql, branch_params

    candidate_branches = [
        _candidate_branch("normalized_title = %s", [normalized_query], title_exact=1),
        _candidate_branch(
            "normalized_title >= %s AND normalized_title < (%s || U&'\\FFFF')",
            [normalized_query, normalized_query],
            title_prefix=1,
        ),
        _candidate_branch("normalized_authors = %s", [normalized_query], author_exact=1),
        _candidate_branch(
            "normalized_authors >= %s AND normalized_authors < (%s || U&'\\FFFF')",
            [normalized_query, normalized_query],
            author_prefix=1,
        ),
    ]

    if len(normalized_query) >= 2:
        candidate_branches.append(_candidate_branch("search_document %% %s", [normalized_query]))

    candidate_sql = "\nUNION ALL\n".join(branch_sql for branch_sql, _ in candidate_branches)
    params = []
    for _, branch_params in candidate_branches:
        params.extend(branch_params)

    meta_expr = _meta_select_expr_for("c")
    query = f"""
        WITH candidate_hits AS (
            {candidate_sql}
        ),
        candidate_rollup AS (
            SELECT
                content_id,
                source,
                MAX(title_exact) AS title_exact,
                MAX(title_prefix) AS title_prefix,
                MAX(author_exact) AS author_exact,
                MAX(author_prefix) AS author_prefix,
                MAX(author_substring) AS author_substring
            FROM candidate_hits
            GROUP BY content_id, source
        )
        SELECT
            c.content_id,
            c.title,
            c.status,
            {meta_expr} AS meta,
            c.source,
            c.content_type
        FROM candidate_rollup rollup
        JOIN contents c
          ON c.content_id = rollup.content_id
         AND c.source = rollup.source
        ORDER BY
            rollup.title_exact DESC,
            rollup.title_prefix DESC,
            CASE WHEN c.normalized_title LIKE %s THEN 1 ELSE 0 END DESC,
            rollup.author_exact DESC,
            rollup.author_prefix DESC,
            CASE WHEN c.normalized_authors LIKE %s THEN 1 ELSE 0 END DESC,
            GREATEST(
                similarity(COALESCE(c.normalized_title, ''), %s),
                similarity(COALESCE(c.normalized_authors, ''), %s),
                similarity(COALESCE(c.search_document, ''), %s)
            ) DESC,
            char_length(c.title) ASC,
            c.title COLLATE "ko-KR-x-icu" ASC,
            c.source COLLATE "ko-KR-x-icu" ASC,
            c.content_id ASC
        LIMIT {search_limit}
    """
    params.extend([substring_param, substring_param, normalized_query, normalized_query, normalized_query])
    return query, tuple(params)


@contents_bp.route('/api/contents/search', methods=['GET'])
def search_contents():
    """전체 DB에서 콘텐츠 제목을 검색하여 결과를 반환합니다."""
    cursor = None
    try:
        query = request.args.get('q', '').strip()
        normalized_query = normalize_search_text(query)
        raw_content_type = request.args.get('type', 'all')
        content_type = str(raw_content_type).strip().lower() if raw_content_type is not None else 'all'
        raw_source = request.args.get('source', 'all')
        source = str(raw_source).strip() if raw_source is not None else 'all'
        allowed_types = {'webtoon', 'novel', 'ott', 'series'}
        if not content_type or content_type == 'all' or content_type not in allowed_types:
            content_type = None
        if not source:
            source = 'all'

        if not normalized_query:
            return _json_response([])

        conn = get_db()
        cursor = get_cursor(conn)
        search_limit = _get_search_limit()
        search_query, params = _build_search_query(
            normalized_query=normalized_query,
            content_type=content_type,
            source=source,
            search_limit=search_limit,
        )
        # Bias the planner toward the new index-backed candidate path for trigram search.
        cursor.execute("SET LOCAL enable_seqscan = off")
        cursor.execute(search_query, params)
        raw_rows = cursor.fetchall()

        results = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced['meta'] = normalize_meta(coerced.get('meta'))
            results.append(coerced)

        return _json_response(results)

    except Exception:
        current_app.logger.exception("Unhandled error in search_contents")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/detail', methods=['GET'])
def get_content_detail():
    cursor = None
    try:
        content_id = (request.args.get("content_id") or "").strip()
        source = (request.args.get("source") or "").strip()
        if not content_id or not source:
            return _json_response(
                {
                    "success": False,
                    "error": {"code": "BAD_REQUEST", "message": "content_id and source are required"},
                },
                status_code=400,
            )

        conn = get_db()
        cursor = get_cursor(conn)
        cursor.execute(
            """
            SELECT content_id, title, status, meta, source, content_type
            FROM contents
            WHERE content_id = %s
              AND source = %s
              AND COALESCE(is_deleted, FALSE) = FALSE
            LIMIT 1
            """,
            (content_id, source),
        )
        row = cursor.fetchone()
        if not row:
            return _json_response(
                {
                    "success": False,
                    "error": {"code": "NOT_FOUND", "message": "Content not found"},
                },
                status_code=404,
            )

        result = coerce_row_dict(row)
        result["meta"] = normalize_meta(result.get("meta"))
        return _json_response(result)
    except Exception:
        current_app.logger.exception("Unhandled error in get_content_detail")
        return _json_response(
            {
                "success": False,
                "error": {"code": "INTERNAL", "message": "Internal Server Error"},
            },
            status_code=500,
        )
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/recommendations', methods=['GET'])
def get_recommendations():
    cursor = None
    cache_enabled, cache_key, cached_payload = _cache_lookup(cacheable=True)
    if cached_payload is not None:
        return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

    try:
        limit = _parse_recommendation_limit(request.args.get('limit'))

        conn = get_db()
        cursor = get_cursor(conn)
        meta_expr = _meta_select_expr()
        rows = _execute_recommendations_query(cursor, limit=limit, meta_expr=meta_expr)

        deduped = []
        for row in rows:
            coerced = coerce_row_dict(row)
            deduped.append({
                'content_id': coerced.get('content_id'),
                'title': coerced.get('title'),
                'status': coerced.get('status'),
                'meta': normalize_meta(coerced.get('meta')),
                'source': coerced.get('source'),
                'content_type': coerced.get('content_type'),
            })

        if cache_key:
            _cache_store(cache_key, deduped)

        return _json_response(deduped, cache_enabled=cache_enabled, cache_hit=False)

    except Exception:
        current_app.logger.exception("Unhandled error in get_recommendations")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/recommendations_v2', methods=['GET'])
def get_recommendations_v2():
    cursor = None
    cache_enabled, cache_key, cached_payload = _cache_lookup(cacheable=True)
    if cached_payload is not None:
        return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

    try:
        limit = _parse_recommendation_limit(request.args.get('limit'))
        conn = get_db()
        cursor = get_cursor(conn)
        rows = _execute_recommendations_query(cursor, limit=limit, meta_expr="meta")

        payload = {
            "contents": [_serialize_card_payload(row) for row in rows],
            "returned": len(rows),
            "limit": limit,
        }

        if cache_key:
            _cache_store(cache_key, payload)

        return _json_response(payload, cache_enabled=cache_enabled, cache_hit=False)
    except Exception:
        current_app.logger.exception("Unhandled error in get_recommendations_v2")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/novels', methods=['GET'])
def get_novel_contents():
    """Web novel list endpoint with genre-group and completion filters."""
    cursor = None
    try:
        source = request.args.get('source', 'all')
        raw_genre_groups = request.args.getlist('genre_group')
        raw_genre_groups.extend(request.args.getlist('genreGroup'))
        genre_groups = _resolve_genre_groups(raw_genre_groups)
        genre_group = _select_compat_genre_group(genre_groups)

        raw_is_completed = request.args.get('is_completed')
        if raw_is_completed is None:
            raw_is_completed = request.args.get('isCompleted')
        is_completed = _parse_bool_arg(raw_is_completed, default=False)

        conn = get_db()
        cursor = get_cursor(conn)

        query_params = ['novel']
        where_clause = "WHERE content_type = %s AND COALESCE(is_deleted, FALSE) = FALSE"

        if source != 'all':
            where_clause += " AND source = %s"
            query_params.append(source)

        if is_completed:
            where_clause += " AND status = %s"
            query_params.append(STATUS_COMPLETED)
        else:
            where_clause += " AND status IN (%s, %s)"
            query_params.extend([STATUS_ONGOING, STATUS_HIATUS])

        meta_expr = _meta_select_expr()
        cursor.execute(
            f"""
            SELECT content_id, title, status, {meta_expr} AS meta, source
            FROM contents
            {where_clause}
            ORDER BY title ASC, content_id ASC
            """,
            tuple(query_params),
        )

        raw_rows = cursor.fetchall()

        results = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced['meta'] = normalize_meta(coerced.get('meta'))
            results.append(coerced)

        filtered = _filter_novel_rows_by_genre_groups(results, genre_groups)

        return jsonify({
            'contents': filtered,
            'filters': {
                'genre_group': genre_group,
                'genre_groups': genre_groups,
                'is_completed': is_completed,
            },
        })

    except Exception:
        current_app.logger.exception("Unhandled error in get_novel_contents")
        return jsonify({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }), 500
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/ongoing', methods=['GET'])
def get_ongoing_contents():
    """요일별 연재중인 콘텐츠 목록을 그룹화하여 반환합니다."""
    cursor = None
    try:
        content_type = request.args.get('type', 'webtoon')
        source = request.args.get('source', 'all')

        conn = get_db()
        cursor = get_cursor(conn)

        meta_expr = _meta_select_expr()
        base_query = (
            f"SELECT content_id, title, status, {meta_expr} AS meta, source "
            "FROM contents "
            "WHERE content_type = %s AND COALESCE(is_deleted, FALSE) = FALSE "
            "AND status IN (%s, %s)"
        )
        params = [content_type, STATUS_ONGOING, STATUS_HIATUS]

        if source != 'all':
            base_query += " AND source = %s"
            params.append(source)

        cursor.execute(base_query, tuple(params))
        raw_rows = cursor.fetchall()

        all_contents = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced['meta'] = normalize_meta(coerced.get('meta'))
            all_contents.append(coerced)

        # 콘텐츠 타입에 따라 분기
        if content_type in ['webtoon', 'novel']:
            grouped_by_day = {
                'mon': [], 'tue': [], 'wed': [], 'thu': [],
                'fri': [], 'sat': [], 'sun': [], 'daily': []
            }
            for content in all_contents:
                try:
                    meta = normalize_meta(content.get('meta'))
                    content['meta'] = meta

                    attrs = safe_get_dict(meta.get('attributes'))
                    day_list = normalize_weekdays(attrs.get('weekdays'))

                    for day_eng in day_list:
                        if day_eng in grouped_by_day:
                            grouped_by_day[day_eng].append(content)
                except Exception as exc:
                    current_app.logger.warning(
                        "Skipping content_id %s due to meta parsing error: %s",
                        content.get('content_id'),
                        exc,
                    )
            return jsonify(grouped_by_day)

        # 다른 콘텐츠 타입(OTT, Series)은 그룹화하지 않고 목록 반환
        return jsonify(all_contents)

    except Exception:
        current_app.logger.exception("Unhandled error in get_ongoing_contents")
        return jsonify({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }), 500
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/ongoing_v2', methods=['GET'])
def get_ongoing_contents_v2():
    """Flat paginated ongoing/hiatus browse endpoint."""
    cursor = None
    cache_enabled = False
    try:
        raw_type = request.args.get("type", "webtoon")
        content_type = str(raw_type).strip().lower() if raw_type is not None else "webtoon"
        if content_type not in ALLOWED_CONTENT_TYPES:
            content_type = "webtoon"

        days = _parse_browse_days_args()
        day = "all" if days == ["all"] else ",".join(days)

        source_filter = _parse_sources_args()
        raw_cursor = request.args.get("cursor")
        cursor_title, cursor_source, cursor_content_id = decode_cursor(raw_cursor)
        per_page = _parse_per_page_arg(
            request.args.get("per_page"),
            default=80,
            min_value=1,
            max_value=200,
        )
        cache_enabled, cache_key, cached_payload = _cache_lookup(cacheable=not raw_cursor)
        if cached_payload is not None:
            return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

        conn = get_db()
        cursor = get_cursor(conn)
        meta_expr = _meta_select_expr()

        where_parts = [
            "content_type = %s",
            "COALESCE(is_deleted, FALSE) = FALSE",
            "status IN (%s, %s)",
        ]
        query_params = [content_type, STATUS_ONGOING, STATUS_HIATUS]

        _append_source_filter(where_parts, query_params, source_filter)
        _append_cursor_filter(where_parts, query_params, cursor_title, cursor_source, cursor_content_id)

        if content_type in {"webtoon", "novel"} and days != ["all"]:
            if len(days) == 1:
                where_parts.append("(meta->'attributes'->'weekdays') ? %s")
                query_params.append(days[0])
            else:
                day_placeholders = ", ".join(["%s"] * len(days))
                where_parts.append(f"(meta->'attributes'->'weekdays') ?| ARRAY[{day_placeholders}]")
                query_params.extend(days)

        cursor.execute(
            f"""
            SELECT content_id, title, status, {meta_expr} AS meta, source, content_type
            FROM contents
            WHERE {' AND '.join(where_parts)}
            ORDER BY title ASC, source ASC, content_id ASC
            LIMIT %s
            """,
            (*query_params, per_page),
        )
        raw_rows = cursor.fetchall()

        results = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced["meta"] = normalize_meta(coerced.get("meta"))
            results.append(coerced)

        last_row = results[-1] if results else None
        next_cursor = None
        if len(results) == per_page and last_row:
            next_cursor = encode_cursor(
                last_row.get("title"),
                last_row.get("content_id"),
                source=last_row.get("source"),
            )

        response_payload = {
            "contents": results,
            "next_cursor": next_cursor,
            "page_size": per_page,
            "returned": len(results),
            "filters": {
                "type": content_type,
                "day": day,
                "days": days,
            },
        }

        current_app.logger.info(
            "[contents.ongoing_v2] type=%s day=%s source_mode=%s per_page=%s cursor=%s returned=%s next_cursor=%s",
            content_type,
            day,
            source_filter.get("mode"),
            per_page,
            bool(raw_cursor),
            len(results),
            bool(next_cursor),
        )

        if cache_key:
            _cache_store(cache_key, response_payload)

        return _json_response(response_payload, cache_enabled=cache_enabled, cache_hit=False)
    except Exception:
        current_app.logger.exception("Unhandled error in get_ongoing_contents_v2")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/browse_v3', methods=['GET'])
def get_browse_contents_v3():
    """Compact browse endpoint optimized for public card rendering."""
    cursor = None
    cache_enabled = False
    try:
        raw_type = request.args.get("type", "webtoon")
        content_type = str(raw_type).strip().lower() if raw_type is not None else "webtoon"
        if content_type not in ALLOWED_CONTENT_TYPES:
            content_type = "webtoon"

        source_filter = _parse_sources_args()
        requested_status = _parse_status_filter(request.args.get("status"), default="ongoing")
        if content_type == "novel" and requested_status == "hiatus":
            requested_status = "ongoing"

        raw_cursor = request.args.get("cursor")
        cursor_title, cursor_source, cursor_content_id = decode_cursor(raw_cursor)
        per_page = _parse_per_page_arg(
            request.args.get("per_page"),
            default=80,
            min_value=1,
            max_value=200,
        )
        cache_enabled, cache_key, cached_payload = _cache_lookup(cacheable=not raw_cursor)
        if cached_payload is not None:
            return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

        conn = get_db()
        cursor = get_cursor(conn)
        next_cursor = None
        filters = {
            "type": content_type,
            "status": requested_status,
        }

        if content_type == "novel":
            raw_genre_groups = request.args.getlist("genre_group")
            raw_genre_groups.extend(request.args.getlist("genreGroup"))
            genre_groups = _resolve_genre_groups(raw_genre_groups)
            genre_group = _select_compat_genre_group(genre_groups)

            raw_is_completed = request.args.get("is_completed")
            if raw_is_completed is None:
                raw_is_completed = request.args.get("isCompleted")
            is_completed = requested_status == "completed" or _parse_bool_arg(raw_is_completed, default=False)

            def _build_base_query(limit_value, scan_title, scan_source, scan_content_id):
                where_parts = [
                    "content_type = %s",
                    "COALESCE(is_deleted, FALSE) = FALSE",
                ]
                query_params = ["novel"]

                if is_completed:
                    where_parts.append("status = %s")
                    query_params.append(STATUS_COMPLETED)
                else:
                    where_parts.append("status IN (%s, %s)")
                    query_params.extend([STATUS_ONGOING, STATUS_HIATUS])

                _append_source_filter(where_parts, query_params, source_filter)
                _append_browse_cursor_filter(where_parts, query_params, scan_title, scan_source, scan_content_id)

                return f"""
                    SELECT content_id, title, status, meta, source, content_type
                    FROM contents
                    WHERE {' AND '.join(where_parts)}
                    ORDER BY {_browse_order_by_clause()}
                    LIMIT %s
                """, (*query_params, limit_value)

            result_rows = []
            if "ALL" in genre_groups:
                query, params = _build_base_query(
                    per_page,
                    cursor_title,
                    cursor_source,
                    cursor_content_id,
                )
                cursor.execute(query, params)
                result_rows = [coerce_row_dict(row) for row in cursor.fetchall()]

                if len(result_rows) == per_page and result_rows:
                    last_row = result_rows[-1]
                    next_cursor = encode_cursor(
                        last_row.get("title"),
                        last_row.get("content_id"),
                        source=last_row.get("source"),
                    )
            else:
                chunk_size = min(per_page * 4, 500)
                scan_title = cursor_title
                scan_source = cursor_source
                scan_content_id = cursor_content_id
                has_more_rows = False
                last_scanned_row = None

                while len(result_rows) < per_page:
                    query, params = _build_base_query(
                        chunk_size,
                        scan_title,
                        scan_source,
                        scan_content_id,
                    )
                    cursor.execute(query, params)
                    raw_rows = cursor.fetchall()
                    if not raw_rows:
                        has_more_rows = False
                        break

                    normalized_rows = []
                    for row in raw_rows:
                        coerced = coerce_row_dict(row)
                        coerced["meta"] = normalize_meta(coerced.get("meta"))
                        normalized_rows.append(coerced)

                    last_scanned_row = normalized_rows[-1]
                    scan_title = last_scanned_row.get("title")
                    scan_source = last_scanned_row.get("source")
                    scan_content_id = last_scanned_row.get("content_id")

                    matched_rows = _filter_novel_rows_by_genre_groups(normalized_rows, genre_groups)
                    if matched_rows:
                        remaining = per_page - len(result_rows)
                        result_rows.extend(matched_rows[:remaining])

                    has_more_rows = len(raw_rows) == chunk_size
                    if len(result_rows) >= per_page:
                        break
                    if len(raw_rows) < chunk_size:
                        has_more_rows = False
                        break

                if has_more_rows and last_scanned_row:
                    next_cursor = encode_cursor(
                        last_scanned_row.get("title"),
                        last_scanned_row.get("content_id"),
                        source=last_scanned_row.get("source"),
                    )

            filters.update({
                "genre_group": genre_group,
                "genre_groups": genre_groups,
                "is_completed": is_completed,
            })
        else:
            where_parts = [
                "content_type = %s",
                "COALESCE(is_deleted, FALSE) = FALSE",
            ]
            query_params = [content_type]

            if requested_status == "completed":
                where_parts.append("status = %s")
                query_params.append(STATUS_COMPLETED)
            elif requested_status == "hiatus":
                where_parts.append("status = %s")
                query_params.append(STATUS_HIATUS)
            else:
                where_parts.append("status IN (%s, %s)")
                query_params.extend([STATUS_ONGOING, STATUS_HIATUS])

            _append_source_filter(where_parts, query_params, source_filter)
            _append_browse_cursor_filter(where_parts, query_params, cursor_title, cursor_source, cursor_content_id)

            if content_type == "webtoon" and requested_status == "ongoing":
                days = _parse_browse_days_args()
                day = "all" if days == ["all"] else ",".join(days)
                filters["day"] = day
                filters["days"] = days
                if days != ["all"]:
                    if len(days) == 1:
                        where_parts.append("(meta->'attributes'->'weekdays') ? %s")
                        query_params.append(days[0])
                    else:
                        day_placeholders = ", ".join(["%s"] * len(days))
                        where_parts.append(f"(meta->'attributes'->'weekdays') ?| ARRAY[{day_placeholders}]")
                        query_params.extend(days)

            cursor.execute(
                f"""
                SELECT content_id, title, status, meta, source, content_type
                FROM contents
                WHERE {' AND '.join(where_parts)}
                ORDER BY {_browse_order_by_clause()}
                LIMIT %s
                """,
                (*query_params, per_page),
            )
            result_rows = [coerce_row_dict(row) for row in cursor.fetchall()]

            if len(result_rows) == per_page and result_rows:
                last_row = result_rows[-1]
                next_cursor = encode_cursor(
                    last_row.get("title"),
                    last_row.get("content_id"),
                    source=last_row.get("source"),
                )

        serialized = [_serialize_card_payload(row) for row in result_rows]
        payload = {
            "contents": serialized,
            "next_cursor": next_cursor,
            "page_size": per_page,
            "returned": len(serialized),
            "filters": filters,
        }

        if cache_key:
            _cache_store(cache_key, payload)

        return _json_response(payload, cache_enabled=cache_enabled, cache_hit=False)
    except Exception:
        current_app.logger.exception("Unhandled error in get_browse_contents_v3")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/novels_v2', methods=['GET'])
def get_novel_contents_v2():
    """Paginated novel list endpoint with genre-group and completion filters."""
    cursor = None
    cache_enabled = False
    try:
        source_filter = _parse_sources_args()

        raw_genre_groups = request.args.getlist("genre_group")
        raw_genre_groups.extend(request.args.getlist("genreGroup"))
        genre_groups = _resolve_genre_groups(raw_genre_groups)
        genre_group = _select_compat_genre_group(genre_groups)

        raw_is_completed = request.args.get("is_completed")
        if raw_is_completed is None:
            raw_is_completed = request.args.get("isCompleted")
        is_completed = _parse_bool_arg(raw_is_completed, default=False)

        raw_cursor = request.args.get("cursor")
        cursor_title, cursor_source, cursor_content_id = decode_cursor(raw_cursor)
        per_page = _parse_per_page_arg(
            request.args.get("per_page"),
            default=80,
            min_value=1,
            max_value=200,
        )
        cache_enabled, cache_key, cached_payload = _cache_lookup(cacheable=not raw_cursor)
        if cached_payload is not None:
            return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

        conn = get_db()
        cursor = get_cursor(conn)
        meta_expr = _meta_select_expr()

        def _build_base_query(limit_value, scan_title, scan_source, scan_content_id):
            where_parts = [
                "content_type = %s",
                "COALESCE(is_deleted, FALSE) = FALSE",
            ]
            query_params = ["novel"]

            if is_completed:
                where_parts.append("status = %s")
                query_params.append(STATUS_COMPLETED)
            else:
                where_parts.append("status IN (%s, %s)")
                query_params.extend([STATUS_ONGOING, STATUS_HIATUS])

            _append_source_filter(where_parts, query_params, source_filter)
            _append_cursor_filter(where_parts, query_params, scan_title, scan_source, scan_content_id)

            return f"""
                SELECT content_id, title, status, {meta_expr} AS meta, source, content_type
                FROM contents
                WHERE {' AND '.join(where_parts)}
                ORDER BY title ASC, source ASC, content_id ASC
                LIMIT %s
            """, (*query_params, limit_value)

        results = []
        next_cursor = None

        if "ALL" in genre_groups:
            query, params = _build_base_query(
                per_page,
                cursor_title,
                cursor_source,
                cursor_content_id,
            )
            cursor.execute(query, params)
            raw_rows = cursor.fetchall()

            for row in raw_rows:
                coerced = coerce_row_dict(row)
                coerced["meta"] = normalize_meta(coerced.get("meta"))
                results.append(coerced)

            if len(results) == per_page and results:
                last_row = results[-1]
                next_cursor = encode_cursor(
                    last_row.get("title"),
                    last_row.get("content_id"),
                    source=last_row.get("source"),
                )
        else:
            chunk_size = min(per_page * 4, 500)
            scan_title = cursor_title
            scan_source = cursor_source
            scan_content_id = cursor_content_id
            has_more_rows = False
            last_scanned_row = None

            while len(results) < per_page:
                query, params = _build_base_query(
                    chunk_size,
                    scan_title,
                    scan_source,
                    scan_content_id,
                )
                cursor.execute(query, params)
                raw_rows = cursor.fetchall()
                if not raw_rows:
                    has_more_rows = False
                    break

                normalized_rows = []
                for row in raw_rows:
                    coerced = coerce_row_dict(row)
                    coerced["meta"] = normalize_meta(coerced.get("meta"))
                    normalized_rows.append(coerced)

                last_scanned_row = normalized_rows[-1]
                scan_title = last_scanned_row.get("title")
                scan_source = last_scanned_row.get("source")
                scan_content_id = last_scanned_row.get("content_id")

                matched_rows = _filter_novel_rows_by_genre_groups(normalized_rows, genre_groups)
                if matched_rows:
                    remaining = per_page - len(results)
                    results.extend(matched_rows[:remaining])

                has_more_rows = len(raw_rows) == chunk_size
                if len(results) >= per_page:
                    break
                if len(raw_rows) < chunk_size:
                    has_more_rows = False
                    break

            if has_more_rows and last_scanned_row:
                next_cursor = encode_cursor(
                    last_scanned_row.get("title"),
                    last_scanned_row.get("content_id"),
                    source=last_scanned_row.get("source"),
                )

        response_payload = {
            "contents": results,
            "next_cursor": next_cursor,
            "page_size": per_page,
            "returned": len(results),
            "filters": {
                "genre_group": genre_group,
                "genre_groups": genre_groups,
                "is_completed": is_completed,
            },
        }

        current_app.logger.info(
            "[contents.novels_v2] source_mode=%s genre_group=%s genre_groups=%s is_completed=%s per_page=%s cursor=%s returned=%s next_cursor=%s",
            source_filter.get("mode"),
            genre_group,
            ",".join(genre_groups),
            is_completed,
            per_page,
            bool(raw_cursor),
            len(results),
            bool(next_cursor),
        )

        if cache_key:
            _cache_store(cache_key, response_payload)

        return _json_response(response_payload, cache_enabled=cache_enabled, cache_hit=False)
    except Exception:
        current_app.logger.exception("Unhandled error in get_novel_contents_v2")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/hiatus', methods=['GET'])
def get_hiatus_contents():
    """[페이지네이션] 휴재중인 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    cursor = None
    cache_enabled = False
    try:
        raw_cursor = request.args.get('cursor')
        last_title = request.args.get('last_title')
        content_type = request.args.get('type', 'webtoon')
        source_filter = _parse_sources_args()

        try:
            per_page = int(request.args.get('per_page', 300))
        except (TypeError, ValueError):
            per_page = 300

        per_page = max(1, min(per_page, 500))

        cursor_title, cursor_source, cursor_content_id = decode_cursor(raw_cursor)
        cache_enabled, cache_key, cached_payload = _cache_lookup(
            cacheable=not raw_cursor and not last_title,
        )
        if cached_payload is not None:
            return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

        conn = get_db()
        cursor = get_cursor(conn)
        meta_expr = _meta_select_expr()

        query_params = [STATUS_HIATUS, content_type]
        where_parts = [
            "status = %s",
            "content_type = %s",
            "COALESCE(is_deleted, FALSE) = FALSE",
        ]

        _append_source_filter(where_parts, query_params, source_filter)
        if not _append_cursor_filter(where_parts, query_params, cursor_title, cursor_source, cursor_content_id) and last_title:
            where_parts.append("title > %s")
            query_params.append(last_title)

        cursor.execute(
            f"""
            SELECT content_id, title, status, {meta_expr} AS meta, source
            FROM contents
            WHERE {' AND '.join(where_parts)}
            ORDER BY title ASC, source ASC, content_id ASC
            LIMIT %s
            """,
            (*query_params, per_page),
        )

        raw_rows = cursor.fetchall()

        results = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced['meta'] = normalize_meta(coerced.get('meta'))
            results.append(coerced)

        last_row = results[-1] if results else None
        next_cursor = None
        if len(results) == per_page and last_row:
            next_cursor = encode_cursor(
                last_row.get('title'),
                last_row.get('content_id'),
                source=last_row.get('source'),
            )

        response_payload = {
            'contents': results,
            'next_cursor': next_cursor,
            'last_title': last_row.get('title') if last_row else None,
            'last_content_id': last_row.get('content_id') if last_row else None,
            'page_size': per_page,
            'returned': len(results),
        }

        current_app.logger.info(
            "[contents.hiatus] type=%s source_mode=%s per_page=%s cursor=%s last_title=%s returned=%s next_cursor=%s",
            content_type,
            source_filter.get("mode"),
            per_page,
            bool(raw_cursor),
            bool(last_title),
            len(results),
            bool(next_cursor),
        )

        if cache_key:
            _cache_store(cache_key, response_payload)

        return _json_response(response_payload, cache_enabled=cache_enabled, cache_hit=False)

    except Exception:
        current_app.logger.exception("Unhandled error in get_hiatus_contents")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass


@contents_bp.route('/api/contents/completed', methods=['GET'])
def get_completed_contents():
    """[페이지네이션] 완결된 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    cursor = None
    cache_enabled = False
    try:
        raw_cursor = request.args.get('cursor')
        last_title = request.args.get('last_title')
        content_type = request.args.get('type', 'webtoon')
        source_filter = _parse_sources_args()

        try:
            per_page = int(request.args.get('per_page', 300))
        except (TypeError, ValueError):
            per_page = 300

        per_page = max(1, min(per_page, 500))

        cursor_title, cursor_source, cursor_content_id = decode_cursor(raw_cursor)
        cache_enabled, cache_key, cached_payload = _cache_lookup(
            cacheable=not raw_cursor and not last_title,
        )
        if cached_payload is not None:
            return _json_response(cached_payload, cache_enabled=cache_enabled, cache_hit=True)

        conn = get_db()
        cursor = get_cursor(conn)
        meta_expr = _meta_select_expr()

        query_params = [STATUS_COMPLETED, content_type]
        where_parts = [
            "status = %s",
            "content_type = %s",
            "COALESCE(is_deleted, FALSE) = FALSE",
        ]

        _append_source_filter(where_parts, query_params, source_filter)
        if not _append_cursor_filter(where_parts, query_params, cursor_title, cursor_source, cursor_content_id) and last_title:
            where_parts.append("title > %s")
            query_params.append(last_title)

        cursor.execute(
            f"""
            SELECT content_id, title, status, {meta_expr} AS meta, source
            FROM contents
            WHERE {' AND '.join(where_parts)}
            ORDER BY title ASC, source ASC, content_id ASC
            LIMIT %s
            """,
            (*query_params, per_page),
        )

        raw_rows = cursor.fetchall()

        results = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced['meta'] = normalize_meta(coerced.get('meta'))
            results.append(coerced)

        last_row = results[-1] if results else None
        next_cursor = None
        if len(results) == per_page and last_row:
            next_cursor = encode_cursor(
                last_row.get('title'),
                last_row.get('content_id'),
                source=last_row.get('source'),
            )

        response_payload = {
            'contents': results,
            'next_cursor': next_cursor,
            'last_title': last_row.get('title') if last_row else None,
            'last_content_id': last_row.get('content_id') if last_row else None,
            'page_size': per_page,
            'returned': len(results),
        }

        current_app.logger.info(
            "[contents.completed] type=%s source_mode=%s per_page=%s cursor=%s last_title=%s returned=%s next_cursor=%s",
            content_type,
            source_filter.get("mode"),
            per_page,
            bool(raw_cursor),
            bool(last_title),
            len(results),
            bool(next_cursor),
        )

        if cache_key:
            _cache_store(cache_key, response_payload)

        return _json_response(response_payload, cache_enabled=cache_enabled, cache_hit=False)

    except Exception:
        current_app.logger.exception("Unhandled error in get_completed_contents")
        return _json_response({
            "success": False,
            "error": {"code": "INTERNAL", "message": "Internal Server Error"}
        }, cache_enabled=cache_enabled, cache_hit=False, status_code=500)
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
