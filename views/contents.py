# views/contents.py

from flask import Blueprint, jsonify, request, current_app
from database import get_db, get_cursor
from utils.text import normalize_search_text
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
    "ROMANCE": ("romance", "\uB85C\uB9E8\uC2A4"),
    "ROMANCE_FANTASY": (
        "romancefantasy",
        "romance_fantasy",
        "\uB85C\uD310",
        "\uB85C\uB9E8\uC2A4\uD310\uD0C0\uC9C0",
    ),
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


def _filter_novel_rows_by_genre_group(rows, genre_group):
    if genre_group == "ALL":
        return rows

    target_tokens = [
        _normalize_genre_token(token)
        for token in GENRE_GROUP_MAPPING.get(genre_group, [])
        if _normalize_genre_token(token)
    ]
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
            return jsonify([])

        q_len = len(normalized_query)
        # 너무 짧은 검색어(1자 이하)는 노이즈가 많으므로 즉시 반환
        if q_len <= 1:
            return jsonify([])

        conn = get_db()
        cursor = get_cursor(conn)

        # Prefer normalized columns for indexed search, but fall back to on-the-fly
        # normalization so rows inserted without normalized_* values still match.
        title_expr = (
            "COALESCE(NULLIF(normalized_title, ''), "
            "regexp_replace(lower(COALESCE(title, '')), '\\s+', '', 'g'))"
        )
        author_expr = (
            "COALESCE(NULLIF(normalized_authors, ''), "
            "regexp_replace(lower(COALESCE(meta->'common'->>'authors', '')), '\\s+', '', 'g'))"
        )
        like_param = f"%{normalized_query}%"

        def compute_thresholds(length):
            if length <= 2:
                return None
            if length == 3:
                return (0.2, 0.25)
            if length == 4:
                return (0.15, 0.2)
            return (0.12, 0.18)

        thresholds = _apply_threshold_overrides(compute_thresholds(q_len), q_len)

        where_clauses = [
            "COALESCE(is_deleted, FALSE) = FALSE",
        ]
        params = []

        if content_type:
            where_clauses.insert(0, "content_type = %s")
            params.insert(0, content_type)

        if source != 'all':
            where_clauses.append("source = %s")
            params.append(source)

        if thresholds:
            title_threshold, author_threshold = thresholds
            where_clauses.append(
                f"(({title_expr} ILIKE %s) OR ({author_expr} ILIKE %s) OR "
                f"(similarity({title_expr}, %s) >= %s OR similarity({author_expr}, %s) >= %s))"
            )
            params.extend(
                [
                    like_param,
                    like_param,
                    normalized_query,
                    title_threshold,
                    normalized_query,
                    author_threshold,
                ]
            )
        else:
            where_clauses.append(f"(({title_expr} ILIKE %s) OR ({author_expr} ILIKE %s))")
            params.extend([like_param, like_param])

        search_limit = _get_search_limit()
        search_query = f"""
            SELECT content_id, title, status, meta, source, content_type
            FROM contents
            WHERE {' AND '.join(where_clauses)}
            ORDER BY
              CASE
                WHEN {title_expr} ILIKE %s THEN 0
                WHEN {author_expr} ILIKE %s THEN 1
                ELSE 2
              END,
              GREATEST(similarity({title_expr}, %s), similarity({author_expr}, %s)) DESC,
              content_id ASC
            LIMIT {search_limit}
        """

        params.extend([like_param, like_param, normalized_query, normalized_query])

        cursor.execute(search_query, tuple(params))
        raw_rows = cursor.fetchall()

        results = []
        for row in raw_rows:
            coerced = coerce_row_dict(row)
            coerced['meta'] = normalize_meta(coerced.get('meta'))
            results.append(coerced)

        return jsonify(results)

    except Exception:
        current_app.logger.exception("Unhandled error in search_contents")
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


@contents_bp.route('/api/contents/recommendations', methods=['GET'])
def get_recommendations():
    cursor = None
    try:
        limit = _parse_recommendation_limit(request.args.get('limit'))
        # Fetch extra per type so we can merge/sort and still keep a mixed top-N.
        per_type = ((limit + 2) // 3) * 2

        conn = get_db()
        cursor = get_cursor(conn)

        merged_rows = []
        content_types = ('webtoon', 'novel', 'ott')
        statuses = (STATUS_ONGOING, STATUS_HIATUS)

        query = """
            SELECT content_id, title, status, meta, source, content_type, updated_at
            FROM contents
            WHERE content_type = %s
              AND COALESCE(is_deleted, FALSE) = FALSE
              AND status IN (%s, %s)
            ORDER BY updated_at DESC, content_id ASC
            LIMIT %s
        """

        for content_type in content_types:
            cursor.execute(query, (content_type, statuses[0], statuses[1], per_type))
            rows = cursor.fetchall()
            for row in rows:
                merged_rows.append(coerce_row_dict(row))

        merged_rows.sort(
            key=lambda row: _updated_at_sort_value(row.get('updated_at')),
            reverse=True,
        )

        deduped = []
        seen_keys = set()
        for row in merged_rows:
            key = (row.get('content_id'), row.get('source'))
            if not key[0] or not key[1] or key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append({
                'content_id': row.get('content_id'),
                'title': row.get('title'),
                'status': row.get('status'),
                'meta': normalize_meta(row.get('meta')),
                'source': row.get('source'),
                'content_type': row.get('content_type'),
            })
            if len(deduped) >= limit:
                break

        return jsonify(deduped)

    except Exception:
        current_app.logger.exception("Unhandled error in get_recommendations")
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


@contents_bp.route('/api/contents/novels', methods=['GET'])
def get_novel_contents():
    """Web novel list endpoint with genre-group and completion filters."""
    cursor = None
    try:
        source = request.args.get('source', 'all')
        raw_genre_group = request.args.get('genre_group')
        if raw_genre_group is None:
            raw_genre_group = request.args.get('genreGroup')
        genre_group = _resolve_genre_group(raw_genre_group)

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

        cursor.execute(
            f"""
            SELECT content_id, title, status, meta, source
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

        filtered = _filter_novel_rows_by_genre_group(results, genre_group)

        return jsonify({
            'contents': filtered,
            'filters': {
                'genre_group': genre_group,
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

        base_query = (
            "SELECT content_id, title, status, meta, source "
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
    try:
        raw_type = request.args.get("type", "webtoon")
        content_type = str(raw_type).strip().lower() if raw_type is not None else "webtoon"
        if content_type not in ALLOWED_CONTENT_TYPES:
            content_type = "webtoon"

        raw_day = request.args.get("day", "all")
        day = str(raw_day).strip().lower() if raw_day is not None else "all"
        if day not in ALLOWED_BROWSE_DAYS:
            day = "all"

        source_filter = _parse_sources_args()
        raw_cursor = request.args.get("cursor")
        cursor_title, cursor_source, cursor_content_id = decode_cursor(raw_cursor)
        per_page = _parse_per_page_arg(
            request.args.get("per_page"),
            default=80,
            min_value=1,
            max_value=200,
        )

        conn = get_db()
        cursor = get_cursor(conn)

        where_parts = [
            "content_type = %s",
            "COALESCE(is_deleted, FALSE) = FALSE",
            "status IN (%s, %s)",
        ]
        query_params = [content_type, STATUS_ONGOING, STATUS_HIATUS]

        _append_source_filter(where_parts, query_params, source_filter)
        _append_cursor_filter(where_parts, query_params, cursor_title, cursor_source, cursor_content_id)

        if content_type in {"webtoon", "novel"} and day != "all":
            where_parts.append("(meta->'attributes'->'weekdays') ? %s")
            query_params.append(day)

        cursor.execute(
            f"""
            SELECT content_id, title, status, meta, source, content_type
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

        return jsonify(response_payload)
    except Exception:
        current_app.logger.exception("Unhandled error in get_ongoing_contents_v2")
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


@contents_bp.route('/api/contents/novels_v2', methods=['GET'])
def get_novel_contents_v2():
    """Paginated novel list endpoint with genre-group and completion filters."""
    cursor = None
    try:
        source_filter = _parse_sources_args()

        raw_genre_group = request.args.get("genre_group")
        if raw_genre_group is None:
            raw_genre_group = request.args.get("genreGroup")
        genre_group = _resolve_genre_group(raw_genre_group)

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

        conn = get_db()
        cursor = get_cursor(conn)

        def _build_base_query(limit_value, scan_title, scan_source, scan_content_id):
            where_parts = [
                "content_type = %s",
                "COALESCE(is_deleted, FALSE) = FALSE",
            ]
            query_params = ["novel"]

            if is_completed:
                where_parts.append("status = %s")
                query_params.append(STATUS_COMPLETED)

            _append_source_filter(where_parts, query_params, source_filter)
            _append_cursor_filter(where_parts, query_params, scan_title, scan_source, scan_content_id)

            return f"""
                SELECT content_id, title, status, meta, source, content_type
                FROM contents
                WHERE {' AND '.join(where_parts)}
                ORDER BY title ASC, source ASC, content_id ASC
                LIMIT %s
            """, (*query_params, limit_value)

        results = []
        next_cursor = None

        if genre_group == "ALL":
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

                matched_rows = _filter_novel_rows_by_genre_group(normalized_rows, genre_group)
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
                "is_completed": is_completed,
            },
        }

        current_app.logger.info(
            "[contents.novels_v2] source_mode=%s genre_group=%s is_completed=%s per_page=%s cursor=%s returned=%s next_cursor=%s",
            source_filter.get("mode"),
            genre_group,
            is_completed,
            per_page,
            bool(raw_cursor),
            len(results),
            bool(next_cursor),
        )

        return jsonify(response_payload)
    except Exception:
        current_app.logger.exception("Unhandled error in get_novel_contents_v2")
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


@contents_bp.route('/api/contents/hiatus', methods=['GET'])
def get_hiatus_contents():
    """[페이지네이션] 휴재중인 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    cursor = None
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

        conn = get_db()
        cursor = get_cursor(conn)

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
            SELECT content_id, title, status, meta, source
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

        return jsonify(response_payload)

    except Exception:
        current_app.logger.exception("Unhandled error in get_hiatus_contents")
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


@contents_bp.route('/api/contents/completed', methods=['GET'])
def get_completed_contents():
    """[페이지네이션] 완결된 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    cursor = None
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

        conn = get_db()
        cursor = get_cursor(conn)

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
            SELECT content_id, title, status, meta, source
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

        return jsonify(response_payload)

    except Exception:
        current_app.logger.exception("Unhandled error in get_completed_contents")
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
