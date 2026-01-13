# views/contents.py

from flask import Blueprint, jsonify, request, current_app
from database import get_db, get_cursor
from utils.text import normalize_search_text
import base64
import json
import math
import os

contents_bp = Blueprint('contents', __name__)


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


def encode_cursor(title, content_id):
    try:
        payload = {"t": title or '', "id": content_id or ''}
        raw = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        return base64.urlsafe_b64encode(raw).decode('utf-8').rstrip('=')
    except Exception:
        return None


def decode_cursor(cursor):
    if not cursor:
        return None, None
    try:
        padded = cursor + '=' * (-len(cursor) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode('utf-8')).decode('utf-8')
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return None, None
        return payload.get('t'), payload.get('id')
    except Exception:
        return None, None


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
        content_type = request.args.get('type', 'webtoon')
        source = request.args.get('source', 'all')
        allowed_types = {'webtoon', 'novel', 'ott', 'series'}
        if not content_type or content_type == 'all' or content_type not in allowed_types:
            content_type = None

        if not normalized_query:
            return jsonify([])

        q_len = len(normalized_query)
        # 너무 짧은 검색어(1자 이하)는 노이즈가 많으므로 즉시 반환
        if q_len <= 1:
            return jsonify([])

        conn = get_db()
        cursor = get_cursor(conn)

        # The normalized columns are precomputed and indexed with pg_trgm to keep
        # whitespace-insensitive searches fast without per-row text manipulation.
        title_expr = "COALESCE(normalized_title, '')"
        author_expr = "COALESCE(normalized_authors, '')"
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
            f"(({title_expr} ILIKE %s) OR ({author_expr} ILIKE %s))",
        ]
        params = [like_param, like_param]

        if content_type:
            where_clauses.insert(0, "content_type = %s")
            params.insert(0, content_type)

        if source != 'all':
            where_clauses.append("source = %s")
            params.append(source)

        if thresholds:
            title_threshold, author_threshold = thresholds
            where_clauses.append(
                f"(similarity({title_expr}, %s) >= %s OR similarity({author_expr}, %s) >= %s)"
            )
            params.extend([normalized_query, title_threshold, normalized_query, author_threshold])

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
            "AND (status = '연재중' OR status = '휴재')"
        )
        params = [content_type]

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


@contents_bp.route('/api/contents/hiatus', methods=['GET'])
def get_hiatus_contents():
    """[페이지네이션] 휴재중인 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    cursor = None
    try:
        raw_cursor = request.args.get('cursor')
        last_title = request.args.get('last_title')
        content_type = request.args.get('type', 'webtoon')
        source = request.args.get('source', 'all')

        try:
            per_page = int(request.args.get('per_page', 300))
        except (TypeError, ValueError):
            per_page = 300

        per_page = max(1, min(per_page, 500))

        cursor_title, cursor_content_id = decode_cursor(raw_cursor)

        conn = get_db()
        cursor = get_cursor(conn)

        query_params = [content_type]
        where_clause = "WHERE status = '휴재' AND content_type = %s AND COALESCE(is_deleted, FALSE) = FALSE"

        if source != 'all':
            where_clause += " AND source = %s"
            query_params.append(source)

        if cursor_title is not None and cursor_content_id is not None:
            where_clause += " AND (title, content_id) > (%s, %s)"
            query_params.extend([cursor_title, cursor_content_id])
        elif last_title:
            where_clause += " AND title > %s"
            query_params.append(last_title)

        cursor.execute(
            f"""
            SELECT content_id, title, status, meta, source
            FROM contents
            {where_clause}
            ORDER BY title ASC, content_id ASC
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
            next_cursor = encode_cursor(last_row.get('title'), last_row.get('content_id'))

        response_payload = {
            'contents': results,
            'next_cursor': next_cursor,
            'last_title': last_row.get('title') if last_row else None,
            'last_content_id': last_row.get('content_id') if last_row else None,
            'page_size': per_page,
            'returned': len(results),
        }

        current_app.logger.info(
            "[contents.hiatus] type=%s source=%s per_page=%s cursor=%s last_title=%s returned=%s next_cursor=%s",
            content_type,
            source,
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
        source = request.args.get('source', 'all')

        try:
            per_page = int(request.args.get('per_page', 300))
        except (TypeError, ValueError):
            per_page = 300

        per_page = max(1, min(per_page, 500))

        cursor_title, cursor_content_id = decode_cursor(raw_cursor)

        conn = get_db()
        cursor = get_cursor(conn)

        query_params = [content_type]
        where_clause = "WHERE status = '완결' AND content_type = %s AND COALESCE(is_deleted, FALSE) = FALSE"

        if source != 'all':
            where_clause += " AND source = %s"
            query_params.append(source)

        if cursor_title is not None and cursor_content_id is not None:
            where_clause += " AND (title, content_id) > (%s, %s)"
            query_params.extend([cursor_title, cursor_content_id])
        elif last_title:
            where_clause += " AND title > %s"
            query_params.append(last_title)

        cursor.execute(
            f"""
            SELECT content_id, title, status, meta, source
            FROM contents
            {where_clause}
            ORDER BY title ASC, content_id ASC
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
            next_cursor = encode_cursor(last_row.get('title'), last_row.get('content_id'))

        response_payload = {
            'contents': results,
            'next_cursor': next_cursor,
            'last_title': last_row.get('title') if last_row else None,
            'last_content_id': last_row.get('content_id') if last_row else None,
            'page_size': per_page,
            'returned': len(results),
        }

        current_app.logger.info(
            "[contents.completed] type=%s source=%s per_page=%s cursor=%s last_title=%s returned=%s next_cursor=%s",
            content_type,
            source,
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
