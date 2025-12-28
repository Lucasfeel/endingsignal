# views/contents.py

from flask import Blueprint, jsonify, request, current_app
from database import get_db, get_cursor
import math
import json

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


@contents_bp.route('/api/contents/search', methods=['GET'])
def search_contents():
    """전체 DB에서 콘텐츠 제목을 검색하여 결과를 반환합니다."""
    cursor = None
    try:
        query = request.args.get('q', '').strip()
        content_type = request.args.get('type', 'webtoon')
        source = request.args.get('source', 'all')

        if not query:
            return jsonify([])

        conn = get_db()
        cursor = get_cursor(conn)

        base_query = """
            SELECT content_id, title, status, meta, source
            FROM contents
            WHERE title %% %s AND content_type = %s
        """
        # placeholders:
        # 1) title %% %s  -> query
        # 2) content_type = %s -> content_type
        # 3) ORDER BY similarity(title, %s) -> query
        params = [query, content_type, query]

        if source != 'all':
            base_query += " AND source = %s"
            # insert before the final similarity param
            params.insert(2, source)

        base_query += " ORDER BY similarity(title, %s) DESC LIMIT 100"

        cursor.execute(base_query, tuple(params))
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
            "WHERE content_type = %s AND (status = '연재중' OR status = '휴재')"
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
        last_title = request.args.get('last_title')
        per_page = 100
        content_type = request.args.get('type', 'webtoon')
        source = request.args.get('source', 'all')

        conn = get_db()
        cursor = get_cursor(conn)

        query_params = [content_type]
        where_clause = "WHERE status = '휴재' AND content_type = %s"

        if source != 'all':
            where_clause += " AND source = %s"
            query_params.append(source)

        if last_title:
            where_clause += " AND title > %s"
            query_params.append(last_title)

        cursor.execute(
            f"""
            SELECT content_id, title, status, meta, source
            FROM contents
            {where_clause}
            ORDER BY title ASC
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

        next_cursor = None
        if len(results) == per_page:
            next_cursor = results[-1].get('title')

        return jsonify({'contents': results, 'next_cursor': next_cursor})

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
        last_title = request.args.get('last_title')
        per_page = 100
        content_type = request.args.get('type', 'webtoon')
        source = request.args.get('source', 'all')

        conn = get_db()
        cursor = get_cursor(conn)

        query_params = [content_type]
        where_clause = "WHERE status = '완결' AND content_type = %s"

        if source != 'all':
            where_clause += " AND source = %s"
            query_params.append(source)

        if last_title:
            where_clause += " AND title > %s"
            query_params.append(last_title)

        cursor.execute(
            f"""
            SELECT content_id, title, status, meta, source
            FROM contents
            {where_clause}
            ORDER BY title ASC
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

        next_cursor = None
        if len(results) == per_page:
            next_cursor = results[-1].get('title')

        return jsonify({'contents': results, 'next_cursor': next_cursor})

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
