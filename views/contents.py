# views/contents.py

from flask import Blueprint, jsonify, request
from database import get_db, get_cursor
import math
import json

contents_bp = Blueprint('contents', __name__)

def process_row(row):
    """
    DB에서 읽어온 row를 처리합니다.
    meta 필드가 None이면 빈 dict로 보장합니다.
    """
    row_dict = dict(row)
    if row_dict.get('meta') is None:
        row_dict['meta'] = {}

    # psycopg2가 JSONB를 dict로 자동 변환하므로,
    # isinstance(..., str) 및 json.loads()가 더 이상 필요하지 않습니다.

    return row_dict

@contents_bp.route('/api/contents/search', methods=['GET'])
def search_contents():
    """전체 DB에서 콘텐츠 제목을 검색하여 결과를 반환합니다."""
    query = request.args.get('q', '').strip()
    content_type = request.args.get('type', 'webtoon')

    if not query:
        return jsonify([])

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute(
        """
        SELECT content_id, title, status, meta, source
        FROM contents
        WHERE title %% %s AND content_type = %s
        ORDER BY similarity(title, %s) DESC
        LIMIT 100
        """,
        (query, content_type, query)
    )

    results = [process_row(row) for row in cursor.fetchall()]
    cursor.close()
    return jsonify(results)


@contents_bp.route('/api/contents/ongoing', methods=['GET'])
def get_ongoing_contents():
    """요일별 연재중인 콘텐츠 목록을 그룹화하여 반환합니다."""
    content_type = request.args.get('type', 'webtoon')

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute(
        "SELECT content_id, title, status, meta, source FROM contents WHERE content_type = %s AND (status = '연재중' OR status = '휴재')",
        (content_type,)
    )

    all_contents = [process_row(row) for row in cursor.fetchall()]
    cursor.close()

    grouped_by_day = { 'mon': [], 'tue': [], 'wed': [], 'thu': [], 'fri': [], 'sat': [], 'sun': [], 'daily': [] }
    for content in all_contents:
        day_list = content.get('meta', {}).get('weekdays', [])
        for day_eng in day_list:
            if day_eng in grouped_by_day:
                grouped_by_day[day_eng].append(content)

    return jsonify(grouped_by_day)

@contents_bp.route('/api/contents/hiatus', methods=['GET'])
def get_hiatus_contents():
    """[페이지네이션] 휴재중인 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    last_title = request.args.get('last_title')
    per_page = 100
    content_type = request.args.get('type', 'webtoon')

    conn = get_db()
    cursor = get_cursor(conn)

    query_params = [content_type]
    where_clause = "WHERE status = '휴재' AND content_type = %s"

    if last_title:
        where_clause += " AND title > %s"
        query_params.append(last_title)

    cursor.execute(
        f"SELECT content_id, title, status, meta, source FROM contents {where_clause} ORDER BY title ASC LIMIT %s",
        (*query_params, per_page)
    )

    results = [process_row(row) for row in cursor.fetchall()]
    cursor.close()

    next_cursor = None
    if len(results) == per_page:
        next_cursor = results[-1]['title']

    return jsonify({
        'contents': results,
        'next_cursor': next_cursor
    })

@contents_bp.route('/api/contents/completed', methods=['GET'])
def get_completed_contents():
    """[페이지네이션] 완결된 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    last_title = request.args.get('last_title')
    per_page = 100
    content_type = request.args.get('type', 'webtoon')

    conn = get_db()
    cursor = get_cursor(conn)

    query_params = [content_type]
    where_clause = "WHERE status = '완결' AND content_type = %s"

    if last_title:
        where_clause += " AND title > %s"
        query_params.append(last_title)

    cursor.execute(
        f"SELECT content_id, title, status, meta, source FROM contents {where_clause} ORDER BY title ASC LIMIT %s",
        (*query_params, per_page)
    )

    results = [process_row(row) for row in cursor.fetchall()]
    cursor.close()

    next_cursor = None
    if len(results) == per_page:
        next_cursor = results[-1]['title']

    return jsonify({
        'contents': results,
        'next_cursor': next_cursor
    })
