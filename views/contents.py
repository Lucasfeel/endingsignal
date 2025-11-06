# views/contents.py

from flask import Blueprint, jsonify, request
from database import get_db, get_cursor
import math
import json

contents_bp = Blueprint('contents', __name__)

@contents_bp.route('/api/contents/search', methods=['GET'])
def search_contents():
    """전체 DB에서 콘텐츠 제목을 검색하여 결과를 반환합니다."""
    query = request.args.get('q', '').strip()
    content_type = request.args.get('type', 'webtoon')

    if not query:
        return jsonify([])

    query_no_spaces = query.replace(' ', '')

    conn = get_db()
    cursor = get_cursor(conn)

    search_pattern = f'%{query_no_spaces}%'
    cursor.execute(
        """
        SELECT content_id, title, status, meta
        FROM contents
        WHERE REPLACE(title, ' ', '') LIKE %s AND content_type = %s
        ORDER BY title
        LIMIT 100
        """,
        (search_pattern, content_type)
    )

    results = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    return jsonify(results)


@contents_bp.route('/api/contents/ongoing', methods=['GET'])
def get_ongoing_contents():
    """요일별 연재중인 콘텐츠 목록을 그룹화하여 반환합니다."""
    content_type = request.args.get('type', 'webtoon')

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute(
        "SELECT content_id, title, status, meta FROM contents WHERE content_type = %s AND status = '연재중'",
        (content_type,)
    )

    all_contents = [dict(row) for row in cursor.fetchall()]
    cursor.close()

    grouped_by_day = { 'mon': [], 'tue': [], 'wed': [], 'thu': [], 'fri': [], 'sat': [], 'sun': [], 'daily': [] }
    for content in all_contents:
        day_eng = content.get('meta', {}).get('weekday')
        if day_eng in grouped_by_day:
            grouped_by_day[day_eng].append(content)

    return jsonify(grouped_by_day)

@contents_bp.route('/api/contents/hiatus', methods=['GET'])
def get_hiatus_contents():
    """[페이지네이션] 휴재중인 콘텐츠 전체 목록을 페이지별로 반환합니다."""
    page = request.args.get('page', 1, type=int)
    per_page = 100
    offset = (page - 1) * per_page
    content_type = request.args.get('type', 'webtoon')

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute("SELECT COUNT(*) FROM contents WHERE status = '휴재' AND content_type = %s", (content_type,))
    total_items = cursor.fetchone()[0]
    total_pages = math.ceil(total_items / per_page)

    cursor.execute(
        "SELECT content_id, title, status, meta FROM contents WHERE status = '휴재' AND content_type = %s ORDER BY title LIMIT %s OFFSET %s",
        (content_type, per_page, offset)
    )

    results = [dict(row) for row in cursor.fetchall()]
    cursor.close()

    return jsonify({
        'contents': results,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_items': total_items
        }
    })

@contents_bp.route('/api/contents/completed', methods=['GET'])
def get_completed_contents():
    """[페이지네이션] 완결된 콘텐츠 목록을 페이지별로 반환합니다."""
    page = request.args.get('page', 1, type=int)
    per_page = 100
    offset = (page - 1) * per_page
    content_type = request.args.get('type', 'webtoon')

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute("SELECT COUNT(*) FROM contents WHERE status = '완결' AND content_type = %s", (content_type,))
    total_items = cursor.fetchone()[0]
    total_pages = math.ceil(total_items / per_page)

    cursor.execute(
        "SELECT content_id, title, status, meta FROM contents WHERE status = '완결' AND content_type = %s ORDER BY title LIMIT %s OFFSET %s",
        (content_type, per_page, offset)
    )

    results = [dict(row) for row in cursor.fetchall()]
    cursor.close()

    return jsonify({
        'contents': results,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_items': total_items
        }
    })
