# views/webtoons.py

from flask import Blueprint, jsonify, request
from database import get_db
import math

webtoons_bp = Blueprint('webtoons', __name__)

@webtoons_bp.route('/api/search', methods=['GET'])
def search_webtoons():
    """전체 DB에서 웹툰 제목을 검색하여 결과를 반환합니다."""
    query = request.args.get('q', '').strip()

    if not query:
        return jsonify([])

    query_no_spaces = query.replace(' ', '')

    conn = get_db()
    cursor = conn.cursor()

    search_pattern = f'%{query_no_spaces}%'
    cursor.execute(
        """
        SELECT title_id, title_text, author, status
        FROM webtoons
        WHERE REPLACE(title_text, ' ', '') LIKE ?
        ORDER BY rowid DESC
        LIMIT 100
        """,
        (search_pattern,)
    )

    search_results = [dict(row) for row in cursor.fetchall()]

    return jsonify(search_results)


@webtoons_bp.route('/api/webtoons/ongoing', methods=['GET'])
def get_ongoing_webtoons():
    """요일별 웹툰 목록(연재 및 단기 휴재 포함)을 그룹화하여 반환합니다."""
    conn = get_db()
    cursor = conn.cursor()

    valid_weekdays = ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'daily')
    query = f"SELECT title_id, title_text, author, weekday, status FROM webtoons WHERE weekday IN {valid_weekdays}"
    cursor.execute(query)

    all_daily_webtoons = [dict(row) for row in cursor.fetchall()]

    grouped_by_day = { 'mon': [], 'tue': [], 'wed': [], 'thu': [], 'fri': [], 'sat': [], 'sun': [], 'daily': [] }
    for webtoon in all_daily_webtoons:
        day_eng = webtoon.get('weekday')
        if day_eng in grouped_by_day:
            grouped_by_day[day_eng].append(webtoon)

    return jsonify(grouped_by_day)

@webtoons_bp.route('/api/webtoons/hiatus', methods=['GET'])
def get_hiatus_webtoons():
    """[페이지네이션] 휴재중인 웹툰 전체 목록을 페이지별로 반환합니다."""
    page = request.args.get('page', 1, type=int)
    per_page = 100
    offset = (page - 1) * per_page

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM webtoons WHERE status = '휴재'")
    total_items = cursor.fetchone()[0]
    total_pages = math.ceil(total_items / per_page)

    cursor.execute(
        "SELECT title_id, title_text, author, status FROM webtoons WHERE status = '휴재' ORDER BY rowid DESC LIMIT ? OFFSET ?",
        (per_page, offset)
    )
    hiatus_webtoons = [dict(row) for row in cursor.fetchall()]

    return jsonify({
        'webtoons': hiatus_webtoons,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_items': total_items
        }
    })

@webtoons_bp.route('/api/webtoons/completed', methods=['GET'])
def get_completed_webtoons():
    """[페이지네이션] 완결된 웹툰 목록을 페이지별로 반환합니다."""
    page = request.args.get('page', 1, type=int)
    per_page = 100
    offset = (page - 1) * per_page

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM webtoons WHERE status = '완결'")
    total_items = cursor.fetchone()[0]
    total_pages = math.ceil(total_items / per_page)

    cursor.execute(
        "SELECT title_id, title_text, author, status FROM webtoons WHERE status = '완결' ORDER BY rowid DESC LIMIT ? OFFSET ?",
        (per_page, offset)
    )
    completed_webtoons = [dict(row) for row in cursor.fetchall()]

    return jsonify({
        'webtoons': completed_webtoons,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_items': total_items
        }
    })
