# views/subscriptions.py

import re
import psycopg2
from flask import Blueprint, jsonify, request
from database import get_db, get_cursor

subscriptions_bp = Blueprint('subscriptions', __name__)

def is_valid_email(email):
    """서버 단에서 이메일 형식의 유효성을 검증합니다."""
    return re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email)

@subscriptions_bp.route('/api/subscriptions', methods=['POST'])
def subscribe():
    """사용자의 구독 요청을 처리합니다."""
    data = request.json
    email = data.get('email')
    content_id = data.get('contentId')
    source = data.get('source', 'naver_webtoon')

    if not all([email, content_id, source]):
        return jsonify({'status': 'error', 'message': '이메일, 콘텐츠 ID, 소스가 필요합니다.'}), 400
    if not is_valid_email(email):
        return jsonify({'status': 'error', 'message': '올바른 이메일 형식이 아닙니다.'}), 400

    try:
        conn = get_db()
        cursor = get_cursor(conn)
        cursor.execute(
            "INSERT INTO subscriptions (email, content_id, source) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (email, str(content_id), source)
        )
        conn.commit()
        cursor.close()
        return jsonify({'status': 'success', 'message': f'ID {content_id} ({source}) 구독 완료!'})
    except psycopg2.Error as e:
        return jsonify({'status': 'error', 'message': f'데이터베이스 오류: {e}'}), 500
