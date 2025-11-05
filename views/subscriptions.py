# views/subscriptions.py

import sqlite3
import re
from flask import Blueprint, jsonify, request
from database import get_db

subscriptions_bp = Blueprint('subscriptions', __name__)

def is_valid_email(email):
    """서버 단에서 이메일 형식의 유효성을 검증합니다."""
    return re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email)

@subscriptions_bp.route('/api/subscribe', methods=['POST'])
def subscribe():
    """사용자의 구독 요청을 처리합니다."""
    data = request.json
    email, title_id = data.get('email'), data.get('titleId')

    if not all([email, title_id]):
        return jsonify({'status': 'error', 'message': '이메일과 웹툰 ID가 필요합니다.'}), 400
    if not is_valid_email(email):
        return jsonify({'status': 'error', 'message': '올바른 이메일 형식이 아닙니다.'}), 400

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO subscriptions (email, title_id) VALUES (?, ?)", (email, str(title_id)))
        conn.commit()
        return jsonify({'status': 'success', 'message': f'ID {title_id} 구독 완료!'})
    except sqlite3.Error as e:
        return jsonify({'status': 'error', 'message': f'데이터베이스 오류: {e}'}), 500
