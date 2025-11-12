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
    source = data.get('source')

    if not all([email, content_id, source]):
        return jsonify({'status': 'error', 'message': '이메일, 콘텐츠 ID, 소스가 필요합니다.'}), 400
    if not is_valid_email(email):
        return jsonify({'status': 'error', 'message': '올바른 이메일 형식이 아닙니다.'}), 400

    try:
        conn = get_db()
        cursor = get_cursor(conn)

        # [추가] 1. 구독 대상 콘텐츠가 DB에 실재하는지 확인
        cursor.execute(
            "SELECT 1 FROM contents WHERE content_id = %s AND source = %s",
            (str(content_id), source)
        )
        if cursor.fetchone() is None:
            # 존재하지 않는 콘텐츠에 대한 요청
            cursor.close()
            return jsonify({'status': 'error', 'message': '존재하지 않는 콘텐츠입니다.'}), 404

        # 2. (콘텐츠가 존재할 경우) 구독 정보 삽입
        cursor.execute(
            "INSERT INTO subscriptions (email, content_id, source) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (email, str(content_id), source)
        )
        conn.commit()
        cursor.close()
        return jsonify({'status': 'success', 'message': f'ID {content_id} ({source}) 구독 완료!'})
    except psycopg2.Error as e:
        conn.rollback()  # 오류 발생 시 롤백 추가
        cursor.close()
        return jsonify({'status': 'error', 'message': f'데이터베이스 오류: {e}'}), 500
    except Exception as e:
        conn.rollback()  # 예상치 못한 오류 발생 시 롤백 추가
        cursor.close()
        return jsonify({'status': 'error', 'message': f'서버 오류: {e}'}), 500
