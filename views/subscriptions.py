# views/subscriptions.py

import psycopg2
from flask import Blueprint, jsonify, request, g

from database import get_db, get_cursor
from services.final_state_payload import build_final_state_payload
from utils.auth import login_required, _error_response
from utils.time import now_kst_naive

subscriptions_bp = Blueprint('subscriptions', __name__)


def _content_exists(cursor, content_id: str, source: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM contents WHERE content_id = %s AND source = %s",
        (str(content_id), source),
    )
    return cursor.fetchone() is not None


@subscriptions_bp.route('/api/me/subscriptions', methods=['GET'])
@login_required
def list_subscriptions():
    """현재 사용자 기준 구독 중인 콘텐츠를 조회합니다."""
    conn = get_db()
    cursor = get_cursor(conn)
    user_id = g.current_user.get('id')

    try:
        cursor.execute(
            """
            SELECT c.content_id, c.source, c.content_type, c.title, c.status, c.meta,
                   o.override_status, o.override_completed_at
            FROM subscriptions s
            JOIN contents c
                ON s.content_id = c.content_id AND s.source = c.source
            LEFT JOIN admin_content_overrides o
                ON o.content_id = c.content_id AND o.source = c.source
            WHERE s.user_id = %s
            ORDER BY c.title
            """,
            (user_id,),
        )
        rows = cursor.fetchall()
        effective_now = now_kst_naive()
        data = []
        for row in rows:
            row_dict = dict(row)
            override_status = row_dict.pop('override_status', None)
            override_completed_at = row_dict.pop('override_completed_at', None)
            override = None
            if override_status is not None or override_completed_at is not None:
                override = {
                    'override_status': override_status,
                    'override_completed_at': override_completed_at,
                }
            row_dict['final_state'] = build_final_state_payload(
                row_dict.get('status'), override, now=effective_now
            )
            data.append(row_dict)

        return jsonify({'success': True, 'data': data}), 200
    except psycopg2.Error:
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()


@subscriptions_bp.route('/api/me/subscriptions', methods=['POST'])
@login_required
def subscribe():
    """현재 사용자 기준 구독을 추가합니다."""
    data = request.get_json() or {}
    content_id = data.get('content_id') or data.get('contentId')
    source = data.get('source')

    if not content_id or not source:
        return _error_response(
            400, 'INVALID_REQUEST', 'content_id/contentId와 source는 필수입니다.',
        )

    conn = get_db()
    cursor = get_cursor(conn)
    user_id = g.current_user.get('id')
    user_email = g.current_user.get('email')

    try:
        if not _content_exists(cursor, content_id, source):
            return _error_response(404, 'CONTENT_NOT_FOUND', '존재하지 않는 콘텐츠입니다.')

        cursor.execute(
            """
            INSERT INTO subscriptions (user_id, email, content_id, source)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, content_id, source) DO NOTHING
            """,
            (user_id, user_email, str(content_id), source),
        )
        conn.commit()
        return jsonify({'success': True}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()


@subscriptions_bp.route('/api/me/subscriptions', methods=['DELETE'])
@login_required
def unsubscribe():
    """현재 사용자 기준 구독을 제거합니다."""
    data = request.get_json() or {}
    content_id = data.get('content_id') or data.get('contentId')
    source = data.get('source')

    if not content_id or not source:
        return _error_response(
            400, 'INVALID_REQUEST', 'content_id/contentId와 source는 필수입니다.',
        )

    conn = get_db()
    cursor = get_cursor(conn)
    user_id = g.current_user.get('id')

    try:
        cursor.execute(
            "DELETE FROM subscriptions WHERE user_id = %s AND content_id = %s AND source = %s",
            (user_id, str(content_id), source),
        )
        conn.commit()
        return jsonify({'success': True}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()
