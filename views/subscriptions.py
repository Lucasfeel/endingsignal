# views/subscriptions.py

import psycopg2
from flask import Blueprint, jsonify, request, g

from database import get_db, get_cursor
from utils.auth import login_required, _error_response

subscriptions_bp = Blueprint('subscriptions', __name__)


def _content_exists(cursor, content_id: str, source: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM contents WHERE content_id = %s AND source = %s",
        (str(content_id), source),
    )
    return cursor.fetchone() is not None


def _extract_content_id(payload: dict):
    """Allow both camelCase and snake_case for backward compatibility."""
    if not payload:
        return None

    if payload.get('content_id') is not None:
        return payload.get('content_id')
    return payload.get('contentId')


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
            SELECT c.content_id, c.source, c.content_type, c.title, c.status, c.meta
            FROM subscriptions s
            JOIN contents c
                ON s.content_id = c.content_id AND s.source = c.source
            WHERE s.user_id = %s
            ORDER BY c.title
            """,
            (user_id,),
        )
        rows = cursor.fetchall()
        return jsonify({'success': True, 'data': [dict(row) for row in rows]}), 200
    except psycopg2.Error:
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()


@subscriptions_bp.route('/api/me/subscriptions', methods=['POST'])
@login_required
def subscribe():
    """현재 사용자 기준 구독을 추가합니다."""
    data = request.get_json() or {}
    content_id = _extract_content_id(data)
    source = data.get('source')

    if not content_id or not source:
        return _error_response(
            400, 'INVALID_REQUEST', 'content_id와 source는 필수입니다.',
        )

    conn = get_db()
    cursor = get_cursor(conn)
    user_id = g.current_user.get('id')

    try:
        if not _content_exists(cursor, content_id, source):
            return _error_response(404, 'CONTENT_NOT_FOUND', '존재하지 않는 콘텐츠입니다.')

        cursor.execute(
            """
            INSERT INTO subscriptions (user_id, content_id, source)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, content_id, source) DO NOTHING
            """,
            (user_id, str(content_id), source),
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
    content_id = _extract_content_id(data)
    source = data.get('source')

    if not content_id or not source:
        return _error_response(
            400, 'INVALID_REQUEST', 'content_id와 source는 필수입니다.',
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
