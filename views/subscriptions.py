# views/subscriptions.py

import psycopg2
from typing import Optional
from flask import Blueprint, jsonify, request, g

from database import get_db, get_cursor
from services.final_state_payload import build_final_state_payload
from utils.auth import login_required, _error_response
from utils.time import now_kst_naive

subscriptions_bp = Blueprint('subscriptions', __name__)

ALERT_COMPLETION_COL = 'wants_completion'
ALERT_PUBLICATION_COL = 'wants_publication'


def _parse_alert_type(payload) -> Optional[str]:
    value = (payload.get('alert_type') or payload.get('alertType') or '').strip().lower()
    if not value:
        return 'completion'
    if value == 'completion':
        return 'completion'
    if value in ('publish', 'publication'):
        return 'publication'
    return None


def _content_exists(cursor, content_id: str, source: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM contents
        WHERE content_id = %s AND source = %s AND COALESCE(is_deleted, FALSE) = FALSE
        """,
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
                   s.wants_completion, s.wants_publication,
                   o.override_status, o.override_completed_at,
                   m.public_at AS public_at
            FROM subscriptions s
            JOIN contents c
                ON s.content_id = c.content_id AND s.source = c.source
            LEFT JOIN admin_content_overrides o
                ON o.content_id = c.content_id AND o.source = c.source
            LEFT JOIN admin_content_metadata m
                ON m.content_id = c.content_id AND m.source = c.source
            WHERE s.user_id = %s AND COALESCE(c.is_deleted, FALSE) = FALSE
            ORDER BY c.title
            """,
            (user_id,),
        )
        rows = cursor.fetchall()
        effective_now = now_kst_naive()
        data = []
        for row in rows:
            row_dict = dict(row)
            wants_completion = bool(row_dict.pop('wants_completion', False))
            wants_publication = bool(row_dict.pop('wants_publication', False))
            override_status = row_dict.pop('override_status', None)
            override_completed_at = row_dict.pop('override_completed_at', None)
            public_at = row_dict.pop('public_at', None)
            is_scheduled = bool(public_at is not None and effective_now < public_at)
            is_published = bool(public_at is not None and effective_now >= public_at)
            override = None
            if override_status is not None or override_completed_at is not None:
                override = {
                    'override_status': override_status,
                    'override_completed_at': override_completed_at,
                }
            row_dict['publication'] = {
                'public_at': public_at.isoformat() if public_at else None,
                'is_scheduled_publication': is_scheduled,
                'is_published': is_published,
            }
            row_dict['subscription'] = {
                'wants_completion': wants_completion,
                'wants_publication': wants_publication,
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
    alert_type = _parse_alert_type(data)

    if not content_id or not source:
        return _error_response(
            400, 'INVALID_REQUEST', 'content_id/contentId와 source는 필수입니다.',
        )
    if alert_type is None:
        return _error_response(
            400, 'INVALID_ALERT_TYPE', 'alert_type이 올바르지 않습니다.',
        )

    conn = get_db()
    cursor = get_cursor(conn)
    user_id = g.current_user.get('id')
    user_email = g.current_user.get('email')
    wants_completion = alert_type == 'completion'
    wants_publication = alert_type == 'publication'

    try:
        if not _content_exists(cursor, content_id, source):
            return _error_response(404, 'CONTENT_NOT_FOUND', '존재하지 않는 콘텐츠입니다.')

        cursor.execute(
            """
            INSERT INTO subscriptions (
                user_id, email, content_id, source, wants_completion, wants_publication
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, content_id, source)
            DO UPDATE SET
                wants_completion = subscriptions.wants_completion OR EXCLUDED.wants_completion,
                wants_publication = subscriptions.wants_publication OR EXCLUDED.wants_publication,
                email = COALESCE(subscriptions.email, EXCLUDED.email)
            RETURNING wants_completion, wants_publication
            """,
            (user_id, user_email, str(content_id), source, wants_completion, wants_publication),
        )
        updated_flags = cursor.fetchone()
        conn.commit()
        subscription_payload = None
        if updated_flags:
            subscription_payload = {
                'wants_completion': bool(updated_flags[0]),
                'wants_publication': bool(updated_flags[1]),
            }
        return jsonify({'success': True, 'subscription': subscription_payload}), 200
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
    alert_type = _parse_alert_type(data)

    if not content_id or not source:
        return _error_response(
            400, 'INVALID_REQUEST', 'content_id/contentId와 source는 필수입니다.',
        )
    if alert_type is None:
        return _error_response(
            400, 'INVALID_ALERT_TYPE', 'alert_type이 올바르지 않습니다.',
        )

    conn = get_db()
    cursor = get_cursor(conn)
    user_id = g.current_user.get('id')

    try:
        cursor.execute(
            """
            UPDATE subscriptions
            SET wants_completion = CASE
                    WHEN %s = 'completion' THEN FALSE
                    ELSE wants_completion
                END,
                wants_publication = CASE
                    WHEN %s = 'publication' THEN FALSE
                    ELSE wants_publication
                END
            WHERE user_id = %s AND content_id = %s AND source = %s
            RETURNING wants_completion, wants_publication
            """,
            (alert_type, alert_type, user_id, str(content_id), source),
        )
        updated_flags = cursor.fetchone()
        if not updated_flags:
            conn.commit()
            return jsonify({'success': True, 'subscription': None}), 200

        wants_completion = bool(updated_flags[0])
        wants_publication = bool(updated_flags[1])
        if not wants_completion and not wants_publication:
            cursor.execute(
                """
                DELETE FROM subscriptions
                WHERE user_id = %s AND content_id = %s AND source = %s
                """,
                (user_id, str(content_id), source),
            )
            conn.commit()
            return jsonify({'success': True, 'subscription': None}), 200

        conn.commit()
        return jsonify(
            {
                'success': True,
                'subscription': {
                    'wants_completion': wants_completion,
                    'wants_publication': wants_publication,
                },
            }
        ), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()
