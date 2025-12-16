import psycopg2
from flask import Blueprint, jsonify, request, g

from database import get_db, get_cursor
from utils.auth import login_required, admin_required, _error_response

admin_bp = Blueprint('admin', __name__)


def _content_exists(cursor, content_id: str, source: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM contents WHERE content_id = %s AND source = %s",
        (str(content_id), source),
    )
    return cursor.fetchone() is not None


@admin_bp.route('/api/admin/contents/override', methods=['POST'])
@login_required
@admin_required
def upsert_override():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')
    override_status = data.get('override_status')
    override_completed_at = data.get('override_completed_at')
    reason = data.get('reason')

    if not content_id or not source or not override_status:
        return _error_response(
            400,
            'INVALID_REQUEST',
            'content_id, source, override_status는 필수입니다.',
        )

    conn = get_db()
    cursor = get_cursor(conn)
    admin_id = g.current_user.get('id')

    try:
        if not _content_exists(cursor, content_id, source):
            return _error_response(404, 'CONTENT_NOT_FOUND', '존재하지 않는 콘텐츠입니다.')

        cursor.execute(
            """
            INSERT INTO admin_content_overrides (
                content_id,
                source,
                override_status,
                override_completed_at,
                reason,
                admin_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_id, source) DO UPDATE SET
                override_status = EXCLUDED.override_status,
                override_completed_at = EXCLUDED.override_completed_at,
                reason = EXCLUDED.reason,
                admin_id = EXCLUDED.admin_id,
                updated_at = NOW()
            """,
            (
                str(content_id),
                source,
                override_status,
                override_completed_at,
                reason,
                admin_id,
            ),
        )
        conn.commit()
        return jsonify({'success': True}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()


@admin_bp.route('/api/admin/contents/overrides', methods=['GET'])
@login_required
@admin_required
def list_overrides():
    limit = request.args.get('limit', default=100, type=int)
    offset = request.args.get('offset', default=0, type=int)

    conn = get_db()
    cursor = get_cursor(conn)

    try:
        cursor.execute(
            """
            SELECT
                id,
                content_id,
                source,
                override_status,
                override_completed_at,
                reason,
                admin_id,
                created_at,
                updated_at
            FROM admin_content_overrides
            ORDER BY updated_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cursor.fetchall()
        return jsonify({'success': True, 'data': [dict(row) for row in rows]}), 200
    except psycopg2.Error:
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()


@admin_bp.route('/api/admin/contents/override', methods=['DELETE'])
@login_required
@admin_required
def delete_override():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id와 source는 필수입니다.')

    conn = get_db()
    cursor = get_cursor(conn)

    try:
        cursor.execute(
            "DELETE FROM admin_content_overrides WHERE content_id = %s AND source = %s",
            (str(content_id), source),
        )
        conn.commit()
        return jsonify({'success': True}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, 'DB_ERROR', '데이터베이스 오류가 발생했습니다.')
    finally:
        cursor.close()
