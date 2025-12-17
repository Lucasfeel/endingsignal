# views/admin.py

from flask import Blueprint, jsonify, request, g

from database import get_db, get_cursor
from services.admin_override_service import upsert_override_and_record_event
from utils.auth import admin_required, login_required
from utils.time import parse_iso_naive_kst


admin_bp = Blueprint('admin', __name__)


def _error_response(status_code: int, code: str, message: str):
    return jsonify({'success': False, 'error': {'code': code, 'message': message}}), status_code


def _serialize_override(row):
    return {
        'id': row['id'],
        'content_id': row['content_id'],
        'source': row['source'],
        'override_status': row['override_status'],
        'override_completed_at': row['override_completed_at'].isoformat() if row['override_completed_at'] else None,
        'reason': row['reason'],
        'admin_id': row['admin_id'],
        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
        'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None,
    }


def _serialize_final_state(state):
    if not state:
        return state

    final_completed_at = state.get('final_completed_at')
    serialized = dict(state)
    if hasattr(final_completed_at, 'isoformat'):
        serialized['final_completed_at'] = final_completed_at.isoformat()
    return serialized


@admin_bp.route('/api/admin/contents/override', methods=['POST'])
@login_required
@admin_required
def upsert_content_override():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')
    override_status = data.get('override_status')
    override_completed_at_raw = data.get('override_completed_at')
    reason = data.get('reason')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')

    if not override_status:
        return _error_response(400, 'INVALID_REQUEST', 'override_status is required')

    override_completed_at = None
    if override_completed_at_raw is not None:
        override_completed_at = parse_iso_naive_kst(override_completed_at_raw)
        if override_completed_at is None:
            return _error_response(400, 'INVALID_REQUEST', 'override_completed_at must be a valid ISO 8601 datetime string')

    conn = get_db()
    result = upsert_override_and_record_event(
        conn,
        admin_id=g.current_user['id'],
        content_id=content_id,
        source=source,
        override_status=override_status,
        override_completed_at=override_completed_at,
        reason=reason,
    )

    if result.get('error') == 'CONTENT_NOT_FOUND':
        return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

    return jsonify(
        {
            'success': True,
            'override': _serialize_override(result['override']),
            'previous_final_state': _serialize_final_state(result.get('previous_final_state')),
            'new_final_state': _serialize_final_state(result.get('new_final_state')),
            'event_recorded': result.get('event_recorded', False),
            'is_scheduled_completion': result.get('final_state', {}).get('is_scheduled_completion'),
            'scheduled_completed_at': result.get('final_state', {}).get('scheduled_completed_at'),
            'final_state': result.get('final_state'),
        }
    )


@admin_bp.route('/api/admin/contents/overrides', methods=['GET'])
@login_required
@admin_required
def list_content_overrides():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute(
        """
        SELECT id, content_id, source, override_status, override_completed_at, reason, admin_id, created_at, updated_at
        FROM admin_content_overrides
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )

    overrides = [_serialize_override(row) for row in cursor.fetchall()]
    cursor.close()

    return jsonify({'success': True, 'overrides': overrides, 'limit': limit, 'offset': offset})


@admin_bp.route('/api/admin/contents/override', methods=['DELETE'])
@login_required
@admin_required
def delete_content_override():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')

    conn = get_db()
    cursor = get_cursor(conn)

    cursor.execute(
        "DELETE FROM admin_content_overrides WHERE content_id = %s AND source = %s",
        (content_id, source),
    )
    conn.commit()
    cursor.close()

    return jsonify({'success': True})
