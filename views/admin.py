# views/admin.py

import json

from flask import Blueprint, jsonify, request, g, render_template

from database import get_db, get_cursor
from services.admin_override_service import upsert_override_and_record_event
from services.admin_publication_service import (
    delete_publication,
    list_publications,
    upsert_publication,
)
from services.admin_audit_service import insert_admin_action_log
from services.admin_delete_service import (
    list_deleted_contents,
    restore_content,
    soft_delete_content,
)
from services.cdc_event_service import record_content_published_event
from utils.auth import admin_required, login_required
from utils.time import now_kst_naive, parse_iso_naive_kst


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


def _serialize_publication(row):
    return {
        'id': row['id'],
        'content_id': row['content_id'],
        'source': row['source'],
        'public_at': row['public_at'].isoformat() if row['public_at'] else None,
        'reason': row['reason'],
        'admin_id': row['admin_id'],
        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
        'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None,
        'title': _get_row_value(row, 'title'),
        'content_type': _get_row_value(row, 'content_type'),
        'status': _get_row_value(row, 'status'),
        'meta': _normalize_meta(_get_row_value(row, 'meta')),
        'is_deleted': _get_row_value(row, 'is_deleted'),
    }


def _serialize_deleted_content(row):
    return {
        'content_id': row['content_id'],
        'source': row['source'],
        'content_type': row['content_type'],
        'title': row['title'],
        'status': row['status'],
        'is_deleted': row['is_deleted'],
        'meta': _normalize_meta(_get_row_value(row, 'meta')),
        'deleted_at': row['deleted_at'].isoformat() if row['deleted_at'] else None,
        'deleted_reason': row['deleted_reason'],
        'deleted_by': row['deleted_by'],
        'override_status': _get_row_value(row, 'override_status'),
        'override_completed_at': (
            _get_row_value(row, 'override_completed_at').isoformat()
            if _get_row_value(row, 'override_completed_at')
            else None
        ),
        'subscription_count': int(_get_row_value(row, 'subscription_count') or 0),
    }


def _serialize_audit_log(row):
    return {
        'id': row['id'],
        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
        'action_type': row['action_type'],
        'reason': row['reason'],
        'admin_id': row['admin_id'],
        'admin_email': _get_row_value(row, 'admin_email'),
        'content_id': row['content_id'],
        'source': row['source'],
        'title': _get_row_value(row, 'title'),
        'content_type': _get_row_value(row, 'content_type'),
        'status': _get_row_value(row, 'status'),
        'meta': _normalize_meta(_get_row_value(row, 'meta')),
        'is_deleted': _get_row_value(row, 'is_deleted'),
        'payload': _get_row_value(row, 'payload'),
    }


def _serialize_missing_content(row):
    return {
        'content_id': row['content_id'],
        'source': row['source'],
        'title': row['title'],
        'content_type': row['content_type'],
        'status': row['status'],
        'meta': _normalize_meta(_get_row_value(row, 'meta')),
        'created_at': row['created_at'].isoformat() if row['created_at'] else None,
        'updated_at': row['updated_at'].isoformat() if row['updated_at'] else None,
        'override_status': _get_row_value(row, 'override_status'),
        'override_completed_at': row['override_completed_at'].isoformat()
        if _get_row_value(row, 'override_completed_at')
        else None,
    }


def _serialize_cdc_event(row):
    return {
        'id': row['id'],
        'created_at': row['created_at'].isoformat() if row.get('created_at') else None,
        'content_id': row['content_id'],
        'source': row['source'],
        'event_type': row['event_type'],
        'final_status': row['final_status'],
        'final_completed_at': row['final_completed_at'].isoformat()
        if row.get('final_completed_at')
        else None,
        'resolved_by': row['resolved_by'],
        'title': _get_row_value(row, 'title'),
        'content_type': _get_row_value(row, 'content_type'),
        'status': _get_row_value(row, 'status'),
        'meta': _normalize_meta(_get_row_value(row, 'meta')),
        'is_deleted': bool(_get_row_value(row, 'is_deleted'))
        if _get_row_value(row, 'is_deleted') is not None
        else None,
    }


def _serialize_daily_crawler_report(row):
    report_data = _get_row_value(row, 'report_data') or {}
    if isinstance(report_data, str):
        try:
            report_data = json.loads(report_data)
        except Exception:
            report_data = {}
    return {
        'id': row['id'],
        'crawler_name': row['crawler_name'],
        'status': row['status'],
        'report_data': report_data,
        'created_at': row['created_at'].isoformat() if row.get('created_at') else None,
    }


def _serialize_final_state(state):
    if not state:
        return state

    final_completed_at = state.get('final_completed_at')
    serialized = dict(state)
    if hasattr(final_completed_at, 'isoformat'):
        serialized['final_completed_at'] = final_completed_at.isoformat()
    return serialized


def _get_row_value(row, key):
    try:
        return row[key]
    except Exception:
        return None


def _normalize_meta(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except Exception:
        return {}


@admin_bp.route('/admin', methods=['GET'])
def admin_page():
    return render_template('admin.html')


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
    try:
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
            if hasattr(conn, 'rollback'):
                conn.rollback()
            return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='OVERRIDE_UPSERT',
            content_id=content_id,
            source=source,
            reason=reason,
            payload={
                'override': _serialize_override(result['override']),
                'event_recorded': result.get('event_recorded', False),
            },
        )
        conn.commit()

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
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


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
    data = request.get_json(silent=True) or {}
    content_id = data.get('content_id')
    source = data.get('source')
    reason = data.get('reason') or request.args.get('reason')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')
    if not isinstance(reason, str) or not reason.strip():
        return _error_response(400, 'INVALID_REQUEST', 'reason is required')

    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            "DELETE FROM admin_content_overrides WHERE content_id = %s AND source = %s",
            (content_id, source),
        )
        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='OVERRIDE_DELETE',
            content_id=content_id,
            source=source,
            reason=reason,
            payload={'deleted': True},
        )
        conn.commit()

        return jsonify({'success': True})
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise
    finally:
        cursor.close()


@admin_bp.route('/api/admin/contents/publication', methods=['POST'])
@login_required
@admin_required
def upsert_content_publication():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')
    public_at_raw = data.get('public_at')
    reason = data.get('reason')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')

    public_at = None
    if public_at_raw not in (None, ''):
        public_at = parse_iso_naive_kst(public_at_raw)
        if public_at is None:
            return _error_response(400, 'INVALID_REQUEST', 'public_at must be a valid ISO 8601 datetime string')

    conn = get_db()
    try:
        result = upsert_publication(
            conn,
            admin_id=g.current_user['id'],
            content_id=content_id,
            source=source,
            public_at=public_at,
            reason=reason,
        )

        if result.get('error') == 'CONTENT_NOT_FOUND':
            if hasattr(conn, 'rollback'):
                conn.rollback()
            return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

        publication = result.get('publication') or {}
        public_at = publication.get('public_at')
        event_due_now = public_at is not None and public_at <= now_kst_naive()
        event_recorded = False
        event_inserted = False
        event_skipped_reason = None

        if event_due_now:
            cursor = None
            try:
                cursor = get_cursor(conn)
                cursor.execute(
                    """
                    SELECT 1 FROM contents
                    WHERE content_id = %s
                      AND source = %s
                      AND COALESCE(is_deleted, FALSE) = FALSE
                    """,
                    (content_id, source),
                )
                content_active = cursor.fetchone() is not None
            finally:
                if cursor:
                    cursor.close()

            if content_active:
                event_inserted = record_content_published_event(
                    conn,
                    content_id=content_id,
                    source=source,
                    public_published_at=public_at,
                    resolved_by="publication",
                )
                event_recorded = True
            else:
                event_skipped_reason = "CONTENT_DELETED"

        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='PUBLICATION_UPSERT',
            content_id=content_id,
            source=source,
            reason=reason,
            payload={
                'public_at': public_at.isoformat() if public_at else None,
                'event_due_now': event_due_now,
                'event_recorded': event_recorded,
                'event_inserted': event_inserted,
                'event_skipped_reason': event_skipped_reason,
            },
        )
        conn.commit()

        return jsonify(
            {
                'success': True,
                'publication': _serialize_publication(result['publication']),
                'event_due_now': event_due_now,
                'event_recorded': event_recorded,
                'event_inserted': event_inserted,
                'event_skipped_reason': event_skipped_reason,
            }
        )
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/contents/publication', methods=['DELETE'])
@login_required
@admin_required
def delete_content_publication():
    data = request.get_json(silent=True) or {}
    content_id = data.get('content_id')
    source = data.get('source')
    reason = data.get('reason') or request.args.get('reason')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')
    if not isinstance(reason, str) or not reason.strip():
        return _error_response(400, 'INVALID_REQUEST', 'reason is required')

    conn = get_db()
    try:
        delete_publication(conn, content_id=content_id, source=source)
        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='PUBLICATION_DELETE',
            content_id=content_id,
            source=source,
            reason=reason,
            payload={'deleted': True},
        )
        conn.commit()

        return jsonify({'success': True})
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/contents/publications', methods=['GET'])
@login_required
@admin_required
def list_content_publications():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    conn = get_db()
    publications = list_publications(conn, limit=limit, offset=offset)

    return jsonify(
        {
            'success': True,
            'publications': [_serialize_publication(row) for row in publications],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/cdc/events', methods=['GET'])
@login_required
@admin_required
def list_cdc_events():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    query = request.args.get('q')
    query = query.strip() if isinstance(query, str) and query.strip() else None
    event_type = request.args.get('event_type')
    source = request.args.get('source')
    content_id = request.args.get('content_id')

    created_from_raw = request.args.get('created_from')
    created_to_raw = request.args.get('created_to')
    created_from = None
    created_to = None
    if created_from_raw:
        created_from = parse_iso_naive_kst(created_from_raw)
        if created_from is None:
            return _error_response(400, 'INVALID_REQUEST', 'created_from must be a valid ISO 8601 datetime string')
    if created_to_raw:
        created_to = parse_iso_naive_kst(created_to_raw)
        if created_to is None:
            return _error_response(400, 'INVALID_REQUEST', 'created_to must be a valid ISO 8601 datetime string')

    params = []
    sql = """
        SELECT
            e.id,
            e.content_id,
            e.source,
            e.event_type,
            e.final_status,
            e.final_completed_at,
            e.resolved_by,
            e.created_at,
            c.title,
            c.content_type,
            c.status,
            c.meta,
            COALESCE(c.is_deleted, FALSE) AS is_deleted
        FROM cdc_events e
        LEFT JOIN contents c
          ON c.content_id = e.content_id AND c.source = e.source
        WHERE 1=1
    """

    if event_type:
        sql += " AND e.event_type = %s"
        params.append(event_type)
    if source:
        sql += " AND e.source = %s"
        params.append(source)
    if content_id:
        sql += " AND e.content_id = %s"
        params.append(content_id)
    if created_from:
        sql += " AND e.created_at >= %s"
        params.append(created_from)
    if created_to:
        sql += " AND e.created_at <= %s"
        params.append(created_to)
    if query:
        sql += " AND (c.title ILIKE %s OR e.content_id ILIKE %s)"
        params.extend([f"%{query}%", f"%{query}%"])

    sql += """
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    cursor.close()

    return jsonify(
        {
            'success': True,
            'events': [_serialize_cdc_event(row) for row in rows],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/reports/daily-crawler', methods=['GET'])
@login_required
@admin_required
def list_daily_crawler_reports():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    crawler_name = request.args.get('crawler_name')
    status = request.args.get('status')
    created_from_raw = request.args.get('created_from')
    created_to_raw = request.args.get('created_to')
    created_from = None
    created_to = None
    if created_from_raw:
        created_from = parse_iso_naive_kst(created_from_raw)
        if created_from is None:
            return _error_response(400, 'INVALID_REQUEST', 'created_from must be a valid ISO 8601 datetime string')
    if created_to_raw:
        created_to = parse_iso_naive_kst(created_to_raw)
        if created_to is None:
            return _error_response(400, 'INVALID_REQUEST', 'created_to must be a valid ISO 8601 datetime string')

    params = []
    sql = """
        SELECT id, crawler_name, status, report_data, created_at
        FROM daily_crawler_reports
        WHERE 1=1
    """

    if crawler_name:
        sql += " AND crawler_name = %s"
        params.append(crawler_name)
    if status:
        sql += " AND status = %s"
        params.append(status)
    if created_from:
        sql += " AND created_at >= %s"
        params.append(created_from)
    if created_to:
        sql += " AND created_at <= %s"
        params.append(created_to)

    sql += """
        ORDER BY created_at DESC, id DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    cursor.close()

    return jsonify(
        {
            'success': True,
            'reports': [_serialize_daily_crawler_report(row) for row in rows],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/contents/missing-completion', methods=['GET'])
@login_required
@admin_required
def list_missing_completion_contents():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    source = request.args.get('source')
    if not source or source == 'all':
        source = None
    content_type = request.args.get('content_type')
    if not content_type or content_type == 'all':
        content_type = None
    query = request.args.get('q')
    query = query.strip() if isinstance(query, str) and query.strip() else None

    params = []
    sql = """
        SELECT
            c.content_id,
            c.source,
            c.title,
            c.content_type,
            c.status,
            c.meta,
            c.created_at,
            c.updated_at,
            o.override_status,
            o.override_completed_at
        FROM contents c
        LEFT JOIN admin_content_overrides o
          ON o.content_id = c.content_id AND o.source = c.source
        WHERE COALESCE(c.is_deleted, FALSE) = FALSE
          AND (
            (o.id IS NULL AND c.status = '완결')
            OR (o.override_status = '완결' AND o.override_completed_at IS NULL)
          )
    """

    if source:
        sql += " AND c.source = %s"
        params.append(source)
    if content_type:
        sql += " AND c.content_type = %s"
        params.append(content_type)
    if query:
        sql += " AND c.title ILIKE %s"
        params.append(f"%{query}%")

    sql += """
        ORDER BY c.updated_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    cursor.close()

    return jsonify(
        {
            'success': True,
            'items': [_serialize_missing_content(row) for row in rows],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/contents/missing-publication', methods=['GET'])
@login_required
@admin_required
def list_missing_publication_contents():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    source = request.args.get('source')
    if not source or source == 'all':
        source = None
    content_type = request.args.get('content_type')
    if not content_type or content_type == 'all':
        content_type = None
    query = request.args.get('q')
    query = query.strip() if isinstance(query, str) and query.strip() else None

    params = []
    sql = """
        SELECT
            c.content_id,
            c.source,
            c.title,
            c.content_type,
            c.status,
            c.meta,
            c.created_at,
            c.updated_at
        FROM contents c
        LEFT JOIN admin_content_metadata m
          ON m.content_id = c.content_id AND m.source = c.source
        WHERE COALESCE(c.is_deleted, FALSE) = FALSE
          AND m.public_at IS NULL
    """

    if source:
        sql += " AND c.source = %s"
        params.append(source)
    if content_type:
        sql += " AND c.content_type = %s"
        params.append(content_type)
    if query:
        sql += " AND c.title ILIKE %s"
        params.append(f"%{query}%")

    sql += """
        ORDER BY c.updated_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    cursor.close()

    return jsonify(
        {
            'success': True,
            'items': [_serialize_missing_content(row) for row in rows],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/contents/delete', methods=['POST'])
@login_required
@admin_required
def soft_delete_content_endpoint():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')
    reason = data.get('reason')

    if not content_id or not source or not reason:
        return _error_response(400, 'INVALID_REQUEST', 'content_id, source, and reason are required')

    conn = get_db()
    try:
        result = soft_delete_content(
            conn,
            admin_id=g.current_user['id'],
            content_id=content_id,
            source=source,
            reason=reason,
        )

        if result.get('error') == 'CONTENT_NOT_FOUND':
            if hasattr(conn, 'rollback'):
                conn.rollback()
            return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='CONTENT_DELETE',
            content_id=content_id,
            source=source,
            reason=reason,
            payload={'subscriptions_retained': True},
        )
        conn.commit()

        response = {
            'success': True,
            'content': _serialize_deleted_content(result['content']),
            'subscriptions_retained': result.get('subscriptions_retained', True),
        }
        return jsonify(response)
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/contents/restore', methods=['POST'])
@login_required
@admin_required
def restore_content_endpoint():
    data = request.get_json() or {}
    content_id = data.get('content_id')
    source = data.get('source')
    reason = data.get('reason')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')

    conn = get_db()
    try:
        result = restore_content(
            conn,
            content_id=content_id,
            source=source,
        )

        if result.get('error') == 'CONTENT_NOT_FOUND':
            if hasattr(conn, 'rollback'):
                conn.rollback()
            return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='CONTENT_RESTORE',
            content_id=content_id,
            source=source,
            reason=reason if isinstance(reason, str) and reason.strip() else None,
            payload={'restored': True},
        )
        conn.commit()

        return jsonify({'success': True, 'content': _serialize_deleted_content(result['content'])})
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/contents/deleted', methods=['GET'])
@login_required
@admin_required
def list_deleted_contents_endpoint():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    query = request.args.get('q')

    conn = get_db()
    deleted_contents = list_deleted_contents(conn, limit=limit, offset=offset, q=query)

    return jsonify(
        {
            'success': True,
            'deleted_contents': [_serialize_deleted_content(row) for row in deleted_contents],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/audit/logs', methods=['GET'])
@login_required
@admin_required
def list_admin_audit_logs():
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
    except ValueError:
        return _error_response(400, 'INVALID_REQUEST', 'limit and offset must be integers')

    limit = max(1, min(limit, 200))
    offset = max(0, offset)
    query = request.args.get('q')
    action_type = request.args.get('action_type')
    admin_id = request.args.get('admin_id')
    content_id = request.args.get('content_id')
    source = request.args.get('source')

    admin_id_value = None
    if admin_id:
        try:
            admin_id_value = int(admin_id)
        except ValueError:
            return _error_response(400, 'INVALID_REQUEST', 'admin_id must be an integer')

    params = []
    sql = """
        SELECT
            l.id,
            l.created_at,
            l.action_type,
            l.reason,
            l.admin_id,
            u.email AS admin_email,
            l.content_id,
            l.source,
            l.payload,
            c.title,
            c.content_type,
            c.status,
            c.meta,
            COALESCE(c.is_deleted, FALSE) AS is_deleted
        FROM admin_action_logs l
        LEFT JOIN users u ON u.id = l.admin_id
        LEFT JOIN contents c ON c.content_id = l.content_id AND c.source = l.source
        WHERE 1=1
    """

    if action_type:
        sql += " AND l.action_type = %s"
        params.append(action_type)
    if admin_id_value is not None:
        sql += " AND l.admin_id = %s"
        params.append(admin_id_value)
    if content_id:
        sql += " AND l.content_id = %s"
        params.append(content_id)
    if source:
        sql += " AND l.source = %s"
        params.append(source)
    if query:
        like_value = f"%{query}%"
        sql += " AND (c.title ILIKE %s OR l.content_id ILIKE %s)"
        params.extend([like_value, like_value])

    sql += """
        ORDER BY l.created_at DESC, l.id DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute(sql, tuple(params))
    logs = cursor.fetchall()
    cursor.close()

    return jsonify(
        {
            'success': True,
            'logs': [_serialize_audit_log(row) for row in logs],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/contents/lookup', methods=['GET'])
@login_required
@admin_required
def lookup_admin_content():
    content_id = request.args.get('content_id')
    source = request.args.get('source')

    if not content_id or not source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id and source are required')

    conn = get_db()
    cursor = get_cursor(conn)

    try:
        cursor.execute(
            """
            SELECT content_id, source, title, content_type, status, meta,
                   COALESCE(is_deleted, FALSE) AS is_deleted, created_at, updated_at
            FROM contents
            WHERE content_id = %s AND source = %s
            """,
            (content_id, source),
        )
        content_row = cursor.fetchone()
        if content_row is None:
            return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

        cursor.execute(
            """
            SELECT id, content_id, source, override_status, override_completed_at, reason,
                   admin_id, created_at, updated_at
            FROM admin_content_overrides
            WHERE content_id = %s AND source = %s
            """,
            (content_id, source),
        )
        override_row = cursor.fetchone()

        cursor.execute(
            """
            SELECT id, content_id, source, public_at, reason, admin_id, created_at, updated_at
            FROM admin_content_metadata
            WHERE content_id = %s AND source = %s
            """,
            (content_id, source),
        )
        publication_row = cursor.fetchone()
    finally:
        cursor.close()

    content = {
        'content_id': content_row['content_id'],
        'source': content_row['source'],
        'title': content_row['title'],
        'content_type': content_row['content_type'],
        'status': content_row['status'],
        'meta': _normalize_meta(_get_row_value(content_row, 'meta')),
        'is_deleted': content_row['is_deleted'],
        'created_at': content_row['created_at'].isoformat() if content_row['created_at'] else None,
        'updated_at': content_row['updated_at'].isoformat() if content_row['updated_at'] else None,
    }

    override = _serialize_override(override_row) if override_row else None
    publication = _serialize_publication(publication_row) if publication_row else None

    return jsonify({'success': True, 'content': content, 'override': override, 'publication': publication})
