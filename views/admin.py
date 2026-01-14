# views/admin.py

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta

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
from services.report_summary_service import (
    build_daily_summary,
    expand_status_filter,
    normalize_report_status,
)
from services.daily_notification_report_service import build_daily_notification_text
from utils.auth import admin_required, login_required
from utils.time import now_kst_naive, parse_iso_naive_kst


admin_bp = Blueprint('admin', __name__)


def _error_response(status_code: int, code: str, message: str):
    return jsonify({'success': False, 'error': {'code': code, 'message': message}}), status_code


@contextmanager
def managed_cursor(conn):
    cursor = get_cursor(conn)
    try:
        yield cursor
    finally:
        try:
            cursor.close()
        except Exception:
            pass


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
    created_at = _get_row_value(row, 'created_at')
    final_completed_at = _get_row_value(row, 'final_completed_at')
    return {
        'id': row['id'],
        'created_at': created_at.isoformat() if created_at else None,
        'content_id': row['content_id'],
        'source': row['source'],
        'event_type': row['event_type'],
        'final_status': row['final_status'],
        'final_completed_at': final_completed_at.isoformat()
        if final_completed_at
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
    created_at = _get_row_value(row, 'created_at')
    return {
        'id': row['id'],
        'crawler_name': row['crawler_name'],
        'status': row['status'],
        'normalized_status': normalize_report_status(row['status']),
        'report_data': report_data,
        'created_at': created_at.isoformat() if created_at else None,
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


def _parse_date_param(date_raw):
    if not date_raw:
        return None
    try:
        parsed = datetime.strptime(date_raw, '%Y-%m-%d')
    except ValueError:
        return None
    return date(parsed.year, parsed.month, parsed.day)


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
    with managed_cursor(conn) as cursor:
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
    with managed_cursor(conn) as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

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
        expanded_statuses = expand_status_filter(status)
        if expanded_statuses:
            sql += " AND status = ANY(%s)"
            params.append(expanded_statuses)
        else:
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
    with managed_cursor(conn) as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    return jsonify(
        {
            'success': True,
            'reports': [_serialize_daily_crawler_report(row) for row in rows],
            'limit': limit,
            'offset': offset,
        }
    )


@admin_bp.route('/api/admin/reports/daily-summary', methods=['GET'])
@login_required
@admin_required
def get_daily_crawler_summary():
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

    if not created_from and not created_to:
        now = now_kst_naive()
        created_from = datetime(now.year, now.month, now.day, 0, 0, 0)
        created_to = now

    params = []
    sql = """
        SELECT id, crawler_name, status, report_data, created_at
        FROM daily_crawler_reports
        WHERE 1=1
    """

    if created_from:
        sql += " AND created_at >= %s"
        params.append(created_from)
    if created_to:
        sql += " AND created_at <= %s"
        params.append(created_to)

    sql += " ORDER BY created_at DESC, id DESC"

    conn = get_db()
    with managed_cursor(conn) as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    items = [_serialize_daily_crawler_report(row) for row in rows]

    range_label = None
    if created_from and created_to:
        range_label = f"{created_from.isoformat()} ~ {created_to.isoformat()}"
    elif created_from:
        range_label = f"{created_from.isoformat()} ~ -"
    elif created_to:
        range_label = f"- ~ {created_to.isoformat()}"

    date_basis = created_to or created_from or now_kst_naive()
    date_label = date_basis.strftime('%Y-%m-%d')

    summary_payload = build_daily_summary(items, range_label, date_label)

    return jsonify(
        {
            'success': True,
            'range': {
                'created_from': created_from.isoformat() if created_from else None,
                'created_to': created_to.isoformat() if created_to else None,
            },
            'overall_status': summary_payload['overall_status'],
            'subject_text': summary_payload['subject_text'],
            'summary_text': summary_payload['summary_text'],
            'total_reports': len(items),
            'counts': summary_payload['counts'],
            'items': items,
        }
    )


@admin_bp.route('/api/admin/reports/daily-notification', methods=['GET'])
@login_required
@admin_required
def get_daily_notification_report():
    date_raw = request.args.get('date')
    include_deleted_raw = request.args.get('include_deleted', '0')
    include_deleted = str(include_deleted_raw).lower() in {'1', 'true', 'yes'}

    report_date = _parse_date_param(date_raw)
    if date_raw and report_date is None:
        return _error_response(400, 'INVALID_REQUEST', 'date must be in YYYY-MM-DD format')

    if report_date is None:
        today = now_kst_naive()
        report_date = date(today.year, today.month, today.day)

    start_dt = datetime(report_date.year, report_date.month, report_date.day, 0, 0, 0)
    end_dt = start_dt + timedelta(days=1)
    generated_at = now_kst_naive()

    conn = get_db()
    completed_items = []
    total_recipients = 0
    duration_seconds_total = 0.0
    duration_found = False
    new_contents_total = 0
    new_contents_by_type = {}

    with managed_cursor(conn) as cursor:
        cursor.execute(
            """
            SELECT report_data
            FROM daily_crawler_reports
            WHERE created_at >= %s AND created_at < %s
            """,
            (start_dt, end_dt),
        )
        report_rows = cursor.fetchall()
        for row in report_rows:
            report_data = _get_row_value(row, 'report_data')
            if isinstance(report_data, str):
                try:
                    report_data = json.loads(report_data)
                except Exception:
                    report_data = None
            if isinstance(report_data, dict):
                duration_value = report_data.get('duration')
                if isinstance(duration_value, (int, float)):
                    duration_seconds_total += float(duration_value)
                    duration_found = True

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM contents
            WHERE created_at >= %s AND created_at < %s
            """,
            (start_dt, end_dt),
        )
        total_row = cursor.fetchone()
        if total_row:
            new_contents_total = int(_get_row_value(total_row, 'total') or 0)

        cursor.execute(
            """
            SELECT content_type, COUNT(*) AS total
            FROM contents
            WHERE created_at >= %s AND created_at < %s
            GROUP BY content_type
            """,
            (start_dt, end_dt),
        )
        type_rows = cursor.fetchall()
        new_contents_by_type = {
            row['content_type']: int(_get_row_value(row, 'total') or 0) for row in type_rows
        }

        cursor.execute(
            """
            SELECT
                e.content_id,
                e.source,
                e.created_at AS event_created_at,
                e.final_completed_at,
                e.resolved_by,
                c.title,
                c.content_type,
                COALESCE(c.is_deleted, FALSE) AS is_deleted
            FROM cdc_events e
            LEFT JOIN contents c
              ON c.content_id = e.content_id AND c.source = e.source
            WHERE e.event_type = 'CONTENT_COMPLETED'
              AND e.created_at >= %s AND e.created_at < %s
            ORDER BY e.created_at DESC, e.id DESC
            """,
            (start_dt, end_dt),
        )
        completed_rows = cursor.fetchall()

        for row in completed_rows:
            is_deleted = bool(_get_row_value(row, 'is_deleted'))
            if is_deleted and not include_deleted:
                continue
            content_id = row['content_id']
            source = row['source']

            cursor.execute(
                """
                SELECT COUNT(*) AS subscriber_count
                FROM subscriptions
                WHERE content_id = %s AND source = %s AND wants_completion = TRUE
                """,
                (content_id, source),
            )
            subscriber_row = cursor.fetchone()
            subscriber_count = int(_get_row_value(subscriber_row, 'subscriber_count') or 0)

            if not is_deleted:
                total_recipients += subscriber_count

            title = _get_row_value(row, 'title') or content_id or '-'
            event_created_at = _get_row_value(row, 'event_created_at')
            final_completed_at = _get_row_value(row, 'final_completed_at')

            completed_items.append(
                {
                    'content_id': content_id,
                    'source': source,
                    'title': title,
                    'content_type': _get_row_value(row, 'content_type'),
                    'event_created_at': event_created_at.isoformat() if event_created_at else None,
                    'final_completed_at': final_completed_at.isoformat() if final_completed_at else None,
                    'resolved_by': _get_row_value(row, 'resolved_by'),
                    'is_deleted': is_deleted,
                    'subscriber_count': subscriber_count,
                    'notification_excluded': is_deleted,
                }
            )

    duration_seconds = duration_seconds_total if duration_found else None

    stats = {
        'duration_seconds': duration_seconds,
        'new_contents_total': new_contents_total,
        'new_contents_by_type': new_contents_by_type,
        'completed_total': len(completed_items),
        'total_recipients': total_recipients,
    }

    text_report = build_daily_notification_text(
        generated_at.isoformat(),
        stats,
        completed_items,
    )

    return jsonify(
        {
            'success': True,
            'date': report_date.isoformat(),
            'range': {'from': start_dt.isoformat(), 'to': end_dt.isoformat()},
            'generated_at': generated_at.isoformat(),
            'stats': stats,
            'completed_items': completed_items,
            'text_report': text_report,
        }
    )


@admin_bp.route('/api/admin/reports/daily-crawler/cleanup', methods=['POST'])
@login_required
@admin_required
def cleanup_daily_crawler_reports():
    payload = request.get_json() or {}
    keep_days_raw = payload.get('keep_days')
    keep_days = 14

    if keep_days_raw is not None:
        try:
            keep_days = int(keep_days_raw)
        except (TypeError, ValueError):
            return _error_response(400, 'INVALID_REQUEST', 'keep_days must be an integer')

    keep_days = max(1, min(keep_days, 365))
    cutoff = now_kst_naive() - timedelta(days=keep_days)

    conn = get_db()
    with managed_cursor(conn) as cursor:
        cursor.execute("DELETE FROM daily_crawler_reports WHERE created_at < %s", (cutoff,))
        deleted_count = cursor.rowcount
    conn.commit()

    return jsonify(
        {
            'success': True,
            'deleted_count': deleted_count,
            'cutoff': cutoff.isoformat(),
            'keep_days': keep_days,
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
    with managed_cursor(conn) as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

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
    with managed_cursor(conn) as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

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
    with managed_cursor(conn) as cursor:
        cursor.execute(sql, tuple(params))
        logs = cursor.fetchall()

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
