# views/admin.py

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from urllib.parse import urlparse
from uuid import uuid4

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
from utils.text import normalize_search_text
from utils.time import now_kst_naive, parse_iso_naive_kst


admin_bp = Blueprint('admin', __name__)


MANUAL_CONTENT_TYPE_MAP = {
    '웹툰': 'webtoon',
    '웹소설': 'novel',
    'OTT': 'ott',
}

MANUAL_CONTENT_SOURCE_MAP = {
    '네이버웹툰': 'naver_webtoon',
    '카카오웹툰': 'kakao_webtoon',
    '네이버 시리즈': 'naver_series',
    '카카오 페이지': 'kakao_page',
    '문피아': 'munpia',
    '리디': 'ridi',
    '넷플릭스': 'netflix',
    '티빙': 'tving',
    '디즈니 플러스': 'disney_plus',
    '웨이브': 'wavve',
    '라프텔': 'laftel',
}

MANUAL_CONTENT_STATUS_ONGOING = '연재중'
MANUAL_CONTENT_STATUS_COMPLETED = '완결'

MANUAL_CONTENT_L2_OPTIONS = {
    'webtoon': {
        'mon': {'id': 'mon', 'label': '월', 'attributes': {'weekdays': ['mon']}},
        'tue': {'id': 'tue', 'label': '화', 'attributes': {'weekdays': ['tue']}},
        'wed': {'id': 'wed', 'label': '수', 'attributes': {'weekdays': ['wed']}},
        'thu': {'id': 'thu', 'label': '목', 'attributes': {'weekdays': ['thu']}},
        'fri': {'id': 'fri', 'label': '금', 'attributes': {'weekdays': ['fri']}},
        'sat': {'id': 'sat', 'label': '토', 'attributes': {'weekdays': ['sat']}},
        'sun': {'id': 'sun', 'label': '일', 'attributes': {'weekdays': ['sun']}},
        'daily': {'id': 'daily', 'label': '매일', 'attributes': {'weekdays': ['daily']}},
    },
    'novel': {
        'fantasy': {'id': 'fantasy', 'label': '판타지', 'attributes': {'genres': ['fantasy']}},
        'hyeonpan': {'id': 'hyeonpan', 'label': '현판', 'attributes': {'genres': ['현판']}},
        'romance': {'id': 'romance', 'label': '로맨스', 'attributes': {'genres': ['romance']}},
        'romance_fantasy': {
            'id': 'romance_fantasy',
            'label': '로판',
            'attributes': {'genres': ['romance_fantasy']},
        },
        'light_novel': {
            'id': 'light_novel',
            'label': '라이트노벨',
            'attributes': {'genres': ['light_novel']},
        },
        'wuxia': {'id': 'wuxia', 'label': '무협', 'attributes': {'genres': ['wuxia']}},
        'bl': {'id': 'bl', 'label': 'BL', 'attributes': {'genres': ['bl']}},
    },
    'ott': {
        'drama': {'id': 'drama', 'label': '드라마', 'attributes': {'genres': ['drama']}},
        'anime': {'id': 'anime', 'label': '애니메이션', 'attributes': {'genres': ['anime']}},
        'variety': {'id': 'variety', 'label': '예능', 'attributes': {'genres': ['variety']}},
        'docu': {'id': 'docu', 'label': '다큐멘터리', 'attributes': {'genres': ['docu']}},
        'etc': {'id': 'etc', 'label': '기타', 'attributes': {'genres': ['etc']}},
        'completed': {
            'id': 'completed',
            'label': '완결',
            'status': MANUAL_CONTENT_STATUS_COMPLETED,
        },
    },
}


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


def _serialize_completion_change(row):
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


def _normalize_input_text(value):
    if not isinstance(value, str):
        return None
    normalized = ' '.join(value.strip().split())
    return normalized if normalized else None


def _parse_optional_http_url(value):
    if value is None:
        return None, True
    if not isinstance(value, str):
        return None, False

    normalized = value.strip()
    if not normalized:
        return None, True

    try:
        parsed = urlparse(normalized)
    except Exception:
        return None, False

    scheme = (parsed.scheme or '').lower()
    if scheme not in ('http', 'https') or not parsed.netloc:
        return None, False
    return normalized, True


def _parse_positive_int(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _normalize_l2_id(value):
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized = value.strip().lower()
    return normalized if normalized else None


def _resolve_manual_l2_option(content_type_value, l2_id):
    if not content_type_value or not l2_id:
        return None
    type_options = MANUAL_CONTENT_L2_OPTIONS.get(content_type_value)
    if not isinstance(type_options, dict):
        return None
    option = type_options.get(l2_id)
    return option if isinstance(option, dict) else None


def _copy_manual_l2_attributes(l2_option):
    if not isinstance(l2_option, dict):
        return None
    raw_attributes = l2_option.get('attributes')
    if not isinstance(raw_attributes, dict) or not raw_attributes:
        return None
    copied = {}
    for key, value in raw_attributes.items():
        if isinstance(value, list):
            copied[key] = [entry for entry in value]
        else:
            copied[key] = value
    return copied if copied else None


def _resolve_manual_content_status(l2_option):
    if isinstance(l2_option, dict):
        status = l2_option.get('status')
        if isinstance(status, str) and status.strip():
            return status.strip()
    return MANUAL_CONTENT_STATUS_ONGOING


def _parse_source_ids_payload(data):
    parsed_source_ids = []

    def _append_source_id(raw_value):
        source_id = _parse_positive_int(raw_value)
        if source_id is None or source_id in parsed_source_ids:
            return
        parsed_source_ids.append(source_id)

    raw_sources = data.get('sources')
    if isinstance(raw_sources, list):
        for entry in raw_sources:
            if isinstance(entry, dict):
                _append_source_id(
                    entry.get('sourceId') or entry.get('source_id') or entry.get('id')
                )
            else:
                _append_source_id(entry)

    raw_source_ids = data.get('sourceIds')
    if raw_source_ids is None:
        raw_source_ids = data.get('source_ids')
    if isinstance(raw_source_ids, list):
        for entry in raw_source_ids:
            _append_source_id(entry)

    _append_source_id(data.get('sourceId') or data.get('source_id'))
    return parsed_source_ids


def _serialize_content_type_option(row):
    return {
        'id': row['id'],
        'name': row['name'],
        'created_at': row['created_at'].isoformat() if _get_row_value(row, 'created_at') else None,
        'updated_at': row['updated_at'].isoformat() if _get_row_value(row, 'updated_at') else None,
    }


def _serialize_content_source_option(row):
    return {
        'id': row['id'],
        'type_id': row['type_id'],
        'name': row['name'],
        'created_at': row['created_at'].isoformat() if _get_row_value(row, 'created_at') else None,
        'updated_at': row['updated_at'].isoformat() if _get_row_value(row, 'updated_at') else None,
    }


def _serialize_content_row(row):
    return {
        'content_id': row['content_id'],
        'source': row['source'],
        'content_type': row['content_type'],
        'title': row['title'],
        'status': row['status'],
        'meta': _normalize_meta(_get_row_value(row, 'meta')),
        'created_at': row['created_at'].isoformat() if _get_row_value(row, 'created_at') else None,
        'updated_at': row['updated_at'].isoformat() if _get_row_value(row, 'updated_at') else None,
    }


@admin_bp.route('/admin', methods=['GET'])
@admin_bp.route('/admin/contents/new', methods=['GET'])
def admin_page():
    return render_template('admin.html')


@admin_bp.route('/api/admin/content-types', methods=['GET'])
@login_required
@admin_required
def list_content_types():
    conn = get_db()
    with managed_cursor(conn) as cursor:
        cursor.execute(
            """
            SELECT id, name, created_at, updated_at
            FROM content_types
            ORDER BY created_at ASC, id ASC
            """
        )
        rows = cursor.fetchall()

    return jsonify(
        {
            'success': True,
            'types': [_serialize_content_type_option(row) for row in rows],
        }
    )


@admin_bp.route('/api/admin/content-types', methods=['POST'])
@login_required
@admin_required
def create_content_type():
    data = request.get_json() or {}
    name = _normalize_input_text(data.get('name'))
    if not name:
        return _error_response(400, 'INVALID_REQUEST', 'name is required')

    conn = get_db()
    try:
        with managed_cursor(conn) as cursor:
            cursor.execute(
                """
                INSERT INTO content_types (name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                RETURNING id, name, created_at, updated_at
                """,
                (name,),
            )
            created_row = cursor.fetchone()

        if created_row is None:
            if hasattr(conn, 'rollback'):
                conn.rollback()
            return _error_response(409, 'DUPLICATE_CONTENT_TYPE', 'Content type already exists')

        conn.commit()
        return jsonify({'success': True, 'type': _serialize_content_type_option(created_row)}), 201
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/content-sources', methods=['GET'])
@login_required
@admin_required
def list_content_sources():
    type_id = _parse_positive_int(request.args.get('typeId') or request.args.get('type_id'))
    if type_id is None:
        return _error_response(400, 'INVALID_REQUEST', 'typeId is required')

    conn = get_db()
    with managed_cursor(conn) as cursor:
        cursor.execute("SELECT id FROM content_types WHERE id = %s", (type_id,))
        if cursor.fetchone() is None:
            return _error_response(400, 'INVALID_TYPE_ID', 'typeId does not exist')

        cursor.execute(
            """
            SELECT id, type_id, name, created_at, updated_at
            FROM content_sources
            WHERE type_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (type_id,),
        )
        rows = cursor.fetchall()

    return jsonify(
        {
            'success': True,
            'sources': [_serialize_content_source_option(row) for row in rows],
        }
    )


@admin_bp.route('/api/admin/content-sources', methods=['POST'])
@login_required
@admin_required
def create_content_source():
    data = request.get_json() or {}
    type_id = _parse_positive_int(data.get('typeId') or data.get('type_id'))
    name = _normalize_input_text(data.get('name'))

    if type_id is None:
        return _error_response(400, 'INVALID_REQUEST', 'typeId is required')
    if not name:
        return _error_response(400, 'INVALID_REQUEST', 'name is required')

    conn = get_db()
    try:
        with managed_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM content_types WHERE id = %s", (type_id,))
            if cursor.fetchone() is None:
                return _error_response(400, 'INVALID_TYPE_ID', 'typeId does not exist')

            cursor.execute(
                """
                INSERT INTO content_sources (type_id, name)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id, type_id, name, created_at, updated_at
                """,
                (type_id, name),
            )
            created_row = cursor.fetchone()

        if created_row is None:
            if hasattr(conn, 'rollback'):
                conn.rollback()
            return _error_response(
                409,
                'DUPLICATE_CONTENT_SOURCE',
                'Content source already exists for this type',
            )

        conn.commit()
        return jsonify({'success': True, 'source': _serialize_content_source_option(created_row)}), 201
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/contents', methods=['POST'])
@login_required
@admin_required
def create_admin_content():
    data = request.get_json() or {}
    title = _normalize_input_text(data.get('title'))
    type_id = _parse_positive_int(data.get('typeId') or data.get('type_id'))
    source_ids = _parse_source_ids_payload(data)
    l2_input = data.get('l2Id')
    if l2_input is None:
        l2_input = data.get('l2_id')
    if l2_input is None:
        l2_input = data.get('l2')
    l2_id = _normalize_l2_id(l2_input)
    author_name = _normalize_input_text(
        data.get('authorName') or data.get('author_name') or data.get('author')
    )
    content_url_input = data.get('contentUrl')
    if content_url_input is None:
        content_url_input = data.get('content_url')
    content_url, is_valid_content_url = _parse_optional_http_url(content_url_input)

    if not title:
        return _error_response(400, 'INVALID_REQUEST', 'title is required')
    if type_id is None:
        return _error_response(400, 'INVALID_REQUEST', 'typeId is required')
    if not source_ids:
        return _error_response(400, 'INVALID_REQUEST', 'sourceId/sourceIds/sources is required')
    if not is_valid_content_url:
        return _error_response(400, 'INVALID_REQUEST', 'contentUrl must be a valid http(s) URL')

    normalized_title_value = normalize_search_text(title)
    normalized_authors_value = normalize_search_text(author_name or "")

    conn = get_db()
    try:
        with managed_cursor(conn) as cursor:
            source_placeholders = ', '.join(['%s'] * len(source_ids))
            cursor.execute(
                f"""
                SELECT
                    s.id AS source_id,
                    s.name AS source_name,
                    t.id AS type_id,
                    t.name AS type_name
                FROM content_sources s
                JOIN content_types t
                  ON t.id = s.type_id
                WHERE s.id IN ({source_placeholders})
                """,
                tuple(source_ids),
            )
            source_rows = cursor.fetchall()
            source_row_by_id = {
                int(_get_row_value(row, 'source_id') or 0): row for row in source_rows
            }
            if len(source_row_by_id) != len(source_ids):
                return _error_response(400, 'INVALID_SOURCE_ID', 'sourceId does not exist')

            ordered_source_rows = []
            for requested_source_id in source_ids:
                row = source_row_by_id.get(int(requested_source_id))
                if row is None:
                    return _error_response(400, 'INVALID_SOURCE_ID', 'sourceId does not exist')
                source_type_id = int(_get_row_value(row, 'type_id') or 0)
                if source_type_id != type_id:
                    return _error_response(
                        400,
                        'SOURCE_TYPE_MISMATCH',
                        'sourceId does not belong to typeId',
                    )
                ordered_source_rows.append(row)

            first_source_row = ordered_source_rows[0]
            source_type_id = int(_get_row_value(first_source_row, 'type_id') or 0)
            type_name = _get_row_value(first_source_row, 'type_name')
            content_type_value = MANUAL_CONTENT_TYPE_MAP.get(type_name, type_name)
            l2_option = _resolve_manual_l2_option(content_type_value, l2_id)
            if l2_id and l2_option is None:
                return _error_response(400, 'INVALID_L2_ID', 'l2Id does not belong to typeId')
            manual_status_value = _resolve_manual_content_status(l2_option)

            normalized_sources = []
            seen_content_sources = set()
            for source_row in ordered_source_rows:
                source_name = _get_row_value(source_row, 'source_name')
                content_source_value = MANUAL_CONTENT_SOURCE_MAP.get(source_name, source_name)
                if content_source_value in seen_content_sources:
                    continue
                seen_content_sources.add(content_source_value)
                normalized_sources.append(
                    {
                        'id': int(_get_row_value(source_row, 'source_id') or 0),
                        'type_id': source_type_id,
                        'name': source_name,
                        'value': content_source_value,
                    }
                )

            duplicate_source_placeholders = ', '.join(['%s'] * len(normalized_sources))
            duplicate_params = [entry['value'] for entry in normalized_sources]
            duplicate_params.extend([content_type_value, title])

            cursor.execute(
                f"""
                SELECT source
                FROM contents
                WHERE source IN ({duplicate_source_placeholders})
                  AND content_type = %s
                  AND LOWER(TRIM(title)) = LOWER(TRIM(%s))
                  AND COALESCE(is_deleted, FALSE) = FALSE
                LIMIT 1
                """,
                tuple(duplicate_params),
            )
            if cursor.fetchone() is not None:
                return _error_response(
                    409,
                    'DUPLICATE_CONTENT',
                    'Content with same title, type, and source already exists',
                )

            manual_content_id = f"manual:{uuid4().hex}"
            common_payload = {}
            if content_url:
                common_payload['content_url'] = content_url
            if author_name:
                common_payload['authors'] = author_name

            created_rows = []
            for source_entry in normalized_sources:
                manual_meta_payload = {
                    'manual_registration': {
                        'type_id': source_type_id,
                        'type_name': type_name,
                        'source_id': source_entry['id'],
                        'source_name': source_entry['name'],
                        'admin_id': g.current_user.get('id'),
                    }
                }
                if l2_option:
                    manual_meta_payload['manual_registration']['l2_id'] = l2_option.get('id')
                    manual_meta_payload['manual_registration']['l2_label'] = l2_option.get('label')
                l2_attributes = _copy_manual_l2_attributes(l2_option)
                if l2_attributes:
                    manual_meta_payload['attributes'] = l2_attributes
                if common_payload:
                    manual_meta_payload['common'] = common_payload
                manual_meta = json.dumps(
                    manual_meta_payload,
                    ensure_ascii=False,
                )

                cursor.execute(
                    """
                    INSERT INTO contents (
                        content_id,
                        source,
                        content_type,
                        title,
                        status,
                        meta,
                        normalized_title,
                        normalized_authors
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    RETURNING content_id, source, content_type, title, status, meta, created_at, updated_at
                    """,
                    (
                        manual_content_id,
                        source_entry['value'],
                        content_type_value,
                        title,
                        manual_status_value,
                        manual_meta,
                        normalized_title_value,
                        normalized_authors_value,
                    ),
                )
                created_rows.append(cursor.fetchone())

        conn.commit()
        serialized_sources = [
            {
                'id': entry['id'],
                'type_id': entry['type_id'],
                'name': entry['name'],
            }
            for entry in normalized_sources
        ]
        serialized_contents = [_serialize_content_row(row) for row in created_rows]
        primary_source = serialized_sources[0]
        primary_content = serialized_contents[0]
        return (
            jsonify(
                {
                    'success': True,
                    'content': primary_content,
                    'contents': serialized_contents,
                    'content_type': {
                        'id': source_type_id,
                        'name': type_name,
                    },
                    'content_source': primary_source,
                    'content_sources': serialized_sources,
                    'sourceIds': [entry['id'] for entry in serialized_sources],
                    'sources': serialized_sources,
                }
            ),
            201,
        )
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


@admin_bp.route('/api/admin/contents/update', methods=['POST'])
@login_required
@admin_required
def update_admin_content():
    data = request.get_json() or {}
    content_id = data.get('content_id') or data.get('contentId')
    current_source = data.get('source') or data.get('currentSource')
    type_id = _parse_positive_int(data.get('typeId') or data.get('type_id'))
    source_id = _parse_positive_int(data.get('sourceId') or data.get('source_id'))

    has_title_input = any(key in data for key in ('title', 'contentTitle', 'content_title'))
    title_input = None
    if 'title' in data:
        title_input = data.get('title')
    elif 'contentTitle' in data:
        title_input = data.get('contentTitle')
    elif 'content_title' in data:
        title_input = data.get('content_title')
    title = _normalize_input_text(title_input) if has_title_input else None

    has_author_input = any(key in data for key in ('authorName', 'author_name', 'author'))
    author_input = None
    if 'authorName' in data:
        author_input = data.get('authorName')
    elif 'author_name' in data:
        author_input = data.get('author_name')
    elif 'author' in data:
        author_input = data.get('author')
    author_name = _normalize_input_text(author_input) if has_author_input else None

    has_content_url_input = 'contentUrl' in data or 'content_url' in data
    content_url_input = None
    if 'contentUrl' in data:
        content_url_input = data.get('contentUrl')
    elif 'content_url' in data:
        content_url_input = data.get('content_url')
    content_url, is_valid_content_url = _parse_optional_http_url(content_url_input)

    has_l2_input = any(key in data for key in ('l2Id', 'l2_id', 'l2'))
    l2_input = None
    if 'l2Id' in data:
        l2_input = data.get('l2Id')
    elif 'l2_id' in data:
        l2_input = data.get('l2_id')
    elif 'l2' in data:
        l2_input = data.get('l2')
    l2_id = _normalize_l2_id(l2_input) if has_l2_input else None

    if not content_id or not current_source:
        return _error_response(400, 'INVALID_REQUEST', 'content_id/contentId and source are required')
    if has_title_input and not title:
        return _error_response(400, 'INVALID_REQUEST', 'title is required')
    if type_id is None:
        return _error_response(400, 'INVALID_REQUEST', 'typeId is required')
    if source_id is None:
        return _error_response(400, 'INVALID_REQUEST', 'sourceId is required')
    if has_content_url_input and not is_valid_content_url:
        return _error_response(400, 'INVALID_REQUEST', 'contentUrl must be a valid http(s) URL')

    conn = get_db()
    try:
        with managed_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT
                    s.id AS source_id,
                    s.name AS source_name,
                    t.id AS type_id,
                    t.name AS type_name
                FROM content_sources s
                JOIN content_types t
                  ON t.id = s.type_id
                WHERE s.id = %s
                """,
                (source_id,),
            )
            source_row = cursor.fetchone()
            if source_row is None:
                return _error_response(400, 'INVALID_SOURCE_ID', 'sourceId does not exist')

            source_type_id = int(_get_row_value(source_row, 'type_id') or 0)
            if source_type_id != type_id:
                return _error_response(400, 'SOURCE_TYPE_MISMATCH', 'sourceId does not belong to typeId')

            type_name = _get_row_value(source_row, 'type_name')
            source_name = _get_row_value(source_row, 'source_name')
            content_type_value = MANUAL_CONTENT_TYPE_MAP.get(type_name, type_name)
            content_source_value = MANUAL_CONTENT_SOURCE_MAP.get(source_name, source_name)

            cursor.execute(
                """
                SELECT content_id, source, title, content_type, status, meta
                FROM contents
                WHERE content_id = %s
                  AND source = %s
                  AND COALESCE(is_deleted, FALSE) = FALSE
                """,
                (content_id, current_source),
            )
            existing_row = cursor.fetchone()
            if existing_row is None:
                return _error_response(404, 'CONTENT_NOT_FOUND', 'Content not found')

            current_title = _get_row_value(existing_row, 'title')
            next_title = title if has_title_input else current_title
            if not next_title:
                return _error_response(400, 'INVALID_REQUEST', 'title is required')
            existing_status = _get_row_value(existing_row, 'status')
            l2_option = _resolve_manual_l2_option(content_type_value, l2_id) if has_l2_input else None
            if has_l2_input and l2_id and l2_option is None:
                return _error_response(400, 'INVALID_L2_ID', 'l2Id does not belong to typeId')
            next_status = _resolve_manual_content_status(l2_option) if has_l2_input else existing_status

            cursor.execute(
                """
                SELECT 1
                FROM contents
                WHERE source = %s
                  AND content_type = %s
                  AND LOWER(TRIM(title)) = LOWER(TRIM(%s))
                  AND COALESCE(is_deleted, FALSE) = FALSE
                  AND NOT (content_id = %s AND source = %s)
                LIMIT 1
                """,
                (content_source_value, content_type_value, next_title, content_id, current_source),
            )
            if cursor.fetchone() is not None:
                return _error_response(
                    409,
                    'DUPLICATE_CONTENT',
                    'Content with same title, type, and source already exists',
                )

            if content_source_value != current_source:
                cursor.execute(
                    """
                    SELECT 1
                    FROM contents
                    WHERE content_id = %s
                      AND source = %s
                    LIMIT 1
                    """,
                    (content_id, content_source_value),
                )
                if cursor.fetchone() is not None:
                    return _error_response(
                        409,
                        'DUPLICATE_CONTENT',
                        'Content with same content_id and source already exists',
                    )

            meta_payload = _normalize_meta(_get_row_value(existing_row, 'meta'))
            if not isinstance(meta_payload, dict):
                meta_payload = {}
            common_payload = meta_payload.get('common')
            if not isinstance(common_payload, dict):
                common_payload = {}

            if has_author_input:
                if author_name:
                    common_payload['authors'] = author_name
                else:
                    common_payload.pop('authors', None)

            if has_content_url_input:
                if content_url:
                    common_payload['content_url'] = content_url
                else:
                    common_payload.pop('content_url', None)

            if common_payload:
                meta_payload['common'] = common_payload
            else:
                meta_payload.pop('common', None)

            manual_registration = meta_payload.get('manual_registration')
            supports_manual_l2 = isinstance(manual_registration, dict)
            if supports_manual_l2:
                manual_registration['type_id'] = source_type_id
                manual_registration['type_name'] = type_name
                manual_registration['source_id'] = int(_get_row_value(source_row, 'source_id') or 0)
                manual_registration['source_name'] = source_name
                if has_l2_input:
                    if l2_option:
                        manual_registration['l2_id'] = l2_option.get('id')
                        manual_registration['l2_label'] = l2_option.get('label')
                    else:
                        manual_registration.pop('l2_id', None)
                        manual_registration.pop('l2_label', None)
                meta_payload['manual_registration'] = manual_registration

            if has_l2_input and supports_manual_l2:
                l2_attributes = _copy_manual_l2_attributes(l2_option)
                attributes_payload = meta_payload.get('attributes')
                if not isinstance(attributes_payload, dict):
                    attributes_payload = {}
                attributes_payload.pop('weekdays', None)
                attributes_payload.pop('genres', None)
                if l2_attributes:
                    attributes_payload.update(l2_attributes)
                if attributes_payload:
                    meta_payload['attributes'] = attributes_payload
                else:
                    meta_payload.pop('attributes', None)

            updated_meta = json.dumps(meta_payload, ensure_ascii=False)
            set_parts = [
                "source = %s",
                "content_type = %s",
                "meta = %s::jsonb",
            ]
            update_params = [
                content_source_value,
                content_type_value,
                updated_meta,
            ]
            if has_title_input:
                set_parts.extend(["title = %s", "normalized_title = %s"])
                update_params.extend([next_title, normalize_search_text(next_title)])
            if has_l2_input and supports_manual_l2:
                set_parts.append("status = %s")
                update_params.append(next_status)
            if has_author_input:
                set_parts.append("normalized_authors = %s")
                update_params.append(normalize_search_text(author_name or ""))
            update_params.extend([content_id, current_source])

            cursor.execute(
                f"""
                UPDATE contents
                SET {', '.join(set_parts)}
                WHERE content_id = %s
                  AND source = %s
                RETURNING content_id, source, content_type, title, status, meta, created_at, updated_at
                """,
                tuple(update_params),
            )
            updated_row = cursor.fetchone()

            if content_source_value != current_source:
                cursor.execute(
                    """
                    UPDATE admin_content_overrides
                    SET source = %s
                    WHERE content_id = %s
                      AND source = %s
                    """,
                    (content_source_value, content_id, current_source),
                )
                cursor.execute(
                    """
                    UPDATE admin_content_metadata
                    SET source = %s
                    WHERE content_id = %s
                      AND source = %s
                    """,
                    (content_source_value, content_id, current_source),
                )
                cursor.execute(
                    """
                    UPDATE subscriptions
                    SET source = %s
                    WHERE content_id = %s
                      AND source = %s
                    """,
                    (content_source_value, content_id, current_source),
                )
                cursor.execute(
                    """
                    UPDATE cdc_events
                    SET source = %s
                    WHERE content_id = %s
                      AND source = %s
                    """,
                    (content_source_value, content_id, current_source),
                )

        insert_admin_action_log(
            conn,
            admin_id=g.current_user['id'],
            action_type='CONTENT_EDIT',
            content_id=content_id,
            source=content_source_value,
            reason=_normalize_input_text(data.get('reason')),
            payload={
                'previous_source': current_source,
                'new_source': content_source_value,
                'previous_content_type': _get_row_value(existing_row, 'content_type'),
                'new_content_type': content_type_value,
                'previous_title': current_title if has_title_input else None,
                'new_title': next_title if has_title_input else None,
                'previous_status': existing_status if has_l2_input and supports_manual_l2 else None,
                'new_status': next_status if has_l2_input and supports_manual_l2 else None,
                'updated_author': author_name if has_author_input else None,
                'updated_content_url': content_url if has_content_url_input else None,
                'updated_l2_id': l2_id if has_l2_input and supports_manual_l2 else None,
            },
        )
        conn.commit()
        return jsonify(
            {
                'success': True,
                'content': _serialize_content_row(updated_row),
                'content_type': {
                    'id': source_type_id,
                    'name': type_name,
                },
                'content_source': {
                    'id': int(_get_row_value(source_row, 'source_id') or 0),
                    'type_id': source_type_id,
                    'name': source_name,
                },
                'previous': {
                    'content_id': content_id,
                    'source': current_source,
                },
            }
        )
    except Exception:
        if hasattr(conn, 'rollback'):
            conn.rollback()
        raise


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


@admin_bp.route('/api/admin/contents/completion-changes', methods=['GET'])
@login_required
@admin_required
def list_content_completion_changes():
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
            SELECT
                o.id,
                o.content_id,
                o.source,
                o.override_status,
                o.override_completed_at,
                o.reason,
                o.admin_id,
                o.created_at,
                o.updated_at,
                c.title,
                c.content_type,
                c.status,
                c.meta,
                COALESCE(c.is_deleted, FALSE) AS is_deleted
            FROM admin_content_overrides o
            LEFT JOIN contents c
              ON c.content_id = o.content_id AND c.source = o.source
            WHERE o.override_status = %s
              AND o.override_completed_at IS NOT NULL
            ORDER BY o.updated_at DESC, o.id DESC
            LIMIT %s OFFSET %s
            """,
            ('\uc644\uacb0', limit, offset),
        )
        rows = cursor.fetchall()

    return jsonify(
        {
            'success': True,
            'changes': [_serialize_completion_change(row) for row in rows],
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
          AND COALESCE(
            m.public_at,
            CASE
              WHEN c.content_type = 'webtoon' THEN c.created_at
              ELSE NULL
            END
          ) IS NULL
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

    if publication_row is None and content_row['content_type'] == 'webtoon':
        publication_row = {
            'id': None,
            'content_id': content_row['content_id'],
            'source': content_row['source'],
            'public_at': content_row['created_at'],
            'reason': 'auto_from_created_at',
            'admin_id': None,
            'created_at': content_row['created_at'],
            'updated_at': content_row['updated_at'],
            'title': content_row['title'],
            'content_type': content_row['content_type'],
            'status': content_row['status'],
            'meta': _normalize_meta(_get_row_value(content_row, 'meta')),
            'is_deleted': content_row['is_deleted'],
        }

    override = _serialize_override(override_row) if override_row else None
    publication = _serialize_publication(publication_row) if publication_row else None

    return jsonify({'success': True, 'content': content, 'override': override, 'publication': publication})
