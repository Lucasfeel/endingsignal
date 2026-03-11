import psycopg2
from typing import Dict, Optional, Tuple

from flask import Blueprint, g, jsonify, request

from database import get_cursor, get_db
from services.final_state_payload import build_final_state_payload
from utils.auth import _error_response, login_required
from utils.content_keys import build_content_key, parse_content_key
from utils.time import now_kst_naive

subscriptions_bp = Blueprint("subscriptions", __name__)

ALERT_COMPLETION_COL = "wants_completion"
ALERT_PUBLICATION_COL = "wants_publication"


def _parse_alert_type(payload) -> Optional[str]:
    value = (payload.get("alert_type") or payload.get("alertType") or "").strip().lower()
    if not value:
        return "completion"
    if value == "completion":
        return "completion"
    return None


def _current_subject() -> Dict:
    user_key = g.current_user.get("user_key")
    return {
        "user_id": g.current_user.get("id"),
        "user_key": None if user_key in (None, "") else str(user_key),
        "email": g.current_user.get("email"),
    }


def _resolve_content_identity(payload=None, *, content_key: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    payload = payload or {}
    candidate_key = content_key or payload.get("contentKey") or payload.get("content_key")
    if candidate_key:
        return parse_content_key(candidate_key)
    content_id = payload.get("content_id") or payload.get("contentId")
    source = payload.get("source")
    return (str(content_id), str(source)) if content_id and source else (content_id, source)


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


def _serialize_subscription_row(row_dict, effective_now):
    wants_completion = bool(row_dict.pop("wants_completion", False))
    wants_publication = bool(row_dict.pop("wants_publication", False))
    override_status = row_dict.pop("override_status", None)
    override_completed_at = row_dict.pop("override_completed_at", None)
    public_at = row_dict.pop("public_at", None)
    is_scheduled = bool(public_at is not None and effective_now < public_at)
    is_published = bool(public_at is not None and effective_now >= public_at)

    override = None
    if override_status is not None or override_completed_at is not None:
        override = {
            "override_status": override_status,
            "override_completed_at": override_completed_at,
        }

    row_dict["contentKey"] = build_content_key(row_dict["content_id"], row_dict["source"])
    row_dict["publication"] = {
        "public_at": public_at.isoformat() if public_at else None,
        "is_scheduled_publication": is_scheduled,
        "is_published": is_published,
    }
    row_dict["subscription"] = {
        "wants_completion": wants_completion,
        "wants_publication": wants_publication,
    }
    row_dict["final_state"] = build_final_state_payload(
        row_dict.get("status"), override, now=effective_now
    )
    return row_dict


def _list_rows(cursor, subject: Dict):
    if subject.get("user_key"):
        where_clause = "s.user_key = %s"
        params = (subject["user_key"],)
    else:
        where_clause = "s.user_id = %s"
        params = (subject["user_id"],)

    cursor.execute(
        f"""
        SELECT c.content_id, c.source, c.content_type, c.title, c.status, c.meta,
               s.wants_completion, s.wants_publication,
               o.override_status, o.override_completed_at,
               COALESCE(
                   m.public_at,
                   CASE
                       WHEN c.content_type = 'webtoon' THEN c.created_at
                       ELSE NULL
                   END
               ) AS public_at
        FROM subscriptions s
        JOIN contents c
            ON s.content_id = c.content_id AND s.source = c.source
        LEFT JOIN admin_content_overrides o
            ON o.content_id = c.content_id AND o.source = c.source
        LEFT JOIN admin_content_metadata m
            ON m.content_id = c.content_id AND m.source = c.source
        WHERE {where_clause} AND COALESCE(c.is_deleted, FALSE) = FALSE
        ORDER BY c.title
        """,
        params,
    )
    return cursor.fetchall()


def _upsert_subscription(cursor, subject: Dict, content_id: str, source: str):
    if subject.get("user_key"):
        cursor.execute(
            """
            INSERT INTO subscriptions (
                user_id, user_key, email, content_id, source, wants_completion, wants_publication
            )
            VALUES (%s, %s, %s, %s, %s, TRUE, FALSE)
            ON CONFLICT (user_key, content_id, source)
            DO UPDATE SET
                wants_completion = TRUE,
                wants_publication = FALSE,
                user_id = EXCLUDED.user_id,
                email = COALESCE(subscriptions.email, EXCLUDED.email)
            RETURNING wants_completion, wants_publication
            """,
            (
                subject.get("user_id"),
                subject.get("user_key"),
                subject.get("email"),
                str(content_id),
                source,
            ),
        )
        return cursor.fetchone()

    cursor.execute(
        """
        INSERT INTO subscriptions (
            user_id, email, content_id, source, wants_completion, wants_publication
        )
        VALUES (%s, %s, %s, %s, TRUE, FALSE)
        ON CONFLICT (user_id, content_id, source)
        DO UPDATE SET
            wants_completion = TRUE,
            wants_publication = FALSE,
            email = COALESCE(subscriptions.email, EXCLUDED.email)
        RETURNING wants_completion, wants_publication
        """,
        (
            subject.get("user_id"),
            subject.get("email"),
            str(content_id),
            source,
        ),
    )
    return cursor.fetchone()


def _delete_subscription(cursor, subject: Dict, content_id: str, source: str):
    if subject.get("user_key"):
        cursor.execute(
            """
            DELETE FROM subscriptions
            WHERE user_key = %s AND content_id = %s AND source = %s
            RETURNING 1
            """,
            (subject["user_key"], str(content_id), source),
        )
        return cursor.fetchone()

    cursor.execute(
        """
        DELETE FROM subscriptions
        WHERE user_id = %s AND content_id = %s AND source = %s
        RETURNING 1
        """,
        (subject["user_id"], str(content_id), source),
    )
    return cursor.fetchone()


def _subscription_response_payload(content_id: str, source: str, flags_row):
    subscription_payload = None
    if flags_row:
        subscription_payload = {
            "contentKey": build_content_key(content_id, source),
            "content_id": str(content_id),
            "source": source,
            "wants_completion": bool(flags_row[0]),
            "wants_publication": bool(flags_row[1]),
        }
    return subscription_payload


@subscriptions_bp.route("/api/me/subscriptions", methods=["GET"])
@login_required
def list_subscriptions():
    conn = get_db()
    cursor = get_cursor(conn)

    try:
        rows = _list_rows(cursor, _current_subject())
        effective_now = now_kst_naive()
        data = []
        for row in rows:
            row_dict = dict(row)
            data.append(_serialize_subscription_row(row_dict, effective_now))

        return jsonify({"success": True, "data": data}), 200
    except psycopg2.Error:
        return _error_response(500, "DB_ERROR", "Database error")
    finally:
        cursor.close()


@subscriptions_bp.route("/v1/me/subscriptions", methods=["GET"])
@login_required
def list_subscriptions_v1():
    conn = get_db()
    cursor = get_cursor(conn)

    try:
        rows = _list_rows(cursor, _current_subject())
        effective_now = now_kst_naive()
        items = []
        for row in rows:
            serialized = _serialize_subscription_row(dict(row), effective_now)
            items.append(serialized)

        return jsonify({"items": items}), 200
    except psycopg2.Error:
        return _error_response(500, "DB_ERROR", "Database error")
    finally:
        cursor.close()


@subscriptions_bp.route("/api/me/subscriptions", methods=["POST"])
@subscriptions_bp.route("/v1/subscriptions", methods=["POST"])
@login_required
def subscribe():
    data = request.get_json() or {}
    content_id, source = _resolve_content_identity(data)
    alert_type = _parse_alert_type(data)

    if not content_id or not source:
        return _error_response(
            400,
            "INVALID_REQUEST",
            "content_id/contentId or contentKey and source are required",
        )
    if alert_type is None:
        return _error_response(400, "INVALID_ALERT_TYPE", "alert_type is invalid")

    conn = get_db()
    cursor = get_cursor(conn)
    subject = _current_subject()

    try:
        if not _content_exists(cursor, content_id, source):
            return _error_response(404, "CONTENT_NOT_FOUND", "content not found")

        flags_row = _upsert_subscription(cursor, subject, content_id, source)
        conn.commit()
        return jsonify({"success": True, "subscription": _subscription_response_payload(content_id, source, flags_row)}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, "DB_ERROR", "Database error")
    finally:
        cursor.close()


@subscriptions_bp.route("/api/me/subscriptions", methods=["DELETE"])
@login_required
def unsubscribe():
    data = request.get_json() or {}
    content_id, source = _resolve_content_identity(data)
    alert_type = _parse_alert_type(data)

    if not content_id or not source:
        return _error_response(
            400,
            "INVALID_REQUEST",
            "content_id/contentId or contentKey and source are required",
        )
    if alert_type is None:
        return _error_response(400, "INVALID_ALERT_TYPE", "alert_type is invalid")

    conn = get_db()
    cursor = get_cursor(conn)

    try:
        _delete_subscription(cursor, _current_subject(), content_id, source)
        conn.commit()
        return jsonify({"success": True, "subscription": None}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, "DB_ERROR", "Database error")
    finally:
        cursor.close()


@subscriptions_bp.route("/v1/subscriptions/<path:content_key>", methods=["DELETE"])
@login_required
def unsubscribe_v1(content_key):
    content_id, source = _resolve_content_identity(content_key=content_key)
    if not content_id or not source:
        return _error_response(400, "INVALID_REQUEST", "contentKey is invalid")

    conn = get_db()
    cursor = get_cursor(conn)
    try:
        _delete_subscription(cursor, _current_subject(), content_id, source)
        conn.commit()
        return jsonify({"success": True, "subscription": None}), 200
    except psycopg2.Error:
        conn.rollback()
        return _error_response(500, "DB_ERROR", "Database error")
    finally:
        cursor.close()
