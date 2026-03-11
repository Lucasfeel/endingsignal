import json
from typing import Dict, List

from database import get_cursor
from repositories.cdc_event_consumptions_repo import mark_consumed
from services.apps_in_toss_message_service import AppsInTossMessageError, send_completion_message
from services.cdc_constants import EVENT_CONTENT_COMPLETED
from utils.content_keys import build_content_key

DISPATCH_CONSUMER = "apps_in_toss_completion_message_v1"


def _commit_if_supported(conn) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def _rollback_if_supported(conn) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()


def _fetch_pending_events(conn, *, limit: int) -> List[Dict]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT e.id, e.content_id, e.source, e.event_type, e.final_completed_at, c.title
            FROM cdc_events e
            JOIN contents c
              ON c.content_id = e.content_id
             AND c.source = e.source
            LEFT JOIN cdc_event_consumptions consumption
              ON consumption.event_id = e.id
             AND consumption.consumer = %s
            WHERE e.event_type = %s
              AND consumption.id IS NULL
              AND COALESCE(c.is_deleted, FALSE) = FALSE
            ORDER BY e.created_at ASC, e.id ASC
            LIMIT %s
            """,
            (DISPATCH_CONSUMER, EVENT_CONTENT_COMPLETED, limit),
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _fetch_subscriber_keys(conn, *, content_id: str, source: str) -> List[str]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT DISTINCT user_key
            FROM subscriptions
            WHERE content_id = %s
              AND source = %s
              AND user_key IS NOT NULL
              AND COALESCE(wants_completion, FALSE) = TRUE
            ORDER BY user_key ASC
            """,
            (content_id, source),
        )
        return [str(row["user_key"]) for row in cursor.fetchall() if row["user_key"] is not None]
    finally:
        cursor.close()


def _create_notification_log(
    conn,
    *,
    event_id: int,
    user_key: str,
    content_id: str,
    source: str,
    template_code: str,
    idempotency_key: str,
):
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            INSERT INTO notification_log (
                event_id,
                user_key,
                content_id,
                source,
                template_code,
                result,
                idempotency_key,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, 'pending', %s, NOW(), NOW())
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            """,
            (event_id, user_key, content_id, source, template_code, idempotency_key),
        )
        row = cursor.fetchone()
        _commit_if_supported(conn)
        return (row["id"], True) if row else (None, False)
    except Exception:
        _rollback_if_supported(conn)
        raise
    finally:
        cursor.close()


def _update_notification_log(
    conn,
    *,
    log_id: int,
    result: str,
    fail_reason: str = None,
    response_payload=None,
):
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            UPDATE notification_log
            SET result = %s,
                fail_reason = %s,
                response_payload = %s::jsonb,
                sent_at = CASE WHEN %s = 'sent' THEN NOW() ELSE sent_at END,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                result,
                fail_reason,
                json.dumps(response_payload, ensure_ascii=False) if response_payload is not None else None,
                result,
                log_id,
            ),
        )
        _commit_if_supported(conn)
    except Exception:
        _rollback_if_supported(conn)
        raise
    finally:
        cursor.close()


def dispatch_pending_completion_events(conn, *, template_code: str, limit: int = 100) -> Dict:
    events = _fetch_pending_events(conn, limit=limit)
    summary = {
        "pending_events": len(events),
        "processed_events": 0,
        "skipped_events": 0,
        "failed_events": 0,
        "sent_notifications": 0,
        "skipped_notifications": 0,
    }

    for event in events:
        event_id = event["id"]
        content_id = str(event["content_id"])
        source = str(event["source"])
        content_title = event.get("title") or content_id
        subscriber_keys = _fetch_subscriber_keys(conn, content_id=content_id, source=source)

        if not subscriber_keys:
            mark_consumed(
                conn,
                consumer=DISPATCH_CONSUMER,
                event_id=event_id,
                status="skipped",
                reason="no_subscribers",
            )
            _commit_if_supported(conn)
            summary["skipped_events"] += 1
            continue

        failed_for_event = False
        content_path = f"/content/{build_content_key(content_id, source)}"

        for user_key in subscriber_keys:
            idempotency_key = f"completion:{event_id}:{user_key}"
            log_id, claimed = _create_notification_log(
                conn,
                event_id=event_id,
                user_key=user_key,
                content_id=content_id,
                source=source,
                template_code=template_code,
                idempotency_key=idempotency_key,
            )
            if not claimed:
                summary["skipped_notifications"] += 1
                continue

            try:
                response_payload = send_completion_message(
                    user_key=user_key,
                    content_title=content_title,
                    source_name=source,
                    content_path=content_path,
                    template_set_code=template_code,
                )
                _update_notification_log(
                    conn,
                    log_id=log_id,
                    result="sent",
                    response_payload=response_payload,
                )
                summary["sent_notifications"] += 1
            except AppsInTossMessageError as exc:
                failed_for_event = True
                _update_notification_log(
                    conn,
                    log_id=log_id,
                    result="failed",
                    fail_reason=exc.message,
                    response_payload=exc.payload,
                )

        if failed_for_event:
            summary["failed_events"] += 1
            continue

        mark_consumed(
            conn,
            consumer=DISPATCH_CONSUMER,
            event_id=event_id,
            status="processed",
            reason="notifications_dispatched",
        )
        _commit_if_supported(conn)
        summary["processed_events"] += 1

    return summary
