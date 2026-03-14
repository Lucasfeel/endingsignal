import json
import os
from collections import defaultdict
from datetime import timedelta
from typing import Dict, Iterable, List, Sequence, Tuple

import psycopg2.extras

from database import get_cursor
from repositories.cdc_event_consumptions_repo import mark_consumed
from services.apps_in_toss_message_service import AppsInTossMessageError, send_completion_message
from services.cdc_constants import EVENT_CONTENT_COMPLETED
from utils.content_keys import build_content_key
from utils.time import now_kst_naive

DISPATCH_CONSUMER = "apps_in_toss_completion_message_v1"
DEFAULT_DISPATCH_PAGE_SIZE = 200
DEFAULT_RESULT_FLUSH_BATCH_SIZE = 25
DEFAULT_PENDING_RETRY_AFTER_SECONDS = 600


def _commit_if_supported(conn) -> None:
    commit = getattr(conn, "commit", None)
    if callable(commit):
        commit()


def _rollback_if_supported(conn) -> None:
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        rollback()


def _read_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _result_flush_batch_size() -> int:
    return _read_int_env("AIT_NOTIFICATION_RESULT_FLUSH_BATCH_SIZE", DEFAULT_RESULT_FLUSH_BATCH_SIZE)


def _pending_retry_after_seconds() -> int:
    return _read_int_env(
        "AIT_NOTIFICATION_PENDING_RETRY_AFTER_SECONDS",
        DEFAULT_PENDING_RETRY_AFTER_SECONDS,
    )


def _row_value(row, key: str, index: int):
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    if hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            pass
    try:
        return row[index]
    except Exception:
        return None


def _normalize_subscriber_keys(user_keys: Iterable[str]) -> List[str]:
    normalized = []
    seen = set()
    for raw_value in user_keys or []:
        user_key = str(raw_value or "").strip()
        if not user_key or user_key in seen:
            continue
        seen.add(user_key)
        normalized.append(user_key)
    return normalized


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


def _fetch_subscriber_keys_for_events(conn, *, events: Sequence[Dict]) -> Dict[Tuple[str, str], List[str]]:
    event_targets = []
    seen_targets = set()
    for event in events or []:
        content_id = str((event or {}).get("content_id") or "").strip()
        source = str((event or {}).get("source") or "").strip()
        if not content_id or not source:
            continue
        target = (content_id, source)
        if target in seen_targets:
            continue
        seen_targets.add(target)
        event_targets.append(target)

    if not event_targets:
        return {}

    cursor = get_cursor(conn)
    try:
        rows = psycopg2.extras.execute_values(
            cursor,
            """
            WITH event_targets(content_id, source) AS (VALUES %s)
            SELECT DISTINCT
                event_targets.content_id,
                event_targets.source,
                subscriptions.user_key
            FROM event_targets
            JOIN subscriptions
              ON subscriptions.content_id = event_targets.content_id
             AND subscriptions.source = event_targets.source
            WHERE subscriptions.user_key IS NOT NULL
              AND COALESCE(subscriptions.wants_completion, FALSE) = TRUE
            ORDER BY event_targets.content_id ASC, event_targets.source ASC, subscriptions.user_key ASC
            """,
            event_targets,
            template="(%s, %s)",
            page_size=min(len(event_targets), DEFAULT_DISPATCH_PAGE_SIZE),
            fetch=True,
        )
    finally:
        cursor.close()

    subscriber_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for row in rows or []:
        content_id = str(_row_value(row, "content_id", 0) or "").strip()
        source = str(_row_value(row, "source", 1) or "").strip()
        user_key = str(_row_value(row, "user_key", 2) or "").strip()
        if not content_id or not source or not user_key:
            continue
        subscriber_map[(content_id, source)].append(user_key)
    return dict(subscriber_map)


def _fetch_subscriber_keys(conn, *, content_id: str, source: str) -> List[str]:
    return _fetch_subscriber_keys_for_events(
        conn,
        events=[{"content_id": content_id, "source": source}],
    ).get((str(content_id), str(source)), [])


def _claim_notification_logs(
    conn,
    *,
    event_id: int,
    content_id: str,
    source: str,
    template_code: str,
    subscriber_keys: Iterable[str],
) -> Dict[str, object]:
    normalized_subscribers = _normalize_subscriber_keys(subscriber_keys)
    if not normalized_subscribers:
        return {
            "claimed": [],
            "claimed_count": 0,
            "skipped_count": 0,
        }

    rows = [
        (
            event_id,
            user_key,
            content_id,
            source,
            template_code,
            f"completion:{event_id}:{user_key}",
        )
        for user_key in normalized_subscribers
    ]

    cursor = get_cursor(conn)
    try:
        results = psycopg2.extras.execute_values(
            cursor,
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
            VALUES %s
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id, user_key
            """,
            rows,
            template="(%s, %s, %s, %s, %s, 'pending', %s, NOW(), NOW())",
            page_size=min(len(rows), DEFAULT_DISPATCH_PAGE_SIZE),
            fetch=True,
        )
        claimed = [
            {
                "log_id": _row_value(row, "id", 0),
                "user_key": str(_row_value(row, "user_key", 1) or "").strip(),
            }
            for row in results or []
            if _row_value(row, "id", 0) is not None and str(_row_value(row, "user_key", 1) or "").strip()
        ]
        _commit_if_supported(conn)
        return {
            "claimed": claimed,
            "claimed_count": len(claimed),
            "skipped_count": max(0, len(normalized_subscribers) - len(claimed)),
        }
    except Exception:
        _rollback_if_supported(conn)
        raise
    finally:
        cursor.close()


def _fetch_event_notification_logs(
    conn,
    *,
    event_id: int,
    subscriber_keys: Iterable[str],
) -> Dict[str, Dict]:
    normalized_subscribers = _normalize_subscriber_keys(subscriber_keys)
    if not normalized_subscribers:
        return {}

    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT id, user_key, result, fail_reason, response_payload, updated_at, sent_at
            FROM notification_log
            WHERE event_id = %s
              AND user_key = ANY(%s)
            ORDER BY user_key ASC
            """,
            (event_id, normalized_subscribers),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()

    return {
        str(row.get("user_key") or "").strip(): row
        for row in rows
        if str(row.get("user_key") or "").strip()
    }


def _lease_pending_notification_logs(conn, *, log_ids: Iterable[int]) -> List[int]:
    normalized_ids = []
    seen_ids = set()
    for raw_value in log_ids or []:
        try:
            log_id = int(raw_value)
        except (TypeError, ValueError):
            continue
        if log_id <= 0 or log_id in seen_ids:
            continue
        seen_ids.add(log_id)
        normalized_ids.append(log_id)

    if not normalized_ids:
        return []

    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            UPDATE notification_log
            SET updated_at = NOW()
            WHERE id = ANY(%s)
              AND result = 'pending'
            RETURNING id
            """,
            (normalized_ids,),
        )
        return [
            int(_row_value(row, "id", 0))
            for row in cursor.fetchall()
            if _row_value(row, "id", 0) is not None
        ]
    finally:
        cursor.close()


def _flush_notification_log_updates(conn, updates: Sequence[Dict]) -> None:
    if not updates:
        return

    rows = [
        (
            update["log_id"],
            update["result"],
            update.get("fail_reason"),
            json.dumps(update["response_payload"], ensure_ascii=False)
            if update.get("response_payload") is not None
            else None,
            update["result"] == "sent",
        )
        for update in updates
        if update.get("log_id") is not None
    ]
    if not rows:
        return

    cursor = get_cursor(conn)
    try:
        psycopg2.extras.execute_values(
            cursor,
            """
            UPDATE notification_log AS notification_log
            SET result = data.result,
                fail_reason = data.fail_reason,
                response_payload = CASE
                    WHEN data.response_payload IS NULL THEN NULL
                    ELSE data.response_payload::jsonb
                END,
                sent_at = CASE
                    WHEN data.mark_sent THEN COALESCE(notification_log.sent_at, NOW())
                    ELSE notification_log.sent_at
                END,
                updated_at = NOW()
            FROM (VALUES %s) AS data(log_id, result, fail_reason, response_payload, mark_sent)
            WHERE notification_log.id = data.log_id
            """,
            rows,
            template="(%s, %s, %s, %s, %s)",
            page_size=min(len(rows), DEFAULT_DISPATCH_PAGE_SIZE),
        )
    finally:
        cursor.close()


def _is_stale_pending(log_row: Dict, *, retry_cutoff) -> bool:
    if not isinstance(log_row, dict):
        return False
    if str(log_row.get("result") or "").strip().lower() != "pending":
        return False
    updated_at = log_row.get("updated_at")
    return updated_at is None or updated_at <= retry_cutoff


def _resolve_event_results(
    *,
    subscriber_keys: Sequence[str],
    existing_logs_by_user: Dict[str, Dict],
    dispatched_results: Dict[str, str],
) -> Dict[str, str]:
    resolved = {}
    for user_key in subscriber_keys:
        if user_key in dispatched_results:
            resolved[user_key] = dispatched_results[user_key]
            continue
        current_log = existing_logs_by_user.get(user_key) or {}
        resolved[user_key] = str(current_log.get("result") or "pending").strip().lower() or "pending"
    return resolved


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
    del idempotency_key
    claim_result = _claim_notification_logs(
        conn,
        event_id=event_id,
        content_id=content_id,
        source=source,
        template_code=template_code,
        subscriber_keys=[user_key],
    )
    claimed_rows = claim_result["claimed"]
    if not claimed_rows:
        return None, False
    return claimed_rows[0]["log_id"], True


def _update_notification_log(
    conn,
    *,
    log_id: int,
    result: str,
    fail_reason: str = None,
    response_payload=None,
):
    try:
        _flush_notification_log_updates(
            conn,
            [
                {
                    "log_id": log_id,
                    "result": result,
                    "fail_reason": fail_reason,
                    "response_payload": response_payload,
                }
            ],
        )
        _commit_if_supported(conn)
    except Exception:
        _rollback_if_supported(conn)
        raise


def dispatch_pending_completion_events(conn, *, template_code: str, limit: int = 100) -> Dict:
    events = _fetch_pending_events(conn, limit=limit)
    summary = {
        "pending_events": len(events),
        "processed_events": 0,
        "skipped_events": 0,
        "failed_events": 0,
        "deferred_events": 0,
        "sent_notifications": 0,
        "skipped_notifications": 0,
        "retried_notifications": 0,
        "already_sent_notifications": 0,
    }
    subscriber_key_map = _fetch_subscriber_keys_for_events(conn, events=events)
    flush_batch_size = _result_flush_batch_size()
    retry_cutoff = now_kst_naive() - timedelta(seconds=_pending_retry_after_seconds())

    for event in events:
        event_id = event["id"]
        content_id = str(event["content_id"])
        source = str(event["source"])
        content_title = event.get("title") or content_id
        subscriber_keys = _normalize_subscriber_keys(subscriber_key_map.get((content_id, source), []))

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
        claim_result = _claim_notification_logs(
            conn,
            event_id=event_id,
            content_id=content_id,
            source=source,
            template_code=template_code,
            subscriber_keys=subscriber_keys,
        )
        summary["skipped_notifications"] += int(claim_result.get("skipped_count") or 0)

        claimed_notifications = {
            str(item.get("user_key") or "").strip(): item
            for item in (claim_result.get("claimed") or [])
            if str(item.get("user_key") or "").strip()
        }
        existing_logs_by_user = _fetch_event_notification_logs(
            conn,
            event_id=event_id,
            subscriber_keys=subscriber_keys,
        )

        dispatchable_notifications = []
        stale_pending_ids = []
        for user_key in subscriber_keys:
            if user_key in claimed_notifications:
                dispatchable_notifications.append(
                    {
                        "log_id": claimed_notifications[user_key].get("log_id"),
                        "user_key": user_key,
                        "recovered": False,
                    }
                )
                continue

            existing_log = existing_logs_by_user.get(user_key)
            if isinstance(existing_log, dict) and str(existing_log.get("result") or "").strip().lower() == "sent":
                summary["already_sent_notifications"] += 1
                continue

            if _is_stale_pending(existing_log, retry_cutoff=retry_cutoff):
                dispatchable_notifications.append(
                    {
                        "log_id": existing_log.get("id"),
                        "user_key": user_key,
                        "recovered": True,
                    }
                )
                if existing_log.get("id") is not None:
                    stale_pending_ids.append(existing_log.get("id"))

        if stale_pending_ids:
            leased_ids = set(_lease_pending_notification_logs(conn, log_ids=stale_pending_ids))
            summary["retried_notifications"] += len(leased_ids)
            dispatchable_notifications = [
                item
                for item in dispatchable_notifications
                if not item["recovered"] or item.get("log_id") in leased_ids
            ]
            _commit_if_supported(conn)

        if not dispatchable_notifications:
            resolved_results = _resolve_event_results(
                subscriber_keys=subscriber_keys,
                existing_logs_by_user=existing_logs_by_user,
                dispatched_results={},
            )
            if resolved_results and all(result == "sent" for result in resolved_results.values()):
                mark_consumed(
                    conn,
                    consumer=DISPATCH_CONSUMER,
                    event_id=event_id,
                    status="processed",
                    reason="notifications_already_sent",
                )
                _commit_if_supported(conn)
                summary["processed_events"] += 1
            elif any(result == "failed" for result in resolved_results.values()):
                summary["failed_events"] += 1
            else:
                summary["deferred_events"] += 1
            continue

        pending_updates: List[Dict] = []
        dispatched_results: Dict[str, str] = {}

        def _flush_pending_updates():
            if not pending_updates:
                return
            _flush_notification_log_updates(conn, pending_updates)
            pending_updates.clear()

        try:
            try:
                for notification in dispatchable_notifications:
                    log_id = notification.get("log_id")
                    user_key = str(notification.get("user_key") or "").strip()
                    if log_id is None or not user_key:
                        continue

                    try:
                        response_payload = send_completion_message(
                            user_key=user_key,
                            content_title=content_title,
                            source_name=source,
                            content_path=content_path,
                            template_set_code=template_code,
                        )
                        dispatched_results[user_key] = "sent"
                        pending_updates.append(
                            {
                                "log_id": log_id,
                                "result": "sent",
                                "fail_reason": None,
                                "response_payload": response_payload,
                            }
                        )
                        summary["sent_notifications"] += 1
                    except AppsInTossMessageError as exc:
                        failed_for_event = True
                        dispatched_results[user_key] = "failed"
                        pending_updates.append(
                            {
                                "log_id": log_id,
                                "result": "failed",
                                "fail_reason": exc.message,
                                "response_payload": exc.payload,
                            }
                        )

                    if len(pending_updates) >= flush_batch_size:
                        _flush_pending_updates()
                        _commit_if_supported(conn)
            except Exception:
                if pending_updates:
                    _flush_pending_updates()
                    _commit_if_supported(conn)
                raise

            if pending_updates:
                _flush_pending_updates()

            resolved_results = _resolve_event_results(
                subscriber_keys=subscriber_keys,
                existing_logs_by_user=existing_logs_by_user,
                dispatched_results=dispatched_results,
            )

            if resolved_results and all(result == "sent" for result in resolved_results.values()):
                mark_consumed(
                    conn,
                    consumer=DISPATCH_CONSUMER,
                    event_id=event_id,
                    status="processed",
                    reason="notifications_dispatched",
                )
                summary["processed_events"] += 1
                _commit_if_supported(conn)
                continue

            if failed_for_event or any(result == "failed" for result in resolved_results.values()):
                summary["failed_events"] += 1
            else:
                summary["deferred_events"] += 1
            _commit_if_supported(conn)
        except Exception:
            _rollback_if_supported(conn)
            raise

    return summary
