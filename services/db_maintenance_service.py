from __future__ import annotations

import os
import time
from datetime import timedelta
from typing import Dict

from database import get_cursor
from services.cdc_constants import EVENT_CONTENT_COMPLETED, EVENT_CONTENT_PUBLISHED
from utils.time import now_kst_naive

DEFAULT_DB_MAINTENANCE_BATCH_SIZE = 1000
DEFAULT_DB_RETENTION_REPORT_DAYS = 90
DEFAULT_DB_RETENTION_NOTIFICATION_DAYS = 365
DEFAULT_DB_RETENTION_CDC_DAYS = 365


def _read_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    if parsed < minimum:
        return default
    return parsed


def resolve_maintenance_settings() -> Dict[str, int]:
    return {
        "batch_size": _read_int_env(
            "DB_MAINTENANCE_BATCH_SIZE",
            DEFAULT_DB_MAINTENANCE_BATCH_SIZE,
            minimum=1,
        ),
        "report_retention_days": _read_int_env(
            "DB_RETENTION_REPORT_DAYS",
            DEFAULT_DB_RETENTION_REPORT_DAYS,
            minimum=1,
        ),
        "notification_retention_days": _read_int_env(
            "DB_RETENTION_NOTIFICATION_DAYS",
            DEFAULT_DB_RETENTION_NOTIFICATION_DAYS,
            minimum=1,
        ),
        "cdc_retention_days": _read_int_env(
            "DB_RETENTION_CDC_DAYS",
            DEFAULT_DB_RETENTION_CDC_DAYS,
            minimum=1,
        ),
    }


def _delete_daily_crawler_reports_batch(conn, *, cutoff, batch_size: int) -> int:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            WITH doomed AS (
                SELECT id
                FROM daily_crawler_reports
                WHERE created_at < %s
                ORDER BY created_at ASC, id ASC
                LIMIT %s
            )
            DELETE FROM daily_crawler_reports report
            USING doomed
            WHERE report.id = doomed.id
            RETURNING report.id
            """,
            (cutoff, batch_size),
        )
        deleted = len(cursor.fetchall())
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _delete_terminal_notification_logs_batch(conn, *, cutoff, batch_size: int) -> int:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            WITH doomed AS (
                SELECT id
                FROM notification_log
                WHERE result IN ('sent', 'failed')
                  AND COALESCE(updated_at, created_at) < %s
                ORDER BY COALESCE(updated_at, created_at) ASC, id ASC
                LIMIT %s
            )
            DELETE FROM notification_log log
            USING doomed
            WHERE log.id = doomed.id
            RETURNING log.id
            """,
            (cutoff, batch_size),
        )
        deleted = len(cursor.fetchall())
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _prune_cdc_batch(conn, *, cdc_cutoff, notification_cutoff, batch_size: int) -> Dict[str, int]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            WITH eligible_events AS (
                SELECT e.id, e.content_id, e.source, e.event_type
                FROM cdc_events e
                WHERE e.created_at < %s
                  AND (
                    (
                        e.event_type = %s
                        AND EXISTS (
                            SELECT 1
                            FROM cdc_event_consumptions consumption
                            WHERE consumption.event_id = e.id
                              AND consumption.status IN ('processed', 'skipped')
                        )
                        AND NOT EXISTS (
                            SELECT 1
                            FROM notification_log log
                            WHERE log.event_id = e.id
                              AND (
                                log.result NOT IN ('sent', 'failed')
                                OR COALESCE(log.updated_at, log.created_at) >= %s
                              )
                        )
                    )
                    OR e.event_type = %s
                  )
                ORDER BY e.created_at ASC, e.id ASC
                LIMIT %s
            ),
            inserted_tombstones AS (
                INSERT INTO cdc_event_tombstones (
                    content_id,
                    source,
                    event_type,
                    tombstoned_at
                )
                SELECT
                    eligible.content_id,
                    eligible.source,
                    eligible.event_type,
                    NOW()
                FROM eligible_events eligible
                ON CONFLICT (content_id, source, event_type) DO NOTHING
                RETURNING id, event_type
            ),
            deleted_logs AS (
                DELETE FROM notification_log log
                USING eligible_events eligible
                WHERE log.event_id = eligible.id
                  AND log.result IN ('sent', 'failed')
                  AND COALESCE(log.updated_at, log.created_at) < %s
                RETURNING log.id
            ),
            deleted_consumptions AS (
                DELETE FROM cdc_event_consumptions consumption
                USING eligible_events eligible
                WHERE consumption.event_id = eligible.id
                RETURNING consumption.id
            ),
            deleted_events AS (
                DELETE FROM cdc_events event
                USING eligible_events eligible
                WHERE event.id = eligible.id
                RETURNING event.id, event.event_type
            )
            SELECT
                (SELECT COUNT(*) FROM eligible_events) AS eligible_count,
                (SELECT COUNT(*) FROM inserted_tombstones) AS tombstones_inserted,
                (SELECT COUNT(*) FROM deleted_logs) AS notification_logs_deleted,
                (SELECT COUNT(*) FROM deleted_consumptions) AS consumptions_deleted,
                (SELECT COUNT(*) FROM deleted_events) AS events_deleted,
                COALESCE((
                    SELECT COUNT(*)
                    FROM deleted_events
                    WHERE event_type = %s
                ), 0) AS completion_events_deleted,
                COALESCE((
                    SELECT COUNT(*)
                    FROM deleted_events
                    WHERE event_type = %s
                ), 0) AS publication_events_deleted
            """,
            (
                cdc_cutoff,
                EVENT_CONTENT_COMPLETED,
                notification_cutoff,
                EVENT_CONTENT_PUBLISHED,
                batch_size,
                notification_cutoff,
                EVENT_CONTENT_COMPLETED,
                EVENT_CONTENT_PUBLISHED,
            ),
        )
        fetched = cursor.fetchone()
        row = dict(fetched) if fetched else {}
        conn.commit()
        return {
            "eligible_count": int(row.get("eligible_count") or 0),
            "tombstones_inserted": int(row.get("tombstones_inserted") or 0),
            "notification_logs_deleted": int(row.get("notification_logs_deleted") or 0),
            "consumptions_deleted": int(row.get("consumptions_deleted") or 0),
            "events_deleted": int(row.get("events_deleted") or 0),
            "completion_events_deleted": int(row.get("completion_events_deleted") or 0),
            "publication_events_deleted": int(row.get("publication_events_deleted") or 0),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _run_batched_delete(delete_batch, *, batch_size: int) -> Dict[str, int]:
    deleted_total = 0
    batches = 0
    while True:
        deleted = int(delete_batch())
        if deleted <= 0:
            break
        deleted_total += deleted
        batches += 1
        if deleted < batch_size:
            break
    return {"deleted_count": deleted_total, "batches": batches}


def run_db_maintenance(conn) -> Dict[str, object]:
    settings = resolve_maintenance_settings()
    started = time.perf_counter()
    now = now_kst_naive()
    report_cutoff = now - timedelta(days=settings["report_retention_days"])
    notification_cutoff = now - timedelta(days=settings["notification_retention_days"])
    cdc_cutoff = now - timedelta(days=settings["cdc_retention_days"])

    summary = {
        "status": "ok",
        "checked_at": now.isoformat(),
        "settings": {
            **settings,
            "report_cutoff": report_cutoff.isoformat(),
            "notification_cutoff": notification_cutoff.isoformat(),
            "cdc_cutoff": cdc_cutoff.isoformat(),
        },
        "daily_crawler_reports": {"deleted_count": 0, "batches": 0},
        "notification_log": {"deleted_count": 0, "batches": 0},
        "cdc": {
            "batches": 0,
            "eligible_count": 0,
            "tombstones_inserted": 0,
            "notification_logs_deleted": 0,
            "consumptions_deleted": 0,
            "events_deleted": 0,
            "completion_events_deleted": 0,
            "publication_events_deleted": 0,
        },
    }

    batch_size = settings["batch_size"]
    summary["daily_crawler_reports"] = _run_batched_delete(
        lambda: _delete_daily_crawler_reports_batch(
            conn,
            cutoff=report_cutoff,
            batch_size=batch_size,
        ),
        batch_size=batch_size,
    )
    summary["notification_log"] = _run_batched_delete(
        lambda: _delete_terminal_notification_logs_batch(
            conn,
            cutoff=notification_cutoff,
            batch_size=batch_size,
        ),
        batch_size=batch_size,
    )

    while True:
        batch_summary = _prune_cdc_batch(
            conn,
            cdc_cutoff=cdc_cutoff,
            notification_cutoff=notification_cutoff,
            batch_size=batch_size,
        )
        if batch_summary["eligible_count"] <= 0:
            break
        summary["cdc"]["batches"] += 1
        for key, value in batch_summary.items():
            if key == "eligible_count":
                summary["cdc"][key] += value
                continue
            summary["cdc"][key] += value
        if batch_summary["eligible_count"] < batch_size:
            break

    summary["duration_seconds"] = round(time.perf_counter() - started, 3)
    return summary
