from __future__ import annotations

import json
import os
import socket
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence

from database import get_cursor
from utils.time import now_kst_naive

VERIFIED_LOCAL_PIPELINE = "verified_local_v1"
VERIFIED_CLOUD_PIPELINE = "verified_cloud_v1"
CLOUD_DISPATCH_PIPELINE = "cloud_dispatch_v1"
LEGACY_INTEGRATED_PIPELINE = "legacy_integrated_v1"
TERMINAL_APPLY_RESULTS = {"applied", "blocked", "skipped", "dry_run"}
DEFAULT_STALE_AFTER_HOURS = 10.0


def parse_report_data(report_data: Any) -> Dict[str, Any]:
    if isinstance(report_data, dict):
        return dict(report_data)
    if isinstance(report_data, str):
        try:
            parsed = json.loads(report_data)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def today_kst() -> date:
    return now_kst_naive().date()


def kst_day_bounds(target_date: Optional[date] = None) -> tuple[datetime, datetime]:
    resolved = target_date or today_kst()
    start = datetime.combine(resolved, time.min)
    return start, start + timedelta(days=1)


def get_stale_after_hours(raw_value: Optional[object] = None) -> float:
    candidate = raw_value
    if candidate is None:
        candidate = os.getenv("VERIFIED_SYNC_STALE_AFTER_HOURS")

    if candidate in (None, ""):
        return DEFAULT_STALE_AFTER_HOURS

    try:
        parsed = float(candidate)
    except (TypeError, ValueError):
        return DEFAULT_STALE_AFTER_HOURS

    if parsed <= 0:
        return DEFAULT_STALE_AFTER_HOURS
    return parsed


def resolve_host_name() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def build_run_context(
    *,
    pipeline: str,
    enabled_sources: Sequence[str],
    attempt_no: int,
    dry_run: bool = False,
    host: Optional[str] = None,
) -> Dict[str, Any]:
    timestamp = now_kst_naive().strftime("%Y%m%d-%H%M%S")
    return {
        "run_id": f"{pipeline}:{timestamp}:{uuid.uuid4().hex[:8]}",
        "pipeline": pipeline,
        "host": host or resolve_host_name(),
        "attempt_no": int(attempt_no),
        "enabled_sources": [str(source) for source in enabled_sources],
        "dry_run": bool(dry_run),
        "started_at": now_kst_naive().isoformat(),
    }


def normalize_verification_gate(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {
            "status": "not_applicable",
            "mode": "none",
            "reason": "not_provided",
            "message": "",
        }

    normalized = {
        "status": str(value.get("status") or value.get("gate") or "not_applicable"),
        "mode": str(value.get("mode") or "none"),
        "reason": str(value.get("reason") or "not_provided"),
        "message": str(value.get("message") or ""),
    }
    for key, raw_value in value.items():
        if key in {"gate", "status", "mode", "reason", "message"}:
            continue
        normalized[key] = raw_value
    return normalized


def enrich_report_data(
    report: Dict[str, Any],
    *,
    run_context: Optional[Dict[str, Any]] = None,
    verification_gate: Optional[Dict[str, Any]] = None,
    apply_result: Optional[str] = None,
) -> Dict[str, Any]:
    enriched = dict(report)
    if run_context:
        enriched["run_id"] = run_context["run_id"]
        enriched["pipeline"] = run_context["pipeline"]
        enriched["host"] = run_context["host"]
        enriched["attempt_no"] = run_context["attempt_no"]
        enriched["enabled_sources"] = list(run_context.get("enabled_sources") or [])
        enriched["dry_run"] = bool(run_context.get("dry_run"))
        enriched["run_started_at"] = run_context.get("started_at")

    if verification_gate is not None:
        enriched["verification_gate"] = normalize_verification_gate(verification_gate)

    if apply_result is not None:
        enriched["apply_result"] = str(apply_result)

    return enriched


def get_next_attempt_no(
    conn,
    *,
    pipeline: str,
    target_date: Optional[date] = None,
) -> int:
    start, end = kst_day_bounds(target_date)
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT MAX(NULLIF(report_data->>'attempt_no', '')::int) AS max_attempt_no
            FROM daily_crawler_reports
            WHERE created_at >= %s
              AND created_at < %s
              AND COALESCE(report_data->>'pipeline', '') = %s
            """,
            (start, end, pipeline),
        )
        row = cursor.fetchone() or {}
    finally:
        cursor.close()

    current = row.get("max_attempt_no") if isinstance(row, dict) else None
    if not isinstance(current, int) or current < 0:
        return 1
    return current + 1


def _build_source_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    data = parse_report_data(row.get("report_data"))
    verification_gate = normalize_verification_gate(data.get("verification_gate"))
    return {
        "crawler_name": row.get("crawler_name"),
        "status": row.get("status"),
        "source_name": data.get("source_name"),
        "apply_result": data.get("apply_result"),
        "verification_gate": verification_gate,
        "run_id": data.get("run_id"),
        "created_at": row.get("created_at"),
        "report_data": data,
    }


def get_verified_sync_freshness(
    conn,
    *,
    enabled_sources: Sequence[str],
    pipeline: str = VERIFIED_LOCAL_PIPELINE,
    stale_after_hours: Optional[float] = None,
) -> Dict[str, Any]:
    enabled = [str(source) for source in enabled_sources]
    checked_at = now_kst_naive()
    stale_after = get_stale_after_hours(stale_after_hours)
    freshness_cutoff = checked_at - timedelta(hours=stale_after)
    cursor = get_cursor(conn)
    try:
        if enabled:
            cursor.execute(
                """
                SELECT DISTINCT ON (COALESCE(report_data->>'source_name', ''))
                    id,
                    crawler_name,
                    status,
                    report_data,
                    created_at
                FROM daily_crawler_reports
                WHERE COALESCE(report_data->>'pipeline', '') = %s
                  AND COALESCE(report_data->>'source_name', '') = ANY(%s)
                ORDER BY
                    COALESCE(report_data->>'source_name', '') ASC,
                    created_at DESC,
                    id DESC
                """,
                (pipeline, enabled),
            )
            rows = [dict(row) for row in cursor.fetchall()]
        else:
            rows = []
    finally:
        cursor.close()

    latest_by_source: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        snapshot = _build_source_snapshot(row)
        source_name = str(snapshot.get("source_name") or "").strip()
        if not source_name or source_name not in enabled or source_name in latest_by_source:
            continue
        latest_by_source[source_name] = snapshot

    missing_sources = [source for source in enabled if source not in latest_by_source]
    non_terminal_sources = [
        source
        for source, snapshot in latest_by_source.items()
        if str(snapshot.get("apply_result") or "") not in TERMINAL_APPLY_RESULTS
    ]
    stale_sources = [
        source
        for source, snapshot in latest_by_source.items()
        if snapshot.get("created_at") is None or snapshot["created_at"] < freshness_cutoff
    ]

    stale = bool(missing_sources or non_terminal_sources or stale_sources)
    reason = None
    if missing_sources:
        reason = "missing_sources"
    elif non_terminal_sources:
        reason = "non_terminal_apply_results"
    elif stale_sources:
        reason = "stale_sources"

    return {
        "pipeline": pipeline,
        "checked_at": checked_at.isoformat(),
        "stale_after_hours": stale_after,
        "freshness_cutoff": freshness_cutoff.isoformat(),
        "enabled_sources": enabled,
        "missing_sources": missing_sources,
        "non_terminal_sources": non_terminal_sources,
        "stale_sources": stale_sources,
        "latest_by_source": latest_by_source,
        "stale": stale,
        "reason": reason,
    }


def get_latest_run_rows(
    conn,
    *,
    pipeline: str,
) -> List[Dict[str, Any]]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT report_data->>'run_id' AS run_id
            FROM daily_crawler_reports
            WHERE COALESCE(report_data->>'pipeline', '') = %s
              AND COALESCE(report_data->>'run_id', '') <> ''
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (pipeline,),
        )
        row = cursor.fetchone()
        run_id = row.get("run_id") if row else None
        if not run_id:
            return []

        cursor.execute(
            """
            SELECT id, crawler_name, status, report_data, created_at
            FROM daily_crawler_reports
            WHERE COALESCE(report_data->>'pipeline', '') = %s
              AND COALESCE(report_data->>'run_id', '') = %s
            ORDER BY created_at ASC, id ASC
            """,
            (pipeline, run_id),
        )
        return [dict(result) for result in cursor.fetchall()]
    finally:
        cursor.close()


def build_latest_run_summary(
    conn,
    *,
    pipeline: str,
) -> Dict[str, Any]:
    rows = get_latest_run_rows(conn, pipeline=pipeline)
    items: List[Dict[str, Any]] = []
    retry_sources: List[str] = []
    run_id = None

    for row in rows:
        snapshot = _build_source_snapshot(row)
        run_id = snapshot.get("run_id") or run_id
        items.append(snapshot)
        status = str(snapshot.get("status") or "").lower()
        apply_result = str(snapshot.get("apply_result") or "").lower()
        source_name = str(snapshot.get("source_name") or "").strip()
        if source_name and (
            status in {"warn", "fail", "failure", "error"}
            or apply_result == "blocked"
        ):
            retry_sources.append(source_name)

    return {
        "pipeline": pipeline,
        "run_id": run_id,
        "items": items,
        "retry_sources": sorted(set(retry_sources)),
    }


def build_freshness_text(freshness: Dict[str, Any]) -> str:
    if not freshness.get("stale"):
        return (
            f"fresh: pipeline={freshness['pipeline']} "
            f"cutoff={freshness['freshness_cutoff']} "
            f"sources={','.join(freshness.get('enabled_sources') or [])}"
        )

    segments = [
        f"stale: pipeline={freshness['pipeline']} cutoff={freshness['freshness_cutoff']}"
    ]
    if freshness.get("missing_sources"):
        segments.append(f"missing={','.join(freshness['missing_sources'])}")
    if freshness.get("non_terminal_sources"):
        segments.append(f"non_terminal={','.join(freshness['non_terminal_sources'])}")
    if freshness.get("stale_sources"):
        segments.append(f"older_than_threshold={','.join(freshness['stale_sources'])}")
    return " ".join(segments)
