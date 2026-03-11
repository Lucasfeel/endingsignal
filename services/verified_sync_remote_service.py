from __future__ import annotations

import json
from typing import Any, Dict, Optional

from database import get_cursor
from services.verified_sync_registry import resolve_crawler_class
from services.verified_sync_service import parse_report_data


def _serialize_value(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _serialize_value(raw_value) for key, raw_value in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def load_source_snapshot(conn, source_name: str) -> Dict[str, Any]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT
                content_id,
                content_type,
                title,
                normalized_title,
                normalized_authors,
                status,
                meta,
                search_document
            FROM contents
            WHERE source = %s
            """,
            (source_name,),
        )
        existing_rows = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT content_id, override_status, override_completed_at
            FROM admin_content_overrides
            WHERE source = %s
            """,
            (source_name,),
        )
        override_rows = [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()

    return {
        "source_name": source_name,
        "existing_rows": _serialize_value(existing_rows),
        "override_rows": _serialize_value(override_rows),
    }


def find_existing_source_report(
    conn,
    *,
    run_id: str,
    source_name: str,
    pipeline: Optional[str],
) -> Optional[Dict[str, Any]]:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            SELECT report_data
            FROM daily_crawler_reports
            WHERE report_data->>'run_id' = %s
              AND report_data->>'source_name' = %s
              AND (%s IS NULL OR report_data->>'pipeline' = %s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (run_id, source_name, pipeline, pipeline),
        )
        row = cursor.fetchone()
    finally:
        cursor.close()

    if not row:
        return None
    return parse_report_data(row.get("report_data"))


def insert_source_report(conn, *, crawler_name: str, status: str, report: Dict[str, Any]) -> None:
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            """
            INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
            VALUES (%s, %s, %s)
            """,
            (
                crawler_name,
                status,
                json.dumps(report, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        cursor.close()


def apply_remote_report(
    conn,
    *,
    report: Dict[str, Any],
    apply_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    source_name = str(report.get("source_name") or "").strip()
    if not source_name:
        raise ValueError("source_name is required")

    final_report = dict(report)
    cdc_info = dict(final_report.get("cdc_info") or {})

    if apply_payload is not None and str(final_report.get("apply_result") or "") == "deferred":
        crawler_class = resolve_crawler_class(source_name)
        crawler = crawler_class()
        write_result = crawler.apply_remote_daily_check_payload(conn, apply_payload)
        final_report["new_contents"] = write_result.get("added", 0)
        final_report["inserted_count"] = write_result.get("inserted_count", 0)
        final_report["updated_count"] = write_result.get("updated_count", 0)
        final_report["unchanged_count"] = write_result.get("unchanged_count", 0)
        final_report["write_skipped_count"] = write_result.get("write_skipped_count", 0)
        cdc_info.update(
            {
                "cdc_skipped": False,
                "cdc_events_inserted_count": write_result.get("cdc_events_inserted_count", 0),
                "cdc_events_inserted_items": write_result.get("cdc_events_inserted_items", []),
                "default_publication_seeded_count": write_result.get(
                    "default_publication_seeded_count",
                    0,
                ),
                "inserted_count": write_result.get("inserted_count", 0),
                "updated_count": write_result.get("updated_count", 0),
                "unchanged_count": write_result.get("unchanged_count", 0),
                "write_skipped_count": write_result.get("write_skipped_count", 0),
                "apply_result": "applied",
            }
        )
        if cdc_info.get("skip_reason") == "remote_apply_pending":
            cdc_info["skip_reason"] = None
        if "default_publication_seed_error" in write_result:
            cdc_info["default_publication_seed_error"] = write_result["default_publication_seed_error"]
        final_report["apply_result"] = "applied"

    final_report["cdc_info"] = cdc_info
    return final_report
