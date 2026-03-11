"""Helpers for extracting Kakao crawler report rows from stored report payloads."""

from __future__ import annotations


def _get_field(row, key, default=None):
    if row is None:
        return default
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _extract_report_row(row):
    report_data = _get_field(row, "report_data", {}) or {}
    if not isinstance(report_data, dict):
        report_data = {}
    cdc_info = report_data.get("cdc_info") if isinstance(report_data.get("cdc_info"), dict) else {}
    fetch_meta = cdc_info.get("fetch_meta") if isinstance(cdc_info.get("fetch_meta"), dict) else {}
    return {
        "id": _get_field(row, "id"),
        "created_at": _get_field(row, "created_at"),
        "errors": list(fetch_meta.get("errors") or []),
        "request_samples": list(fetch_meta.get("request_samples") or []),
        "status": fetch_meta.get("status"),
        "summary": report_data.get("summary"),
    }
