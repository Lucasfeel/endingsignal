from __future__ import annotations

import hmac
import os

from flask import Blueprint, jsonify, request, current_app

from database import DatabaseUnavailableError, get_db
from services.internal_content_sync_service import summarize_contents, upsert_contents_batch
from services.verified_sync_registry import resolve_crawler_class
from services.verified_sync_remote_service import (
    apply_remote_report,
    find_existing_source_report,
    insert_source_report,
    load_source_snapshot,
)
from utils.auth import _error_response


internal_verified_sync_bp = Blueprint("internal_verified_sync", __name__)


def _require_internal_token():
    configured = str(os.getenv("VERIFIED_SYNC_INTERNAL_TOKEN") or "").strip()
    if not configured:
        return _error_response(503, "INTERNAL_TOKEN_MISSING", "internal token is not configured")

    auth_header = str(request.headers.get("Authorization") or "")
    if not auth_header.startswith("Bearer "):
        return _error_response(401, "AUTH_REQUIRED", "internal authentication required")

    provided = auth_header.split(" ", 1)[1].strip()
    if not provided or not hmac.compare_digest(provided, configured):
        return _error_response(403, "INVALID_TOKEN", "invalid internal token")
    return None


@internal_verified_sync_bp.route("/api/internal/content-sync/summary", methods=["GET"])
def get_internal_content_sync_summary():
    auth_error = _require_internal_token()
    if auth_error:
        return auth_error

    try:
        summary = summarize_contents(get_db())
        return jsonify({"success": True, "summary": summary}), 200
    except DatabaseUnavailableError:
        return _error_response(503, "DATABASE_UNAVAILABLE", "database not configured")
    except Exception:
        current_app.logger.exception("internal content sync summary failed")
        return _error_response(500, "INTERNAL_ERROR", "internal server error")


@internal_verified_sync_bp.route("/api/internal/content-sync/upsert-batch", methods=["POST"])
def post_internal_content_sync_upsert_batch():
    auth_error = _require_internal_token()
    if auth_error:
        return auth_error

    payload = request.get_json() or {}
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return _error_response(400, "INVALID_REQUEST", "rows must be an array")

    try:
        result = upsert_contents_batch(get_db(), rows)
        return jsonify({"success": True, "result": result}), 200
    except ValueError as exc:
        return _error_response(400, "INVALID_REQUEST", str(exc))
    except DatabaseUnavailableError:
        return _error_response(503, "DATABASE_UNAVAILABLE", "database not configured")
    except Exception:
        current_app.logger.exception("internal content sync batch upsert failed")
        return _error_response(500, "INTERNAL_ERROR", "internal server error")


@internal_verified_sync_bp.route("/api/internal/verified-sync/source-snapshot", methods=["GET"])
def get_verified_sync_source_snapshot():
    auth_error = _require_internal_token()
    if auth_error:
        return auth_error

    source_name = str(request.args.get("source") or "").strip()
    if not source_name:
        return _error_response(400, "INVALID_REQUEST", "source is required")

    try:
        resolve_crawler_class(source_name)
        snapshot = load_source_snapshot(get_db(), source_name)
        return jsonify({"success": True, "snapshot": snapshot}), 200
    except ValueError as exc:
        return _error_response(400, "INVALID_SOURCE", str(exc))
    except DatabaseUnavailableError:
        return _error_response(503, "DATABASE_UNAVAILABLE", "database not configured")
    except Exception:
        current_app.logger.exception("verified sync source snapshot failed")
        return _error_response(500, "INTERNAL_ERROR", "internal server error")


@internal_verified_sync_bp.route("/api/internal/verified-sync/source-apply", methods=["POST"])
def apply_verified_sync_source_report():
    auth_error = _require_internal_token()
    if auth_error:
        return auth_error

    payload = request.get_json() or {}
    report = payload.get("report")
    if not isinstance(report, dict):
        return _error_response(400, "INVALID_REQUEST", "report is required")

    source_name = str(report.get("source_name") or "").strip()
    run_id = str(report.get("run_id") or "").strip()
    pipeline = report.get("pipeline")
    crawler_name = str(report.get("crawler_name") or source_name or "verified-sync").strip()
    status = str(report.get("status") or "fail").strip() or "fail"
    apply_payload = payload.get("apply_payload")
    if apply_payload is not None and not isinstance(apply_payload, dict):
        return _error_response(400, "INVALID_REQUEST", "apply_payload must be an object")

    if not source_name or not run_id:
        return _error_response(400, "INVALID_REQUEST", "report.source_name and report.run_id are required")

    try:
        resolve_crawler_class(source_name)
        conn = get_db()
        existing = find_existing_source_report(
            conn,
            run_id=run_id,
            source_name=source_name,
            pipeline=str(pipeline or "").strip() or None,
        )
        if existing is not None:
            return jsonify({"success": True, "report": existing, "idempotent": True}), 200

        final_report = apply_remote_report(
            conn,
            report=report,
            apply_payload=apply_payload,
        )
        insert_source_report(
            conn,
            crawler_name=crawler_name,
            status=status,
            report=final_report,
        )
        return jsonify({"success": True, "report": final_report, "idempotent": False}), 200
    except ValueError as exc:
        return _error_response(400, "INVALID_REQUEST", str(exc))
    except DatabaseUnavailableError:
        return _error_response(503, "DATABASE_UNAVAILABLE", "database not configured")
    except Exception:
        current_app.logger.exception("verified sync source apply failed")
        return _error_response(500, "INTERNAL_ERROR", "internal server error")
