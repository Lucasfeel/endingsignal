import argparse
import asyncio
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Dict, List, Sequence
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

load_dotenv()

from run_all_crawlers import (
    normalize_runtime_status,
    normalize_status_for_storage,
    run_cli,
    run_crawler_suite,
)
from services.crawler_verification_service import build_verification_gate
from services.verified_sync_registry import resolve_crawler_classes
from services.verified_sync_service import (
    VERIFIED_CLOUD_PIPELINE,
    build_run_context,
    enrich_report_data,
    normalize_verification_gate,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "verified-cloud-runs"
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("VERIFIED_SYNC_REQUEST_TIMEOUT_SECONDS", "60"))


def _parse_sources(raw_value: str) -> List[str]:
    return [token.strip() for token in str(raw_value or "").split(",") if token.strip()]


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _build_artifact_dir(run_context: Dict[str, object]) -> Path:
    safe_run_id = str(run_context["run_id"]).replace(":", "_")
    return OUTPUT_DIR / safe_run_id


def _extract_verification_gate(report: Dict) -> Dict | None:
    cdc_info = report.get("cdc_info")
    if isinstance(cdc_info, dict):
        verification = cdc_info.get("verification")
        if isinstance(verification, dict):
            return verification

    verification = report.get("verification_gate")
    if isinstance(verification, dict):
        return verification
    return None


def _build_result_handler(captured: Dict[str, Dict]) -> None:
    def handle(payload: Dict) -> None:
        captured["payload"] = payload

    return handle


def _write_artifacts(run_context: Dict[str, object], payload: Dict) -> None:
    artifact_dir = _build_artifact_dir(run_context)
    verification_rows = []
    apply_rows = []

    for result in payload.get("results") or []:
        verification_rows.append(
            {
                "source_name": result.get("source_name"),
                "crawler_name": result.get("crawler_name"),
                "status": result.get("status"),
                "verification_gate": result.get("verification_gate"),
            }
        )
        apply_rows.append(
            {
                "source_name": result.get("source_name"),
                "crawler_name": result.get("crawler_name"),
                "status": result.get("status"),
                "apply_result": result.get("apply_result"),
            }
        )

    _write_json(artifact_dir / "run-summary.json", {"run_context": run_context, **payload})
    _write_json(artifact_dir / "verification.json", verification_rows)
    _write_json(artifact_dir / "apply-result.json", apply_rows)


def _build_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _fetch_snapshot(*, base_url: str, token: str, source_name: str, timeout_seconds: float) -> Dict:
    response = requests.get(
        urljoin(base_url.rstrip("/") + "/", "api/internal/verified-sync/source-snapshot"),
        headers=_build_headers(token),
        params={"source": source_name},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success") or not isinstance(payload.get("snapshot"), dict):
        raise RuntimeError(f"snapshot request failed for {source_name}")
    return payload["snapshot"]


def _commit_source_report(
    *,
    base_url: str,
    token: str,
    report: Dict,
    apply_payload: Dict | None,
    timeout_seconds: float,
) -> Dict:
    response = requests.post(
        urljoin(base_url.rstrip("/") + "/", "api/internal/verified-sync/source-apply"),
        headers=_build_headers(token),
        json={
            "report": report,
            "apply_payload": apply_payload,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success") or not isinstance(payload.get("report"), dict):
        raise RuntimeError(
            f"remote apply request failed for {report.get('source_name') or report.get('crawler_name')}"
        )
    return payload["report"]


def _final_status_from_payload(payload: Dict) -> str:
    existing_status = normalize_runtime_status(payload.get("final_status"))
    has_error = existing_status == "error"
    has_warn = existing_status == "warn"
    for result in payload.get("results") or []:
        status = normalize_runtime_status((result or {}).get("status"))
        if status == "error":
            has_error = True
        elif status == "warn":
            has_warn = True
    return "error" if has_error else "warn" if has_warn else "ok"


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the verified crawler sync through Codex cloud.")
    parser.add_argument("--sources", help="Comma-separated source names to run.")
    parser.add_argument("--dry-run", action="store_true", help="Run crawl and verification without remote DB apply.")
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("VERIFIED_SYNC_API_BASE_URL", ""),
        help="Base URL for the endingsignal API that provides snapshot/apply endpoints.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for snapshot/apply requests.",
    )
    return parser.parse_args(argv)


async def _run_one_cloud_crawler(
    crawler_class,
    *,
    report_context: Dict,
    dry_run: bool,
    api_base_url: str,
    api_token: str,
    timeout_seconds: float,
):
    report = {"status": "ok", "fetched_count": 0}
    crawler_start_time = time.time()
    report_status = "ok"

    crawler_display_name = getattr(crawler_class, "DISPLAY_NAME", crawler_class.__name__)
    source_name = None
    required_env_vars = getattr(crawler_class, "REQUIRED_ENV_VARS", [])
    missing_env_vars = [key for key in required_env_vars if not os.getenv(key)]

    try:
        if missing_env_vars:
            report_status = normalize_status_for_storage("skip")
            report.update(
                {
                    "status": report_status,
                    "skip_reason": "missing_required_env_vars",
                    "missing_env_vars": missing_env_vars,
                    "summary": {
                        "crawler": crawler_display_name,
                        "reason": "missing_env",
                        "message": "crawler skipped due to missing required env vars",
                    },
                }
            )
        else:
            crawler_instance = crawler_class()
            source_name = getattr(crawler_instance, "source_name", None)
            crawler_display_name = getattr(
                crawler_class,
                "DISPLAY_NAME",
                crawler_class.__name__,
            )

            snapshot = _fetch_snapshot(
                base_url=api_base_url,
                token=api_token,
                source_name=str(source_name),
                timeout_seconds=timeout_seconds,
            )
            added, newly_completed_items, cdc_info, apply_payload = await crawler_instance.prepare_remote_daily_check(
                snapshot,
                verification_gate=build_verification_gate(dry_run=dry_run),
                write_enabled=not dry_run,
            )
            if not isinstance(cdc_info, dict):
                cdc_info = {}

            report_status = normalize_status_for_storage(cdc_info.get("status", "ok"))
            report.update(
                {
                    "status": report_status,
                    "new_contents": added,
                    "newly_completed_items": newly_completed_items,
                    "cdc_info": cdc_info,
                    "inserted_count": int(cdc_info.get("inserted_count") or 0),
                    "updated_count": int(cdc_info.get("updated_count") or 0),
                    "unchanged_count": int(cdc_info.get("unchanged_count") or 0),
                    "write_skipped_count": int(cdc_info.get("write_skipped_count") or 0),
                    "fetched_count": int(((cdc_info.get("health") or {}).get("fetched_count")) or 0),
                    "summary": cdc_info.get("summary"),
                }
            )
            if isinstance(apply_payload, dict):
                report["__cloud_apply_payload"] = apply_payload
    except Exception as exc:
        report_status = normalize_status_for_storage("fail")
        report.update(
            {
                "status": report_status,
                "error_message": traceback.format_exc(),
                "summary": {
                    "crawler": crawler_display_name,
                    "reason": "exception",
                    "message": str(exc),
                },
            }
        )
    finally:
        report["duration"] = time.time() - crawler_start_time
        report["status"] = report_status
        report["crawler_name"] = crawler_display_name
        report.setdefault("inserted_count", 0)
        report.setdefault("updated_count", 0)
        report.setdefault("unchanged_count", 0)
        report.setdefault("write_skipped_count", 0)
        if source_name:
            report["source_name"] = source_name
        verification_gate_summary = _extract_verification_gate(report)
        apply_result = str((report.get("cdc_info") or {}).get("apply_result") or ("dry_run" if dry_run else "applied"))
        if verification_gate_summary is not None:
            report["verification_gate"] = normalize_verification_gate(verification_gate_summary)
        report["apply_result"] = apply_result
        return enrich_report_data(
            report,
            run_context=report_context,
            verification_gate=verification_gate_summary,
            apply_result=apply_result,
        )


async def _run_verified_sync_cloud(args: argparse.Namespace) -> int:
    api_base_url = str(args.api_base_url or "").strip()
    api_token = str(os.getenv("VERIFIED_SYNC_INTERNAL_TOKEN") or "").strip()
    if not api_base_url:
        raise RuntimeError("VERIFIED_SYNC_API_BASE_URL or --api-base-url is required")
    if not api_token:
        raise RuntimeError("VERIFIED_SYNC_INTERNAL_TOKEN is required")

    crawler_classes = resolve_crawler_classes(_parse_sources(args.sources))
    enabled_sources = [str(crawler_class().source_name) for crawler_class in crawler_classes]
    run_context = build_run_context(
        pipeline=VERIFIED_CLOUD_PIPELINE,
        enabled_sources=enabled_sources,
        attempt_no=1,
        dry_run=args.dry_run,
    )

    captured_payload: Dict[str, Dict] = {}

    async def runner(crawler_class):
        return await _run_one_cloud_crawler(
            crawler_class,
            report_context=run_context,
            dry_run=args.dry_run,
            api_base_url=api_base_url,
            api_token=api_token,
            timeout_seconds=args.timeout_seconds,
        )

    exit_code = await run_crawler_suite(
        crawler_classes,
        suite_display_name="verified cloud crawler sync",
        runner=runner,
        include_target_total_check=True,
        include_kakao_fetch_check=True,
        result_handler=_build_result_handler(captured_payload),
    )

    payload = captured_payload.get("payload") or {
        "results": [],
        "action_reports": [],
        "duration_seconds": 0,
        "rollup": {},
        "final_status": "error",
    }

    if not args.dry_run:
        for index, result in enumerate(payload.get("results") or []):
            apply_payload = result.pop("__cloud_apply_payload", None)
            try:
                payload["results"][index] = _commit_source_report(
                    base_url=api_base_url,
                    token=api_token,
                    report=result,
                    apply_payload=apply_payload,
                    timeout_seconds=args.timeout_seconds,
                )
            except Exception as exc:
                failed = dict(result)
                failed["status"] = normalize_status_for_storage("fail")
                failed["apply_result"] = "error"
                failed["error_message"] = traceback.format_exc()
                failed["summary"] = {
                    "crawler": failed.get("crawler_name") or failed.get("source_name") or "verified-sync-cloud",
                    "reason": "remote_apply_exception",
                    "message": str(exc),
                }
                cdc_info = dict(failed.get("cdc_info") or {})
                cdc_info["apply_result"] = "error"
                cdc_info["skip_reason"] = "remote_apply_error"
                failed["cdc_info"] = cdc_info
                payload["results"][index] = failed
                exit_code = 1

    payload["final_status"] = _final_status_from_payload(payload)
    if payload["final_status"] == "error":
        exit_code = 1

    _write_artifacts(run_context, payload)
    return exit_code


async def main() -> int:
    args = parse_args(sys.argv[1:])
    return await _run_verified_sync_cloud(args)


if __name__ == "__main__":
    sys.exit(run_cli(main, "verified-sync-cloud"))
