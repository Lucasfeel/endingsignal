import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Sequence

from dotenv import load_dotenv

load_dotenv()

from database import create_standalone_connection
from run_all_crawlers import ALL_CRAWLERS, build_crawler_runner, run_cli, run_crawler_suite
from services.crawler_verification_service import build_verification_gate
from services.verified_sync_service import (
    VERIFIED_LOCAL_PIPELINE,
    build_freshness_text,
    build_latest_run_summary,
    build_run_context,
    get_stale_after_hours,
    get_next_attempt_no,
    get_verified_sync_freshness,
)
from utils.power import is_on_ac_power

OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "verified-runs"


def _parse_sources(raw_value: str) -> List[str]:
    return [token.strip() for token in str(raw_value or "").split(",") if token.strip()]


def _build_source_lookup() -> Dict[str, type]:
    lookup: Dict[str, type] = {}
    for crawler_class in ALL_CRAWLERS:
        instance = crawler_class()
        source_name = str(getattr(instance, "source_name", "")).strip()
        display_name = str(getattr(crawler_class, "DISPLAY_NAME", crawler_class.__name__)).strip()
        for token in {
            source_name,
            crawler_class.__name__,
            crawler_class.__name__.lower(),
            display_name,
            display_name.lower().replace(" ", "_"),
        }:
            if token:
                lookup[token] = crawler_class
    return lookup


def _resolve_crawler_classes(requested_sources: Sequence[str]) -> List[type]:
    if not requested_sources:
        return list(ALL_CRAWLERS)

    lookup = _build_source_lookup()
    resolved: List[type] = []
    seen = set()
    for token in requested_sources:
        crawler_class = lookup.get(str(token).strip())
        if crawler_class is None:
            raise ValueError(f"Unknown source: {token}")
        if crawler_class not in seen:
            resolved.append(crawler_class)
            seen.add(crawler_class)
    return resolved


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _build_artifact_dir(run_context: Dict[str, object]) -> Path:
    safe_run_id = str(run_context["run_id"]).replace(":", "_")
    return OUTPUT_DIR / safe_run_id


def _build_result_handler(run_context: Dict[str, object]):
    artifact_dir = _build_artifact_dir(run_context)

    def handle(payload: Dict) -> None:
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

    return handle


def _print_json(payload) -> None:
    def _json_default(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), flush=True)


def _select_retry_sources() -> List[str]:
    conn = create_standalone_connection()
    try:
        summary = build_latest_run_summary(conn, pipeline=VERIFIED_LOCAL_PIPELINE)
    finally:
        conn.close()
    return list(summary.get("retry_sources") or [])


def _show_latest_run_summary() -> int:
    conn = create_standalone_connection()
    try:
        summary = build_latest_run_summary(conn, pipeline=VERIFIED_LOCAL_PIPELINE)
    finally:
        conn.close()
    _print_json(summary)
    return 0


def _show_freshness(selected_sources: Sequence[str], *, stale_after_hours: float) -> int:
    conn = create_standalone_connection()
    try:
        freshness = get_verified_sync_freshness(
            conn,
            enabled_sources=selected_sources,
            pipeline=VERIFIED_LOCAL_PIPELINE,
            stale_after_hours=stale_after_hours,
        )
    finally:
        conn.close()
    print(build_freshness_text(freshness), flush=True)
    _print_json(freshness)
    return 0


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the verified local crawler sync.")
    parser.add_argument("--sources", help="Comma-separated source names to run.")
    parser.add_argument("--dry-run", action="store_true", help="Run crawl and verification without DB apply.")
    parser.add_argument(
        "--if-stale",
        action="store_true",
        help="Run only if the last successful verified sync is older than the configured staleness threshold.",
    )
    parser.add_argument("--require-ac", action="store_true", help="Defer automatic execution unless AC power is connected.")
    parser.add_argument(
        "--stale-after-hours",
        type=float,
        default=get_stale_after_hours(),
        help="Staleness threshold in hours for --if-stale and --explain-stale.",
    )
    parser.add_argument(
        "--retry-failed-sources",
        action="store_true",
        help="Resolve the source set from the latest verified run's failed or blocked sources.",
    )
    parser.add_argument(
        "--last-run-summary",
        action="store_true",
        help="Print the latest verified run summary and exit.",
    )
    parser.add_argument(
        "--explain-stale",
        action="store_true",
        help="Print today's verified freshness snapshot and exit.",
    )
    return parser.parse_args(argv)


async def _run_verified_sync(args: argparse.Namespace) -> int:
    if args.last_run_summary:
        return _show_latest_run_summary()

    requested_sources = _parse_sources(args.sources)
    if args.retry_failed_sources:
        requested_sources = _select_retry_sources()
        if not requested_sources:
            print("No failed or blocked sources found in the latest verified run.", flush=True)
            return 0

    crawler_classes = _resolve_crawler_classes(requested_sources)
    enabled_sources = [str(crawler_class().source_name) for crawler_class in crawler_classes]

    if args.explain_stale:
        return _show_freshness(enabled_sources, stale_after_hours=args.stale_after_hours)

    conn = create_standalone_connection()
    try:
        freshness = get_verified_sync_freshness(
            conn,
            enabled_sources=enabled_sources,
            pipeline=VERIFIED_LOCAL_PIPELINE,
            stale_after_hours=args.stale_after_hours,
        )
        if args.if_stale and not freshness.get("stale"):
            print(build_freshness_text(freshness), flush=True)
            print("Verified sync is still within the staleness threshold; skipping run.", flush=True)
            return 0

        if args.require_ac:
            ac_state = is_on_ac_power()
            if ac_state is False:
                print("Verified sync deferred because AC power is not connected.", flush=True)
                print(build_freshness_text(freshness), flush=True)
                return 0

        attempt_no = get_next_attempt_no(conn, pipeline=VERIFIED_LOCAL_PIPELINE)
    finally:
        conn.close()

    run_context = build_run_context(
        pipeline=VERIFIED_LOCAL_PIPELINE,
        enabled_sources=enabled_sources,
        attempt_no=attempt_no,
        dry_run=args.dry_run,
    )
    runner = build_crawler_runner(
        report_context=run_context,
        verification_gate_factory=lambda _crawler_class: build_verification_gate(dry_run=args.dry_run),
        write_enabled=not args.dry_run,
    )

    print(
        f"verified sync start: run_id={run_context['run_id']} "
        f"attempt_no={run_context['attempt_no']} "
        f"sources={','.join(enabled_sources)}",
        flush=True,
    )

    return await run_crawler_suite(
        crawler_classes,
        suite_display_name="verified local crawler sync",
        runner=runner,
        include_target_total_check=True,
        include_kakao_fetch_check=True,
        result_handler=_build_result_handler(run_context),
    )


async def main() -> int:
    args = parse_args(sys.argv[1:])
    return await _run_verified_sync(args)


if __name__ == "__main__":
    sys.exit(run_cli(main, "verified-sync"))
