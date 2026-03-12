import asyncio
import json
import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional, Sequence, TextIO

from dotenv import load_dotenv

load_dotenv()

from crawlers.kakaopage_novel_crawler import KakaoPageNovelCrawler
from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler
from crawlers.coupang_play_ott_crawler import CoupangPlayOttCrawler
from crawlers.disney_plus_ott_crawler import DisneyPlusOttCrawler
from crawlers.laftel_ott_crawler import LaftelOttCrawler
from crawlers.netflix_ott_crawler import NetflixOttCrawler
from crawlers.naver_series_novel_crawler import NaverSeriesNovelCrawler
from crawlers.naver_webtoon_crawler import NaverWebtoonCrawler
from crawlers.ridi_novel_crawler import RidiNovelCrawler
from crawlers.tving_ott_crawler import TvingOttCrawler
from crawlers.wavve_ott_crawler import WavveOttCrawler
from database import create_standalone_connection, get_cursor
from services.cdc_event_service import (
    record_due_scheduled_completions,
    record_due_scheduled_publications,
)
from services.notification_dispatch_service import dispatch_pending_completion_events
from services.verified_sync_service import enrich_report_data, normalize_verification_gate
from utils.time import now_kst_naive

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

ALL_CRAWLERS = [
    NaverWebtoonCrawler,
    KakaoWebtoonCrawler,
    NaverSeriesNovelCrawler,
    KakaoPageNovelCrawler,
    RidiNovelCrawler,
    TvingOttCrawler,
    WavveOttCrawler,
    CoupangPlayOttCrawler,
    DisneyPlusOttCrawler,
    NetflixOttCrawler,
    LaftelOttCrawler,
]

ERROR_STATUS_ALIASES = {"error", "fail", "failure", "fatal", "\uc2e4\ud328"}
WARN_STATUS_ALIASES = {"warn", "warning", "\uacbd\uace0"}
OK_STATUS_ALIASES = {"ok", "success", "\uc131\uacf5"}
SKIP_STATUS_ALIASES = {"skip", "skipped", "\uc2a4\ud0b5"}

FATAL_KAKAO_ERROR_SIGNATURES = (
    "SECTION_PARSE_ERROR",
    "SUSPICIOUS_EMPTY_RESULT",
    "suspicious empty result",
)

FINAL_COLLECTION_ORDER = (
    "naver_webtoon",
    "kakaowebtoon",
    "naver_series",
    "kakao_page",
    "ridi",
    "tving",
    "wavve",
    "coupangplay",
    "disney_plus",
    "netflix",
    "laftel",
)

FINAL_COLLECTION_LABELS = {
    "naver_webtoon": "\ub124\uc774\ubc84 \uc6f9\ud230",
    "kakaowebtoon": "\uce74\uce74\uc624\uc6f9\ud230",
    "naver_series": "\ub124\uc774\ubc84 \uc2dc\ub9ac\uc988",
    "kakao_page": "\uce74\uce74\uc624\ud398\uc774\uc9c0",
    "ridi": "\ub9ac\ub514",
    "tving": "\ud2f0\ube59",
    "wavve": "\uc6e8\uc774\ube0c",
    "coupangplay": "\ucfe0\ud321\ud50c\ub808\uc774",
    "disney_plus": "\ub514\uc988\ub2c8 \ud50c\ub7ec\uc2a4",
    "netflix": "\ub137\ud50c\ub9ad\uc2a4",
    "laftel": "\ub77c\ud504\ud154",
}

SOURCE_NAME_ALIASES = {
    "kakaowebtoon": "kakaowebtoon",
    "kakao_webtoon": "kakaowebtoon",
    "kakao_webtoon_crawler": "kakaowebtoon",
    "kakao_webtoon_crawler.py": "kakaowebtoon",
    "kakao_webtoon_crawler_class": "kakaowebtoon",
    "kakao_webtoon_display": "kakaowebtoon",
    "kakao_webtoon_name": "kakaowebtoon",
    "kakao_webtoon_title": "kakaowebtoon",
    "kakao_webtoon_label": "kakaowebtoon",
    "kakaopage_novel": "kakao_page",
    "kakaopage_novel_crawler": "kakao_page",
    "kakao_page": "kakao_page",
    "coupang_play": "coupangplay",
    "coupangplay": "coupangplay",
    "coupang_play_ott": "coupangplay",
    "coupang_play_ott_crawler": "coupangplay",
    "disney_plus": "disney_plus",
    "disneyplus": "disney_plus",
    "disney_plus_ott": "disney_plus",
    "disney_plus_ott_crawler": "disney_plus",
    "laftel": "laftel",
    "laftel_ott": "laftel",
    "netflix": "netflix",
    "netflix_ott": "netflix",
    "netflix_ott_crawler": "netflix",
    "naver_series": "naver_series",
    "naver_series_novel": "naver_series",
    "naver_series_novel_crawler": "naver_series",
    "naver_webtoon": "naver_webtoon",
    "naver_webtoon_crawler": "naver_webtoon",
    "ridi": "ridi",
    "ridi_novel": "ridi",
    "ridi_novel_crawler": "ridi",
    "tving": "tving",
    "tving_ott": "tving",
    "tving_ott_crawler": "tving",
    "wavve": "wavve",
    "wavve_ott": "wavve",
    "wavve_ott_crawler": "wavve",
}


class TeeStream:
    def __init__(self, *streams: TextIO):
        self._streams = streams

    def write(self, data: str) -> int:
        written = len(data)
        for stream in self._streams:
            self._write_to_stream(stream, data)
        return written

    def flush(self) -> None:
        for stream in self._streams:
            try:
                stream.flush()
            except Exception:
                pass

    def isatty(self) -> bool:
        for stream in self._streams:
            try:
                if stream.isatty():
                    return True
            except Exception:
                continue
        return False

    @property
    def encoding(self) -> str:
        for stream in self._streams:
            encoding = getattr(stream, "encoding", None)
            if encoding:
                return encoding
        return "utf-8"

    def writable(self) -> bool:
        return True

    @staticmethod
    def _write_to_stream(stream: TextIO, data: str) -> None:
        try:
            stream.write(data)
        except UnicodeEncodeError:
            encoding = getattr(stream, "encoding", None) or "utf-8"
            safe_text = data.encode(encoding, errors="backslashreplace").decode(
                encoding,
                errors="ignore",
            )
            stream.write(safe_text)


def _ensure_unique_sources(crawler_classes: Sequence[type]) -> None:
    source_map: Dict[str, List[str]] = {}
    for crawler_class in crawler_classes:
        instance = crawler_class()
        source_name = getattr(instance, "source_name", None)
        source_map.setdefault(str(source_name), []).append(crawler_class.__name__)

    duplicates = {key: value for key, value in source_map.items() if key and len(value) > 1}
    if duplicates:
        raise ValueError(
            f"Duplicate crawler registrations detected for sources: {duplicates}."
        )


_ensure_unique_sources(ALL_CRAWLERS)


def _normalize_source_token(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


def _normalize_source_name(value: Optional[str]) -> Optional[str]:
    token = _normalize_source_token(value)
    if not token:
        return None
    return SOURCE_NAME_ALIASES.get(token, token)


def _default_display_name(source_name: Optional[str], fallback: str) -> str:
    if source_name:
        normalized = _normalize_source_name(source_name)
        if normalized and normalized in FINAL_COLLECTION_LABELS:
            return FINAL_COLLECTION_LABELS[normalized]
        return str(source_name).replace("_", " ").title()
    return fallback


def _safe_non_negative_int(value) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def normalize_runtime_status(status) -> str:
    if status is None:
        return "ok"

    raw = str(status).strip().lower()
    if not raw:
        return "ok"
    if raw in ERROR_STATUS_ALIASES:
        return "error"
    if raw in WARN_STATUS_ALIASES:
        return "warn"
    if raw in OK_STATUS_ALIASES:
        return "ok"
    if raw in SKIP_STATUS_ALIASES:
        return "skip"
    return raw


def normalize_status_for_storage(status, *, success_value: str = "ok") -> str:
    normalized = normalize_runtime_status(status)
    if normalized == "error":
        return "fail"
    if normalized == "warn":
        return "warn"
    if normalized == "skip":
        return "skip"
    return success_value


def severity_prefix(status) -> str:
    normalized = normalize_runtime_status(status)
    if normalized == "error":
        return "ERROR"
    if normalized == "warn":
        return "WARNING"
    return "LOG"


def get_rollup_target_total_unique():
    raw = os.getenv("ROLLUP_TARGET_TOTAL_UNIQUE")
    if raw is None:
        return None, None

    raw = str(raw).strip()
    if not raw:
        return None, None

    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return None, f"Invalid ROLLUP_TARGET_TOTAL_UNIQUE={raw!r}; disabling threshold check."

    if parsed <= 0:
        return None, (
            f"Invalid ROLLUP_TARGET_TOTAL_UNIQUE={raw!r}; "
            "must be a positive integer, disabling threshold check."
        )

    return parsed, None


def _contains_fatal_kakao_errors(kakao_fetch_meta) -> bool:
    if not isinstance(kakao_fetch_meta, dict):
        return False

    errors = kakao_fetch_meta.get("errors")
    if not isinstance(errors, list):
        return False

    for error in errors:
        lowered = str(error).lower()
        for signature in FATAL_KAKAO_ERROR_SIGNATURES:
            if signature.lower() in lowered:
                return True
    return False


def is_kakao_fetch_failed(kakao_status, kakao_unique, kakao_fetch_meta) -> bool:
    if normalize_runtime_status(kakao_status) == "error":
        return True

    unique_count = _safe_non_negative_int(kakao_unique)
    if unique_count <= 0:
        return True

    if isinstance(kakao_fetch_meta, dict):
        health_warnings = kakao_fetch_meta.get("health_warnings")
        if isinstance(health_warnings, list) and health_warnings:
            return True
        if _contains_fatal_kakao_errors(kakao_fetch_meta):
            return True

    return False


def build_rollup_warning_reasons(actual_total_unique, target_total_unique, kakao_fetch_failed):
    warning_reasons = []

    if target_total_unique is not None and actual_total_unique < target_total_unique:
        warning_reasons.append("TOTAL_UNIQUE_BELOW_TARGET")

    if kakao_fetch_failed:
        warning_reasons.append("KAKAO_FETCH_FAILED")

    return warning_reasons


def _infer_source_name(result: Dict) -> Optional[str]:
    source_name = _normalize_source_name(result.get("source_name"))
    if source_name:
        return source_name

    summary = result.get("summary")
    if isinstance(summary, dict):
        summary_source = _normalize_source_name(summary.get("crawler"))
        if summary_source:
            return summary_source

    crawler_name = _normalize_source_name(result.get("crawler_name"))
    if crawler_name:
        return crawler_name

    return None


def _build_counts_by_source(results: Sequence[dict]) -> Dict[str, int]:
    counts_by_source: Dict[str, int] = {}

    for result in results:
        if not isinstance(result, dict):
            continue
        source_name = _infer_source_name(result)
        if not source_name:
            continue
        fetched_count = _safe_non_negative_int(result.get("fetched_count"))
        counts_by_source[source_name] = counts_by_source.get(source_name, 0) + fetched_count

    return counts_by_source


def format_final_collection_summary(results: List[dict]) -> str:
    counts_by_source = _build_counts_by_source(results)
    ordered_sources: List[str] = []

    for source_name in FINAL_COLLECTION_ORDER:
        if source_name in counts_by_source:
            ordered_sources.append(source_name)

    for source_name in sorted(counts_by_source):
        if source_name not in FINAL_COLLECTION_ORDER:
            ordered_sources.append(source_name)

    total = sum(counts_by_source.values())
    if not ordered_sources:
        return f"\ucd1d {total}\uac1c \uc218\uc9d1"

    segments = [
        f"{FINAL_COLLECTION_LABELS.get(source_name, source_name)} {counts_by_source[source_name]}\uac1c \uc218\uc9d1"
        for source_name in ordered_sources
    ]
    segments.append(f"\ucd1d {total}\uac1c \uc218\uc9d1")
    return ", ".join(segments)


def build_rollup_payload(
    results: Sequence[dict],
    *,
    include_target_total_check: bool,
    include_kakao_fetch_check: bool,
):
    counts_by_source = _build_counts_by_source(results)
    actual_total_unique = sum(counts_by_source.values())
    target_total_unique, target_warning = (
        get_rollup_target_total_unique() if include_target_total_check else (None, None)
    )

    kakao_unique = counts_by_source.get("kakaowebtoon", 0)
    naver_unique = counts_by_source.get("naver_webtoon", 0)
    kakao_present = "kakaowebtoon" in counts_by_source
    kakao_status = None
    kakao_summary = None
    kakao_fetch_meta = None

    for result in results:
        if _infer_source_name(result) != "kakaowebtoon":
            continue
        kakao_status = result.get("status")
        kakao_summary = result.get("summary")
        cdc_info = result.get("cdc_info")
        if isinstance(cdc_info, dict):
            kakao_fetch_meta = cdc_info.get("fetch_meta")
        break

    kakao_fetch_failed = False
    if include_kakao_fetch_check and kakao_present:
        kakao_fetch_failed = is_kakao_fetch_failed(
            kakao_status=kakao_status,
            kakao_unique=kakao_unique,
            kakao_fetch_meta=kakao_fetch_meta,
        )

    warning_reasons = build_rollup_warning_reasons(
        actual_total_unique=actual_total_unique,
        target_total_unique=target_total_unique,
        kakao_fetch_failed=kakao_fetch_failed,
    )
    warning = ",".join(warning_reasons) if warning_reasons else None

    kakao_rollup_log = None
    if kakao_fetch_failed:
        summary_reason = None
        summary_message = None
        if isinstance(kakao_summary, dict):
            summary_reason = kakao_summary.get("reason")
            summary_message = kakao_summary.get("message")

        kakao_rollup_status = normalize_runtime_status(kakao_status)
        kakao_rollup_status = "error" if kakao_rollup_status == "error" else "warn"
        kakao_rollup_log = {
            "status": kakao_rollup_status,
            "reason": summary_reason,
            "message": summary_message,
        }

    rollup = {
        "counts_by_source": counts_by_source,
        "naver_unique": naver_unique,
        "kakao_unique": kakao_unique,
        "actual_total_unique": actual_total_unique,
        "target_total_unique": target_total_unique,
        "warning": warning,
    }
    return rollup, target_warning, warning_reasons, kakao_rollup_log


def format_rollup_uniques(rollup: Dict[str, object]) -> str:
    counts_by_source = rollup.get("counts_by_source") or {}
    segments = []

    for source_name in FINAL_COLLECTION_ORDER:
        if source_name in counts_by_source:
            label = FINAL_COLLECTION_LABELS.get(source_name, source_name)
            segments.append(f"{label}: {counts_by_source[source_name]}")

    for source_name in sorted(counts_by_source):
        if source_name in FINAL_COLLECTION_ORDER:
            continue
        label = FINAL_COLLECTION_LABELS.get(source_name, source_name)
        segments.append(f"{label}: {counts_by_source[source_name]}")

    target_total_unique = rollup.get("target_total_unique")
    target_label = target_total_unique if target_total_unique is not None else "disabled"
    total_label = f"Total: {rollup.get('actual_total_unique', 0)} / {target_label}"

    if segments:
        return f"Rollup uniques -> {', '.join(segments)}, {total_label}"
    return f"Rollup uniques -> {total_label}"


def _configure_stream_encoding(stream: TextIO) -> None:
    reconfigure = getattr(stream, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:
            pass


@contextmanager
def capture_runner_output(log_prefix: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    log_path = OUTPUT_DIR / f"{log_prefix}-run-{timestamp}.log"

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    _configure_stream_encoding(original_stdout)
    _configure_stream_encoding(original_stderr)

    with log_path.open("w", encoding="utf-8-sig", newline="") as log_file:
        sys.stdout = TeeStream(log_file, original_stdout)
        sys.stderr = TeeStream(log_file, original_stderr)
        try:
            print(f"LOG_PATH: {log_path.resolve()}", flush=True)
            yield log_path
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            sys.stdout = original_stdout
            sys.stderr = original_stderr


def _serialize_report(report: Dict) -> str:
    return json.dumps(report, ensure_ascii=False)


def _write_daily_report(crawler_name: str, status: str, report: Dict) -> None:
    report_conn = None
    try:
        report_conn = create_standalone_connection()
        report_cursor = get_cursor(report_conn)
        report_cursor.execute(
            """
            INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
            VALUES (%s, %s, %s)
            """,
            (crawler_name, status, _serialize_report(report)),
        )
        report_conn.commit()
        report_cursor.close()
        print(f"LOG: [{crawler_name}] report written to daily_crawler_reports", flush=True)
    except Exception as report_exc:
        print(
            f"FATAL: [{crawler_name}] report write failed: {report_exc}",
            file=sys.stderr,
            flush=True,
        )
    finally:
        if report_conn:
            report_conn.close()


def _extract_report_verification_gate(report: Dict) -> Optional[Dict]:
    cdc_info = report.get("cdc_info")
    if isinstance(cdc_info, dict):
        verification = cdc_info.get("verification")
        if isinstance(verification, dict):
            return verification

    verification = report.get("verification_gate")
    if isinstance(verification, dict):
        return verification
    return None


def _resolve_apply_result(report: Dict, *, write_enabled: bool) -> str:
    status = normalize_runtime_status(report.get("status"))
    if status == "skip":
        return "skipped"

    cdc_info = report.get("cdc_info")
    if isinstance(cdc_info, dict):
        apply_result = cdc_info.get("apply_result")
        if apply_result:
            return str(apply_result)
        if cdc_info.get("db_sync_skipped"):
            return "skipped"

    if not write_enabled:
        return "dry_run"
    return "applied"


async def run_one_crawler(
    crawler_class,
    *,
    report_context: Optional[Dict] = None,
    verification_gate=None,
    write_enabled: bool = True,
):
    report = {"status": "ok", "fetched_count": 0}
    crawler_start_time = time.time()
    report_status = "ok"

    db_conn = None
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
            print(
                f"WARNING: [{crawler_display_name}] skipped (missing env vars): {missing_env_vars}",
                flush=True,
            )
        else:
            crawler_instance = crawler_class()
            source_name = getattr(crawler_instance, "source_name", None)
            crawler_display_name = getattr(
                crawler_class,
                "DISPLAY_NAME",
                _default_display_name(source_name, crawler_class.__name__),
            )

            print(f"--- [{crawler_display_name}] crawler run start ---", flush=True)

            db_conn = create_standalone_connection()
            new_contents, newly_completed_items, cdc_info = await crawler_instance.run_daily_check(
                db_conn,
                verification_gate=verification_gate,
                write_enabled=write_enabled,
            )
            if not isinstance(cdc_info, dict):
                cdc_info = {}

            report_status = normalize_status_for_storage(cdc_info.get("status", "ok"))
            report.update(
                {
                    "status": report_status,
                    "new_contents": new_contents,
                    "newly_completed_items": newly_completed_items,
                    "cdc_info": cdc_info,
                    "inserted_count": _safe_non_negative_int(cdc_info.get("inserted_count")),
                    "updated_count": _safe_non_negative_int(cdc_info.get("updated_count")),
                    "unchanged_count": _safe_non_negative_int(cdc_info.get("unchanged_count")),
                    "write_skipped_count": _safe_non_negative_int(cdc_info.get("write_skipped_count")),
                    "fetched_count": _safe_non_negative_int(
                        (cdc_info.get("health") or {}).get("fetched_count")
                    ),
                    "summary": cdc_info.get("summary"),
                }
            )

    except Exception as exc:
        report_status = normalize_status_for_storage("fail")
        print(
            f"FATAL: [{crawler_display_name}] crawler execution failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
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
        verification_gate_summary = _extract_report_verification_gate(report)
        apply_result = _resolve_apply_result(report, write_enabled=write_enabled)
        if verification_gate_summary is not None:
            report["verification_gate"] = normalize_verification_gate(verification_gate_summary)
        report["apply_result"] = apply_result
        report = enrich_report_data(
            report,
            run_context=report_context,
            verification_gate=verification_gate_summary,
            apply_result=apply_result,
        )
        if db_conn:
            db_conn.close()

        _write_daily_report(crawler_display_name, report_status, report)

        normalized_status = normalize_runtime_status(report_status)
        if normalized_status in ("error", "warn"):
            summary = report.get("summary") or {}
            reason = summary.get("reason") if isinstance(summary, dict) else None
            message = summary.get("message") if isinstance(summary, dict) else None
            prefix = severity_prefix(normalized_status)
            print(
                f"{prefix}: [{crawler_display_name}] run status={normalized_status} "
                f"reason={reason} message={message}",
                file=sys.stderr,
                flush=True,
            )

        return report


def build_crawler_runner(
    *,
    report_context: Optional[Dict] = None,
    verification_gate_factory=None,
    write_enabled: bool = True,
):
    async def runner(crawler_class):
        verification_gate = (
            verification_gate_factory(crawler_class)
            if callable(verification_gate_factory)
            else verification_gate_factory
        )
        return await run_one_crawler(
            crawler_class,
            report_context=report_context,
            verification_gate=verification_gate,
            write_enabled=write_enabled,
        )

    return runner


def _run_reporting_job(
    crawler_name: str,
    callback: Callable[..., Dict],
    *,
    callback_args: Sequence = (),
    success_value: str = "success",
    report_context: Optional[Dict] = None,
    verification_gate: Optional[Dict] = None,
    apply_result: Optional[str] = None,
):
    report = {"status": success_value}
    start_time = time.time()
    conn = None
    cursor = None

    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        result = callback(conn, cursor, *callback_args)
        conn.commit()
        report.update(result)
        report["status"] = normalize_status_for_storage(
            report.get("status"),
            success_value=success_value,
        )
    except Exception as exc:
        print(f"FATAL: [{crawler_name}] execution failed: {exc}", file=sys.stderr, flush=True)
        report["status"] = normalize_status_for_storage("fail", success_value=success_value)
        report["error_message"] = traceback.format_exc()
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            conn.close()

    report["duration"] = time.time() - start_time
    resolved_apply_result = apply_result or (
        "applied" if normalize_runtime_status(report.get("status")) not in {"error", "skip"} else "skipped"
    )
    report = enrich_report_data(
        report,
        run_context=report_context,
        verification_gate=verification_gate,
        apply_result=resolved_apply_result,
    )
    _write_daily_report(crawler_name, report["status"], report)
    return report


def run_scheduled_completion_cdc(report_context: Optional[Dict] = None):
    return _run_reporting_job(
        "scheduled completion cdc",
        record_due_scheduled_completions,
        callback_args=(now_kst_naive(),),
        report_context=report_context,
        verification_gate={"status": "not_applicable", "mode": "dispatch_only", "reason": "dispatch_job"},
    )


def run_scheduled_publication_cdc(report_context: Optional[Dict] = None):
    return _run_reporting_job(
        "scheduled publication cdc",
        record_due_scheduled_publications,
        callback_args=(now_kst_naive(),),
        report_context=report_context,
        verification_gate={"status": "not_applicable", "mode": "dispatch_only", "reason": "dispatch_job"},
    )


def run_completion_notification_dispatch(report_context: Optional[Dict] = None):
    report = {"status": "success"}
    start_time = time.time()
    conn = None
    template_code = (os.getenv("AIT_COMPLETION_TEMPLATE_CODE") or "").strip()

    try:
        conn = create_standalone_connection()
        result = dispatch_pending_completion_events(
            conn,
            template_code=template_code,
            limit=int(os.getenv("AIT_NOTIFICATION_DISPATCH_LIMIT", "100")),
        )
        report.update(result)
        report["status"] = normalize_status_for_storage(
            report.get("status"),
            success_value="success",
        )
    except Exception as exc:
        print(
            f"FATAL: [completion notification dispatch] execution failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        report["status"] = normalize_status_for_storage("fail", success_value="success")
        report["error_message"] = traceback.format_exc()
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            conn.close()

    report["duration"] = time.time() - start_time
    resolved_apply_result = (
        "applied" if normalize_runtime_status(report.get("status")) not in {"error", "skip"} else "skipped"
    )
    report = enrich_report_data(
        report,
        run_context=report_context,
        verification_gate={"status": "not_applicable", "mode": "dispatch_only", "reason": "dispatch_job"},
        apply_result=resolved_apply_result,
    )
    _write_daily_report("completion notification dispatch", report["status"], report)
    return report


async def run_crawler_suite(
    crawler_classes: Sequence[type],
    *,
    suite_display_name: str,
    runner: Optional[Callable[[type], Awaitable[dict]]] = None,
    post_run_actions: Sequence[Callable[[], Dict]] = (),
    include_target_total_check: bool = False,
    include_kakao_fetch_check: bool = False,
    emit_json_payload: bool = False,
    result_handler: Optional[Callable[[Dict], None]] = None,
) -> int:
    start_time = time.time()
    runner = runner or run_one_crawler
    print("==========================================", flush=True)
    print(f"   {suite_display_name}", flush=True)
    print("==========================================", flush=True)

    results = await asyncio.gather(
        *(runner(crawler_class) for crawler_class in crawler_classes),
        return_exceptions=True,
    )

    has_error = False
    has_warn = False
    summarized_results = []

    for result in results:
        if isinstance(result, Exception):
            has_error = True
            print(
                f"ERROR: crawler gather failure: {result}",
                file=sys.stderr,
                flush=True,
            )
            continue

        summarized_results.append(result)
        status_label = normalize_runtime_status(result.get("status"))
        if status_label == "error":
            has_error = True
        elif status_label == "warn":
            has_warn = True

    rollup, target_warning, warning_reasons, kakao_rollup_log = build_rollup_payload(
        summarized_results,
        include_target_total_check=include_target_total_check,
        include_kakao_fetch_check=include_kakao_fetch_check,
    )

    if target_warning:
        has_warn = True
        print(f"WARNING: {target_warning}", file=sys.stderr, flush=True)

    if warning_reasons:
        has_warn = True

    if kakao_rollup_log:
        kakao_rollup_status = kakao_rollup_log["status"]
        kakao_prefix = severity_prefix(kakao_rollup_status)
        log_file = sys.stderr if kakao_rollup_status == "error" else sys.stdout
        print(
            f"{kakao_prefix}: [Kakaowebtoon] rollup status={kakao_rollup_status} "
            f"reason={kakao_rollup_log.get('reason')} message={kakao_rollup_log.get('message')}",
            file=log_file,
            flush=True,
        )

    rollup_status = "error" if has_error else "warn" if has_warn else "ok"
    print(format_rollup_uniques(rollup), flush=True)
    if rollup.get("warning"):
        print(f"{severity_prefix(rollup_status)}: {rollup['warning']}", flush=True)
    print(f"Final rollup payload: {json.dumps(rollup, ensure_ascii=False)}", flush=True)
    print(format_final_collection_summary(summarized_results), flush=True)

    if emit_json_payload:
        payload = {
            "results": summarized_results,
            "duration_seconds": round(time.time() - start_time, 3),
            "rollup": rollup,
        }
        print(json.dumps(payload, ensure_ascii=False), flush=True)

    action_reports = []
    for action in post_run_actions:
        action_report = action()
        action_reports.append(action_report)
        action_status = normalize_runtime_status((action_report or {}).get("status"))
        if action_status == "error":
            has_error = True
        elif action_status == "warn":
            has_warn = True

    total_duration = time.time() - start_time
    final_status = "error" if has_error else "warn" if has_warn else "ok"
    print("==========================================", flush=True)
    print(
        f"   {suite_display_name} complete (total duration: {total_duration:.2f}s)",
        flush=True,
    )
    print("==========================================", flush=True)

    payload = {
        "results": summarized_results,
        "action_reports": action_reports,
        "duration_seconds": round(total_duration, 3),
        "rollup": rollup,
        "final_status": final_status,
    }
    if callable(result_handler):
        result_handler(payload)

    return 1 if final_status == "error" else 0


async def main():
    return await run_crawler_suite(
        ALL_CRAWLERS,
        suite_display_name="integrated crawler run start",
        post_run_actions=(
            run_scheduled_completion_cdc,
            run_scheduled_publication_cdc,
            run_completion_notification_dispatch,
        ),
        include_target_total_check=True,
        include_kakao_fetch_check=True,
    )


def run_cli(main_coro: Callable[[], Awaitable[int]], log_prefix: str) -> int:
    exit_code = 1
    with capture_runner_output(log_prefix):
        try:
            exit_code = asyncio.run(main_coro())
        except Exception:
            print("ERROR: integrated crawler execution crashed.", file=sys.stderr, flush=True)
            traceback.print_exc()
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(run_cli(main, "crawler"))
