# run_all_crawlers.py
import asyncio
import json
import os
import sys
import time
import traceback

from dotenv import load_dotenv

load_dotenv()

from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler
from crawlers.laftel_ott_crawler import LaftelOttCrawler
from crawlers.naver_webtoon_crawler import NaverWebtoonCrawler
from crawlers.ridi_novel_crawler import RidiNovelCrawler
from database import create_standalone_connection, get_cursor
from services.cdc_event_service import (
    record_due_scheduled_completions,
    record_due_scheduled_publications,
)
from utils.time import now_kst_naive

ALL_CRAWLERS = [
    NaverWebtoonCrawler,
    KakaoWebtoonCrawler,
    RidiNovelCrawler,
    LaftelOttCrawler,
]

ERROR_STATUS_ALIASES = {"error", "fail", "fatal", "실패"}
WARN_STATUS_ALIASES = {"warn", "warning", "경고"}
OK_STATUS_ALIASES = {"ok", "success", "성공"}
SKIP_STATUS_ALIASES = {"skip", "스킵"}

FATAL_KAKAO_ERROR_SIGNATURES = (
    "SECTION_PARSE_ERROR",
    "SUSPICIOUS_EMPTY_RESULT",
    "suspicious empty result",
)


def _ensure_unique_sources(crawler_classes):
    source_map = {}
    for crawler_class in crawler_classes:
        instance = crawler_class()
        source_name = getattr(instance, "source_name", None)
        source_map.setdefault(source_name, []).append(crawler_class.__name__)

    duplicates = {k: v for k, v in source_map.items() if k and len(v) > 1}
    if duplicates:
        raise ValueError(
            f"Duplicate crawler registrations detected for sources: {duplicates}."
        )


_ensure_unique_sources(ALL_CRAWLERS)


def normalize_runtime_status(status):
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


def severity_prefix(status):
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


def _contains_fatal_kakao_errors(kakao_fetch_meta):
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


def is_kakao_fetch_failed(kakao_status, kakao_unique, kakao_fetch_meta):
    if normalize_runtime_status(kakao_status) == "error":
        return True

    try:
        unique_count = int(kakao_unique or 0)
    except (TypeError, ValueError):
        unique_count = 0
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


async def run_one_crawler(crawler_class):
    report = {"status": "ok", "fetched_count": 0}
    crawler_start_time = time.time()
    report_status = "ok"

    db_conn = None
    crawler_display_name = getattr(crawler_class, "DISPLAY_NAME", crawler_class.__name__)
    required_env_vars = getattr(crawler_class, "REQUIRED_ENV_VARS", [])
    missing_env_vars = [key for key in required_env_vars if not os.getenv(key)]

    try:
        if missing_env_vars:
            crawler_display_name = crawler_display_name.replace("_", " ").title()
            print(
                f"WARNING: [{crawler_display_name}] skipped (missing env vars): {missing_env_vars}",
                flush=True,
            )
            report.update(
                {
                    "status": "스킵",
                    "skip_reason": "missing_required_env_vars",
                    "missing_env_vars": missing_env_vars,
                    "note": "Set KAKAOWEBTOON_WEBID / KAKAOWEBTOON_T_ANO in Render env to enable this crawler.",
                    "summary": {
                        "crawler": crawler_display_name,
                        "reason": "missing_env",
                        "message": "crawler skipped due to missing required env vars",
                    },
                }
            )
            report_status = "skip"
        else:
            try:
                crawler_instance = crawler_class()
                crawler_display_name = getattr(crawler_instance, "source_name", crawler_class.__name__)
                crawler_display_name = crawler_display_name.replace("_", " ").title()

                print(f"\n--- [{crawler_display_name}] 크롤러 작업 시작 ---", flush=True)

                db_conn = create_standalone_connection()
                new_contents, newly_completed_items, cdc_info = await crawler_instance.run_daily_check(db_conn)
                if not isinstance(cdc_info, dict):
                    cdc_info = {}

                report.update(
                    {
                        "new_contents": new_contents,
                        "newly_completed_items": newly_completed_items,
                        "cdc_info": cdc_info,
                        "fetched_count": cdc_info.get("health", {}).get("fetched_count", 0),
                        "summary": cdc_info.get("summary"),
                    }
                )
                report_status = cdc_info.get("status", "ok")

            except Exception as exc:
                crawler_display_name = crawler_display_name.replace("_", " ").title()
                print(
                    f"FATAL: [{crawler_display_name}] 크롤러 실행 중 치명적 오류 발생: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                report["status"] = "fail"
                report_status = "fail"
                report["error_message"] = traceback.format_exc()
                report["summary"] = {
                    "crawler": crawler_display_name,
                    "reason": "exception",
                    "message": str(exc),
                }

    finally:
        report["duration"] = time.time() - crawler_start_time
        report["status"] = report_status
        report["crawler_name"] = crawler_display_name
        if db_conn:
            db_conn.close()

        report_conn = None
        try:
            report_conn = create_standalone_connection()
            report_cursor = get_cursor(report_conn)
            report_cursor.execute(
                """
                INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
                VALUES (%s, %s, %s)
                """,
                (crawler_display_name, report_status, json.dumps(report)),
            )
            report_conn.commit()
            report_cursor.close()
            print(f"LOG: [{crawler_display_name}]의 실행 결과를 DB에 성공적으로 저장했습니다.", flush=True)
        except Exception as report_exc:
            print(
                f"FATAL: [{crawler_display_name}]의 보고서를 DB에 저장하는 데 실패했습니다: {report_exc}",
                file=sys.stderr,
                flush=True,
            )
        finally:
            if report_conn:
                report_conn.close()

        normalized_status = normalize_runtime_status(report_status)
        if normalized_status in ("error", "warn"):
            summary = report.get("summary") or {}
            reason = summary.get("reason") if isinstance(summary, dict) else None
            message = summary.get("message") if isinstance(summary, dict) else None
            prefix = severity_prefix(normalized_status)
            print(
                f"{prefix}: [{crawler_display_name}] run status={normalized_status} reason={reason} message={message}",
                file=sys.stderr,
                flush=True,
            )

        return report


def run_scheduled_completion_cdc():
    report = {"status": "성공"}
    start_time = time.time()
    conn = None
    cursor = None
    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        result = record_due_scheduled_completions(conn, cursor, now_kst_naive())
        conn.commit()
        report.update(result)
    except Exception as exc:
        print(f"FATAL: [scheduled completion cdc] 실행 실패: {exc}", file=sys.stderr, flush=True)
        report["status"] = "실패"
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
    report_conn = None
    try:
        report_conn = create_standalone_connection()
        report_cursor = get_cursor(report_conn)
        report_cursor.execute(
            """
            INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
            VALUES (%s, %s, %s)
            """,
            ("scheduled completion cdc", report["status"], json.dumps(report)),
        )
        report_conn.commit()
        report_cursor.close()
        print("LOG: [scheduled completion cdc] 실행 결과를 DB에 성공적으로 저장했습니다.", flush=True)
    except Exception as report_exc:
        print(f"FATAL: [scheduled completion cdc] 보고서 저장 실패: {report_exc}", file=sys.stderr, flush=True)
    finally:
        if report_conn:
            report_conn.close()

    return report


def run_scheduled_publication_cdc():
    report = {"status": "성공"}
    start_time = time.time()
    conn = None
    cursor = None
    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        result = record_due_scheduled_publications(conn, cursor, now_kst_naive())
        conn.commit()
        report.update(result)
    except Exception as exc:
        print(f"FATAL: [scheduled publication cdc] 실행 실패: {exc}", file=sys.stderr, flush=True)
        report["status"] = "실패"
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
    report_conn = None
    try:
        report_conn = create_standalone_connection()
        report_cursor = get_cursor(report_conn)
        report_cursor.execute(
            """
            INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
            VALUES (%s, %s, %s)
            """,
            ("scheduled publication cdc", report["status"], json.dumps(report)),
        )
        report_conn.commit()
        report_cursor.close()
        print("LOG: [scheduled publication cdc] 실행 결과를 DB에 성공적으로 저장했습니다.", flush=True)
    except Exception as report_exc:
        print(f"FATAL: [scheduled publication cdc] 보고서 저장 실패: {report_exc}", file=sys.stderr, flush=True)
    finally:
        if report_conn:
            report_conn.close()

    return report


async def main():
    start_time = time.time()
    print("==========================================", flush=True)
    print("   통합 크롤러 실행 스크립트 시작", flush=True)
    print("==========================================", flush=True)

    tasks = [run_one_crawler(crawler_class) for crawler_class in ALL_CRAWLERS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    has_error = False
    has_warn = False

    for result in results:
        if isinstance(result, Exception):
            has_error = True
            print(
                f"ERROR: 크롤러 작업 중 gather 레벨 예외가 발생했습니다: {result}",
                file=sys.stderr,
                flush=True,
            )

    naver_unique = 0
    kakao_unique = 0
    kakao_status = None
    kakao_summary = None
    kakao_fetch_meta = None

    for result in results:
        if not isinstance(result, dict):
            continue

        status_label = normalize_runtime_status(result.get("status"))
        if status_label == "error":
            has_error = True
        elif status_label == "warn":
            has_warn = True

        name_lower = str(result.get("crawler_name", "")).lower()
        fetched = result.get("fetched_count") or 0
        if "naver" in name_lower:
            naver_unique = fetched
        elif "kakao" in name_lower:
            kakao_unique = fetched
            kakao_status = status_label
            kakao_summary = result.get("summary")
            cdc_info = result.get("cdc_info")
            if isinstance(cdc_info, dict):
                kakao_fetch_meta = cdc_info.get("fetch_meta")

    actual_total_unique = naver_unique + kakao_unique

    target_total_unique, target_warning = get_rollup_target_total_unique()
    if target_warning:
        has_warn = True
        print(f"WARNING: {target_warning}", file=sys.stderr, flush=True)

    kakao_fetch_failed = is_kakao_fetch_failed(kakao_status, kakao_unique, kakao_fetch_meta)
    warning_reasons = build_rollup_warning_reasons(
        actual_total_unique=actual_total_unique,
        target_total_unique=target_total_unique,
        kakao_fetch_failed=kakao_fetch_failed,
    )
    if warning_reasons:
        has_warn = True

    warning = ",".join(warning_reasons) if warning_reasons else None

    rollup = {
        "naver_unique": naver_unique,
        "kakao_unique": kakao_unique,
        "actual_total_unique": actual_total_unique,
        "target_total_unique": target_total_unique,
        "warning": warning,
    }

    if kakao_fetch_failed:
        summary_reason = None
        summary_message = None
        if isinstance(kakao_summary, dict):
            summary_reason = kakao_summary.get("reason")
            summary_message = kakao_summary.get("message")

        kakao_rollup_status = normalize_runtime_status(kakao_status)
        kakao_rollup_status = "error" if kakao_rollup_status == "error" else "warn"
        kakao_prefix = severity_prefix(kakao_rollup_status)
        log_file = sys.stderr if kakao_rollup_status == "error" else sys.stdout
        print(
            f"{kakao_prefix}: [Kakaowebtoon] rollup status={kakao_rollup_status} "
            f"reason={summary_reason} message={summary_message}",
            file=log_file,
            flush=True,
        )

    rollup_status = "error" if has_error else "warn" if has_warn else "ok"
    target_total_label = target_total_unique if target_total_unique is not None else "disabled"
    print(
        f"Rollup uniques -> Naver: {naver_unique}, Kakao: {kakao_unique}, "
        f"Total: {actual_total_unique} / {target_total_label}",
        flush=True,
    )
    if warning:
        print(f"{severity_prefix(rollup_status)}: {warning}", flush=True)
    print(f"Final rollup payload: {json.dumps(rollup, ensure_ascii=False)}", flush=True)

    completion_report = run_scheduled_completion_cdc()
    publication_report = run_scheduled_publication_cdc()

    completion_status = normalize_runtime_status((completion_report or {}).get("status"))
    publication_status = normalize_runtime_status((publication_report or {}).get("status"))
    if completion_status == "error" or publication_status == "error":
        has_error = True
    elif completion_status == "warn" or publication_status == "warn":
        has_warn = True

    total_duration = time.time() - start_time
    final_status = "error" if has_error else "warn" if has_warn else "ok"
    print("\n==========================================", flush=True)
    print(f"  통합 크롤러 실행 완료 (총 소요 시간: {total_duration:.2f}초)", flush=True)
    print("==========================================", flush=True)

    return 1 if final_status == "error" else 0


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = asyncio.run(main())
    except Exception:
        print("ERROR: integrated crawler execution crashed.", file=sys.stderr, flush=True)
        traceback.print_exc()
        exit_code = 1
    sys.exit(exit_code)
