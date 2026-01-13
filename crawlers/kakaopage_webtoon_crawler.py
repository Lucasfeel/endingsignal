import asyncio
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from itertools import product
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import aiohttp
from yarl import URL

import config
from database import create_standalone_connection, get_cursor
from services.kakaopage_graphql import (
    build_section_id,
    normalize_kakaopage_param,
    parse_section_payload,
    STATIC_LANDING_QUERY,
)
from utils.reporting import (
    add_request_sample,
    append_error,
    now_iso,
    redact_headers,
)
from utils.record import read_field
from utils.text import normalize_search_text
from .base_crawler import ContentCrawler


HEADERS = {
    **config.CRAWLER_HEADERS,
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
    "Referer": "https://page.kakao.com/",
    "Origin": "https://page.kakao.com",
}

DEFAULT_PARAM = {
    "categoryUid": config.KAKAOPAGE_CATEGORY_UID,
    "bnType": "A",
    "subcategoryUid": "0",
    "dayTabUid": "2",
    "screenUid": config.KAKAOPAGE_DAYOFWEEK_SCREEN_UID,
    "page": 1,
}


class KakaoPageWebtoonCrawler(ContentCrawler):
    """KakaoPage 기반 웹툰 크롤러.

    Kakao 웹소스는 KakaoPage GraphQL(staticLandingDayOfWeekSection)에 기반하여 수집/검증합니다.
    GraphQL endpoint: https://bff-page.kakao.com/graphql (operation: StaticLandingDayOfWeekSection).
    Legacy KakaoWebtoon ID 검증(/content/{legacy_id}) 경로는 404를 유발하므로 제거되었습니다.

    실행 예시:
        # 일회성 초기화 + 부트스트랩(필요시 purge)
        KAKAO_LEGACY_PURGE=YES KAKAOPAGE_MODE=collect python run_all_crawlers.py

        # 파이프라인 헬스 체크(기본 페이지 1개 수집)
        KAKAOPAGE_MODE=verify python run_all_crawlers.py
    """

    DISPLAY_NAME = "KakaoPage Webtoon"
    REQUIRED_ENV_VARS: List[str] = []

    def __init__(self):
        super().__init__("kakaowebtoon")
        self.mode = os.getenv("KAKAOPAGE_MODE", config.KAKAOPAGE_MODE_DEFAULT).lower()
        if self.mode == "bootstrap":
            self.mode = "collect"
        self.concurrency = config.KAKAOPAGE_VERIFY_CONCURRENCY
        self.timeout_seconds = config.KAKAOPAGE_VERIFY_TIMEOUT_SECONDS
        self.jitter_range = (
            config.KAKAOPAGE_VERIFY_JITTER_MIN_SECONDS,
            config.KAKAOPAGE_VERIFY_JITTER_MAX_SECONDS,
        )
        self.purge_requested = os.getenv("KAKAO_LEGACY_PURGE", "").upper() == "YES"
        self._purge_executed = False
        self._db_count_after_sync: Optional[int] = None
        self._last_fetch_meta: Dict = {}

    async def _bootstrap_cookies(self, session: aiohttp.ClientSession):
        try:
            async with session.get(config.KAKAOPAGE_GRAPHQL_BOOTSTRAP_URL, headers=HEADERS):
                return
        except Exception:
            return

    def _seed_cookies(self, session: aiohttp.ClientSession) -> None:
        cookies = {}
        env_map = {
            "webid": os.getenv("KAKAOWEBTOON_WEBID"),
            "_T_ANO": os.getenv("KAKAOWEBTOON_T_ANO"),
        }
        for name, value in env_map.items():
            if value:
                cookies[name] = value

        if cookies:
            session.cookie_jar.update_cookies(cookies, response_url=URL("https://page.kakao.com/"))

    def _cookie_names(self, session: aiohttp.ClientSession) -> List[str]:
        filtered = session.cookie_jar.filter_cookies(URL("https://page.kakao.com/"))
        return list(filtered.keys())

    @staticmethod
    def _normalize_weekdays(day_tab_uid: str) -> List[str]:
        mapping = {
            "1": "mon",
            "2": "tue",
            "3": "wed",
            "4": "thu",
            "5": "fri",
            "6": "sat",
            "7": "sun",
            "11": "daily",
            "12": "daily",
        }
        normalized = mapping.get(str(day_tab_uid), "daily")
        return [normalized]

    def _to_entry(self, series_id: str, title: str, thumbnail: str, day_tab_uid: str) -> Dict:
        return {
            "title": title,
            "content_url": f"https://page.kakao.com/content/{series_id}",
            "thumbnail_url": thumbnail,
            "weekdays": self._normalize_weekdays(day_tab_uid),
            "day_tab_uid": day_tab_uid,
        }

    def _ingest_items(
        self,
        items: List[Dict],
        day_tab_uid: str,
        accumulator: Dict[str, Dict],
        seen_ids: Set[str],
    ) -> None:
        for item in items:
            if item.get("isLegacy"):
                continue
            series_id = str(item.get("series_id") or "").strip()
            if not series_id or series_id in seen_ids:
                continue
            title = (item.get("title") or "").strip()
            if not title:
                continue
            seen_ids.add(series_id)
            accumulator[series_id] = self._to_entry(
                series_id,
                title,
                item.get("thumbnail"),
                day_tab_uid,
            )

    async def _request_section_payload(
        self,
        session: aiohttp.ClientSession,
        section_id: str,
        param: Dict[str, Any],
        fetch_meta: Dict[str, Any],
        label: str,
        attempt: int,
    ) -> Optional[Dict[str, Any]]:
        normalized_param = normalize_kakaopage_param(param)
        payload = {
            "operationName": "staticLandingDayOfWeekSection",
            "query": STATIC_LANDING_QUERY,
            "variables": {"sectionId": section_id, "param": normalized_param},
        }
        headers = {
            **HEADERS,
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Origin": "https://page.kakao.com",
            "Referer": "https://page.kakao.com/",
        }
        request_sample: Dict[str, Any] = {
            "endpoint": config.KAKAOPAGE_GRAPHQL_URL,
            "operation": "staticLandingDayOfWeekSection",
            "variables_sample": payload["variables"],
            "http_status": None,
            "error_type": None,
            "error_message": None,
            "response_snippet": None,
            "attempt": attempt,
            "label": label,
            "headers": redact_headers(headers),
            "cookies": {"cookie_names": self._cookie_names(session)},
            "started_at": now_iso(),
        }

        started_monotonic = time.monotonic()

        try:
            async with session.post(
                config.KAKAOPAGE_GRAPHQL_URL, json=payload, headers=headers
            ) as resp:
                request_sample["http_status"] = resp.status
                text = await resp.text()
                request_sample["response_snippet"] = text[:300]
                response_errors = None
                try:
                    data = json.loads(text)
                    response_errors = data.get("errors") if isinstance(data, dict) else None
                except Exception:
                    data = None

                if resp.status >= 400 or (response_errors):
                    message = str(response_errors or text)[:300]
                    request_sample["error_type"] = "GraphQLError" if response_errors else "HttpError"
                    request_sample["error_message"] = message
                    add_request_sample(fetch_meta, request_sample)
                    self._record_graphql_failure(
                        fetch_meta=fetch_meta,
                        section_id=section_id,
                        operation_name="staticLandingDayOfWeekSection",
                        normalized_param=normalized_param,
                        response_errors=response_errors,
                        response_text=text,
                        status=resp.status,
                        headers=headers,
                    )
                    error_code = "HTTP_ERROR"
                    if response_errors:
                        error_code = "GRAPHQL_ERROR"
                        if any(
                            "GRAPHQL_VALIDATION_FAILED" in str(err)
                            for err in response_errors
                        ):
                            error_code = "GRAPHQL_VALIDATION_FAILED"
                    append_error(
                        fetch_meta,
                        error_code,
                        message,
                        {
                            "label": label,
                            "http_status": resp.status,
                            "operation": "staticLandingDayOfWeekSection",
                        },
                    )
                    print(
                        (
                            "ERROR: [Kakaowebtoon] fetch failed: "
                            f"{error_code} http={resp.status} op=staticLandingDayOfWeekSection "
                            f"dayTabUid={normalized_param.get('dayTabUid')} page={normalized_param.get('page')} "
                            f"msg={message}"
                        ),
                        file=sys.stderr,
                    )
                    return None

                add_request_sample(fetch_meta, request_sample)
                return data.get("data", {}) if isinstance(data, dict) else {}

        except Exception as exc:  # noqa: PERF203
            message = str(exc)
            request_sample["error_type"] = exc.__class__.__name__
            request_sample["error_message"] = message
            add_request_sample(fetch_meta, request_sample)
            self._record_graphql_failure(
                fetch_meta=fetch_meta,
                section_id=section_id,
                operation_name="staticLandingDayOfWeekSection",
                normalized_param=normalized_param,
                response_errors=None,
                response_text=message,
                status=request_sample.get("http_status"),
                headers=headers,
            )
            append_error(
                fetch_meta,
                "HTTP_ERROR",
                message,
                {"label": label, "operation": "staticLandingDayOfWeekSection"},
            )
            print(
                (
                    "ERROR: [Kakaowebtoon] fetch failed: "
                    f"HTTP_ERROR http={request_sample.get('http_status')} op=staticLandingDayOfWeekSection "
                    f"dayTabUid={normalized_param.get('dayTabUid')} page={normalized_param.get('page')} msg={message}"
                ),
                file=sys.stderr,
            )
            return None
        finally:
            request_sample["duration_ms"] = int((time.monotonic() - started_monotonic) * 1000)

    def _record_graphql_failure(
        self,
        *,
        fetch_meta: Dict[str, Any],
        section_id: str,
        operation_name: str,
        normalized_param: Dict[str, Any],
        response_errors: Optional[Any],
        response_text: str,
        status: Optional[int],
        headers: Dict[str, Any],
    ) -> None:
        preview = (response_text or "")[:2000]
        header_subset = {}
        for key in ("user-agent", "origin", "referer"):
            for candidate in headers.keys():
                if candidate.lower() == key:
                    header_subset[key] = headers.get(candidate)
                    break

        failure_payload = {
            "timestamp": now_iso(),
            "http_status": status,
            "operationName": operation_name,
            "sectionId": section_id,
            "variables_param": normalized_param,
            "response_errors": response_errors,
            "response_text_preview": preview,
            "request_headers_used": header_subset,
        }

        failures = fetch_meta.setdefault("graphql_failures", [])
        failures.append(failure_payload)
        fetch_meta["last_graphql_failure"] = failure_payload

        if config.DEBUG_KAKAOPAGE_GRAPHQL:
            print(json.dumps({"kakaopage_graphql_failure": failure_payload}, ensure_ascii=False))


    async def _fetch_section(
        self,
        session: aiohttp.ClientSession,
        section_id: str,
        param: Dict,
        fetch_meta: Dict,
        label: str,
    ) -> Tuple[List[Dict], Dict]:
        attempt = fetch_meta.get("pages", 0) + 1
        try:
            data = await self._request_section_payload(
                session, section_id, param, fetch_meta, label, attempt
            )
            if not data:
                return [], {}

            payload = data.get("staticLandingDayOfWeekSection") or {}
            items, meta = parse_section_payload(payload)
            fetch_meta.setdefault("pages", 0)
            fetch_meta["pages"] += 1
            return items, meta
        except Exception as exc:  # noqa: PERF203
            append_error(
                fetch_meta,
                "SECTION_PARSE_ERROR",
                str(exc),
                {"label": label, "section_id": section_id},
            )
            print(
                f"ERROR: [Kakaowebtoon] fetch failed: SECTION_PARSE_ERROR label={label} msg={exc}",
                file=sys.stderr,
            )
            return [], {}

    async def _run_verify_mode(self):
        fetch_meta: Dict = {
            "mode": "verify",
            "errors": [],
            "request_samples": [],
            "health_notes": [],
            "started_at": now_iso(),
        }
        ongoing_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.concurrency, ttl_dns_cache=120)
        param = normalize_kakaopage_param({**DEFAULT_PARAM})
        section_id = build_section_id(
            param["categoryUid"],
            param["subcategoryUid"],
            param["bnType"],
            param["dayTabUid"],
            param["screenUid"],
        )

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=HEADERS) as session:
            self._seed_cookies(session)
            await self._bootstrap_cookies(session)
            cookie_names = self._cookie_names(session)
            items, meta = await self._fetch_section(session, section_id, param, fetch_meta, "verify:page1")

        seen_ids: Set[str] = set()
        day_tab_uid = str(param.get("dayTabUid", "2"))

        filtered_items = [item for item in items if not item.get("isLegacy")]
        filtered_ids = [item.get("series_id") for item in filtered_items if item.get("series_id")]
        expected_raw = len(items)
        expected_filtered = len(filtered_ids)

        self._ingest_items(filtered_items, day_tab_uid, ongoing_today, seen_ids)

        expected_total = meta.get("totalCount") if isinstance(meta, dict) else None
        fetch_meta.update(
            {
                "section_id_used": section_id,
                "expected_count": expected_filtered,
                "expected_count_raw": expected_raw,
                "expected_count_filtered": expected_filtered,
                "fetched_ids": filtered_ids[:20],
                "fetched_count": expected_filtered,
                "health_db_count": expected_total if isinstance(expected_total, int) and expected_total > 0 else None,
                "force_no_ratio": not isinstance(expected_total, int) or expected_total <= 0,
                "cookie_names": cookie_names,
            }
        )

        all_content_today = {**ongoing_today}
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    async def _collect_sections(self):
        fetch_meta: Dict = {
            "mode": "collect",
            "errors": [],
            "pages": 0,
            "sections": 0,
            "request_samples": [],
            "health_notes": [],
            "started_at": now_iso(),
        }
        ongoing_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}
        seen_ids: Set[str] = set()

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=HEADERS) as session:
            self._seed_cookies(session)
            await self._bootstrap_cookies(session)

            param = normalize_kakaopage_param({**DEFAULT_PARAM})
            section_id = build_section_id(
                param["categoryUid"],
                param["subcategoryUid"],
                param["bnType"],
                param["dayTabUid"],
                param["screenUid"],
            )
            items, meta = await self._fetch_section(session, section_id, param, fetch_meta, "collect:init")
            self._ingest_items(items, param.get("dayTabUid", "2"), ongoing_today, seen_ids)

            bn_list = meta.get("businessModelList") or [param["bnType"]]
            subcategory_list = meta.get("subcategoryList") or [param["subcategoryUid"]]
            day_tab_list = meta.get("dayTabList") or [param["dayTabUid"]]

            expected_total = 0
            counted_sections: Set[Tuple[str, str, str]] = set()

            for bn_type, subcategory_uid, day_tab_uid in product(bn_list, subcategory_list, day_tab_list):
                page = 1
                while True:
                    current_param = normalize_kakaopage_param(
                        {
                            "categoryUid": param["categoryUid"],
                            "bnType": bn_type,
                            "subcategoryUid": subcategory_uid,
                            "dayTabUid": day_tab_uid,
                            "screenUid": param["screenUid"],
                            "page": page,
                        }
                    )
                    section_id = build_section_id(
                        current_param["categoryUid"],
                        current_param["subcategoryUid"],
                        current_param["bnType"],
                        current_param["dayTabUid"],
                        current_param["screenUid"],
                    )
                    label = f"collect:{bn_type}:{subcategory_uid}:{day_tab_uid}:p{page}"
                    fetch_meta["sections"] += 1
                    items, meta = await self._fetch_section(session, section_id, current_param, fetch_meta, label)

                    if meta:
                        section_key = (bn_type, subcategory_uid, day_tab_uid)
                        if section_key not in counted_sections:
                            total_count = meta.get("totalCount")
                            if isinstance(total_count, int) and total_count > 0:
                                expected_total += total_count
                            counted_sections.add(section_key)

                    self._ingest_items(items, day_tab_uid, ongoing_today, seen_ids)

                    is_end = meta.get("isEnd") if isinstance(meta, dict) else True
                    if is_end or not items:
                        break
                    page += 1
                    await asyncio.sleep(random.uniform(*self.jitter_range))

        fetch_meta.update(
            {
                "expected_count": len(seen_ids),
                "fetched_count": len(seen_ids),
                "fetched_ids": list(seen_ids)[:20],
                "health_db_count": expected_total if expected_total > 0 else None,
                "force_no_ratio": expected_total <= 0,
            }
        )

        all_content_today = {**ongoing_today}
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    def _get_db_count(self, conn) -> Optional[int]:
        try:
            cursor = get_cursor(conn)
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM contents WHERE source=%s",
                (self.source_name,),
            )
            row = cursor.fetchone()
            cursor.close()
            return int(row["cnt"]) if row else 0
        except Exception:
            return None

    def _load_recent_bootstrap_reports(
        self, conn, limit: int = 10
    ) -> Sequence[Tuple[datetime, bool, Optional[bool]]]:
        cursor = get_cursor(conn)
        cursor.execute(
            """
            SELECT created_at, report_data
            FROM daily_crawler_reports
            WHERE lower(crawler_name) = lower(%s)
              AND (report_data->'cdc_info'->'fetch_meta'->>'bootstrap_attempted') IS NOT NULL
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (self.source_name.replace("_", " ").title(), limit),
        )

        attempts = []
        for row in cursor.fetchall():
            meta = (
                (read_field(row, "report_data") or {})
                .get("cdc_info", {})
                .get("fetch_meta", {})
            )
            attempted = bool(meta.get("bootstrap_attempted"))
            success_value = meta.get("bootstrap_success")
            if success_value is None:
                bootstrap_success = None
            else:
                bootstrap_success = bool(success_value)
            attempts.append(
                (read_field(row, "created_at"), attempted, bootstrap_success)
            )

        cursor.close()
        return attempts

    def _evaluate_circuit_breaker(
        self, db_count_before: Optional[int]
    ) -> Tuple[bool, Optional[str]]:
        if db_count_before and db_count_before > 0:
            return False, None

        if config.KAKAOPAGE_FORCE_BOOTSTRAP:
            return False, None

        if not config.KAKAOPAGE_AUTO_BOOTSTRAP:
            return True, "auto_bootstrap_disabled"

        conn = None
        try:
            conn = create_standalone_connection()
            attempts = self._load_recent_bootstrap_reports(conn, limit=10)
        except Exception:
            attempts = []
        finally:
            if conn:
                conn.close()

        consecutive_failures = 0
        last_attempt_time: Optional[datetime] = None

        for created_at, attempted, success in attempts:
            if attempted:
                if last_attempt_time is None:
                    last_attempt_time = created_at
                if success:
                    break
                consecutive_failures += 1
            else:
                continue

        if (
            config.KAKAOPAGE_BOOTSTRAP_MAX_CONSECUTIVE_FAILURES >= 0
            and consecutive_failures
            >= config.KAKAOPAGE_BOOTSTRAP_MAX_CONSECUTIVE_FAILURES
        ):
            return True, "bootstrap_circuit_breaker_tripped"

        if last_attempt_time:
            cooldown_until = last_attempt_time + timedelta(
                hours=config.KAKAOPAGE_BOOTSTRAP_COOLDOWN_HOURS
            )
            if datetime.utcnow() < cooldown_until:
                return True, "bootstrap_cooldown_active"

        return False, None

    async def fetch_all_data(self):
        requested_mode = self.mode
        effective_mode = requested_mode
        bootstrap_attempted = False
        bootstrap_success = False
        bootstrap_skipped_reason: Optional[str] = None
        db_count_before: Optional[int] = None

        db_conn = None
        try:
            db_conn = create_standalone_connection()
            db_count_before = self._get_db_count(db_conn)
        except Exception:
            db_count_before = None
        finally:
            if db_conn:
                db_conn.close()

        bootstrap_needed = db_count_before == 0 if db_count_before is not None else False

        if requested_mode != "collect" and bootstrap_needed:
            skip_bootstrap, reason = self._evaluate_circuit_breaker(db_count_before)
            if skip_bootstrap:
                bootstrap_skipped_reason = reason
            else:
                effective_mode = "collect"
                bootstrap_attempted = True

        if requested_mode == "collect" and bootstrap_needed:
            bootstrap_attempted = True

        self.mode = effective_mode

        if bootstrap_attempted:
            print(
                "INFO: [KakaoPage] Auto bootstrap triggered (empty DB detected, running collect)."
            )
        elif bootstrap_skipped_reason:
            print(
                f"INFO: [KakaoPage] Bootstrap skipped ({bootstrap_skipped_reason}); running {effective_mode}."
            )

        fetch_run_started = time.monotonic()
        if effective_mode == "collect":
            (
                ongoing_today,
                hiatus_today,
                finished_today,
                all_content_today,
                fetch_meta,
            ) = await self._collect_sections()
        else:
            (
                ongoing_today,
                hiatus_today,
                finished_today,
                all_content_today,
                fetch_meta,
            ) = await self._run_verify_mode()

        fetch_meta["finished_at"] = now_iso()
        fetch_meta["duration_ms"] = int((time.monotonic() - fetch_run_started) * 1000)

        fetched_count = len(all_content_today)
        if bootstrap_attempted and fetched_count <= 0:
            append_error(fetch_meta, "BOOTSTRAP_FETCH_EMPTY", "Bootstrap fetch returned zero items")
        if bootstrap_attempted:
            bootstrap_success = fetched_count > 0 and not fetch_meta.get("errors")

        expected_count = fetch_meta.get("expected_count")
        if not isinstance(expected_count, int):
            expected_count = None

        errors = fetch_meta.get("errors") or []
        is_suspicious_empty = (
            fetched_count == 0 and (expected_count is None or expected_count == 0) and not errors
        )
        health_notes = fetch_meta.get("health_notes") or []
        if is_suspicious_empty:
            fetch_meta["is_suspicious_empty"] = True
            health_notes.append("suspicious_empty_result")
            append_error(
                fetch_meta,
                "EMPTY_RESULT",
                "No items fetched from KakaoPage",
                {"mode": effective_mode},
            )
            print(
                f"ERROR: [Kakaowebtoon] suspicious empty result (mode={effective_mode}) fetched=0 expected={expected_count}",
                file=sys.stderr,
            )

        fetch_meta["health_notes"] = health_notes

        if not fetch_meta.get("health_db_count"):
            if isinstance(expected_count, int) and expected_count > 0:
                fetch_meta["health_db_count"] = expected_count
            elif isinstance(db_count_before, int) and db_count_before > 0:
                fetch_meta["health_db_count"] = db_count_before
            else:
                fetch_meta["health_db_count"] = 1

        status = "ok"
        reason = "ok"
        message = "Fetch completed successfully"
        if errors:
            last_error = errors[-1]
            status = "fail"
            if isinstance(last_error, dict):
                reason = last_error.get("code") or "fetch_error"
                message = last_error.get("message") or message
            else:
                reason = "fetch_error"
                message = str(last_error)
        if is_suspicious_empty:
            status = "fail"
            reason = "empty_result"
            message = f"suspicious empty result (mode={effective_mode})"

        fetch_meta.update(
            {
                "status": status,
                "summary": {
                    "crawler": self.DISPLAY_NAME,
                    "reason": reason,
                    "message": message,
                },
                "mode_requested": requested_mode,
                "mode_effective": effective_mode,
                "bootstrap_needed": bootstrap_needed,
                "bootstrap_attempted": bootstrap_attempted,
                "bootstrap_success": bootstrap_success,
                "bootstrap_skipped_reason": bootstrap_skipped_reason,
                "db_count_before": db_count_before,
                "fetched_count": fetched_count,
                "is_success": status == "ok",
            }
        )

        self._last_fetch_meta = fetch_meta
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    def synchronize_database(
        self,
        conn,
        all_content_today: Dict,
        ongoing_today: Dict,
        hiatus_today: Dict,
        finished_today: Dict,
    ):
        cursor = get_cursor(conn)
        mode = "collect" if self.mode == "collect" else "verify"

        if mode == "collect" and self.purge_requested and not self._purge_executed:
            cursor.execute("DELETE FROM subscriptions WHERE source=%s", (self.source_name,))
            cursor.execute("DELETE FROM contents WHERE source=%s", (self.source_name,))
            conn.commit()
            self._purge_executed = True

        if not all_content_today:
            cursor.close()
            if self._last_fetch_meta is not None:
                try:
                    db_count_before = self._last_fetch_meta.get("db_count_before")
                    if isinstance(db_count_before, int):
                        self._db_count_after_sync = db_count_before
                        self._last_fetch_meta["db_count_after"] = self._db_count_after_sync
                except Exception:
                    pass
            return 0

        status_map = {cid: "연재중" for cid in all_content_today.keys()}

        cursor.execute(
            "SELECT content_id FROM contents WHERE source = %s AND content_id = ANY(%s)",
            (self.source_name, list(status_map.keys())),
        )
        existing_ids = {str(row["content_id"]) for row in cursor.fetchall()}

        updates = []
        inserts = []

        for content_id, status in status_map.items():
            data = all_content_today.get(content_id) or {}
            title = data.get("title") if isinstance(data, dict) else None
            author = data.get("author") if isinstance(data, dict) else None
            normalized_title = normalize_search_text(title) if title else None
            normalized_authors = normalize_search_text(author) if author else None
            meta_data = None
            if isinstance(data, dict) and data:
                weekdays = data.get("weekdays") or ["daily"]
                meta_data = {
                    "common": {
                        "authors": [author] if author else [],
                        "content_url": data.get("content_url"),
                        "thumbnail_url": data.get("thumbnail_url"),
                    },
                    "attributes": {"weekdays": weekdays, "day_tab_uid": data.get("day_tab_uid")},
                }

            if content_id in existing_ids:
                updates.append((status, content_id))
            elif mode == "collect":
                inserts.append(
                    (
                        content_id,
                        self.source_name,
                        "webtoon",
                        title,
                        normalized_title,
                        normalized_authors,
                        status,
                        json.dumps(meta_data) if meta_data else None,
                    )
                )

        if updates:
            cursor.executemany(
                "UPDATE contents SET status=%s WHERE source=%s AND content_id=%s",
                [(status, self.source_name, cid) for status, cid in updates],
            )

        if inserts:
            cursor.executemany(
                """
                INSERT INTO contents (content_id, source, content_type, title, normalized_title, normalized_authors, status, meta)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_id, source) DO NOTHING
                """,
                inserts,
            )

        cursor.close()
        inserted_count = len(inserts)
        if self._last_fetch_meta is not None:
            try:
                db_count_before = self._last_fetch_meta.get("db_count_before")
                if isinstance(db_count_before, int):
                    self._db_count_after_sync = db_count_before + inserted_count
                    self._last_fetch_meta["db_count_after"] = self._db_count_after_sync
            except Exception:
                pass

        return inserted_count
