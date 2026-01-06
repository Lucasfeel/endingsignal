import asyncio
import json
import os
import random
from itertools import product
from typing import Dict, List, Set, Tuple

import aiohttp

import config
from database import get_cursor
from services.kakaopage_graphql import (
    build_section_id,
    fetch_static_landing_section,
    parse_section_payload,
)
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
    "bmType": "A",
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

    async def _bootstrap_cookies(self, session: aiohttp.ClientSession):
        try:
            async with session.get(config.KAKAOPAGE_GRAPHQL_BOOTSTRAP_URL, headers=HEADERS):
                return
        except Exception:
            return

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

    async def _fetch_section(
        self,
        session: aiohttp.ClientSession,
        section_id: str,
        param: Dict,
        fetch_meta: Dict,
        label: str,
    ) -> Tuple[List[Dict], Dict]:
        try:
            data = await fetch_static_landing_section(session, section_id, param)
            payload = data.get("staticLandingDayOfWeekSection") or {}
            items, meta = parse_section_payload(payload)
            fetch_meta.setdefault("pages", 0)
            fetch_meta["pages"] += 1
            return items, meta
        except Exception as exc:  # noqa: PERF203
            fetch_meta.setdefault("errors", []).append(
                {"where": label, "message": str(exc)}
            )
            return [], {}

    async def _run_verify_mode(self):
        fetch_meta: Dict = {"mode": "verify", "errors": []}
        ongoing_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.concurrency, ttl_dns_cache=120)
        param = {**DEFAULT_PARAM}
        section_id = build_section_id(
            param["categoryUid"],
            param["subcategoryUid"],
            param["bmType"],
            param["dayTabUid"],
            param["screenUid"],
        )

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=HEADERS) as session:
            await self._bootstrap_cookies(session)
            items, meta = await self._fetch_section(session, section_id, param, fetch_meta, "verify:page1")

        seen_ids: Set[str] = set()
        self._ingest_items(items, param.get("dayTabUid", "2"), ongoing_today, seen_ids)

        expected_total = meta.get("totalCount") if isinstance(meta, dict) else None
        fetch_meta.update(
            {
                "expected_count": len(seen_ids),
                "fetched_ids": list(seen_ids)[:20],
                "fetched_count": len(seen_ids),
                "health_db_count": expected_total if isinstance(expected_total, int) and expected_total > 0 else None,
                "force_no_ratio": not isinstance(expected_total, int) or expected_total <= 0,
            }
        )

        all_content_today = {**ongoing_today}
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    async def _collect_sections(self):
        fetch_meta: Dict = {"mode": "collect", "errors": [], "pages": 0, "sections": 0}
        ongoing_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}
        seen_ids: Set[str] = set()

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=HEADERS) as session:
            await self._bootstrap_cookies(session)

            param = {**DEFAULT_PARAM}
            section_id = build_section_id(
                param["categoryUid"],
                param["subcategoryUid"],
                param["bmType"],
                param["dayTabUid"],
                param["screenUid"],
            )
            items, meta = await self._fetch_section(session, section_id, param, fetch_meta, "collect:init")
            self._ingest_items(items, param.get("dayTabUid", "2"), ongoing_today, seen_ids)

            bm_list = meta.get("businessModelList") or [param["bmType"]]
            subcategory_list = meta.get("subcategoryList") or [param["subcategoryUid"]]
            day_tab_list = meta.get("dayTabList") or [param["dayTabUid"]]

            expected_total = 0
            counted_sections: Set[Tuple[str, str, str]] = set()

            for bm_type, subcategory_uid, day_tab_uid in product(bm_list, subcategory_list, day_tab_list):
                page = 1
                while True:
                    current_param = {
                        "categoryUid": param["categoryUid"],
                        "bmType": bm_type,
                        "subcategoryUid": subcategory_uid,
                        "dayTabUid": day_tab_uid,
                        "screenUid": param["screenUid"],
                        "page": page,
                    }
                    section_id = build_section_id(
                        current_param["categoryUid"],
                        current_param["subcategoryUid"],
                        current_param["bmType"],
                        current_param["dayTabUid"],
                        current_param["screenUid"],
                    )
                    label = f"collect:{bm_type}:{subcategory_uid}:{day_tab_uid}:p{page}"
                    fetch_meta["sections"] += 1
                    items, meta = await self._fetch_section(session, section_id, current_param, fetch_meta, label)

                    if meta:
                        section_key = (bm_type, subcategory_uid, day_tab_uid)
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

    async def fetch_all_data(self):
        if self.mode == "collect":
            return await self._collect_sections()
        return await self._run_verify_mode()

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
        return len(inserts)
