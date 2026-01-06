import asyncio
import aiohttp
import json
import os
import random
from typing import Dict, List, Tuple

import config
from .base_crawler import ContentCrawler
from database import create_standalone_connection, get_cursor
from utils.text import normalize_search_text

HEADERS = {
    **config.CRAWLER_HEADERS,
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Referer": "https://page.kakao.com/",
    "Origin": "https://page.kakao.com",
}


class KakaoPageWebtoonCrawler(ContentCrawler):
    """KakaoPage 기반 웹툰 크롤러 (verify/bootstrap 모드).

    기본 실행 예시:
        KAKAOPAGE_MODE=verify python run_all_crawlers.py
        KAKAOPAGE_MODE=bootstrap python run_all_crawlers.py
    """

    DISPLAY_NAME = "KakaoPage Webtoon"
    REQUIRED_ENV_VARS: List[str] = []

    def __init__(self):
        super().__init__("kakaowebtoon")
        self.mode = os.getenv("KAKAOPAGE_MODE", config.KAKAOPAGE_MODE_DEFAULT).lower()
        self.verify_only_subscribed = config.KAKAOPAGE_VERIFY_ONLY_SUBSCRIBED
        self.concurrency = config.KAKAOPAGE_VERIFY_CONCURRENCY
        self.timeout_seconds = config.KAKAOPAGE_VERIFY_TIMEOUT_SECONDS
        self.jitter_range = (
            config.KAKAOPAGE_VERIFY_JITTER_MIN_SECONDS,
            config.KAKAOPAGE_VERIFY_JITTER_MAX_SECONDS,
        )

    def _load_verify_target_ids(self) -> List[str]:
        """DB에서 verify 대상 content_id 목록을 조회합니다."""
        conn = create_standalone_connection()
        cursor = None
        try:
            cursor = get_cursor(conn)
            preferred_sql = """
                SELECT DISTINCT c.content_id
                FROM contents c
                JOIN subscriptions s ON (s.content_id = c.content_id AND s.source = c.source)
                WHERE c.source = %s AND c.status != '완결'
            """
            fallback_sql = """
                SELECT content_id
                FROM contents
                WHERE source = %s AND status != '완결'
            """

            target_ids: List[str] = []
            if self.verify_only_subscribed:
                cursor.execute(preferred_sql, (self.source_name,))
                target_ids = [str(row["content_id"]) for row in cursor.fetchall()]

            if not target_ids:
                cursor.execute(fallback_sql, (self.source_name,))
                target_ids = [str(row["content_id"]) for row in cursor.fetchall()]

            return target_ids
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            conn.close()

    @staticmethod
    def _parse_status_from_text(text: str) -> str:
        if "완결" in text:
            return "완결"
        if "휴재" in text or "시즌완결" in text:
            return "휴재"
        return "연재중"

    async def _fetch_content_status(self, session: aiohttp.ClientSession, content_id: str, fetch_meta: Dict) -> Tuple[str, str]:
        url = f"https://page.kakao.com/content/{content_id}?tab_type=about"
        attempts = 3
        for attempt in range(attempts):
            try:
                await asyncio.sleep(random.uniform(*self.jitter_range))
                async with session.get(url, headers=HEADERS) as resp:
                    text = await resp.text()
                    status = self._parse_status_from_text(text)
                    return content_id, status
            except Exception as exc:  # noqa: PERF203
                if attempt == attempts - 1:
                    fetch_meta.setdefault("errors", []).append(f"fetch:{content_id}:{exc}")
                    return content_id, ""
                await asyncio.sleep(2 ** attempt)
        return content_id, ""

    async def _fetch_verify_mode(self):
        target_ids = self._load_verify_target_ids()
        fetch_meta = {
            "mode": "verify",
            "expected_count": len(target_ids),
            "errors": [],
        }

        ongoing_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}

        if not target_ids:
            return ongoing_today, hiatus_today, finished_today, {}, fetch_meta

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.concurrency, ttl_dns_cache=120)
        semaphore = asyncio.Semaphore(self.concurrency)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async def worker(cid: str):
                async with semaphore:
                    return await self._fetch_content_status(session, cid, fetch_meta)

            results = await asyncio.gather(
                *(worker(cid) for cid in target_ids), return_exceptions=False
            )

        all_content_today: Dict[str, Dict] = {}
        for cid, status in results:
            if not cid:
                continue
            if status == "완결":
                finished_today[cid] = {}
            elif status == "휴재":
                hiatus_today[cid] = {}
            elif status:
                ongoing_today[cid] = {}
            all_content_today[cid] = {}

        fetch_meta["fetched_ids"] = len(all_content_today)
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    @staticmethod
    def _extract_status_from_graphql(item: Dict) -> str:
        status_raw = str(
            item.get("state")
            or item.get("status")
            or item.get("restStatus")
            or item.get("defaultSortStatus")
            or ""
        )
        return KakaoPageWebtoonCrawler._parse_status_from_text(status_raw)

    async def _fetch_bootstrap_mode(self):
        fetch_meta = {"mode": "bootstrap", "errors": [], "pages": 0}
        ongoing_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}

        query = """
            query SearchKeyword($input: SearchKeywordInput!) {
                searchKeyword(input: $input) {
                    list {
                        seriesId
                        title
                        status
                        restStatus
                        authorName
                    }
                    pageInfo {
                        isEnd
                        nextToken
                    }
                }
            }
        """

        variables = {
            "input": {
                "keyword": ".",
                "categoryUid": "10",
                "page": 1,
            }
        }

        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.concurrency, ttl_dns_cache=120)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            while True:
                try:
                    async with session.post(
                        config.KAKAOPAGE_GRAPHQL_URL,
                        json={"operationName": "SearchKeyword", "query": query, "variables": variables},
                        headers={**HEADERS, "Content-Type": "application/json"},
                    ) as resp:
                        text = await resp.text()
                        data = json.loads(text).get("data", {}) if text else {}
                except Exception as exc:  # noqa: PERF203
                    fetch_meta.setdefault("errors", []).append(f"graphql:{exc}")
                    break

                fetch_meta["pages"] += 1
                payload = data.get("searchKeyword") or {}
                items = payload.get("list") or []
                page_info = payload.get("pageInfo") or {}

                for item in items:
                    cid = str(item.get("seriesId") or "").strip()
                    if not cid:
                        continue
                    status = self._extract_status_from_graphql(item)
                    title = (item.get("title") or "").strip() or None
                    author = (item.get("authorName") or "").strip() or None
                    entry = {
                        "title": title,
                        "author": author,
                        "content_url": f"https://page.kakao.com/content/{cid}",
                    }
                    if status == "완결":
                        finished_today[cid] = entry
                    elif status == "휴재":
                        hiatus_today[cid] = entry
                    else:
                        ongoing_today[cid] = entry

                is_end = page_info.get("isEnd", False)
                next_token = page_info.get("nextToken")
                if is_end or not items or not next_token:
                    break

                variables["input"]["page"] = next_token

        all_content_today = {
            **ongoing_today,
            **hiatus_today,
            **finished_today,
        }
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    async def fetch_all_data(self):
        if self.mode == "bootstrap":
            return await self._fetch_bootstrap_mode()
        return await self._fetch_verify_mode()

    def synchronize_database(
        self,
        conn,
        all_content_today: Dict,
        ongoing_today: Dict,
        hiatus_today: Dict,
        finished_today: Dict,
    ):
        cursor = get_cursor(conn)
        mode = "bootstrap" if self.mode == "bootstrap" else "verify"

        if not all_content_today:
            cursor.close()
            return 0

        status_map = {}
        for content_id in all_content_today.keys():
            if content_id in finished_today:
                status_map[content_id] = "완결"
            elif content_id in hiatus_today:
                status_map[content_id] = "휴재"
            elif content_id in ongoing_today:
                status_map[content_id] = "연재중"

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
                meta_data = {
                    "common": {
                        "authors": [author] if author else [],
                        "content_url": data.get("content_url"),
                    }
                }

            if content_id in existing_ids:
                updates.append((status, content_id))
            elif mode == "bootstrap":
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
                "UPDATE contents SET status=%s, updated_at=NOW() WHERE source=%s AND content_id=%s",
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
