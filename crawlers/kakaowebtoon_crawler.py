import asyncio
import aiohttp
import json
import os
import re
import time
import urllib.parse

from tenacity import retry, stop_after_attempt, wait_exponential

import config
from .base_crawler import ContentCrawler
from database import get_cursor

# --- KakaoWebtoon API Configuration ---
API_BASE_URL = "https://gateway-kw.kakao.com/section/v1/pages"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "Referer": "https://webtoon.kakao.com/",
    "Accept-Language": "ko",
}


class KakaowebtoonCrawler(ContentCrawler):
    """webtoon.kakao.com에서 웹툰 정보를 수집하는 크롤러입니다."""

    # (run_all_crawlers.py에서 인스턴스 생성 전에 스킵 판단용)
    DISPLAY_NAME = "Kakao Webtoon"
    REQUIRED_ENV_VARS = []

    @classmethod
    def get_missing_env_vars(cls):
        return []

    def __init__(self):
        super().__init__("kakaowebtoon")
        self.cookies = self._get_cookies_from_env()

    def _get_cookies_from_env(self):
        """환경 변수에서 쿠키 값을 로드합니다. 없으면 None 반환(익명 부트스트랩 시도)."""
        webid = os.getenv("KAKAOWEBTOON_WEBID")
        t_ano = os.getenv("KAKAOWEBTOON_T_ANO")

        if webid and t_ano:
            return {"webid": webid, "_T_ANO": t_ano}
        return None

    async def _bootstrap_anonymous_cookies(self, session, fetch_meta=None):
        """로그인 없이 발급되는 쿠키를 한 번의 요청으로 받아옵니다(베스트 에포트)."""
        try:
            async with session.get(
                "https://webtoon.kakao.com/",
                headers=HEADERS,
                allow_redirects=True,
            ) as resp:
                # consume body to complete request and allow cookie jar update
                await resp.text()

            cookies = session.cookie_jar.filter_cookies("https://webtoon.kakao.com/")
            webid = cookies.get("webid")
            t_ano = cookies.get("_T_ANO")

            if webid and t_ano:
                # Never print cookie values
                self.cookies = {"webid": webid.value, "_T_ANO": t_ano.value}
                print("부트스트랩된 쿠키: ['webid', '_T_ANO']")
            else:
                if fetch_meta is not None:
                    fetch_meta.setdefault("errors", []).append("cookies:anonymous_bootstrap_missing")

        except Exception as e:
            print(f"익명 쿠키 부트스트랩 실패: {e}")
            if fetch_meta is not None:
                fetch_meta.setdefault("errors", []).append(f"cookies:anonymous_bootstrap_failed:{e}")

    def _iter_cards_from_sections(self, sections):
        """gateway-kw 응답의 sections/cardGroups/cards 전체를 안전하게 순회."""
        for section in sections or []:
            for card_group in (section.get("cardGroups", []) or []):
                for card in (card_group.get("cards", []) or []):
                    yield card

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(self, session, url, params=None):
        """주어진 URL과 파라미터로 API에 GET 요청을 보내고 JSON 응답을 반환합니다."""
        async with session.get(
            url,
            headers=HEADERS,
            cookies=self.cookies if self.cookies else None,
            params=params,
        ) as response:
            response.raise_for_status()
            return await response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_paginated_completed(self, session, *, start_time=None, fetch_meta=None):
        """'completed' 엔드포인트의 모든 페이지를 순회하며 데이터를 수집합니다."""
        all_completed_content = []
        offset = 0
        limit = 100
        seen_ids = set()

        while True:
            try:
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    if fetch_meta is not None:
                        fetch_meta.setdefault("errors", []).append("completed:WALL_TIMEOUT_EXCEEDED")
                    break

                url = f"{API_BASE_URL}/completed"
                data = await self._fetch_from_api(session, url, params={"offset": offset, "limit": limit})

                cards = list(self._iter_cards_from_sections(data.get("data", {}).get("sections", [])))
                if not cards:
                    break

                new_cards = 0
                for card in cards:
                    cid = str(card.get("id") or "").strip()
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        all_completed_content.append(card)
                        new_cards += 1

                offset += len(cards)

                # Stop if last page or no progress
                if len(cards) < limit or new_cards == 0:
                    break

                await asyncio.sleep(0.1)

            except Exception as e:
                print(f"Error fetching completed page at offset {offset}: {e}")
                if fetch_meta is not None:
                    fetch_meta.setdefault("errors", []).append(f"completed:{e}")
                break

        return all_completed_content

    async def _discover_official_slugs(self, session, *, start_time=None, fetch_meta=None):
        """
        webtoon.kakao.com 메인 HTML + JS 번들에서 section/v1/pages/<slug> 패턴을 찾아 slug 목록을 수집합니다.
        (베스트 에포트; 실패해도 크롤러 전체 실패로 이어지지 않도록 errors에만 기록)
        """
        slugs = set()
        meta = fetch_meta if fetch_meta is not None else {}

        # Safe config defaults
        max_bundles = int(
            getattr(
                config,
                "KAKAO_DISCOVERY_MAX_BUNDLES",
                getattr(config, "KAKAOWEBTOON_MAX_JS_BUNDLES", 10),
            )
        )

        try:
            if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
                return slugs

            async with session.get("https://webtoon.kakao.com/", headers=HEADERS, allow_redirects=True) as resp:
                html = await resp.text()

            # script src 추출 (상대/절대 모두)
            script_srcs = re.findall(r'<script[^>]+src="([^"]+)"', html)
            bundle_urls = []
            for src in script_srcs:
                if not src:
                    continue
                # normalize to absolute
                abs_url = urllib.parse.urljoin("https://webtoon.kakao.com/", src)
                if abs_url.endswith(".js"):
                    bundle_urls.append(abs_url)

            # fallback: 기존 방식(절대 js 링크)
            if not bundle_urls:
                bundle_urls = list({url for url in re.findall(r"https://[^'\"]+\.js", html)})

            bundle_urls = list(dict.fromkeys(bundle_urls))  # preserve order, dedupe

            for bundle_url in bundle_urls[:max_bundles]:
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
                    break

                try:
                    async with session.get(bundle_url, headers=HEADERS) as resp:
                        bundle_text = await resp.text()

                    for slug in re.findall(r"/section/v1/pages/([A-Za-z0-9_-]+)", bundle_text):
                        if slug:
                            slugs.add(slug)

                except Exception as e:
                    meta.setdefault("errors", []).append(f"discover:bundle_fetch_failed:{e}")

        except Exception as e:
            meta.setdefault("errors", []).append(f"discover:bootstrap_failed:{e}")

        return slugs

    async def _fetch_official_section_cards(
        self, session, slug, *, start_time=None, fetch_meta=None, seen_ids=None
    ):
        """
        발견된 slug 페이지를 offset/limit 기반으로 최대한 순회하며 cards 수집.
        (베스트 에포트: slug별 오류는 errors에 기록 후 해당 slug만 중단)
        """
        collected = []
        offset = 0
        limit = 100
        page = 0

        if seen_ids is None:
            seen_ids = set()

        meta = fetch_meta if fetch_meta is not None else {}

        max_pages_per_slug = int(
            getattr(
                config,
                "KAKAO_DISCOVERY_MAX_PAGES_PER_SLUG",
                getattr(config, "KAKAOWEBTOON_MAX_PAGES_PER_SLUG", 400),
            )
        )
        soft_cap = int(
            getattr(
                config,
                "KAKAO_DISCOVERY_SOFT_CAP",
                getattr(config, "KAKAOWEBTOON_TARGET_UNIQUE_TITLES", 20000),
            )
        )

        while page < max_pages_per_slug:
            try:
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    meta.setdefault("errors", []).append(f"discover:{slug}:WALL_TIMEOUT_EXCEEDED")
                    break

                url = f"{API_BASE_URL}/{slug}"
                data = await self._fetch_from_api(session, url, params={"offset": offset, "limit": limit})
                cards = list(self._iter_cards_from_sections(data.get("data", {}).get("sections", [])))

                if not cards:
                    break

                new_cards = 0
                for card in cards:
                    cid = str(card.get("id") or "").strip()
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        collected.append(card)
                        new_cards += 1

                offset += len(cards)
                page += 1

                if len(cards) < limit or new_cards == 0:
                    break

                await asyncio.sleep(0.1)

                if len(seen_ids) >= soft_cap:
                    meta.setdefault("errors", []).append("discover:soft_cap_reached")
                    break

            except Exception as e:
                meta.setdefault("errors", []).append(f"discover:{slug}:{e}")
                break

        return collected

    async def fetch_all_data(self):
        """카카오웹툰의 '요일별'과 '완결' API에서 모든 웹툰 데이터를 비동기적으로 가져옵니다."""
        print("카카오웹툰 서버에서 최신 데이터를 가져옵니다...")

        start_time = time.monotonic()

        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        fetch_meta = {"errors": []}

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            if not self.cookies:
                await self._bootstrap_anonymous_cookies(session, fetch_meta=fetch_meta)

            weekday_url = f"{API_BASE_URL}/general-weekdays"

            if (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                fetch_meta["errors"].append("weekday:WALL_TIMEOUT_EXCEEDED")
                weekday_data, completed_data = {}, []
            else:
                tasks = [
                    self._fetch_from_api(session, weekday_url),
                    self._fetch_paginated_completed(session, start_time=start_time, fetch_meta=fetch_meta),
                ]
                weekday_data, completed_data = await asyncio.gather(*tasks, return_exceptions=True)

            discovered_cards = []
            try:
                discovered_slugs = await self._discover_official_slugs(
                    session, start_time=start_time, fetch_meta=fetch_meta
                )

                # Build initial seen_ids from baseline surfaces
                seen_ids = set()
                if isinstance(completed_data, list):
                    for card in completed_data:
                        cid = str(card.get("id") or "").strip()
                        if cid:
                            seen_ids.add(cid)

                if isinstance(weekday_data, dict):
                    for card in self._iter_cards_from_sections(weekday_data.get("data", {}).get("sections", [])):
                        cid = str(card.get("id") or "").strip()
                        if cid:
                            seen_ids.add(cid)

                soft_cap = int(
                    getattr(
                        config,
                        "KAKAO_DISCOVERY_SOFT_CAP",
                        getattr(config, "KAKAOWEBTOON_TARGET_UNIQUE_TITLES", 20000),
                    )
                )

                for slug in discovered_slugs:
                    if len(seen_ids) >= soft_cap:
                        fetch_meta.setdefault("errors", []).append("discover:soft_cap_reached")
                        break

                    cards = await self._fetch_official_section_cards(
                        session, slug, start_time=start_time, fetch_meta=fetch_meta, seen_ids=seen_ids
                    )
                    discovered_cards.extend(cards)

            except Exception as e:
                fetch_meta.setdefault("errors", []).append(f"discover:failed:{e}")

        # Normalize exceptions
        if isinstance(weekday_data, Exception):
            print(f"❌ 요일별 데이터 수집 실패: {weekday_data}")
            fetch_meta["errors"].append(f"weekday:{weekday_data}")
            weekday_data = {}

        if isinstance(completed_data, Exception):
            print(f"❌ 완결 데이터 수집 실패: {completed_data}")
            fetch_meta["errors"].append(f"completed:{completed_data}")
            completed_data = []

        print("\n--- 데이터 정규화 시작 ---")
        ongoing_today, hiatus_today, finished_today = {}, {}, {}
        status_counts = {}

        day_map = {"월": "mon", "화": "tue", "수": "wed", "목": "thu", "금": "fri", "토": "sat", "일": "sun"}

        # Weekday sections
        if weekday_data.get("data", {}).get("sections"):
            for section in weekday_data["data"]["sections"]:
                weekday_kor = section.get("title", "").replace("요일", "")
                weekday_eng = day_map.get(weekday_kor)
                if not weekday_eng:
                    continue

                for webtoon in self._iter_cards_from_sections([section]):
                    content_id = str(webtoon.get("id") or "").strip()
                    if not content_id:
                        continue

                    # ensure weekdays are accumulated
                    groups = webtoon.get("weekdayDisplayGroups")
                    if not isinstance(groups, list):
                        webtoon["weekdayDisplayGroups"] = []
                    if weekday_eng not in webtoon["weekdayDisplayGroups"]:
                        webtoon["weekdayDisplayGroups"].append(weekday_eng)

                    content_payload = webtoon.get("content", {}) or {}
                    if "title" not in webtoon:
                        webtoon["title"] = content_payload.get("title")

                    status_text = content_payload.get("onGoingStatus")
                    status_counts[status_text] = status_counts.get(status_text, 0) + 1

                    if status_text == "PAUSE":
                        hiatus_today.setdefault(content_id, webtoon)
                    else:
                        ongoing_today.setdefau_
