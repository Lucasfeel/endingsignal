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
        """환경 변수에서 쿠키 값을 로드하고 유효성을 검사합니다."""
        webid = os.getenv("KAKAOWEBTOON_WEBID")
        t_ano = os.getenv("KAKAOWEBTOON_T_ANO")

        if webid and t_ano:
            return {"webid": webid, "_T_ANO": t_ano}

        return None

    async def _bootstrap_anonymous_cookies(self, session, fetch_meta=None):
        """로그인 없이 발급되는 쿠키를 한 번의 요청으로 받아옵니다."""

        try:
            async with session.get("https://webtoon.kakao.com/", headers=HEADERS) as resp:
                await resp.text()

            cookies = session.cookie_jar.filter_cookies("https://webtoon.kakao.com/")
            webid = cookies.get("webid")
            t_ano = cookies.get("_T_ANO")

            if webid and t_ano:
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
        for section in sections or []:
            for card_group in section.get("cardGroups", []) or []:
                for card in card_group.get("cards", []) or []:
                    yield card

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(self, session, url, params=None):
        """주어진 URL과 파라미터로 API에 GET 요청을 보내고 JSON 응답을 반환합니다."""
        async with session.get(
            url, headers=HEADERS, cookies=self.cookies if self.cookies else None, params=params
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
                # Run-level watchdog
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
        slugs = set()
        meta = fetch_meta if fetch_meta is not None else {}
        try:
            if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
                return slugs

            async with session.get("https://webtoon.kakao.com/", headers=HEADERS) as resp:
                html = await resp.text()

            bundle_urls = list({url for url in re.findall(r"https://[^'\"]+\\.js", html)})
            if not bundle_urls:
                return slugs

            for bundle_url in bundle_urls[: config.KAKAO_DISCOVERY_MAX_BUNDLES]:
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
                    break

                try:
                    async with session.get(bundle_url, headers=HEADERS) as resp:
                        bundle_text = await resp.text()
                    for slug in re.findall(r"/section/v1/pages/([A-Za-z0-9_-]+)", bundle_text):
                        slugs.add(slug)
                except Exception as e:
                    meta.setdefault("errors", []).append(f"discover:bundle_fetch_failed:{e}")

        except Exception as e:
            meta.setdefault("errors", []).append(f"discover:bootstrap_failed:{e}")

        return slugs

    async def _fetch_official_section_cards(self, session, slug, *, start_time=None, fetch_meta=None, seen_ids=None):
        collected = []
        offset = 0
        limit = 100
        page = 0
        seen_ids = seen_ids or set()
        meta = fetch_meta if fetch_meta is not None else {}

        while page < config.KAKAO_DISCOVERY_MAX_PAGES_PER_SLUG:
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

            except Exception as e:
                meta.setdefault("errors", []).append(f"discover:{slug}:{e}")
                break

            if len(seen_ids) >= config.KAKAO_DISCOVERY_SOFT_CAP:
                meta.setdefault("errors", []).append("discover:soft_cap_reached")
                break

        return collected

    async def fetch_all_data(self):
        """카카오웹툰의 '요일별'과 '완결' API에서 모든 웹툰 데이터를 비동기적으로 가져옵니다."""
        print("카카오웹툰 서버에서 최신 데이터를 가져옵니다...")

        # Run-level watchdog start
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

            # Run-level watchdog gate before making requests
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
                seen_ids = set()
                for card in completed_data if isinstance(completed_data, list) else []:
                    cid = str(card.get("id") or "").strip()
                    if cid:
                        seen_ids.add(cid)
                if isinstance(weekday_data, dict):
                    for card in self._iter_cards_from_sections(weekday_data.get("data", {}).get("sections", [])):
                        cid = str(card.get("id") or "").strip()
                        if cid:
                            seen_ids.add(cid)

                for slug in discovered_slugs:
                    if len(seen_ids) >= config.KAKAO_DISCOVERY_SOFT_CAP:
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

        # 요일 한글 -> 영문 변환 맵
        day_map = {"월": "mon", "화": "tue", "수": "wed", "목": "thu", "금": "fri", "토": "sat", "일": "sun"}

        if weekday_data.get("data", {}).get("sections"):
            for section in weekday_data["data"]["sections"]:
                weekday_kor = section.get("title", "").replace("요일", "")  # "월요일" -> "월"
                weekday_eng = day_map.get(weekday_kor)
                if not weekday_eng:
                    continue

                for webtoon in self._iter_cards_from_sections([section]):
                    content_id = str(webtoon.get("id") or "").strip()
                    if not content_id:
                        continue

                    webtoon.setdefault("weekdayDisplayGroups", []).append(weekday_eng)

                    content_payload = webtoon.get("content", {})
                    if "title" not in webtoon:
                        webtoon["title"] = content_payload.get("title")

                    status_text = content_payload.get("onGoingStatus")
                    status_counts[status_text] = status_counts.get(status_text, 0) + 1

                    if status_text == "PAUSE":
                        hiatus_today.setdefault(content_id, webtoon)
                    else:
                        ongoing_today.setdefault(content_id, webtoon)

        for webtoon in discovered_cards:
            content_id = str(webtoon.get("id") or "").strip()
            if not content_id:
                continue

            content_payload = webtoon.get("content", {})
            if "title" not in webtoon:
                webtoon["title"] = content_payload.get("title")

            status_text = content_payload.get("onGoingStatus")
            status_counts[status_text] = status_counts.get(status_text, 0) + 1

            if content_id in ongoing_today or content_id in hiatus_today:
                continue

            if status_text == "PAUSE":
                hiatus_today[content_id] = webtoon
            else:
                ongoing_today[content_id] = webtoon

        for webtoon in completed_data:
            content_id = str(webtoon.get("id") or "").strip()
            if not content_id:
                continue

            if content_id not in ongoing_today and content_id not in hiatus_today:
                webtoon["status"] = "완결"
                content_payload = webtoon.get("content", {})
                if "title" not in webtoon:
                    webtoon["title"] = content_payload.get("title")
                finished_today[content_id] = webtoon

        all_content_today = {**ongoing_today, **hiatus_today, **finished_today}
        for webtoon in all_content_today.values():
            if "title" not in webtoon:
                webtoon["title"] = webtoon.get("content", {}).get("title")

        print(f"오늘자 데이터 수집 완료: 총 {len(all_content_today)}개 고유 웹툰 확인")
        print(f"  - 연재중: {len(ongoing_today)}개, 휴재: {len(hiatus_today)}개, 완결: {len(finished_today)}개")
        if status_counts:
            print(f"  - 수집된 onGoingStatus 집계: {status_counts}")

        # Return 5-tuple (base_crawler supports 4/5 tuple)
        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        """수집된 최신 웹툰 데이터를 데이터베이스와 동기화합니다."""
        print("\nDB를 오늘의 최신 상태로 전체 동기화를 시작합니다...")
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id FROM contents WHERE source = %s", (self.source_name,))
        db_existing_ids = {row["content_id"] for row in cursor.fetchall()}
        updates, inserts = [], []

        for content_id, webtoon_data in all_content_today.items():
            status = ""
            if content_id in finished_today:
                status = "완결"
            elif content_id in hiatus_today:
                status = "휴재"
            elif content_id in ongoing_today:
                status = "연재중"
            else:
                continue

            content_data = webtoon_data.get("content", {})
            author_names = [
                author.get("name") for author in content_data.get("authors", []) if author.get("name")
            ]

            title = content_data.get("title") or webtoon_data.get("title")
            if not title:
                continue

            encoded_title = urllib.parse.quote(str(title), safe="")

            # 우선순위: lookThroughImage (단일) -> featuredCharacterImageA (캐릭터) -> lookThroughImages[0] (슬라이스)
            thumbnail_url = content_data.get("lookThroughImage")
            if not thumbnail_url and content_data.get("featuredCharacterImageA"):
                thumbnail_url = content_data.get("featuredCharacterImageA")
            if not thumbnail_url and content_data.get("lookThroughImages"):
                thumbnail_url = content_data["lookThroughImages"][0]

            meta_data = {
                "common": {
                    "authors": author_names,
                    "thumbnail_url": thumbnail_url,
                    "content_url": f"https://webtoon.kakao.com/content/{encoded_title}/{content_id}",
                },
                "attributes": {
                    "weekdays": webtoon_data.get("weekdayDisplayGroups", []),
                    "lookThroughImage": content_data.get("lookThroughImage"),
                    "backgroundImage": content_data.get("backgroundImage"),
                    "featuredCharacterImageA": content_data.get("featuredCharacterImageA"),
                    "featuredCharacterImageB": content_data.get("featuredCharacterImageB"),
                    "titleImageA": content_data.get("titleImageA"),
                    "titleImageB": content_data.get("titleImageB"),
                    "lookThroughImages": content_data.get("lookThroughImages"),
                },
            }

            if content_id in db_existing_ids:
                record = ("webtoon", title, status, json.dumps(meta_data), content_id, self.source_name)
                updates.append(record)
            else:
                record = (content_id, self.source_name, "webtoon", title, status, json.dumps(meta_data))
                inserts.append(record)

        if updates:
            cursor.executemany(
                "UPDATE contents SET content_type=%s, title=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s",
                updates,
            )
            print(f"{len(updates)}개 웹툰 정보 업데이트 완료.")

        if inserts:
            cursor.executemany(
                "INSERT INTO contents (content_id, source, content_type, title, status, meta) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (content_id, source) DO NOTHING",
                inserts,
            )
            print(f"{len(inserts)}개 신규 웹툰 DB 추가 완료.")

        cursor.close()
        print("DB 동기화 완료.")
        return len(inserts)
