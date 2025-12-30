import asyncio
import aiohttp
import json
import os
import time
import urllib.parse

from tenacity import retry, stop_after_attempt, wait_exponential

import config
from .base_crawler import ContentCrawler
from database import get_cursor

# --- KakaoWebtoon API Configuration ---
API_BASE_URL = "https://gateway-kw.kakao.com/section/v1/pages"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Referer": "https://webtoon.kakao.com/",
    "Accept-Language": "ko",
}


class KakaowebtoonCrawler(ContentCrawler):
    """webtoon.kakao.com에서 웹툰 정보를 수집하는 크롤러입니다."""

    # (run_all_crawlers.py에서 인스턴스 생성 전에 스킵 판단용)
    DISPLAY_NAME = "Kakao Webtoon"
    REQUIRED_ENV_VARS = ["KAKAOWEBTOON_WEBID", "KAKAOWEBTOON_T_ANO"]

    @classmethod
    def get_missing_env_vars(cls):
        required = getattr(cls, "REQUIRED_ENV_VARS", [])
        return [key for key in required if not os.getenv(key)]

    def __init__(self):
        super().__init__("kakaowebtoon")
        self.cookies = self._get_cookies_from_env()

    def _get_cookies_from_env(self):
        """환경 변수에서 쿠키 값을 로드하고 유효성을 검사합니다."""
        webid = os.getenv("KAKAOWEBTOON_WEBID")
        t_ano = os.getenv("KAKAOWEBTOON_T_ANO")

        if not webid or not t_ano:
            raise ValueError(
                "Kakaowebtoon 크롤러를 위해 KAKAOWEBTOON_WEBID와 KAKAOWEBTOON_T_ANO 환경 변수를 설정해야 합니다."
            )

        return {"webid": webid, "_T_ANO": t_ano}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(self, session, url, params=None):
        """주어진 URL과 파라미터로 API에 GET 요청을 보내고 JSON 응답을 반환합니다."""
        async with session.get(url, headers=HEADERS, cookies=self.cookies, params=params) as response:
            response.raise_for_status()
            return await response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_paginated_completed(self, session, *, start_time=None, fetch_meta=None):
        """'completed' 엔드포인트의 모든 페이지를 순회하며 데이터를 수집합니다."""
        all_completed_content = []
        offset = 0
        limit = 100

        while True:
            try:
                # Run-level watchdog
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    if fetch_meta is not None:
                        fetch_meta.setdefault("errors", []).append("completed:WALL_TIMEOUT_EXCEEDED")
                    break

                url = f"{API_BASE_URL}/completed"
                data = await self._fetch_from_api(session, url, params={"offset": offset, "limit": limit})

                if not data.get("data", {}).get("sections"):
                    break

                cards = data["data"]["sections"][0]["cardGroups"][0]["cards"]
                if not cards:
                    break

                all_completed_content.extend(cards)
                offset += len(cards)

                if len(cards) < limit:
                    break

                await asyncio.sleep(0.1)

            except Exception as e:
                print(f"Error fetching completed page at offset {offset}: {e}")
                if fetch_meta is not None:
                    fetch_meta.setdefault("errors", []).append(f"completed:{e}")
                break

        return all_completed_content

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
        DAY_MAP = {"월": "mon", "화": "tue", "수": "wed", "목": "thu", "금": "fri", "토": "sat", "일": "sun"}

        if weekday_data.get("data", {}).get("sections"):
            for section in weekday_data["data"]["sections"]:
                weekday_kor = section.get("title", "").replace("요일", "")  # "월요일" -> "월"
                weekday_eng = DAY_MAP.get(weekday_kor)
                if not weekday_eng:
                    continue

                for card_group in section.get("cardGroups", []):
                    for webtoon in card_group.get("cards", []):
                        content_id = str(webtoon.get("id") or "").strip()
                        if not content_id:
                            continue

                        webtoon["weekdayDisplayGroups"] = [weekday_eng]

                        content_payload = webtoon.get("content", {})
                        if "title" not in webtoon:
                            webtoon["title"] = content_payload.get("title")

                        status_text = webtoon.get("content", {}).get("onGoingStatus")
                        status_counts[status_text] = status_counts.get(status_text, 0) + 1

                        if status_text == "PAUSE":
                            if content_id not in hiatus_today:
                                hiatus_today[content_id] = webtoon
                        else:
                            if content_id not in ongoing_today:
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

        for content_id, webtoon_data in all_content_today.
