# crawlers/kakaowebtoon_crawler.py

import asyncio
import aiohttp
import json
import os
from tenacity import retry, stop_after_attempt, wait_exponential

from .base_crawler import ContentCrawler
from database import get_cursor
from services.notification_service import send_completion_notifications

# --- KakaoWebtoon API Configuration ---
API_BASE_URL = "https://gateway-kw.kakao.com/section/v1/pages"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Referer": "https://webtoon.kakao.com/",
    "Accept-Language": "ko"
}

class KakaowebtoonCrawler(ContentCrawler):
    """
    webtoon.kakao.com에서 웹툰 정보를 수집하는 크롤러입니다.
    """

    def __init__(self):
        super().__init__('kakaowebtoon')
        self.cookies = self._get_cookies_from_env()

    def _get_cookies_from_env(self):
        """환경 변수에서 쿠키 값을 로드하고 유효성을 검사합니다."""
        webid = os.getenv('KAKAOWEBTOON_WEBID')
        t_ano = os.getenv('KAKAOWEBTOON_T_ANO')

        if not webid or not t_ano:
            raise ValueError(
                "Kakaowebtoon 크롤러를 위해 KAKAOWEBTOON_WEBID와 KAKAOWEBTOON_T_ANO 환경 변수를 설정해야 합니다."
            )

        return {"webid": webid, "_T_ANO": t_ano}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(self, session, url, params=None):
        """
        주어진 URL과 파라미터로 API에 GET 요청을 보내고 JSON 응답을 반환합니다.
        """
        async with session.get(url, headers=HEADERS, cookies=self.cookies, params=params) as response:
            response.raise_for_status()
            return await response.json()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_paginated_completed(self, session):
        """'completed' 엔드포인트의 모든 페이지를 순회하며 데이터를 수집합니다."""
        all_completed_content = []
        offset = 0
        limit = 100
        while True:
            try:
                url = f"{API_BASE_URL}/completed"
                data = await self._fetch_from_api(session, url, params={"offset": offset, "limit": limit})

                if not data.get('data', {}).get('sections'):
                    break

                cards = data['data']['sections'][0]['cardGroups'][0]['cards']
                if not cards:
                    break

                all_completed_content.extend(cards)
                offset += len(cards)

                if len(cards) < limit:
                    break

                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"Error fetching completed page at offset {offset}: {e}")
                break
        return all_completed_content

    async def fetch_all_data(self):
        """
        카카오웹툰의 '요일별'과 '완결' API에서 모든 웹툰 데이터를 비동기적으로 가져옵니다.
        """
        print("카카오웹툰 서버에서 최신 데이터를 가져옵니다...")
        async with aiohttp.ClientSession() as session:
            weekday_url = f"{API_BASE_URL}/general-weekdays"

            tasks = [
                self._fetch_from_api(session, weekday_url),
                self._fetch_paginated_completed(session)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            weekday_data, completed_data = results


        if isinstance(weekday_data, Exception):
            print(f"❌ 요일별 데이터 수집 실패: {weekday_data}")
            weekday_data = {}
        if isinstance(completed_data, Exception):
            print(f"❌ 완결 데이터 수집 실패: {completed_data}")
            completed_data = []

        print("\n--- 데이터 정규화 시작 ---")
        ongoing_today, hiatus_today, finished_today = {}, {}, {}

        # 요일 한글 -> 영문 변환 맵
        DAY_MAP = {"월": "mon", "화": "tue", "수": "wed", "목": "thu", "금": "fri", "토": "sat", "일": "sun"}

        if weekday_data.get('data', {}).get('sections'):
            for section in weekday_data['data']['sections']:
                weekday_kor = section.get('title', '').replace('요일', '') # "월요일" -> "월"
                weekday_eng = DAY_MAP.get(weekday_kor)
                if not weekday_eng: continue

                for card_group in section.get('cardGroups', []):

                    for webtoon in card_group.get('cards', []):
                        content_id = str(webtoon['id'])
                        webtoon['weekdayDisplayGroups'] = [weekday_eng]

                        status_text = webtoon.get('content', {}).get('onGoingStatus') # 'onGoingStatus' 사용
                        if status_text == 'PAUSE': # 휴재 상태 키 확인
                            if content_id not in hiatus_today:
                                hiatus_today[content_id] = webtoon
                        else:
                            if content_id not in ongoing_today:
                                ongoing_today[content_id] = webtoon

        for webtoon in completed_data:
            content_id = str(webtoon['id'])
            if content_id not in ongoing_today and content_id not in hiatus_today:
                # 완결 데이터는 'status'가 최상위에 있을 수 있음
                webtoon['status'] = '완결'
                finished_today[content_id] = webtoon

        all_content_today = {**ongoing_today, **hiatus_today, **finished_today}
        print(f"오늘자 데이터 수집 완료: 총 {len(all_content_today)}개 고유 웹툰 확인")
        print(f"  - 연재중: {len(ongoing_today)}개, 휴재: {len(hiatus_today)}개, 완결: {len(finished_today)}개")
        return ongoing_today, hiatus_today, finished_today, all_content_today

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        """
        수집된 최신 웹툰 데이터를 데이터베이스와 동기화합니다.
        """
        print("\nDB를 오늘의 최신 상태로 전체 동기화를 시작합니다...")
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id FROM contents WHERE source = %s", (self.source_name,))
        db_existing_ids = {row['content_id'] for row in cursor.fetchall()}
        updates, inserts = [], []

        for content_id, webtoon_data in all_content_today.items():
            status = ''
            if content_id in finished_today: status = '완결'
            elif content_id in hiatus_today: status = '휴재'
            elif content_id in ongoing_today: status = '연재중'
            else: continue

            content_data = webtoon_data.get('content', {})
            author_names = [author['name'] for author in content_data.get('authors', [])]

            # 우선순위: lookThroughImage (단일) -> featuredCharacterImageA (캐릭터) -> lookThroughImages[0] (슬라이스)
            thumbnail_url = content_data.get('lookThroughImage')
            if not thumbnail_url and content_data.get('featuredCharacterImageA'):
                thumbnail_url = content_data.get('featuredCharacterImageA')
            if not thumbnail_url and content_data.get('lookThroughImages'):
                thumbnail_url = content_data['lookThroughImages'][0]

            meta_data = {
                "common": {
                    "authors": author_names,
                    "thumbnail_url": thumbnail_url
                },
                "attributes": {
                    "weekdays": webtoon_data.get('weekdayDisplayGroups', []),
                    "lookThroughImage": content_data.get('lookThroughImage'),
                    "backgroundImage": content_data.get('backgroundImage'),
                    "featuredCharacterImageA": content_data.get('featuredCharacterImageA'),
                    "featuredCharacterImageB": content_data.get('featuredCharacterImageB'),
                    "titleImageA": content_data.get('titleImageA'),
                    "titleImageB": content_data.get('titleImageB'),
                    "lookThroughImages": content_data.get('lookThroughImages')
                }
            }

            title = content_data.get('title')
            if not title:
                continue

            if content_id in db_existing_ids:
                record = ('webtoon', title, status, json.dumps(meta_data), content_id, self.source_name)
                updates.append(record)
            else:
                record = (content_id, self.source_name, 'webtoon', title, status, json.dumps(meta_data))
                inserts.append(record)

        if updates:
            cursor.executemany("UPDATE contents SET content_type=%s, title=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s", updates)
            print(f"{len(updates)}개 웹툰 정보 업데이트 완료.")

        if inserts:
            cursor.executemany("INSERT INTO contents (content_id, source, content_type, title, status, meta) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (content_id, source) DO NOTHING", inserts)
            print(f"{len(inserts)}개 신규 웹툰 DB 추가 완료.")

        conn.commit()
        cursor.close()
        print("DB 동기화 완료.")
        return len(inserts)

    async def run_daily_check(self, conn):
        """
        매일 실행되는 메인 로직입니다.
        """
        print(f"=== [{self.source_name.title()}] 일일 점검 시작 ===")
        cursor = get_cursor(conn)

        cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
        db_state_before_sync = {row['content_id']: row['status'] for row in cursor.fetchall()}
        cursor.close()
        print(f"  -> DB에서 {len(db_state_before_sync)}개의 기존 콘텐츠 상태를 로드했습니다.")

        ongoing, hiatus, finished, all_content = await self.fetch_all_data()

        newly_completed_ids = {
            cid for cid, status in db_state_before_sync.items()
            if status in ('연재중', '휴재') and cid in finished
        }
        print(f"  -> {len(newly_completed_ids)}개의 신규 완결 작품을 감지했습니다.")

        details, notified = send_completion_notifications(get_cursor(conn), newly_completed_ids, all_content, self.source_name)

        added = self.synchronize_database(conn, all_content, ongoing, hiatus, finished)

        print(f"=== [{self.source_name.title()}] 일일 점검 완료 ===")
        return added, details, notified
