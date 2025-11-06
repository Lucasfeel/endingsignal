# crawlers/naver_webtoon_crawler.py

import os
import time
import traceback
import asyncio
import aiohttp
import json
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

import config
from services.notification_service import send_completion_notifications, send_admin_report
from .base_crawler import ContentCrawler
from database import get_db, get_cursor, close_db

load_dotenv()

HEADERS = config.CRAWLER_HEADERS
WEEKDAYS = config.WEEKDAYS

class NaverWebtoonCrawler(ContentCrawler):
    """네이버 웹툰 크롤러"""

    def __init__(self):
        super().__init__('naver_webtoon')

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(self, session, url):
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get('titleList', data.get('list', []))

    async def _fetch_paginated_finished_candidates(self, session):
        all_candidates = {}
        page = 1
        MAX_PAGES = 20
        print("\n'완결/장기 휴재 후보' 목록 확보를 위해 페이지네이션 수집 시작...")
        while page <= MAX_PAGES:
            try:
                api_url = f"{config.NAVER_API_URL}/finished?order=UPDATE&page={page}&pageSize=100"
                webtoons_on_page = await self._fetch_from_api(session, api_url)
                if not webtoons_on_page:
                    print(f"  -> {page-1} 페이지에서 수집 종료 (데이터 없음).")
                    break
                for webtoon in webtoons_on_page:
                    if webtoon['titleId'] not in all_candidates:
                        all_candidates[webtoon['titleId']] = webtoon
                print(f"  -> {page} 페이지 수집 완료. (현재 후보군: {len(all_candidates)}개)")
                page += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"  -> {page} 페이지 수집 중 오류 발생: {e}")
                break
        if page > MAX_PAGES:
            print(f"  -> 최대 {MAX_PAGES} 페이지까지 수집하여 종료합니다.")
        return all_candidates

    async def fetch_all_data(self):
        print("네이버 웹툰 서버에서 오늘의 최신 데이터를 가져옵니다...")
        async with aiohttp.ClientSession() as session:
            ongoing_tasks = [self._fetch_from_api(session, f"{config.NAVER_API_URL}/weekday?week={api_day}") for api_day in WEEKDAYS.keys()]
            ongoing_results = await asyncio.gather(*ongoing_tasks, return_exceptions=True)
            finished_candidates = await self._fetch_paginated_finished_candidates(session)

        print("\n--- 데이터 수집 결과 ---")
        naver_ongoing_today, naver_hiatus_today, naver_finished_today = {}, {}, {}
        api_days = list(WEEKDAYS.keys())
        for i, result in enumerate(ongoing_results):
            day_key = api_days[i]
            if isinstance(result, Exception):
                print(f"❌ '{day_key}'요일 데이터 수집 실패: {result}")
                continue
            for webtoon in result:
                webtoon['normalized_weekday'] = WEEKDAYS[day_key]
                if webtoon.get('rest', False):
                    naver_hiatus_today[webtoon['titleId']] = webtoon
                else:
                    naver_ongoing_today[webtoon['titleId']] = webtoon

        for tid, data in finished_candidates.items():
            if tid not in naver_ongoing_today and tid not in naver_hiatus_today:
                if data.get('rest', False):
                    naver_hiatus_today[tid] = data
                else:
                    naver_finished_today[tid] = data

        all_naver_webtoons_today = {**naver_finished_today, **naver_hiatus_today, **naver_ongoing_today}
        print(f"오늘자 데이터 수집 완료: 총 {len(all_naver_webtoons_today)}개 고유 웹툰 확인")
        return naver_ongoing_today, naver_hiatus_today, naver_finished_today, all_naver_webtoons_today

    def synchronize_database(self, conn, all_naver_webtoons_today, naver_ongoing_today, naver_hiatus_today, naver_finished_today):
        print("\nDB를 오늘의 최신 상태로 전체 동기화를 시작합니다...")
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id FROM contents WHERE source = %s", (self.source_name,))
        db_existing_ids = {row['content_id'] for row in cursor.fetchall()}
        updates, inserts = [], []

        for content_id, webtoon_data in all_naver_webtoons_today.items():
            status = ''
            if content_id in naver_ongoing_today: status = '연재중'
            elif content_id in naver_hiatus_today: status = '휴재'
            elif content_id in naver_finished_today: status = '완결'
            else: continue

            meta_data = json.dumps({
                'author': webtoon_data.get('author'),
                'weekday': webtoon_data.get('normalized_weekday', webtoon_data.get('weekday'))
            })

            record = ('webtoon', webtoon_data['titleName'], status, meta_data, content_id, self.source_name)
            if content_id in db_existing_ids:
                updates.append(record)
            else:
                inserts.append((content_id, self.source_name) + record)

        if updates:
            cursor.executemany("UPDATE contents SET content_type=%s, title=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s", updates)
            print(f"{len(updates)}개 웹툰 정보 업데이트 완료.")
        if inserts:
            cursor.executemany("INSERT INTO contents (content_id, source, content_type, title, status, meta) VALUES (%s, %s, %s, %s, %s, %s)", inserts)
            print(f"{len(inserts)}개 신규 웹툰 DB 추가 완료.")
        conn.commit()
        cursor.close()
        print("DB 동기화 완료.")
        return len(inserts)

    async def run_daily_check(self, conn):
        cursor = get_cursor(conn)
        print(f"=== {self.source_name} 일일 점검 시작 ===")
        cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
        db_state_before_sync = {row['content_id']: row['status'] for row in cursor.fetchall()}
        cursor.close()

        ongoing, hiatus, finished, all_content = await self.fetch_all_data()

        newly_completed_ids = {cid for cid, s in db_state_before_sync.items() if s in ('연재중', '휴재') and cid in finished}

        details, notified = send_completion_notifications(get_cursor(conn), newly_completed_ids, all_content, self.source_name)
        added = self.synchronize_database(conn, all_content, ongoing, hiatus, finished)

        print("\n=== 일일 점검 완료 ===")
        return added, details, notified

def setup_database():
    """데이터베이스와 테이블이 없는 경우 초기 설정"""
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS contents (
        content_id TEXT NOT NULL,
        source TEXT NOT NULL,
        content_type TEXT NOT NULL,
        title TEXT NOT NULL,
        status TEXT NOT NULL,
        meta JSONB,
        PRIMARY KEY (content_id, source)
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS subscriptions (
        id SERIAL PRIMARY KEY,
        email TEXT NOT NULL,
        content_id TEXT NOT NULL,
        source TEXT NOT NULL,
        UNIQUE(email, content_id, source)
    )""")
    conn.commit()
    cursor.close()
    close_db()

if __name__ == '__main__':
    start_time = time.time()
    report = {'status': '성공'}
    db_conn = None
    try:
        setup_database()
        db_conn = get_db()
        crawler = NaverWebtoonCrawler()
        new_contents, completed_details, total_notified = asyncio.run(crawler.run_daily_check(db_conn))
        report.update({'new_webtoons': new_contents, 'completed_details': completed_details, 'total_notified': total_notified})
    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        report['status'] = '실패'
        report['error_message'] = traceback.format_exc()
    finally:
        if db_conn:
            close_db()
        report['duration'] = time.time() - start_time
        send_admin_report(report)
