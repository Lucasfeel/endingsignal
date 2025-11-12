# crawlers/naver_webtoon_crawler.py

import os
import time
import traceback
import asyncio
import aiohttp
import json
import sys
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

import config
from services.notification_service import send_completion_notifications
from .base_crawler import ContentCrawler
from database import get_cursor, create_standalone_connection, setup_database_standalone

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
        MAX_PAGES = 150
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

    async def _fetch_paginated_weekday_data(self, session, api_day):
        all_candidates = {}
        page = 1
        MAX_PAGES = 50  # You can adjust this if needed
        # print(f"\n'{api_day}' 요일 웹툰 목록 확보를 위해 페이지네이션 수집 시작...")
        while page <= MAX_PAGES:
            try:
                api_url = f"{config.NAVER_API_URL}/weekday?week={api_day}&page={page}&pageSize=100"
                webtoons_on_page = await self._fetch_from_api(session, api_url)
                if not webtoons_on_page:
                    # print(f"  -> {api_day}: {page-1} 페이지에서 수집 종료 (데이터 없음).")
                    break
                for webtoon in webtoons_on_page:
                    if webtoon['titleId'] not in all_candidates:
                        all_candidates[webtoon['titleId']] = webtoon
                # print(f"  -> {api_day}: {page} 페이지 수집 완료. (현재 후보군: {len(all_candidates)}개)")
                page += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                # print(f"  -> {api_day}: {page} 페이지 수집 중 오류 발생: {e}")
                break
        # if page > MAX_PAGES:
        #     print(f"  -> {api_day}: 최대 {MAX_PAGES} 페이지까지 수집하여 종료합니다.")
        return all_candidates

    async def fetch_all_data(self):
        print("네이버 웹툰 서버에서 오늘의 최신 데이터를 가져옵니다...")
        async with aiohttp.ClientSession() as session:
            ongoing_tasks = [self._fetch_paginated_weekday_data(session, api_day) for api_day in WEEKDAYS.keys()]
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

            for webtoon in result.values():
                titleId = webtoon['titleId']

                if titleId not in naver_ongoing_today:
                    naver_ongoing_today[titleId] = webtoon
                    naver_ongoing_today[titleId]['normalized_weekdays'] = set()

                naver_ongoing_today[titleId]['normalized_weekdays'].add(WEEKDAYS[day_key])

                if webtoon.get('rest', False):
                    naver_hiatus_today[titleId] = webtoon

        print("  -> 수집된 요일 정보를 list로 변환합니다...")
        for webtoon in naver_ongoing_today.values():
            webtoon['normalized_weekdays'] = list(webtoon['normalized_weekdays'])

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
            if content_id in naver_finished_today: status = '완결'
            elif content_id in naver_hiatus_today: status = '휴재'
            elif content_id in naver_ongoing_today: status = '연재중'
            else: continue

            author = webtoon_data.get('author')
            meta_data = {
                "common": {
                    "authors": [author] if author else [],
                    "thumbnail_url": None
                },
                "attributes": {
                    "weekdays": webtoon_data.get('normalized_weekdays', [])
                }
            }

            if content_id in db_existing_ids:
                record = ('webtoon', webtoon_data['titleName'], status, json.dumps(meta_data), content_id, self.source_name)
                updates.append(record)
            else:
                record = (content_id, self.source_name, 'webtoon', webtoon_data['titleName'], status, json.dumps(meta_data))
                inserts.append(record)

        if updates:
            cursor.executemany("UPDATE contents SET content_type=%s, title=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s", updates)
            print(f"{len(updates)}개 웹툰 정보 업데이트 완료.")

        if inserts:
            # === 🚨 [버그 수정] INSERT 리스트의 잠재적 중복 제거 ===
            seen_keys = set()
            unique_inserts = []
            for record in inserts:
                key = (record[0], record[1]) # (content_id, source)
                if key not in seen_keys:
                    unique_inserts.append(record)
                    seen_keys.add(key)
            # =======================================================

            cursor.executemany("INSERT INTO contents (content_id, source, content_type, title, status, meta) VALUES (%s, %s, %s, %s, %s, %s)", unique_inserts) # 👈 unique_inserts 사용
            print(f"{len(unique_inserts)}개 신규 웹툰 DB 추가 완료. (중복 {len(inserts) - len(unique_inserts)}개 제거)")

        conn.commit()
        cursor.close()
        print("DB 동기화 완료.")
        return len(unique_inserts)

    async def run_daily_check(self, conn):
        print("LOG: run_daily_check started.")
        cursor = get_cursor(conn)
        print(f"=== {self.source_name} 일일 점검 시작 ===")
        cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
        db_state_before_sync = {row['content_id']: row['status'] for row in cursor.fetchall()}
        cursor.close()
        print("LOG: Initial database state loaded.")

        ongoing, hiatus, finished, all_content = await self.fetch_all_data()
        print("LOG: Data fetched from API.")

        newly_completed_ids = {cid for cid, s in db_state_before_sync.items() if s in ('연재중', '휴재') and cid in finished}
        print(f"LOG: Found {len(newly_completed_ids)} newly completed items.")

        details, notified = send_completion_notifications(get_cursor(conn), newly_completed_ids, all_content, self.source_name)
        print("LOG: Notification service executed.")

        added = self.synchronize_database(conn, all_content, ongoing, hiatus, finished)
        print("LOG: Database synchronization executed.")

        print("\n=== 일일 점검 완료 ===")
        return added, details, notified

if __name__ == '__main__':
    print("==========================================")
    print("  CRAWLER SCRIPT STARTED (STANDALONE)")
    print("==========================================")
    start_time = time.time()
    report = {'status': '성공'}
    db_conn = None
    CRAWLER_DISPLAY_NAME = "네이버 웹툰"

    try:
        # [삭제] 크롤러가 DB 셋업을 직접 호출하는 위험한 코드 제거
        # print("LOG: Calling setup_database_standalone()...")
        # setup_database_standalone()
        # print("LOG: setup_database_standalone() finished.")

        print("LOG: Calling create_standalone_connection()...")
        db_conn = create_standalone_connection()
        print("LOG: create_standalone_connection() finished.")

        crawler = NaverWebtoonCrawler()
        print("LOG: NaverWebtoonCrawler instance created.")

        print("LOG: Calling asyncio.run(crawler.run_daily_check())...")
        new_contents, completed_details, total_notified = asyncio.run(crawler.run_daily_check(db_conn))
        print("LOG: asyncio.run(crawler.run_daily_check()) finished.")

        report.update({'new_webtoons': new_contents, 'completed_details': completed_details, 'total_notified': total_notified})

    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        report['status'] = '실패'
        report['error_message'] = traceback.format_exc()

    finally:
        if db_conn:
            print("LOG: Closing database connection.")
            db_conn.close()

        report['duration'] = time.time() - start_time

        # === 🚨 [리팩토링] 메일 발송 대신 DB에 보고서 저장 ===
        report_conn = None
        try:
            report_conn = create_standalone_connection()
            report_cursor = get_cursor(report_conn)
            print(f"LOG: Saving report to 'daily_crawler_reports' table...")
            report_cursor.execute(
                """
                INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
                VALUES (%s, %s, %s)
                """,
                (CRAWLER_DISPLAY_NAME, report['status'], json.dumps(report))
            )
            report_conn.commit()
            report_cursor.close()
            print("LOG: Report saved successfully.")
        except Exception as report_e:
            print(f"FATAL: [실패] 보고서 DB 저장 실패: {report_e}", file=sys.stderr)
        finally:
            if report_conn:
                report_conn.close()
        # =================================================

        print("==========================================")
        print("  CRAWLER SCRIPT FINISHED")
        print("==========================================")

        if report['status'] == '실패':
            sys.exit(1)
