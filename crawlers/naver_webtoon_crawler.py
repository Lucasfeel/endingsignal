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
from .base_crawler import ContentCrawler
from database import get_cursor, create_standalone_connection
from utils.text import normalize_search_text

load_dotenv()

HEADERS = config.CRAWLER_HEADERS
WEEKDAYS = config.WEEKDAYS


class NaverWebtoonCrawler(ContentCrawler):
    """네이버 웹툰 크롤러"""

    def __init__(self):
        super().__init__("naver_webtoon")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(self, session, url):
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("titleList", data.get("list", []))

    async def _fetch_paginated_data(self, session, base_url, max_pages, description, start_time=None):
        """주어진 API URL의 모든 페이지를 순회하며 데이터를 수집하는 범용 함수"""
        all_candidates = {}
        meta = {"pages_fetched": 0, "errors": [], "stopped_reason": None}

        print(f"\n'{description}' 목록 확보를 위해 페이지네이션 수집 시작...")
        for page in range(1, max_pages + 1):
            # Run-level watchdog
            if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                meta["errors"].append("WALL_TIMEOUT_EXCEEDED")
                meta["stopped_reason"] = meta.get("stopped_reason") or "wall_timeout"
                print(f"  -> {page} 페이지 수집 중 실행 시간 한도를 초과하여 중단합니다.")
                break

            try:
                api_url = f"{base_url}&page={page}&pageSize=100"
                webtoons_on_page = await self._fetch_from_api(session, api_url)

                if not webtoons_on_page:
                    meta["stopped_reason"] = "no_data"
                    print(f"  -> {page-1} 페이지에서 수집 종료 (데이터 없음).")
                    break

                new_ids_in_page = 0
                for webtoon in webtoons_on_page:
                    tid = str(webtoon.get("titleId") or "").strip()
                    if not tid:
                        continue
                    if tid not in all_candidates:
                        all_candidates[tid] = webtoon
                        new_ids_in_page += 1

                meta["pages_fetched"] += 1
                print(
                    f"  -> {page} 페이지 수집 완료. (현재 후보군: {len(all_candidates)}개, 신규: {new_ids_in_page}개)"
                )

                if new_ids_in_page == 0:
                    meta["stopped_reason"] = "no_new_ids"
                    print(f"  -> {page} 페이지에서 신규 ID 없음으로 조기 종료")
                    break

                await asyncio.sleep(0.1)

            except Exception as e:
                meta["errors"].append(str(e))
                meta["stopped_reason"] = meta.get("stopped_reason") or "exception"
                print(f"  -> {page} 페이지 수집 중 오류 발생: {e}")
                break

        else:
            meta["stopped_reason"] = "max_pages"
            print(f"  -> 최대 {max_pages} 페이지까지 수집하여 종료합니다.")

        return all_candidates, meta

    async def fetch_all_data(self):
        print("네이버 웹툰 서버에서 오늘의 최신 데이터를 가져옵니다...")

        # Run-level watchdog start
        start_time = time.monotonic()

        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        # IMPORTANT: base_crawler relies on fetch_meta['errors'] for degraded-fetch 판단
        fetch_meta = {"ongoing": {}, "finished": {}, "errors": []}

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            ongoing_tasks = []
            for api_day in WEEKDAYS.keys():
                base_url = f"{config.NAVER_API_URL}/weekday?week={api_day}"
                task = self._fetch_paginated_data(
                    session, base_url, 50, f"'{api_day}'요일 웹툰", start_time=start_time
                )
                ongoing_tasks.append(task)

            finished_tasks = []
            finished_orders = []
            for order in config.NAVER_FINISHED_ORDERS:
                finished_orders.append(order)
                finished_base_url = f"{config.NAVER_API_URL}/finished?order={order}"
                finished_tasks.append(
                    self._fetch_paginated_data(
                        session,
                        finished_base_url,
                        config.NAVER_FINISHED_MAX_PAGES,
                        f"완결/장기 휴재 후보(order={order})",
                        start_time=start_time,
                    )
                )

            results = await asyncio.gather(*ongoing_tasks, *finished_tasks, return_exceptions=True)

            ongoing_results = results[: len(ongoing_tasks)]
            finished_results = results[len(ongoing_tasks) :]

            finished_candidates = {}
            fetch_meta["finished"] = {}
            for idx, result in enumerate(finished_results):
                order = finished_orders[idx]
                if isinstance(result, Exception):
                    print(f"❌ 완결/장기 휴재 데이터 수집 실패(order={order}): {result}")
                    fetch_meta["errors"].append(f"finished:{order}:{result}")
                    continue

                order_candidates, order_meta = result
                fetch_meta["finished"][order] = order_meta

                for err in order_meta.get("errors", []):
                    fetch_meta["errors"].append(f"finished:{order}:{err}")

                finished_candidates.update(order_candidates)

        print("\n--- 데이터 수집 결과 ---")
        naver_ongoing_today, naver_hiatus_today, naver_finished_today = {}, {}, {}

        api_days = list(WEEKDAYS.keys())
        for i, result in enumerate(ongoing_results):
            day_key = api_days[i]

            if isinstance(result, Exception):
                print(f"❌ '{day_key}'요일 데이터 수집 실패: {result}")
                fetch_meta["errors"].append(f"ongoing:{day_key}:{result}")
                continue

            day_candidates, day_meta = result
            fetch_meta["ongoing"][day_key] = day_meta

            # CRITICAL: propagate nested day errors to top-level
            for err in day_meta.get("errors", []):
                fetch_meta["errors"].append(f"ongoing:{day_key}:{err}")

            for webtoon in day_candidates.values():
                title_id = str(webtoon.get("titleId") or "").strip()
                if not title_id:
                    continue

                if title_id not in naver_ongoing_today:
                    naver_ongoing_today[title_id] = webtoon
                    naver_ongoing_today[title_id]["normalized_weekdays"] = set()
                    if "title" not in naver_ongoing_today[title_id]:
                        naver_ongoing_today[title_id]["title"] = webtoon.get("title") or webtoon.get("titleName")

                naver_ongoing_today[title_id]["normalized_weekdays"].add(WEEKDAYS[day_key])

                if webtoon.get("rest", False):
                    naver_hiatus_today[title_id] = webtoon

        print("  -> 수집된 요일 정보를 list로 변환합니다...")
        for webtoon in naver_ongoing_today.values():
            webtoon["normalized_weekdays"] = list(webtoon["normalized_weekdays"])

        for tid, data in finished_candidates.items():
            tid_str = str(tid).strip()
            if not tid_str:
                continue

            if tid_str not in naver_ongoing_today and tid_str not in naver_hiatus_today:
                if data.get("rest", False):
                    naver_hiatus_today[tid_str] = data
                else:
                    naver_finished_today[tid_str] = data

                if "title" not in data:
                    data["title"] = data.get("title") or data.get("titleName")

        all_naver_webtoons_today = {**naver_finished_today, **naver_hiatus_today, **naver_ongoing_today}
        for webtoon in all_naver_webtoons_today.values():
            if "title" not in webtoon:
                webtoon["title"] = webtoon.get("title") or webtoon.get("titleName")

        print(f"오늘자 데이터 수집 완료: 총 {len(all_naver_webtoons_today)}개 고유 웹툰 확인")
        return naver_ongoing_today, naver_hiatus_today, naver_finished_today, all_naver_webtoons_today, fetch_meta

    def synchronize_database(
        self,
        conn,
        all_naver_webtoons_today,
        naver_ongoing_today,
        naver_hiatus_today,
        naver_finished_today,
    ):
        print("\nDB를 오늘의 최신 상태로 전체 동기화를 시작합니다...")
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id FROM contents WHERE source = %s", (self.source_name,))
        db_existing_ids = {row["content_id"] for row in cursor.fetchall()}
        updates, inserts = [], []

        for content_id, webtoon_data in all_naver_webtoons_today.items():
            status = ""
            if content_id in naver_finished_today:
                status = "완결"
            elif content_id in naver_hiatus_today:
                status = "휴재"
            elif content_id in naver_ongoing_today:
                status = "연재중"
            else:
                continue

            title = webtoon_data.get("title") or webtoon_data.get("titleName")
            if not title:
                continue

            author = webtoon_data.get("author")
            normalized_title = normalize_search_text(title)
            normalized_authors = normalize_search_text(author)
            meta_data = {
                "common": {
                    "authors": [author] if author else [],
                    "thumbnail_url": webtoon_data.get("thumbnailUrl"),
                    "content_url": f"https://m.comic.naver.com/webtoon/list?titleId={content_id}",
                },
                "attributes": {
                    "weekdays": webtoon_data.get("normalized_weekdays", []),
                },
            }

            if content_id in db_existing_ids:
                record = (
                    "webtoon",
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    json.dumps(meta_data),
                    content_id,
                    self.source_name,
                )
                updates.append(record)
            else:
                record = (
                    content_id,
                    self.source_name,
                    "webtoon",
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    json.dumps(meta_data),
                )
                inserts.append(record)

        if updates:
            cursor.executemany(
                "UPDATE contents SET content_type=%s, title=%s, normalized_title=%s, normalized_authors=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s",
                updates,
            )
            print(f"{len(updates)}개 웹툰 정보 업데이트 완료.")

        if inserts:
            cursor.executemany(
                "INSERT INTO contents (content_id, source, content_type, title, normalized_title, normalized_authors, status, meta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (content_id, source) DO NOTHING",
                inserts,
            )
            print(f"{len(inserts)}개 신규 웹툰 DB 추가 완료.")

        cursor.close()
        print("DB 동기화 완료.")
        return len(inserts)


if __name__ == "__main__":
    print("==========================================")
    print("  CRAWLER SCRIPT STARTED (STANDALONE)")
    print("==========================================")

    start_time = time.time()
    report = {"status": "성공"}
    db_conn = None
    CRAWLER_DISPLAY_NAME = "네이버 웹툰"

    try:
        print("LOG: Calling create_standalone_connection()...")
        db_conn = create_standalone_connection()
        print("LOG: create_standalone_connection() finished.")

        crawler = NaverWebtoonCrawler()
        print("LOG: NaverWebtoonCrawler instance created.")

        print("LOG: Calling asyncio.run(crawler.run_daily_check())...")
        new_contents, newly_completed_items, cdc_info = asyncio.run(crawler.run_daily_check(db_conn))
        print("LOG: asyncio.run(crawler.run_daily_check()) finished.")

        report.update(
            {
                "new_webtoons": new_contents,
                "newly_completed_items": newly_completed_items,
                "cdc_info": cdc_info,
            }
        )

    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        report["status"] = "실패"
        report["error_message"] = traceback.format_exc()

    finally:
        if db_conn:
            print("LOG: Closing database connection.")
            db_conn.close()

        report["duration"] = time.time() - start_time

        report_conn = None
        try:
            report_conn = create_standalone_connection()
            report_cursor = get_cursor(report_conn)
            print("LOG: Saving report to 'daily_crawler_reports' table...")
            report_cursor.execute(
                """
                INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
                VALUES (%s, %s, %s)
                """,
                (CRAWLER_DISPLAY_NAME, report["status"], json.dumps(report)),
            )
            report_conn.commit()
            report_cursor.close()
            print("LOG: Report saved successfully.")
        except Exception as report_e:
            print(f"FATAL: [실패] 보고서 DB 저장 실패: {report_e}", file=sys.stderr)
        finally:
            if report_conn:
                report_conn.close()

        print("==========================================")
        print("  CRAWLER SCRIPT FINISHED")
        print("==========================================")

        if report["status"] == "실패":
            sys.exit(1)
