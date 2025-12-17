# run_all_crawlers.py
import asyncio
import time
import traceback
import json
import sys
from dotenv import load_dotenv

load_dotenv()

from database import create_standalone_connection, get_cursor
from crawlers.naver_webtoon_crawler import NaverWebtoonCrawler
from crawlers.kakaowebtoon_crawler import KakaowebtoonCrawler

ALL_CRAWLERS = [
    NaverWebtoonCrawler,
    KakaowebtoonCrawler,
]


async def run_one_crawler(crawler_class):
    """
    단일 크롤러 인스턴스를 생성하고 실행한 뒤, 그 결과를 DB에 보고합니다.
    """
    report = {'status': '성공'}
    crawler_start_time = time.time()

    db_conn = None
    crawler_display_name = crawler_class.__name__
    try:
        crawler_instance = crawler_class()
        crawler_display_name = getattr(crawler_instance, 'source_name', crawler_class.__name__)
        crawler_display_name = crawler_display_name.replace('_', ' ').title()

        print(f"\n--- [{crawler_display_name}] 크롤러 작업 시작 ---")

        db_conn = create_standalone_connection()
        new_contents, newly_completed_items, cdc_info = await crawler_instance.run_daily_check(db_conn)

        report.update({
            'new_contents': new_contents,
            'newly_completed_items': newly_completed_items,
            'cdc_info': cdc_info,
        })

    except Exception as e:
        crawler_display_name = crawler_display_name.replace('_', ' ').title()
        print(f"FATAL: [{crawler_display_name}] 크롤러 실행 중 치명적 오류 발생: {e}", file=sys.stderr)
        report['status'] = '실패'
        report['error_message'] = traceback.format_exc()

    finally:
        report['duration'] = time.time() - crawler_start_time
        if db_conn:
            db_conn.close()

        report_conn = None
        try:
            report_conn = create_standalone_connection()
            report_cursor = get_cursor(report_conn)
            report_cursor.execute(
                """
                INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
                VALUES (%s, %s, %s)
                """,
                (crawler_display_name, report['status'], json.dumps(report))
            )
            report_conn.commit()
            report_cursor.close()
            print(f"LOG: [{crawler_display_name}]의 실행 결과를 DB에 성공적으로 저장했습니다.")
        except Exception as report_e:
            print(f"FATAL: [{crawler_display_name}]의 보고서를 DB에 저장하는 데 실패했습니다: {report_e}", file=sys.stderr)
        finally:
            if report_conn:
                report_conn.close()


async def main():
    """
    등록된 모든 크롤러를 병렬로 실행하고, 각 크롤러의 실행 결과를 DB에 저장합니다.
    """
    start_time = time.time()
    print("==========================================")
    print("   통합 크롤러 실행 스크립트 시작")
    print("==========================================")

    tasks = [run_one_crawler(crawler_class) for crawler_class in ALL_CRAWLERS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            print(f"WARNING: 크롤러 작업 중 일부가 gather 레벨에서 예외를 반환했습니다: {result}", file=sys.stderr)

    total_duration = time.time() - start_time
    print("\n==========================================")
    print(f"  통합 크롤러 실행 완료 (총 소요 시간: {total_duration:.2f}초)")
    print("==========================================")


if __name__ == '__main__':
    asyncio.run(main())
