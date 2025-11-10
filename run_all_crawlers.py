# run_all_crawlers.py
import asyncio
import time
import traceback
import json
import sys
from dotenv import load_dotenv

from database import create_standalone_connection, get_cursor, setup_database_standalone
from crawlers.naver_webtoon_crawler import NaverWebtoonCrawler
from crawlers.kakaopage_crawler import KakaopageCrawler

# ----------------------------------------------------------------------
# [중요] 실행할 모든 크롤러를 이곳에 등록합니다.
# ----------------------------------------------------------------------
ALL_CRAWLERS = [
    NaverWebtoonCrawler,
    KakaopageCrawler,
    # (향후 새로운 크롤러 클래스를 여기에 추가)
]
# ----------------------------------------------------------------------

async def main():
    """
    등록된 모든 크롤러를 순차적으로 실행하고, 각 크롤러의 실행 결과를 DB에 저장합니다.
    """
    start_time = time.time()
    print("==========================================")
    print("   통합 크롤러 실행 스크립트 시작")
    print("==========================================")

    load_dotenv()

    # 🚨 [수정] 이 7줄을 반드시 주석 처리하거나 삭제하세요.
    #
    # 데이터베이스 초기화
    # try:
    #     setup_database_standalone()
    # except Exception as e:
    #     print(f"FATAL: 데이터베이스 초기화 중 오류 발생: {e}", file=sys.stderr)
    #     sys.exit(1)

    db_conn = None
    try:
        db_conn = create_standalone_connection()

        for crawler_class in ALL_CRAWLERS:
            crawler_instance = crawler_class()
            crawler_display_name = crawler_instance.source_name.replace('_', ' ').title()

            print(f"\n--- [{crawler_display_name}] 크롤러 작업 시작 ---")

            report = {'status': '성공'}
            crawler_start_time = time.time()

            try:
                new_contents, completed_details, total_notified = await crawler_instance.run_daily_check(db_conn)
                report.update({
                    'new_contents': new_contents,
                    'completed_details': completed_details,
                    'total_notified': total_notified
                })
            except Exception as e:
                print(f"FATAL: [{crawler_display_name}] 크롤러 실행 중 치명적 오류 발생: {e}", file=sys.stderr)
                report['status'] = '실패'
                report['error_message'] = traceback.format_exc()
            finally:
                report['duration'] = time.time() - crawler_start_time

                # 각 크롤러의 실행 결과를 DB에 저장
                report_conn = None
                try:
                    # DB 연결이 끊어졌을 경우를 대비해 새로운 연결 생성
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

    finally:
        if db_conn:
            db_conn.close()

        total_duration = time.time() - start_time
        print("\n==========================================")
        print(f"  통합 크롤러 실행 완료 (총 소요 시간: {total_duration:.2f}초)")
        print("==========================================")

if __name__ == '__main__':
    # Python 3.7+
    asyncio.run(main())
