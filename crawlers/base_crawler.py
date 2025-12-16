# crawlers/base_crawler.py

from abc import ABC, abstractmethod

from database import get_cursor
from services.notification_service import send_completion_notifications

class ContentCrawler(ABC):
    """
    모든 콘텐츠 크롤러를 위한 추상 기본 클래스입니다.
    각 크롤러는 이 클래스를 상속받아 특정 콘텐츠 소스에 대한
    데이터 수집, 동기화, 점검 로직을 구현해야 합니다.
    """

    def __init__(self, source_name):
        self.source_name = source_name

    @abstractmethod
    async def fetch_all_data(self):
        """
        소스에서 모든 콘텐츠 데이터를 비동기적으로 가져옵니다.
        """
        pass

    @abstractmethod
    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        """
        데이터베이스를 최신 상태로 동기화합니다.
        """
        pass

    async def run_daily_check(self, conn):
        """
        일일 데이터 점검 및 완결 알림 프로세스를 실행합니다.

        Template Method 패턴 구현:
        1) DB 스냅샷 로드
        2) 원격 데이터 수집(fetch_all_data)
        3) 신규 완결 감지 및 알림 발송
        4) DB 동기화(synchronize_database)
        """
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
        db_state_before_sync = {row['content_id']: row['status'] for row in cursor.fetchall()}
        cursor.close()

        ongoing_today, hiatus_today, finished_today, all_content_today = await self.fetch_all_data()

        newly_completed_ids = {
            cid for cid, status in db_state_before_sync.items()
            if status in ('연재중', '휴재') and cid in finished_today
        }

        details, notified = send_completion_notifications(
            get_cursor(conn), newly_completed_ids, all_content_today, self.source_name
        )

        added = self.synchronize_database(
            conn, all_content_today, ongoing_today, hiatus_today, finished_today
        )

        return added, details, notified
