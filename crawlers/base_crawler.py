#crawlers/base_crawler.py
from abc import ABC, abstractmethod

from database import get_cursor
from services.cdc_event_service import record_content_completed_event
from services.final_state_resolver import resolve_final_state


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
        raise NotImplementedError

    @abstractmethod
    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        """
        데이터베이스를 최신 상태로 동기화합니다.
        """
        raise NotImplementedError

    async def run_daily_check(self, conn):
        """
        일일 데이터 점검 및 완결 알림 프로세스를 실행합니다.

        Template Method 패턴 구현:
        1) DB 스냅샷 로드 (Final State)
        2) 원격 데이터 수집(fetch_all_data)
        3) 신규 완결 감지 및 CDC 이벤트 기록 (Final-State CDC)
        4) DB 동기화(synchronize_database)
        """
        cursor = get_cursor(conn)

        # 1) Load previous crawler status snapshot (raw)
        cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
        db_status_map = {row['content_id']: row['status'] for row in cursor.fetchall()}

        # 2) Load overrides (overlay)
        cursor.execute(
            "SELECT content_id, override_status, override_completed_at FROM admin_content_overrides WHERE source = %s",
            (self.source_name,),
        )
        override_map = {row['content_id']: row for row in cursor.fetchall()}
        cursor.close()

        # 3) Resolve previous final states
        db_state_before_sync = {}
        for content_id in set(db_status_map.keys()) | set(override_map.keys()):
            db_state_before_sync[content_id] = resolve_final_state(
                db_status_map.get(content_id), override_map.get(content_id)
            )

        # 4) Fetch today's data
        ongoing_today, hiatus_today, finished_today, all_content_today = await self.fetch_all_data()

        # 5) Build current raw status map
        current_status_map = {}
        for content_id in all_content_today.keys():
            if content_id in finished_today:
                current_status_map[content_id] = '완결'
            elif content_id in hiatus_today:
                current_status_map[content_id] = '휴재'
            elif content_id in ongoing_today:
                current_status_map[content_id] = '연재중'

        # Fill missing ids with previous known final status (stability for partial fetch)
        for content_id, previous_state in db_state_before_sync.items():
            current_status_map.setdefault(content_id, previous_state['final_status'])

        # 6) Resolve current final states
        current_final_state_map = {}
        for content_id in set(current_status_map.keys()) | set(override_map.keys()):
            current_final_state_map[content_id] = resolve_final_state(
                current_status_map.get(content_id), override_map.get(content_id)
            )

        # 7) Final-State CDC: newly completed
        newly_completed_items = []
        cdc_events_inserted_count = 0
        cdc_events_inserted_items = []
        for content_id, current_final_state in current_final_state_map.items():
            previous_final_state = db_state_before_sync.get(content_id, {'final_status': None})
            if previous_final_state.get('final_status') != '완결' and current_final_state['final_status'] == '완결':
                final_completed_at = current_final_state.get('final_completed_at')
                display_completed_at = final_completed_at.isoformat() if hasattr(final_completed_at, 'isoformat') else final_completed_at

                newly_completed_items.append(
                    (content_id, self.source_name, display_completed_at, current_final_state.get('resolved_by'))
                )

                if record_content_completed_event(
                    conn,
                    content_id=content_id,
                    source=self.source_name,
                    final_completed_at=final_completed_at,
                    resolved_by=current_final_state.get('resolved_by'),
                ):
                    cdc_events_inserted_count += 1
                    cdc_events_inserted_items.append(content_id)

        resolved_by_counts = {}
        for _, _, _, resolved_by in newly_completed_items:
            resolved_by_counts[resolved_by] = resolved_by_counts.get(resolved_by, 0) + 1

        cdc_info = {
            'cdc_mode': 'final_state',
            'newly_completed_count': len(newly_completed_items),
            'resolved_by_counts': resolved_by_counts,
            'cdc_events_inserted_count': cdc_events_inserted_count,
            'cdc_events_inserted_items': cdc_events_inserted_items,
        }

        # 8) DB sync
        added = self.synchronize_database(conn, all_content_today, ongoing_today, hiatus_today, finished_today)

        # IMPORTANT: keep return signature stable (3-tuple)
        return added, newly_completed_items, cdc_info
