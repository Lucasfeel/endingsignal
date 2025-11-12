# crawlers/kakaopage_crawler.py
# ... (파일 상단은 이전과 동일) ...
import asyncio
import aiohttp
import json
import config
from tenacity import retry, stop_after_attempt, wait_exponential
from .base_crawler import ContentCrawler
from database import get_cursor

GRAPHQL_QUERY_ONGOING = """
query staticLandingDayOfWeekLayout($queryInput: StaticLandingDayOfWeekParamInput!) {
  staticLandingDayOfWeekLayout(input: $queryInput) {
    ...Layout
  }
}
fragment Layout on Layout {
  id, type, sections { ...Section }, screenUid
}
fragment Section on Section {
  id, uid, type, title
  ... on StaticLandingDayOfWeekSection {
    isEnd, totalCount
    items: groups {
      items {
        id, title, thumbnail, badgeList, statusBadge, ageGrade, seriesId
        authors { name, type }
      }
    }
  }
}
"""

GRAPHQL_QUERY_FINISHED = """
query staticLandingGenreSection($sectionId: ID!, $param: StaticLandingGenreParamInput!) {
  staticLandingGenreSection(sectionId: $sectionId, param: $param) {
    ... on StaticLandingGenreSection {
      isEnd, totalCount
      items: groups {
        items {
          id, title, thumbnail, badgeList, statusBadge, ageGrade, seriesId
          authors { name, type }
        }
      }
    }
  }
}
"""

DAY_TAB_UIDS = {
    'mon': '1', 'tue': '2', 'wed': '3', 'thu': '4', 'fri': '5', 'sat': '6', 'sun': '7',
    'hiatus': '8' # 👈 '휴재' 탭에서 찾은 UID 값으로 '8'을 대체하세요.
}

class KakaopageCrawler(ContentCrawler):
    def __init__(self):
        super().__init__('kakaopage')
        self.GRAPHQL_URL = 'https://page.kakao.com/graphql'
        self.HEADERS = {
            'User-Agent': config.CRAWLER_HEADERS['User-Agent'],
            'Content-Type': 'application/json',
            'Accept': 'application/graphql+json, application/json',
            'Referer': 'https://page.kakao.com/',
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_page_data(self, session, page, size=100, day_tab_uid=None, is_complete=False):
        if is_complete:
            query = GRAPHQL_QUERY_FINISHED
            variables = {"sectionId": "static-landing-Genre-section-Layout-10-0-view", "param": { "categoryUid": 10, "page": page, "size": size, "sortType": "view", "isComplete": True }}
        else:
            query = GRAPHQL_QUERY_ONGOING
            variables = {"queryInput": { "categoryUid": 10, "dayTabUid": day_tab_uid, "type": "Layout", "screenUid": 52, "page": page, "size": size }}

        payload = {"query": query, "variables": variables}
        try:
            async with session.post(self.GRAPHQL_URL, headers=self.HEADERS, json=payload, timeout=30) as response:
                response.raise_for_status()

                # 👈 2. aiohttp 내장 json 파서 사용
                # content_type=None : 'application/graphql+json'을 json으로 인식시킴
                data = await response.json(content_type=None)

                data_root = data.get('data', {})
                if is_complete:
                    # 완결 탭: data.staticLandingGenreSection.items[0].items
                    section = data_root.get('staticLandingGenreSection', {})
                    groups = section.get('items', [])
                else:
                    # 요일별 탭: data.staticLandingDayOfWeekLayout.sections[0].items[0].items
                    layout = data_root.get('staticLandingDayOfWeekLayout', {})
                    sections = layout.get('sections', [])
                    groups = sections[0].get('items', []) if sections else [] # 👈 1. sections[0] 안전하게 접근

                # 👈 2. groups[0] 안전하게 접근
                items = groups[0].get('items', []) if groups else []
                return items
        except Exception as e:
            print(f"[{self.source_name}] Page {page} (day: {day_tab_uid}, complete: {is_complete}) 로드 실패: {e}")
            raise

    async def _fetch_ongoing_category(self, session, day_key, day_tab_uid, data_maps):
        print(f"[{self.source_name}] '{day_key}' (TabUID:{day_tab_uid}) 목록 수집 시작...")
        page = 1
        while True:
            try:
                items = await self._fetch_page_data(session, page=page, day_tab_uid=day_tab_uid)
                if not items: break
                for item in items:
                    content_id = str(item.get('seriesId'))
                    if content_id not in data_maps['all_content_today']:
                        data_maps['all_content_today'][content_id] = item
                        item['normalized_weekdays'] = set()
                    data_maps['all_content_today'][content_id]['normalized_weekdays'].add(day_key)
                    if '휴재' in (item.get('statusBadge') or ''): data_maps['hiatus_today'][content_id] = item
                    else: data_maps['ongoing_today'][content_id] = item
                page += 1
                await asyncio.sleep(0.1)
            except Exception:
                print(f"[{self.source_name}] '{day_key}' 페이지 {page}에서 최종 실패.")
                break
        print(f"[{self.source_name}] '{day_key}' 목록 수집 완료.")

    async def fetch_all_data(self):
        print(f"[{self.source_name}] 서버에서 최신 데이터를 가져옵니다...")
        data_maps = {'all_content_today': {}, 'ongoing_today': {}, 'hiatus_today': {}, 'finished_today': {}}
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[self._fetch_ongoing_category(session, day, uid, data_maps) for day, uid in DAY_TAB_UIDS.items()])
            print(f"[{self.source_name}] '완결' 목록 수집 시작...")
            total_finished_found = 0 # 👈 1. 수집한 완결작 총 개수
            for page in range(1, 250): # 최대 25000개 (100 * 250)
                try:
                    items = await self._fetch_page_data(session, page=page, size=100, is_complete=True)
                    if not items:
                        print(f"[{self.source_name}] '완결' 목록 {page-1} 페이지에서 수집 종료.")
                        break # 👈 2. 데이터 없으면 루프 탈출

                    for item in items:
                        content_id = str(item.get('seriesId'))
                        total_finished_found += 1 # 👈 3. DB 저장 여부와 관계없이 무조건 카운트

                        # (1) 전체 목록에 추가 (신규인 경우)
                        if content_id not in data_maps['all_content_today']:
                            data_maps['all_content_today'][content_id] = item

                        # (2) 'finished_today' 맵에 무조건 추가 (가장 중요)
                        # 👈 4. '연재중'이었다가 완결된 작품도 여기에 포함되어야 함
                        data_maps['finished_today'][content_id] = item

                    print(f"[{self.source_name}] '완결' {page} 페이지 수집 완료 (누적 {total_finished_found}개)")

                    if total_finished_found >= 2000: # 👈 5. 총 수집 개수로 2000개 돌파 확인
                        print(f"[{self.source_name}] 목표 수량({total_finished_found}개) 달성. 수집 종료.")
                        break
                except Exception:
                    print(f"[{self.source_name}] '완결' 페이지 {page}에서 최종 실패.")
                    break
            for content in data_maps['all_content_today'].values():
                if 'normalized_weekdays' in content: content['normalized_weekdays'] = list(content['normalized_weekdays'])
        print(f"[{self.source_name}] 데이터 수집 완료: 총 {len(data_maps['all_content_today'])}개 고유 콘텐츠 확인")
        return (data_maps['ongoing_today'], data_maps['hiatus_today'], data_maps['finished_today'], data_maps['all_content_today'])

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        print(f"\n[{self.source_name}] DB 동기화를 시작합니다...")
        cursor = get_cursor(conn)
        # [BUG FIX] 업데이트 로직 강화를 위해 status와 meta도 함께 조회
        cursor.execute("SELECT content_id, title, status, meta FROM contents WHERE source = %s", (self.source_name,))
        db_data = {row['content_id']: {'title': row['title'], 'status': row['status'], 'meta': row['meta']} for row in cursor.fetchall()}

        updates, inserts, unique_inserts_count = [], [], 0

        for cid, cdata in all_content_today.items():
            status = '완결' if cid in finished_today else '휴재' if cid in hiatus_today else '연재중'
            title = cdata.get('title', '제목 없음')

            # [수정] naver_webtoon_crawler.py와 동일한 표준 meta 구조로 변경
            meta = {
                "common": {
                    "authors": [a.get('name') for a in cdata.get('authors', []) if a.get('name')],
                    "thumbnail_url": cdata.get('thumbnail')
                },
                "attributes": {
                    "weekdays": cdata.get('normalized_weekdays', [])
                }
            }

            if cid in db_data:
                # [BUG FIX] 제목, 상태, 메타데이터 중 하나라도 변경되면 업데이트
                db_item = db_data[cid]
                if db_item['title'] != title or db_item['status'] != status or db_item['meta'] != meta:
                    updates.append(('webtoon', title, status, json.dumps(meta), cid, self.source_name))
            else:
                inserts.append((cid, self.source_name, 'webtoon', title, status, json.dumps(meta)))

        if updates:
            cursor.executemany("UPDATE contents SET content_type=%s, title=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s", updates)
            print(f"[{self.source_name}] {len(updates)}개 콘텐츠 정보 업데이트 완료.")
        if inserts:
            seen, u_inserts = set(), []
            for i in inserts:
                k = (i[0], i[1])
                if k not in seen: u_inserts.append(i); seen.add(k)
            cursor.executemany("INSERT INTO contents (content_id, source, content_type, title, status, meta) VALUES (%s, %s, %s, %s, %s, %s)", u_inserts)
            unique_inserts_count = len(u_inserts)
            print(f"[{self.source_name}] {unique_inserts_count}개 신규 콘텐츠 DB 추가 완료.")
        conn.commit(); cursor.close(); print(f"[{self.source_name}] DB 동기화 완료.")
        return unique_inserts_count

    async def run_daily_check(self, conn):
        from services.notification_service import send_completion_notifications
        print(f"LOG: [{self.source_name}] 일일 점검 시작...")
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
        db_state = {row['content_id']: row['status'] for row in cursor.fetchall()}
        cursor.close()
        ongoing, hiatus, finished, all_content = await self.fetch_all_data()
        newly_completed = {cid for cid, s in db_state.items() if s != '완결' and cid in finished}
        print(f"LOG: [{self.source_name}] {len(newly_completed)}개 신규 완결 콘텐츠 발견.")
        details, notified = [], 0
        if newly_completed:
            for cid in newly_completed:
                if cid in all_content and 'title' in all_content[cid]:
                    all_content[cid]['titleName'] = all_content[cid]['title']
            try:
                details, notified = send_completion_notifications(get_cursor(conn), newly_completed, all_content, self.source_name)
            except ValueError as e:
                print(f"경고: [{self.source_name}] 알림 발송 불가: {e}")
        added = self.synchronize_database(conn, all_content, ongoing, hiatus, finished)
        print(f"LOG: [{self.source_name}] 일일 점검 완료.")
        return added, details, notified

if __name__ == '__main__':
    print("KakaopageCrawler 구현 파일입니다. 직접 실행 시 별도 동작은 없습니다.")
