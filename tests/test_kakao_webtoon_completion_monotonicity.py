import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import crawlers.kakao_webtoon_crawler as crawler_module


class FakeCursor:
    def __init__(self, existing_rows):
        self._existing_rows = existing_rows
        self._last_query = ""
        self.executemany_calls = []
        self.closed = False

    def execute(self, query, params=None):
        self._last_query = query

    def fetchall(self):
        if "SELECT content_id, status FROM contents" in self._last_query:
            return self._existing_rows
        return []

    def executemany(self, query, rows):
        self.executemany_calls.append((query, list(rows)))

    def close(self):
        self.closed = True


def _make_webtoon_data(content_id, *, completed_candidate=False):
    return {
        "title": f"title-{content_id}",
        "authors": ["author"],
        "thumbnail_url": "https://example.com/thumb.webp",
        "content_url": f"https://webtoon.kakao.com/content/test/{content_id}",
        "kakao_completed_candidate": completed_candidate,
    }


def _updated_statuses(fake_cursor):
    for query, rows in fake_cursor.executemany_calls:
        if query.startswith("UPDATE contents SET"):
            return [row[4] for row in rows]
    return []


def test_existing_ongoing_is_promoted_to_completed_when_completed_placement_seen(monkeypatch):
    fake_cursor = FakeCursor([{"content_id": "1001", "status": "연재중"}])
    monkeypatch.setattr(crawler_module, "get_cursor", lambda conn: fake_cursor)
    crawler = crawler_module.KakaoWebtoonCrawler()

    webtoon_data = _make_webtoon_data("1001", completed_candidate=True)
    all_content_today = {"1001": webtoon_data}
    ongoing_today = {}
    hiatus_today = {}
    finished_today = {"1001": webtoon_data}

    crawler.synchronize_database(
        conn=object(),
        all_content_today=all_content_today,
        ongoing_today=ongoing_today,
        hiatus_today=hiatus_today,
        finished_today=finished_today,
    )

    assert _updated_statuses(fake_cursor) == ["완결"]


def test_existing_completed_is_not_downgraded_by_non_completed_placement(monkeypatch):
    fake_cursor = FakeCursor([{"content_id": "2002", "status": "완결"}])
    monkeypatch.setattr(crawler_module, "get_cursor", lambda conn: fake_cursor)
    crawler = crawler_module.KakaoWebtoonCrawler()

    webtoon_data = _make_webtoon_data("2002", completed_candidate=False)
    all_content_today = {"2002": webtoon_data}
    ongoing_today = {"2002": webtoon_data}
    hiatus_today = {}
    finished_today = {}

    crawler.synchronize_database(
        conn=object(),
        all_content_today=all_content_today,
        ongoing_today=ongoing_today,
        hiatus_today=hiatus_today,
        finished_today=finished_today,
    )

    assert _updated_statuses(fake_cursor) == ["완결"]
