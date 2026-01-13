from datetime import datetime

import crawlers.kakaopage_webtoon_crawler as crawler_module


class RowNoGet:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def __contains__(self, key):
        return key in self._data


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.closed = False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


def test_load_recent_bootstrap_reports_handles_row_without_get(monkeypatch):
    created_at = datetime(2024, 3, 1, 9, 0, 0)
    rows = [
        RowNoGet(
            {
                "report_data": {
                    "cdc_info": {"fetch_meta": {"bootstrap_attempted": True, "bootstrap_success": True}}
                },
                "created_at": created_at,
            }
        ),
        RowNoGet({"report_data": None, "created_at": None}),
    ]
    fake_cursor = FakeCursor(rows)

    def fake_get_cursor(_conn):
        return fake_cursor

    monkeypatch.setattr(crawler_module, "get_cursor", fake_get_cursor)

    crawler = crawler_module.KakaoPageWebtoonCrawler()

    attempts = crawler._load_recent_bootstrap_reports(conn=object(), limit=2)

    assert attempts == [
        (created_at, True, True),
        (None, False, None),
    ]
    assert fake_cursor.closed is True
