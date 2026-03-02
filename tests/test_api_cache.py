from datetime import datetime, timedelta

import pytest

from app import app as flask_app
import views.contents as contents_view


class RecordingCursor:
    def __init__(self, rows_by_type):
        self.rows_by_type = rows_by_type
        self.executed = []
        self.closed = False
        self._last_type = None
        self._last_limit = None

    def execute(self, query, params=None):
        self.executed.append((query, params))
        self._last_type = params[0] if params else None
        self._last_limit = params[3] if params and len(params) > 3 else None

    def fetchall(self):
        rows = list(self.rows_by_type.get(self._last_type, []))
        if isinstance(self._last_limit, int):
            return rows[: self._last_limit]
        return rows

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _make_row(content_id, source, content_type, updated_rank):
    updated_at = datetime(2026, 1, 1, 0, 0, 0) + timedelta(minutes=updated_rank)
    return {
        "content_id": content_id,
        "title": f"title-{content_id}",
        "status": contents_view.STATUS_ONGOING,
        "meta": {"common": {"authors": ["a"]}},
        "source": source,
        "content_type": content_type,
        "updated_at": updated_at,
    }


def test_recommendations_cache_hits_on_second_request(monkeypatch, client):
    rows_by_type = {
        "webtoon": [_make_row("w-1", "naver_webtoon", "webtoon", 3)],
        "novel": [_make_row("n-1", "ridi", "novel", 2)],
        "ott": [_make_row("o-1", "netflix", "ott", 1)],
    }
    fake_cursor = RecordingCursor(rows_by_type)

    monkeypatch.setenv("ES_API_CACHE_ENABLED", "1")
    monkeypatch.setenv("ES_API_CACHE_TTL_SECONDS", "60")
    monkeypatch.setenv("ES_META_MODE", "full")
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    monkeypatch.setattr(contents_view, "_API_CACHE", None)
    monkeypatch.setattr(contents_view, "_API_CACHE_MAX_ENTRIES", None)
    contents_view._get_api_cache().clear()

    first = client.get("/api/contents/recommendations?limit=12")
    second = client.get("/api/contents/recommendations?limit=12")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.headers.get("X-Cache") == "MISS"
    assert second.headers.get("X-Cache") == "HIT"
    assert len(fake_cursor.executed) == 3
    assert first.get_json() == second.get_json()
