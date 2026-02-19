from datetime import datetime, timedelta

import pytest

from app import app as flask_app
import views.contents as contents_view


class FakeCursor:
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


def _make_row(content_id, source, content_type, updated_rank, *, status="연재중"):
    updated_at = datetime(2026, 1, 1, 0, 0, 0) + timedelta(minutes=updated_rank)
    return {
        "content_id": content_id,
        "title": f"title-{content_id}",
        "status": status,
        "meta": {"common": {"authors": ["a"]}},
        "source": source,
        "content_type": content_type,
        "updated_at": updated_at,
    }


def _stub_contents_db(monkeypatch, rows_by_type):
    fake_cursor = FakeCursor(rows_by_type)
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    return fake_cursor


def test_recommendations_returns_list_with_required_fields(monkeypatch, client):
    fake_cursor = _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [_make_row("w-1", "naver_webtoon", "webtoon", 3)],
            "novel": [_make_row("n-1", "ridi", "novel", 2)],
            "ott": [_make_row("o-1", "netflix", "ott", 1)],
        },
    )

    response = client.get("/api/contents/recommendations")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 3
    required_keys = {"content_id", "title", "status", "meta", "source", "content_type"}
    assert required_keys.issubset(payload[0].keys())
    assert all(isinstance(item.get("meta"), dict) for item in payload)
    assert fake_cursor.closed is True


def test_recommendations_limit_clamps_low(monkeypatch, client):
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [_make_row("w-1", "naver_webtoon", "webtoon", 30)],
            "novel": [_make_row("n-1", "ridi", "novel", 20)],
            "ott": [_make_row("o-1", "netflix", "ott", 10)],
        },
    )

    response = client.get("/api/contents/recommendations?limit=0")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 1


def test_recommendations_limit_clamps_high(monkeypatch, client):
    webtoon_rows = [_make_row(f"w-{i}", "naver_webtoon", "webtoon", 300 - i) for i in range(60)]
    novel_rows = [_make_row(f"n-{i}", "ridi", "novel", 200 - i) for i in range(60)]
    ott_rows = [_make_row(f"o-{i}", "netflix", "ott", 100 - i) for i in range(60)]
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": webtoon_rows,
            "novel": novel_rows,
            "ott": ott_rows,
        },
    )

    response = client.get("/api/contents/recommendations?limit=999")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 50


def test_recommendations_dedupes_by_content_and_source(monkeypatch, client):
    duplicate_old = _make_row("dup-1", "shared", "webtoon", 10)
    duplicate_new = _make_row("dup-1", "shared", "novel", 50)
    unique_row = _make_row("unique-1", "netflix", "ott", 40)

    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [duplicate_old],
            "novel": [duplicate_new],
            "ott": [unique_row],
        },
    )

    response = client.get("/api/contents/recommendations?limit=12")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 2
    assert len({(item["content_id"], item["source"]) for item in payload}) == 2
    duplicate_item = next(item for item in payload if item["content_id"] == "dup-1")
    assert duplicate_item["content_type"] == "novel"
