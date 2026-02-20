import pytest

from app import app as flask_app
import views.contents as contents_view


class RecordingCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _stub_db(monkeypatch, rows):
    fake_cursor = RecordingCursor(rows)
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    return fake_cursor


def _row(content_id, *, status=contents_view.STATUS_ONGOING, meta=None, source="ridi"):
    return {
        "content_id": content_id,
        "title": f"title-{content_id}",
        "status": status,
        "meta": meta or {},
        "source": source,
    }


def test_genre_mapping_fantasy_includes_modern_fantasy_aliases():
    fantasy = contents_view.GENRE_GROUP_MAPPING["FANTASY"]
    normalized = {contents_view._normalize_genre_token(token) for token in fantasy}

    assert contents_view._normalize_genre_token("\uD604\uD310") in normalized
    assert contents_view._normalize_genre_token("modern fantasy") in normalized


def test_novel_endpoint_applies_completed_filter_only_when_true(monkeypatch, client):
    cursor_true = _stub_db(monkeypatch, [])
    response_true = client.get("/api/contents/novels?is_completed=true")
    assert response_true.status_code == 200
    executed_query_true, params_true = cursor_true.executed[0]
    assert "status = %s" in executed_query_true
    assert params_true[-1] == contents_view.STATUS_COMPLETED

    cursor_false = _stub_db(monkeypatch, [])
    response_false = client.get("/api/contents/novels?is_completed=false")
    assert response_false.status_code == 200
    executed_query_false, _ = cursor_false.executed[0]
    assert "status = %s" not in executed_query_false


def test_novel_endpoint_ignores_weekday_params(monkeypatch, client):
    cursor = _stub_db(monkeypatch, [])
    response = client.get("/api/contents/novels?weekday=mon&day=tue&genre_group=all")
    assert response.status_code == 200
    executed_query, _ = cursor.executed[0]
    assert "weekday" not in executed_query.lower()
    assert "weekdays" not in executed_query.lower()


def test_novel_genre_group_filters_fantasy_and_includes_hyeonpan(monkeypatch, client):
    rows = [
        _row(
            "fantasy-1",
            meta={"attributes": {"genres": ["\uD604\uD310"]}},
        ),
        _row(
            "romance-1",
            meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
        ),
    ]
    _stub_db(monkeypatch, rows)

    response = client.get("/api/contents/novels?genre_group=fantasy")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert "fantasy-1" in ids
    assert "romance-1" not in ids


def test_novel_genre_group_filters_light_novel(monkeypatch, client):
    rows = [
        _row(
            "light-1",
            meta={"attributes": {"genres": ["\uB77C\uB178\uBCA8"]}},
        ),
        _row(
            "wuxia-1",
            meta={"attributes": {"genres": ["\uBB34\uD611"]}},
        ),
    ]
    _stub_db(monkeypatch, rows)

    response = client.get("/api/contents/novels?genre_group=light_novel")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert "light-1" in ids
    assert "wuxia-1" not in ids


def test_novel_genre_group_does_not_hide_all_when_genre_metadata_missing(monkeypatch, client):
    rows = [
        _row("no-genre-1", meta={"attributes": {}}),
        _row("no-genre-2", meta={}),
    ]
    _stub_db(monkeypatch, rows)

    response = client.get("/api/contents/novels?genre_group=bl")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"no-genre-1", "no-genre-2"}
