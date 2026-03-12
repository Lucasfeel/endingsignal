import pytest

from app import app as flask_app
import views.contents as contents_view


class FlexibleCursor:
    def __init__(self, *, fetchall_rows=None, fetchone_row=None):
        self.fetchall_rows = list(fetchall_rows or [])
        self.fetchone_row = fetchone_row
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.fetchall_rows)

    def fetchone(self):
        return self.fetchone_row

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _ott_row():
    return {
        "content_id": "ott_series:2026:test-title:abc123def456",
        "title": "테스트 타이틀",
        "status": contents_view.STATUS_ONGOING,
        "source": "ott_canonical",
        "content_type": "ott",
        "meta": {
            "common": {
                "authors": ["배우 A", "배우 B"],
                "primary_source": "tving",
            },
            "ott": {
                "cast": ["배우 A", "배우 B"],
                "release_end_status": "unknown",
                "needs_end_date_verification": True,
                "platforms": [
                    {
                        "source": "tving",
                        "content_url": "https://www.tving.com/contents/P001",
                        "thumbnail_url": "https://img.example/tving.jpg",
                    },
                    {
                        "source": "wavve",
                        "content_url": "https://www.wavve.com/player/contents/12345",
                        "thumbnail_url": "https://img.example/wavve.jpg",
                    },
                ],
            },
        },
    }


def test_browse_v3_ott_source_filter_uses_platform_links(monkeypatch, client):
    fake_cursor = FlexibleCursor(fetchall_rows=[_ott_row()])
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = client.get("/api/contents/browse_v3?type=ott&source=tving&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[0]
    assert "content_platform_links" in query
    assert params.count(contents_view.OTT_CANONICAL_SOURCE) >= 1
    assert params.count("tving") >= 1
    item = payload["contents"][0]
    assert item["source"] == "tving"
    assert item["content_url"] == "https://www.tving.com/contents/P001"
    cursor_title, cursor_source, cursor_content_id = contents_view.decode_cursor(item["cursor"])
    assert cursor_title == "테스트 타이틀"
    assert cursor_source == contents_view.OTT_CANONICAL_SOURCE
    assert cursor_content_id == _ott_row()["content_id"]


def test_search_ott_platform_source_uses_canonical_query(monkeypatch, client):
    fake_cursor = FlexibleCursor(fetchall_rows=[_ott_row()])
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = client.get("/api/contents/search?q=테스트&source=tving")
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[-1]
    assert "content_platform_links" in query
    assert params.count(contents_view.OTT_CANONICAL_SOURCE) >= 1
    assert params.count("tving") >= 1
    assert payload[0]["source"] == "tving"
    assert payload[0]["meta"]["common"]["content_url"] == "https://www.tving.com/contents/P001"


def test_detail_ott_platform_source_resolves_canonical_row(monkeypatch, client):
    fake_cursor = FlexibleCursor(fetchone_row=_ott_row())
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = client.get(
        "/api/contents/detail?content_id=ott_series:2026:test-title:abc123def456&source=wavve"
    )
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[0]
    assert "content_platform_links" in query
    assert params == (
        "ott_series:2026:test-title:abc123def456",
        contents_view.OTT_CANONICAL_SOURCE,
        "wavve",
    )
    assert payload["source"] == "wavve"
    assert payload["meta"]["common"]["content_url"] == "https://www.wavve.com/player/contents/12345"
