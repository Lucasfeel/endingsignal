from datetime import datetime

import pytest

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = None
        self.params = None

    def execute(self, query, params=None):
        self.executed = query
        self.params = params

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    pass


@pytest.fixture(autouse=True)
def stub_decode_token(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {"uid": 1, "email": "admin@example.com", "role": "admin"},
    )


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer testtoken"}


def test_missing_completion_returns_items_and_filters(monkeypatch, client, auth_headers):
    now = datetime(2024, 6, 1, 10, 0, 0)
    fake_cursor = FakeCursor(
        rows=[
            {
                "content_id": "CID",
                "source": "naver_webtoon",
                "title": "Title",
                "content_type": "webtoon",
                "status": "완결",
                "meta": {"common": {"thumbnail_url": "https://example.com/thumb.png"}},
                "created_at": now,
                "updated_at": now,
                "override_status": "완결",
                "override_completed_at": None,
            }
        ]
    )
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/contents/missing-completion?limit=10&offset=0&source=naver_webtoon&content_type=webtoon&q=Title",
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["items"][0]["content_id"] == "CID"
    assert payload["items"][0]["override_status"] == "완결"
    assert fake_cursor.params == ("naver_webtoon", "webtoon", "%Title%", 10, 0)


def test_missing_publication_returns_items(monkeypatch, client, auth_headers):
    now = datetime(2024, 6, 1, 10, 0, 0)
    fake_cursor = FakeCursor(
        rows=[
            {
                "content_id": "CID2",
                "source": "kakao_webtoon",
                "title": "Another",
                "content_type": "webtoon",
                "status": "연재중",
                "meta": {},
                "created_at": now,
                "updated_at": now,
            }
        ]
    )
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/contents/missing-publication?limit=5&offset=0",
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["items"][0]["content_id"] == "CID2"
    assert fake_cursor.params == (5, 0)


def test_missing_completion_requires_admin(monkeypatch, client, auth_headers):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {"uid": 2, "email": "user@example.com", "role": "user"},
    )

    response = client.get(
        "/api/admin/contents/missing-completion",
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 403
    assert payload["error"]["code"] == "FORBIDDEN"
