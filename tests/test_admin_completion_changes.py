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


def test_completion_changes_returns_items(monkeypatch, client, auth_headers):
    now = datetime(2024, 7, 1, 10, 30, 0)
    fake_cursor = FakeCursor(
        rows=[
            {
                "id": 11,
                "content_id": "CID",
                "source": "naver_webtoon",
                "override_status": "\uc644\uacb0",
                "override_completed_at": now,
                "reason": "manual_update",
                "admin_id": 7,
                "created_at": now,
                "updated_at": now,
                "title": "Sample Title",
                "content_type": "webtoon",
                "status": "\uc5f0\uc7ac\uc911",
                "meta": {"common": {"thumbnail_url": "https://example.com/thumb.png"}},
                "is_deleted": False,
            }
        ]
    )

    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/contents/completion-changes?limit=10&offset=5",
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["limit"] == 10
    assert payload["offset"] == 5
    assert len(payload["changes"]) == 1
    item = payload["changes"][0]
    assert item["content_id"] == "CID"
    assert item["override_status"] == "\uc644\uacb0"
    assert item["override_completed_at"] == now.isoformat()
    assert item["title"] == "Sample Title"
    assert isinstance(item["meta"], dict)
    assert fake_cursor.params == ("\uc644\uacb0", 10, 5)
    assert "FROM admin_content_overrides" in fake_cursor.executed


def test_completion_changes_validates_limit_offset(client, auth_headers):
    response = client.get(
        "/api/admin/contents/completion-changes?limit=abc&offset=0",
        headers=auth_headers,
    )
    payload = response.get_json()
    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_REQUEST"


def test_completion_changes_requires_admin(monkeypatch, client, auth_headers):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {"uid": 2, "email": "user@example.com", "role": "user"},
    )

    response = client.get(
        "/api/admin/contents/completion-changes",
        headers=auth_headers,
    )
    payload = response.get_json()
    assert response.status_code == 403
    assert payload["error"]["code"] == "FORBIDDEN"
