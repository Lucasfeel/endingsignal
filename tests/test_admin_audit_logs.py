from datetime import datetime

import pytest

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.committed = False

    def commit(self):
        self.committed = True


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


def test_override_delete_writes_audit_log(monkeypatch, client, auth_headers):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor()
    logged = {}

    def fake_insert_admin_action_log(conn, **kwargs):
        logged.update(kwargs)

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "insert_admin_action_log", fake_insert_admin_action_log)

    response = client.delete(
        "/api/admin/contents/override",
        json={"content_id": "CID", "source": "SRC", "reason": "cleanup"},
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert logged["action_type"] == "OVERRIDE_DELETE"
    assert logged["content_id"] == "CID"
    assert logged["source"] == "SRC"
    assert logged["reason"] == "cleanup"


def test_list_audit_logs_includes_admin_email_and_title(monkeypatch, client, auth_headers):
    now = datetime(2024, 6, 1, 10, 0, 0)
    fake_cursor = FakeCursor(
        rows=[
            {
                "id": 1,
                "created_at": now,
                "action_type": "CONTENT_DELETE",
                "reason": "spam",
                "admin_id": 1,
                "admin_email": "admin@example.com",
                "content_id": "CID",
                "source": "SRC",
                "payload": {"subscriptions_deleted": 3},
                "title": "Title",
                "content_type": "webtoon",
                "status": "연재중",
                "meta": {"common": {"thumbnail_url": "https://example.com/thumb.png"}},
                "is_deleted": True,
            }
        ]
    )
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get("/api/admin/audit/logs", headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    log = payload["logs"][0]
    assert log["admin_email"] == "admin@example.com"
    assert log["title"] == "Title"
