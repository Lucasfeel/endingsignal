from datetime import datetime

import pytest

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeConnection:
    def __init__(self):
        self.commit_count = 0
        self.rollback_count = 0

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


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


def test_admin_delete_retains_subscriptions_and_logs_payload(monkeypatch, client, auth_headers):
    now = datetime(2024, 7, 1, 12, 0, 0)
    fake_conn = FakeConnection()
    payloads = []
    result = {
        "content": {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "webtoon",
            "title": "Title",
            "status": "active",
            "is_deleted": True,
            "meta": {},
            "deleted_at": now,
            "deleted_reason": "spam",
            "deleted_by": 1,
        },
        "subscriptions_retained": True,
    }

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "soft_delete_content", lambda *args, **kwargs: result)

    def fake_insert_admin_action_log(conn, *, payload, **kwargs):
        payloads.append(payload)

    monkeypatch.setattr(admin_view, "insert_admin_action_log", fake_insert_admin_action_log)

    response = client.post(
        "/api/admin/contents/delete",
        json={"content_id": "CID", "source": "SRC", "reason": "spam"},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data["success"] is True
    assert data["content"]["content_id"] == "CID"
    assert data["subscriptions_retained"] is True
    assert "subscriptions_deleted" not in data
    assert payloads == [{"subscriptions_retained": True}]
    assert fake_conn.commit_count == 1
