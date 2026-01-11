import pytest
from datetime import datetime, timedelta

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


class FakeConnection:
    def close(self):
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


def test_admin_cdc_events_list_basic_serialization(monkeypatch, client, auth_headers):
    created_at = datetime(2025, 1, 2, 12, 0, 0)
    final_completed_at = created_at - timedelta(days=1)
    rows = [
        {
            "id": 1,
            "created_at": created_at,
            "content_id": "CID",
            "source": "SRC",
            "event_type": "CONTENT_COMPLETED",
            "final_status": "완결",
            "final_completed_at": final_completed_at,
            "resolved_by": "override",
            "title": "Title",
            "content_type": "webtoon",
            "status": "연재중",
            "meta": {"k": "v"},
            "is_deleted": False,
        }
    ]
    fake_cursor = FakeCursor(rows)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get("/api/admin/cdc/events", headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    event = payload["events"][0]
    assert event["created_at"] == created_at.isoformat()
    assert event["final_completed_at"] == final_completed_at.isoformat()
    assert event["meta"] == {"k": "v"}
    assert event["is_deleted"] is False


def test_admin_cdc_events_list_with_filters_does_not_error(monkeypatch, client, auth_headers):
    rows = []
    fake_cursor = FakeCursor(rows)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/cdc/events",
        query_string={
            "limit": "10",
            "offset": "5",
            "q": "title",
            "event_type": "CONTENT_PUBLISHED",
            "source": "SRC",
            "content_id": "CID",
            "created_from": "2025-01-01T00:00:00",
            "created_to": "2025-01-02T00:00:00",
        },
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert fake_cursor.executed
