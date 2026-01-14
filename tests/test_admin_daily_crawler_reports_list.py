import pytest
from datetime import datetime

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


def test_admin_daily_reports_list_basic_serialization(monkeypatch, client, auth_headers):
    created_at = datetime(2025, 1, 2, 12, 0, 0)
    rows = [
        {
            "id": 1,
            "crawler_name": "scheduled completion cdc",
            "status": "성공",
            "report_data": {"status": "성공"},
            "created_at": created_at,
        }
    ]
    fake_cursor = FakeCursor(rows)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get("/api/admin/reports/daily-crawler", headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    report = payload["reports"][0]
    assert report["created_at"] == created_at.isoformat()
    assert report["report_data"] == {"status": "성공"}
    assert report["normalized_status"] == "success"


def test_admin_daily_reports_list_with_filters_does_not_error(monkeypatch, client, auth_headers):
    rows = []
    fake_cursor = FakeCursor(rows)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/reports/daily-crawler",
        query_string={
            "limit": "10",
            "offset": "5",
            "crawler_name": "scheduled completion cdc",
            "status": "success",
            "created_from": "2025-01-01T00:00:00",
            "created_to": "2025-01-02T00:00:00",
        },
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert fake_cursor.executed
    query, params = fake_cursor.executed[0]
    assert "ANY" in query
    assert isinstance(params, tuple)
    assert isinstance(params[1], list)
