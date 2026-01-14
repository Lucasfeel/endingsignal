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


def test_admin_daily_summary_status_and_subject(monkeypatch, client, auth_headers):
    created_at = datetime(2025, 1, 2, 10, 0, 0)
    rows = [
        {
            "id": 1,
            "crawler_name": "crawler_a",
            "status": "ok",
            "report_data": {"duration": 1.2},
            "created_at": created_at,
        },
        {
            "id": 2,
            "crawler_name": "crawler_b",
            "status": "warn",
            "report_data": {"error_message": "timeout"},
            "created_at": created_at,
        },
    ]
    fake_cursor = FakeCursor(rows)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/reports/daily-summary",
        query_string={
            "created_from": "2025-01-02T00:00:00",
            "created_to": "2025-01-02T23:59:59",
        },
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["overall_status"] == "warning"
    assert payload["counts"]["success"] == 1
    assert payload["counts"]["warning"] == 1
    assert "2025-01-02" in payload["subject_text"]
    assert "crawler_a" in payload["summary_text"]
    assert payload["items"][0]["normalized_status"] in {"success", "warning"}
