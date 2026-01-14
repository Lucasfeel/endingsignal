import pytest
from datetime import datetime

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.committed = False

    def commit(self):
        self.committed = True

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


def test_admin_daily_reports_cleanup_defaults(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(rowcount=3)
    fake_conn = FakeConnection()
    fixed_now = datetime(2025, 1, 20, 12, 0, 0)

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: fixed_now)

    response = client.post(
        "/api/admin/reports/daily-crawler/cleanup",
        headers=auth_headers,
        json={},
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["deleted_count"] == 3
    assert payload["keep_days"] == 14
    assert payload["cutoff"] == "2025-01-06T12:00:00"
    assert fake_conn.committed is True
    assert fake_cursor.executed
