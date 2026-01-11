import pytest
from datetime import datetime, timedelta

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self, active=True):
        self.active = active
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return (1,) if self.active else None

    def close(self):
        self.closed = True


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


@pytest.fixture
def now_time():
    return datetime(2025, 1, 2, 12, 0, 0)


def _stub_upsert(public_at):
    return {
        "publication": {
            "id": 1,
            "content_id": "CID",
            "source": "SRC",
            "public_at": public_at,
            "reason": None,
            "admin_id": 1,
            "created_at": public_at,
            "updated_at": public_at,
            "title": "Title",
            "content_type": "webtoon",
            "status": "연재중",
            "meta": {},
            "is_deleted": False,
        }
    }


def test_publication_due_records_event(monkeypatch, client, auth_headers, now_time):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor(active=True)
    payloads = []

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: now_time)
    monkeypatch.setattr(admin_view, "upsert_publication", lambda *args, **kwargs: _stub_upsert(now_time))
    monkeypatch.setattr(admin_view, "record_content_published_event", lambda *args, **kwargs: True)

    def fake_insert_admin_action_log(conn, *, payload, **kwargs):
        payloads.append(payload)

    monkeypatch.setattr(admin_view, "insert_admin_action_log", fake_insert_admin_action_log)

    response = client.post(
        "/api/admin/contents/publication",
        json={"content_id": "CID", "source": "SRC", "public_at": now_time.isoformat()},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data["event_due_now"] is True
    assert data["event_recorded"] is True
    assert data["event_inserted"] is True
    assert data["event_skipped_reason"] is None
    assert fake_conn.commit_count == 1
    assert fake_conn.rollback_count == 0
    assert payloads and payloads[0]["event_recorded"] is True


def test_publication_due_existing_event(monkeypatch, client, auth_headers, now_time):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor(active=True)

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: now_time)
    monkeypatch.setattr(admin_view, "upsert_publication", lambda *args, **kwargs: _stub_upsert(now_time))
    monkeypatch.setattr(admin_view, "record_content_published_event", lambda *args, **kwargs: False)
    monkeypatch.setattr(admin_view, "insert_admin_action_log", lambda *args, **kwargs: None)

    response = client.post(
        "/api/admin/contents/publication",
        json={"content_id": "CID", "source": "SRC", "public_at": now_time.isoformat()},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data["event_due_now"] is True
    assert data["event_recorded"] is True
    assert data["event_inserted"] is False


def test_publication_future_no_event(monkeypatch, client, auth_headers, now_time):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor(active=True)
    called = {"recorded": False}
    future_time = now_time + timedelta(days=1)

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: now_time)
    monkeypatch.setattr(admin_view, "upsert_publication", lambda *args, **kwargs: _stub_upsert(future_time))

    def fake_record(*args, **kwargs):
        called["recorded"] = True
        return True

    monkeypatch.setattr(admin_view, "record_content_published_event", fake_record)
    monkeypatch.setattr(admin_view, "insert_admin_action_log", lambda *args, **kwargs: None)

    response = client.post(
        "/api/admin/contents/publication",
        json={"content_id": "CID", "source": "SRC", "public_at": future_time.isoformat()},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data["event_due_now"] is False
    assert data["event_recorded"] is False
    assert data["event_inserted"] is False
    assert called["recorded"] is False


def test_publication_due_deleted_skips_event(monkeypatch, client, auth_headers, now_time):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor(active=False)

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: now_time)
    monkeypatch.setattr(admin_view, "upsert_publication", lambda *args, **kwargs: _stub_upsert(now_time))
    monkeypatch.setattr(admin_view, "record_content_published_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(admin_view, "insert_admin_action_log", lambda *args, **kwargs: None)

    response = client.post(
        "/api/admin/contents/publication",
        json={"content_id": "CID", "source": "SRC", "public_at": now_time.isoformat()},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data["event_due_now"] is True
    assert data["event_recorded"] is False
    assert data["event_inserted"] is False
    assert data["event_skipped_reason"] == "CONTENT_DELETED"
