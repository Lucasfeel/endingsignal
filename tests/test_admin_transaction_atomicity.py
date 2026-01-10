import pytest

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def close(self):
        pass


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


def test_override_delete_rolls_back_on_audit_failure(monkeypatch, client, auth_headers):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor()

    def fake_insert_admin_action_log(*args, **kwargs):
        raise RuntimeError("audit failed")

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "insert_admin_action_log", fake_insert_admin_action_log)

    with pytest.raises(RuntimeError):
        client.delete(
            "/api/admin/contents/override",
            json={"content_id": "CID", "source": "SRC", "reason": "cleanup"},
            headers=auth_headers,
        )

    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 1


def test_override_delete_commits_once_on_success(monkeypatch, client, auth_headers):
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
    assert fake_conn.commit_count == 1
    assert fake_conn.rollback_count == 0
