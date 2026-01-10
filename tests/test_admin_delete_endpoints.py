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


def test_delete_override_requires_reason(client, auth_headers):
    response = client.delete(
        "/api/admin/contents/override",
        json={"content_id": "CID", "source": "SRC"},
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_REQUEST"


def test_delete_publication_requires_reason(client, auth_headers):
    response = client.delete(
        "/api/admin/contents/publication",
        json={"content_id": "CID", "source": "SRC"},
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_REQUEST"


def test_delete_override_accepts_reason(monkeypatch, client, auth_headers):
    fake_conn = FakeConnection()
    fake_cursor = FakeCursor()
    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.delete(
        "/api/admin/contents/override",
        json={"content_id": "CID", "source": "SRC", "reason": "cleanup"},
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed


def test_delete_publication_accepts_reason_from_query(monkeypatch, client, auth_headers):
    fake_conn = FakeConnection()
    called = {}

    def fake_delete_publication(conn, *, content_id, source):
        called["content_id"] = content_id
        called["source"] = source

    monkeypatch.setattr(admin_view, "get_db", lambda: fake_conn)
    monkeypatch.setattr(admin_view, "delete_publication", fake_delete_publication)

    response = client.delete(
        "/api/admin/contents/publication?reason=cleanup",
        json={"content_id": "CID", "source": "SRC"},
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert called == {"content_id": "CID", "source": "SRC"}
