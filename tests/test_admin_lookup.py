from datetime import datetime

import pytest

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view


class FakeCursor:
    def __init__(self, content_row=None, override_row=None, publication_row=None):
        self.content_row = content_row
        self.override_row = override_row
        self.publication_row = publication_row
        self.last_result = None

    def execute(self, query, params):
        if "FROM contents" in query:
            self.last_result = [self.content_row] if self.content_row else []
        elif "FROM admin_content_overrides" in query:
            self.last_result = [self.override_row] if self.override_row else []
        elif "FROM admin_content_metadata" in query:
            self.last_result = [self.publication_row] if self.publication_row else []
        else:
            raise NotImplementedError(query)

    def fetchone(self):
        if not self.last_result:
            return None
        return self.last_result[0]

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self.cursor = cursor


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


def test_admin_publications_includes_title(monkeypatch, client, auth_headers):
    now = datetime(2024, 6, 1, 10, 0, 0)
    monkeypatch.setattr(admin_view, "get_db", lambda: object())
    monkeypatch.setattr(
        admin_view,
        "list_publications",
        lambda conn, limit, offset: [
            {
                "id": 1,
                "content_id": "CID",
                "source": "SRC",
                "public_at": now,
                "reason": "manual",
                "admin_id": 5,
                "created_at": now,
                "updated_at": now,
                "title": "Sample Title",
                "content_type": "webtoon",
                "status": "연재중",
                "meta": {"common": {"thumbnail_url": "https://example.com/thumb.png"}},
                "is_deleted": False,
            }
        ],
    )

    response = client.get("/api/admin/contents/publications", headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    item = payload["publications"][0]
    assert item["title"] == "Sample Title"
    assert item["content_type"] == "webtoon"
    assert item["status"] == "연재중"
    assert isinstance(item["meta"], dict)


def test_admin_deleted_includes_title(monkeypatch, client, auth_headers):
    now = datetime(2024, 6, 1, 12, 0, 0)
    monkeypatch.setattr(admin_view, "get_db", lambda: object())
    monkeypatch.setattr(
        admin_view,
        "list_deleted_contents",
        lambda conn, limit, offset, q=None: [
            {
                "content_id": "CID",
                "source": "SRC",
                "content_type": "novel",
                "title": "Deleted Title",
                "status": "완결",
                "is_deleted": True,
                "meta": '{"common": {"thumbnail_url": "https://example.com/deleted.png"}}',
                "deleted_at": now,
                "deleted_reason": "spam",
                "deleted_by": 2,
                "override_status": "완결",
                "override_completed_at": now,
                "subscription_count": 12,
            }
        ],
    )

    response = client.get("/api/admin/contents/deleted", headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    item = payload["deleted_contents"][0]
    assert item["title"] == "Deleted Title"
    assert isinstance(item["meta"], dict)
    assert item["override_status"] == "완결"
    assert item["override_completed_at"] == now.isoformat()
    assert item["subscription_count"] == 12


def test_admin_lookup(monkeypatch, client, auth_headers):
    now = datetime(2024, 6, 2, 9, 0, 0)
    content_row = {
        "content_id": "CID",
        "source": "SRC",
        "title": "Lookup Title",
        "content_type": "webtoon",
        "status": "연재중",
        "meta": '{"common": {"thumbnail_url": "https://example.com/lookup.png"}}',
        "is_deleted": False,
        "created_at": now,
        "updated_at": now,
    }
    override_row = {
        "id": 3,
        "content_id": "CID",
        "source": "SRC",
        "override_status": "완결",
        "override_completed_at": now,
        "reason": "manual",
        "admin_id": 1,
        "created_at": now,
        "updated_at": now,
    }
    publication_row = {
        "id": 7,
        "content_id": "CID",
        "source": "SRC",
        "public_at": now,
        "reason": "publish",
        "admin_id": 1,
        "created_at": now,
        "updated_at": now,
    }
    fake_cursor = FakeCursor(
        content_row=content_row,
        override_row=override_row,
        publication_row=publication_row,
    )
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection(fake_cursor))
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/contents/lookup?content_id=CID&source=SRC",
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["content"]["title"] == "Lookup Title"
    assert isinstance(payload["content"]["meta"], dict)
    assert payload["override"]["override_status"] == "완결"
    assert payload["publication"]["reason"] == "publish"


def test_admin_lookup_missing_content_returns_404(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(content_row=None)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection(fake_cursor))
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)

    response = client.get(
        "/api/admin/contents/lookup?content_id=missing&source=SRC",
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 404
    assert payload["error"]["code"] == "CONTENT_NOT_FOUND"
