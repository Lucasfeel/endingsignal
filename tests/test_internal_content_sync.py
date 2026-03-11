from app import app as flask_app
from views import internal_verified_sync as internal_view


def _auth_headers():
    return {"Authorization": "Bearer secret-token"}


def test_internal_content_sync_summary_requires_token(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.get("/api/internal/content-sync/summary")

    assert response.status_code == 401
    assert response.get_json()["error"]["code"] == "AUTH_REQUIRED"


def test_internal_content_sync_summary_returns_data(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    monkeypatch.setattr(internal_view, "get_db", lambda: object())
    monkeypatch.setattr(
        internal_view,
        "summarize_contents",
        lambda conn: {
            "rows": [{"source": "kakao_page", "content_type": "novel", "total_count": 10}],
            "total_count": 10,
            "active_count": 10,
            "deleted_count": 0,
        },
    )

    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.get("/api/internal/content-sync/summary", headers=_auth_headers())

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["summary"]["total_count"] == 10


def test_internal_content_sync_upsert_batch_returns_result(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    monkeypatch.setattr(internal_view, "get_db", lambda: object())
    monkeypatch.setattr(
        internal_view,
        "upsert_contents_batch",
        lambda conn, rows: {
            "received_count": len(rows),
            "inserted_count": 1,
            "updated_count": 1,
        },
    )

    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.post(
        "/api/internal/content-sync/upsert-batch",
        headers=_auth_headers(),
        json={
            "rows": [
                {"content_id": "1", "source": "a", "content_type": "novel", "title": "t", "status": "ongoing"},
                {"content_id": "2", "source": "a", "content_type": "novel", "title": "u", "status": "completed"},
            ]
        },
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["result"]["received_count"] == 2
    assert payload["result"]["inserted_count"] == 1
    assert payload["result"]["updated_count"] == 1


def test_internal_content_sync_upsert_batch_validates_rows(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.post(
        "/api/internal/content-sync/upsert-batch",
        headers=_auth_headers(),
        json={"rows": {"not": "a-list"}},
    )

    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "INVALID_REQUEST"
