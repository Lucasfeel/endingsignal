from app import app as flask_app
from views import internal_verified_sync as internal_view


def _auth_headers():
    return {"Authorization": "Bearer secret-token"}


def test_internal_snapshot_requires_token(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.get("/api/internal/verified-sync/source-snapshot?source=naver_webtoon")

    assert response.status_code == 401
    assert response.get_json()["error"]["code"] == "AUTH_REQUIRED"


def test_internal_snapshot_returns_rows(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    monkeypatch.setattr(internal_view, "resolve_crawler_class", lambda source_name: object())
    monkeypatch.setattr(internal_view, "get_db", lambda: object())
    monkeypatch.setattr(
        internal_view,
        "load_source_snapshot",
        lambda conn, source_name: {
            "source_name": source_name,
            "existing_rows": [{"content_id": "CID-1", "status": "연재중"}],
            "override_rows": [{"content_id": "CID-1", "override_status": "완결"}],
        },
    )

    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.get(
        "/api/internal/verified-sync/source-snapshot?source=naver_webtoon",
        headers=_auth_headers(),
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["snapshot"]["existing_rows"][0]["content_id"] == "CID-1"


def test_internal_source_apply_returns_existing_report(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    monkeypatch.setattr(internal_view, "resolve_crawler_class", lambda source_name: object())
    monkeypatch.setattr(internal_view, "get_db", lambda: object())
    monkeypatch.setattr(
        internal_view,
        "find_existing_source_report",
        lambda conn, run_id, source_name, pipeline: {
            "run_id": run_id,
            "source_name": source_name,
            "pipeline": pipeline,
            "apply_result": "applied",
        },
    )

    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.post(
        "/api/internal/verified-sync/source-apply",
        headers=_auth_headers(),
        json={
            "report": {
                "run_id": "verified_cloud_v1:20260311-090000:test",
                "pipeline": "verified_cloud_v1",
                "source_name": "naver_webtoon",
                "crawler_name": "Naver Webtoon",
                "status": "ok",
            }
        },
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["idempotent"] is True
    assert payload["report"]["apply_result"] == "applied"


def test_internal_source_apply_stores_final_report(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    inserted = {}
    monkeypatch.setattr(internal_view, "resolve_crawler_class", lambda source_name: object())
    monkeypatch.setattr(internal_view, "get_db", lambda: object())
    monkeypatch.setattr(internal_view, "find_existing_source_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        internal_view,
        "apply_remote_report",
        lambda conn, report, apply_payload: {
            **report,
            "apply_result": "applied",
            "new_contents": 3,
            "cdc_info": {"apply_result": "applied", "inserted_count": 3},
        },
    )
    monkeypatch.setattr(
        internal_view,
        "insert_source_report",
        lambda conn, crawler_name, status, report: inserted.update(
            {
                "crawler_name": crawler_name,
                "status": status,
                "report": report,
            }
        ),
    )

    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.post(
        "/api/internal/verified-sync/source-apply",
        headers=_auth_headers(),
        json={
            "report": {
                "run_id": "verified_cloud_v1:20260311-090000:test",
                "pipeline": "verified_cloud_v1",
                "source_name": "naver_webtoon",
                "crawler_name": "Naver Webtoon",
                "status": "ok",
                "apply_result": "deferred",
                "cdc_info": {"apply_result": "deferred"},
            },
            "apply_payload": {"source_name": "naver_webtoon"},
        },
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["idempotent"] is False
    assert inserted["crawler_name"] == "Naver Webtoon"
    assert inserted["report"]["apply_result"] == "applied"
