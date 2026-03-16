import utils.auth as auth
from app import app as flask_app
from views import auth as auth_views
from views import contents as contents_view
from views import subscriptions as subscriptions_view


class RecordingCursor:
    def __init__(self, *, fetchall_result=None, fetchone_result=None):
        self.fetchall_result = list(fetchall_result or [])
        self.fetchone_result = fetchone_result
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.fetchall_result)

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        self.closed = True


def _client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def test_miniapp_auth_login_contract(monkeypatch):
    monkeypatch.setattr(
        auth_views,
        "login_with_authorization_code",
        lambda **kwargs: {
            "accessToken": "token-123",
            "expiresAt": "2026-03-07T00:00:00+00:00",
            "expiresIn": 3600,
            "me": {
                "id": 7,
                "userKey": "443731104",
                "displayName": "Toss User",
                "authProvider": "apps_in_toss",
            },
        },
    )

    response = _client().post(
        "/v1/auth/login",
        json={"authorizationCode": "auth-code", "referrer": "DEFAULT"},
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert {"accessToken", "expiresAt", "expiresIn", "me"} <= set(payload.keys())
    assert {"id", "userKey", "displayName", "authProvider"} <= set(payload["me"].keys())


def test_miniapp_recommendations_v2_contract(monkeypatch):
    fake_cursor = RecordingCursor(
        fetchall_result=[
            {
                "content_id": "w-1",
                "title": "Sample",
                "status": contents_view.STATUS_ONGOING,
                "meta": {"common": {"authors": ["author"]}},
                "source": "naver_webtoon",
                "content_type": "webtoon",
            }
        ]
    )
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    monkeypatch.setattr(
        contents_view,
        "_execute_recommendations_query",
        lambda cursor, *, limit, meta_expr: fake_cursor.fetchall_result[:limit],
    )

    response = _client().get("/api/contents/recommendations_v2?limit=12")
    payload = response.get_json()

    assert response.status_code == 200
    assert {"contents", "returned", "limit"} <= set(payload.keys())
    assert payload["returned"] == 1
    card = payload["contents"][0]
    assert {
        "content_id",
        "title",
        "status",
        "source",
        "content_type",
        "display_meta",
        "cursor",
    } <= set(card.keys())


def test_miniapp_browse_v3_contract(monkeypatch):
    fake_cursor = RecordingCursor(
        fetchall_result=[
            {
                "content_id": "n-1",
                "title": "Novel",
                "status": contents_view.STATUS_ONGOING,
                "meta": {"common": {"authors": ["writer"]}, "attributes": {"genres": ["판타지"]}},
                "source": "naver_series",
                "content_type": "novel",
            }
        ]
    )
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = _client().get("/api/contents/browse_v3?type=novel&genre_group=fantasy&per_page=12")
    payload = response.get_json()

    assert response.status_code == 200
    assert {"contents", "next_cursor", "page_size", "returned", "filters"} <= set(payload.keys())
    card = payload["contents"][0]
    assert {"content_id", "title", "source", "content_type", "display_meta"} <= set(card.keys())


def test_miniapp_detail_contract(monkeypatch):
    fake_cursor = RecordingCursor(
        fetchone_result={
            "content_id": "cid-1",
            "title": "Sample",
            "status": contents_view.STATUS_ONGOING,
            "meta": {"common": {"authors": ["a"]}},
            "source": "ridi",
            "content_type": "novel",
        }
    )
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = _client().get("/api/contents/detail?content_id=cid-1&source=ridi")
    payload = response.get_json()

    assert response.status_code == 200
    assert {"content_id", "title", "status", "source", "content_type", "meta"} <= set(payload.keys())


def test_miniapp_subscriptions_v1_contract(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {
            "uid": 7,
            "email": "toss-user-443731104@apps-in-toss.local",
            "role": "user",
            "user_key": "443731104",
            "auth_provider": "apps_in_toss",
            "display_name": "Toss User",
        },
    )
    monkeypatch.setattr(subscriptions_view, "get_db", lambda: object())
    monkeypatch.setattr(subscriptions_view, "get_cursor", lambda _conn: RecordingCursor())
    monkeypatch.setattr(
        subscriptions_view,
        "_list_rows",
        lambda cursor, subject: [
            {
                "content_id": "abc-123",
                "source": "naver_webtoon",
                "content_type": "webtoon",
                "title": "Sample",
                "status": "연재중",
                "meta": {"common": {"authors": ["writer"]}},
                "wants_completion": True,
                "wants_publication": False,
                "override_status": None,
                "override_completed_at": None,
                "public_at": None,
            }
        ],
    )
    monkeypatch.setattr(subscriptions_view, "now_kst_naive", lambda: None)

    response = _client().get(
        "/v1/me/subscriptions",
        headers={"Authorization": "Bearer testtoken"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert {"items"} <= set(payload.keys())
    item = payload["items"][0]
    assert {
        "contentKey",
        "content_id",
        "source",
        "title",
        "status",
        "content_type",
        "meta",
        "subscription",
        "publication",
        "final_state",
    } <= set(item.keys())
