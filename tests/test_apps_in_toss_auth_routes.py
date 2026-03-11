import utils.auth as auth
from app import app as flask_app
from views import auth as auth_views


def test_v1_auth_login_returns_apps_in_toss_payload(monkeypatch):
    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

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
                "displayName": "토스 사용자",
                "authProvider": "apps_in_toss",
            },
        },
    )

    response = client.post(
        "/v1/auth/login",
        json={"authorizationCode": "auth-code", "referrer": "DEFAULT"},
    )

    assert response.status_code == 200
    data = response.get_json()
    assert data["accessToken"] == "token-123"
    assert data["me"]["userKey"] == "443731104"


def test_v1_auth_me_returns_user_key(monkeypatch):
    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {
            "uid": 7,
            "email": "toss-user-443731104@apps-in-toss.local",
            "role": "user",
            "user_key": "443731104",
            "auth_provider": "apps_in_toss",
            "display_name": "토스 사용자",
        },
    )

    response = client.get("/v1/auth/me", headers={"Authorization": "Bearer testtoken"})

    assert response.status_code == 200
    data = response.get_json()
    assert data["me"]["userKey"] == "443731104"
    assert data["me"]["authProvider"] == "apps_in_toss"
