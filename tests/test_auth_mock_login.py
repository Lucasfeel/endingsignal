from app import app as flask_app
import views.auth as auth_views


def setup_client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def test_mock_login_disabled_by_default():
    client = setup_client()

    response = client.post("/api/auth/mock-login", json={})

    assert response.status_code == 404
    payload = response.get_json()
    assert payload["error"]["code"] == "NOT_FOUND"


def test_mock_login_returns_token_when_enabled(monkeypatch):
    client = setup_client()

    monkeypatch.setattr(auth_views, "_mock_login_enabled", lambda: True)
    monkeypatch.setattr(
        auth_views,
        "upsert_mock_user",
        lambda **kwargs: {
            "id": 101,
            "email": kwargs["email"],
            "role": kwargs["role"],
            "user_key": kwargs.get("user_key"),
            "auth_provider": "mock",
            "display_name": kwargs.get("display_name"),
        },
    )
    monkeypatch.setattr(auth_views, "create_access_token", lambda user: ("mock-token", 3600))

    response = client.post(
        "/api/auth/mock-login",
        json={"role": "admin", "display_name": "Mock Admin", "email": "qa@endingsignal.local"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["access_token"] == "mock-token"
    assert payload["user"]["role"] == "admin"
    assert payload["user"]["display_name"] == "Mock Admin"
    assert payload["mock"] is True
