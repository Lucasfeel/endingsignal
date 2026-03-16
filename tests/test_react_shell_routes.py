from app import app


def test_public_shell_routes_render_react_entry():
    client = app.test_client()

    for path in ["/", "/search", "/browse/novel", "/content/ridi/cid-1", "/subscriptions", "/mypage"]:
        response = client.get(path)
        body = response.get_data(as_text=True)

        assert response.status_code == 200
        assert 'id="app-root"' in body
        assert "build/public-app.js" in body


def test_admin_shell_routes_render_react_entry():
    client = app.test_client()

    for path in ["/admin", "/admin/manage", "/admin/reports", "/admin/daily-notification"]:
        response = client.get(path)
        body = response.get_data(as_text=True)

        assert response.status_code == 200
        assert 'id="app-root"' in body
        assert "build/admin-app.js" in body


def test_public_shell_runtime_uses_forwarded_https_scheme():
    client = app.test_client()

    response = client.get(
        "/",
        headers={
            "X-Forwarded-Host": "www.endingsignal.com",
            "X-Forwarded-Proto": "https",
        },
    )
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'apiBaseUrl: "https://www.endingsignal.com"' in body
