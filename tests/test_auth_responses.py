from app import app as flask_app
import views.auth as auth_views


def setup_client():
    flask_app.config['TESTING'] = True
    return flask_app.test_client()


def test_login_invalid_credentials_returns_standard_error(monkeypatch):
    client = setup_client()

    monkeypatch.setattr(auth_views, 'authenticate_user', lambda email, password: None)

    response = client.post(
        '/api/auth/login',
        json={'email': 'user@example.com', 'password': 'wrong'},
    )

    data = response.get_json()
    assert response.status_code == 401
    assert data['success'] is False
    assert data['error']['code'] == 'INVALID_CREDENTIALS'
    assert '이메일 또는 비밀번호' in data['error']['message']


def test_me_requires_authentication_standard_error():
    client = setup_client()

    response = client.get('/api/auth/me')

    data = response.get_json()
    assert response.status_code == 401
    assert data['success'] is False
    assert data['error']['code'] == 'AUTH_REQUIRED'
    assert data['error']['message'] == 'Authentication required'


def test_admin_ping_requires_authentication_standard_error():
    client = setup_client()

    response = client.get('/api/auth/admin/ping')

    data = response.get_json()
    assert response.status_code == 401
    assert data['success'] is False
    assert data['error']['code'] == 'AUTH_REQUIRED'
    assert data['error']['message'] == 'Authentication required'
