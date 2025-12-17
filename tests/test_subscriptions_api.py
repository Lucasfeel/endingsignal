import pytest

from app import app as flask_app
from views import subscriptions
import utils.auth as auth


class FakeCursor:
    def __init__(self, fetchone_result=None):
        self.fetchone_result = fetchone_result
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


@pytest.fixture(autouse=True)
def stub_decode_token(monkeypatch):
    monkeypatch.setattr(
        auth,
        '_decode_token',
        lambda token: {'uid': 1, 'email': 'user@example.com', 'role': 'user'},
    )


@pytest.fixture
def client():
    flask_app.config['TESTING'] = True
    return flask_app.test_client()


@pytest.fixture
def auth_headers():
    return {'Authorization': 'Bearer testtoken'}


def test_subscribe_accepts_canonical_content_id(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_result=(1,))
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.post(
        '/api/me/subscriptions',
        json={'content_id': 'abc-123', 'source': 'rss'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed[-1][1][2] == 'abc-123'


def test_subscribe_accepts_legacy_contentId(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_result=(1,))
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.post(
        '/api/me/subscriptions',
        json={'contentId': 'legacy-42', 'source': 'email'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed[-1][1][2] == 'legacy-42'


def test_unsubscribe_accepts_canonical_content_id(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.delete(
        '/api/me/subscriptions',
        json={'content_id': 'to-delete', 'source': 'rss'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed[-1][1][1] == 'to-delete'


def test_unsubscribe_accepts_legacy_contentId(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.delete(
        '/api/me/subscriptions',
        json={'contentId': 'legacy-delete', 'source': 'web'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed[-1][1][1] == 'legacy-delete'


def test_subscribe_validation_mentions_both_keys(client, auth_headers):
    response = client.post(
        '/api/me/subscriptions', json={'source': 'rss'}, headers=auth_headers
    )

    data = response.get_json()
    assert response.status_code == 400
    assert 'content_id/contentId' in data['error']['message']
