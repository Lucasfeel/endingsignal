import pytest

from app import app as flask_app
from views import subscriptions
import utils.auth as auth


class FakeCursor:
    def __init__(self, fetchone_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

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
    fake_cursor = FakeCursor(fetchone_results=[(1,), (True, False)])
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
    assert fake_cursor.executed[-1][1][4] is True
    assert fake_cursor.executed[-1][1][5] is False


def test_subscribe_accepts_legacy_contentId(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(1,), (True, False)])
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
    assert fake_cursor.executed[-1][1][4] is True
    assert fake_cursor.executed[-1][1][5] is False


def test_subscribe_with_publication_alert_type(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(1,), (False, True)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.post(
        '/api/me/subscriptions',
        json={'content_id': 'pub-1', 'source': 'rss', 'alert_type': 'publication'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed[-1][1][4] is False
    assert fake_cursor.executed[-1][1][5] is True


def test_subscribe_upsert_preserves_existing_flags(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(1,), (True, True)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.post(
        '/api/me/subscriptions',
        json={'content_id': 'both-1', 'source': 'rss', 'alert_type': 'completion'},
        headers=auth_headers,
    )

    assert response.status_code == 200
    insert_query = fake_cursor.executed[-1][0]
    assert 'wants_completion = subscriptions.wants_completion OR EXCLUDED.wants_completion' in insert_query
    assert 'wants_publication = subscriptions.wants_publication OR EXCLUDED.wants_publication' in insert_query


def test_unsubscribe_accepts_canonical_content_id(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(False, False)])
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
    assert data['subscription'] is None
    assert fake_conn.committed is True
    assert fake_cursor.executed[0][1][0] == 'completion'
    assert fake_cursor.executed[0][1][3] == 'to-delete'
    assert 'DELETE FROM subscriptions' in fake_cursor.executed[1][0]


def test_unsubscribe_accepts_legacy_contentId(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(False, False)])
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
    assert data['subscription'] is None
    assert fake_conn.committed is True
    assert fake_cursor.executed[0][1][0] == 'completion'
    assert fake_cursor.executed[0][1][3] == 'legacy-delete'
    assert 'DELETE FROM subscriptions' in fake_cursor.executed[1][0]


def test_unsubscribe_with_publication_alert_type(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(False, True)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.delete(
        '/api/me/subscriptions',
        json={'content_id': 'pub-2', 'source': 'rss', 'alert_type': 'publication'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert data['subscription']['wants_completion'] is False
    assert data['subscription']['wants_publication'] is True
    assert fake_conn.committed is True
    assert fake_cursor.executed[0][1][1] == 'publication'
    assert fake_cursor.executed[0][1][3] == 'pub-2'


def test_unsubscribe_preserves_other_flag(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(False, True)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.delete(
        '/api/me/subscriptions',
        json={'content_id': 'keep-pub', 'source': 'rss', 'alert_type': 'completion'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert data['subscription']['wants_completion'] is False
    assert data['subscription']['wants_publication'] is True
    assert fake_conn.committed is True
    assert len(fake_cursor.executed) == 1


def test_unsubscribe_last_flag_deletes_row(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[(False, False)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.delete(
        '/api/me/subscriptions',
        json={'content_id': 'remove-all', 'source': 'rss', 'alert_type': 'completion'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert data['subscription'] is None
    assert fake_conn.committed is True
    assert 'DELETE FROM subscriptions' in fake_cursor.executed[1][0]


def test_unsubscribe_idempotent_when_missing(monkeypatch, client, auth_headers):
    fake_cursor = FakeCursor(fetchone_results=[None])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)

    response = client.delete(
        '/api/me/subscriptions',
        json={'content_id': 'missing', 'source': 'rss', 'alert_type': 'completion'},
        headers=auth_headers,
    )

    data = response.get_json()
    assert response.status_code == 200
    assert data['success'] is True
    assert data['subscription'] is None
    assert fake_conn.committed is True
    assert len(fake_cursor.executed) == 1


def test_subscribe_validation_mentions_both_keys(client, auth_headers):
    response = client.post(
        '/api/me/subscriptions', json={'source': 'rss'}, headers=auth_headers
    )

    data = response.get_json()
    assert response.status_code == 400
    assert 'content_id/contentId' in data['error']['message']
