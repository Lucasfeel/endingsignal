import pytest
from datetime import datetime, timedelta

from app import app as flask_app
from views import subscriptions
import utils.auth as auth


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


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


def _base_row(public_at):
    return {
        'content_id': 'CID',
        'source': 'SRC',
        'content_type': 'webtoon',
        'title': 'Title',
        'status': '연재중',
        'meta': {},
        'override_status': None,
        'override_completed_at': None,
        'public_at': public_at,
    }


def test_list_subscriptions_future_publication(monkeypatch, client, auth_headers):
    now = datetime(2025, 1, 2, 12, 0, 0)
    future_at = now + timedelta(days=1)
    fake_cursor = FakeCursor([_base_row(future_at)])
    fake_conn = FakeConnection()

    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)
    monkeypatch.setattr(subscriptions, 'now_kst_naive', lambda: now)

    response = client.get('/api/me/subscriptions', headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload['success'] is True
    publication = payload['data'][0]['publication']
    assert publication['public_at'] == future_at.isoformat()
    assert publication['is_scheduled_publication'] is True
    assert publication['is_published'] is False


def test_list_subscriptions_past_publication(monkeypatch, client, auth_headers):
    now = datetime(2025, 1, 2, 12, 0, 0)
    past_at = now - timedelta(days=1)
    fake_cursor = FakeCursor([_base_row(past_at)])
    fake_conn = FakeConnection()

    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)
    monkeypatch.setattr(subscriptions, 'now_kst_naive', lambda: now)

    response = client.get('/api/me/subscriptions', headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload['success'] is True
    publication = payload['data'][0]['publication']
    assert publication['public_at'] == past_at.isoformat()
    assert publication['is_scheduled_publication'] is False
    assert publication['is_published'] is True


def test_list_subscriptions_no_publication(monkeypatch, client, auth_headers):
    now = datetime(2025, 1, 2, 12, 0, 0)
    fake_cursor = FakeCursor([_base_row(None)])
    fake_conn = FakeConnection()

    monkeypatch.setattr(subscriptions, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(subscriptions, 'get_cursor', lambda conn: fake_cursor)
    monkeypatch.setattr(subscriptions, 'now_kst_naive', lambda: now)

    response = client.get('/api/me/subscriptions', headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 200
    assert payload['success'] is True
    publication = payload['data'][0]['publication']
    assert publication['public_at'] is None
    assert publication['is_scheduled_publication'] is False
    assert publication['is_published'] is False
