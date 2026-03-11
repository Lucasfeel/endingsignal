import utils.auth as auth
from app import app as flask_app
from views import subscriptions


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


def _client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _auth_headers():
    return {"Authorization": "Bearer testtoken"}


def test_v1_subscribe_uses_user_key_conflict_path(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {
            "uid": 7,
            "email": "toss-user-443731104@apps-in-toss.local",
            "role": "user",
            "user_key": "443731104",
        },
    )
    fake_cursor = FakeCursor(fetchone_results=[(1,), (True, False)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, "get_db", lambda: fake_conn)
    monkeypatch.setattr(subscriptions, "get_cursor", lambda conn: fake_cursor)

    response = _client().post(
        "/v1/subscriptions",
        json={"contentKey": "naver_webtoon%3Aabc-123"},
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    insert_query, params = fake_cursor.executed[-1]
    assert "ON CONFLICT (user_key, content_id, source)" in insert_query
    assert params[1] == "443731104"
    assert params[3] == "abc-123"
    assert params[4] == "naver_webtoon"


def test_v1_unsubscribe_path_uses_user_key(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {
            "uid": 7,
            "email": "toss-user-443731104@apps-in-toss.local",
            "role": "user",
            "user_key": "443731104",
        },
    )
    fake_cursor = FakeCursor(fetchone_results=[(1,)])
    fake_conn = FakeConnection()
    monkeypatch.setattr(subscriptions, "get_db", lambda: fake_conn)
    monkeypatch.setattr(subscriptions, "get_cursor", lambda conn: fake_cursor)

    response = _client().delete(
        "/v1/subscriptions/naver_webtoon%3Aabc-123",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    delete_query, params = fake_cursor.executed[0]
    assert "WHERE user_key = %s" in delete_query
    assert params == ("443731104", "abc-123", "naver_webtoon")
