import services.auth_service as auth_service


class FakeCursor:
    def __init__(self, fetch_results=None):
        self.fetch_results = list(fetch_results or [])
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if self.fetch_results:
            return self.fetch_results.pop(0)
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


class FakeCursorContext:
    def __init__(self, cursor):
        self.cursor = cursor

    def __call__(self, conn):
        return self.cursor


def test_register_user_does_not_promote_first_user_to_admin(monkeypatch):
    # Simulate: first fetchone -> email not found; second -> inserted user id
    fake_cursor = FakeCursor(fetch_results=[None, [1]])
    fake_conn = FakeConnection()

    monkeypatch.setattr(auth_service, 'get_db', lambda: fake_conn)
    monkeypatch.setattr(auth_service, 'get_cursor', FakeCursorContext(fake_cursor))

    user, error = auth_service.register_user('first@example.com', 'pw123')

    assert error is None
    assert user['role'] == 'user'
    # Insert statement should store 'user' role
    assert fake_cursor.executed[-1][1][2] == 'user'
    assert fake_conn.committed is True
