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
        self.closed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


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


def test_bootstrap_admin_from_env_missing_env_vars(monkeypatch):
    monkeypatch.delenv("ADMIN_ID", raising=False)
    monkeypatch.delenv("ADMIN_EMAIL", raising=False)
    monkeypatch.delenv("ADMIN_PASSWORD", raising=False)

    def fail_connection():
        raise AssertionError("Connection should not be created when env vars are missing.")

    monkeypatch.setattr(auth_service, "create_standalone_connection", fail_connection)

    ran, admin_id = auth_service.bootstrap_admin_from_env()

    assert ran is False
    assert admin_id is None


def test_bootstrap_admin_from_env_creates_or_updates_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_ID", "admin@example.com")
    monkeypatch.setenv("ADMIN_PASSWORD", "strongpassword")

    fake_cursor = FakeCursor()
    fake_conn = FakeConnection()

    monkeypatch.setattr(auth_service, "create_standalone_connection", lambda: fake_conn)
    monkeypatch.setattr(auth_service, "get_cursor", FakeCursorContext(fake_cursor))
    monkeypatch.setattr(auth_service, "hash_password", lambda password: "HASHED")

    ran, admin_id = auth_service.bootstrap_admin_from_env()

    assert ran is True
    assert admin_id == "admin@example.com"
    assert len(fake_cursor.executed) == 1
    query, params = fake_cursor.executed[0]
    assert "ON CONFLICT (email)" in query
    assert params == ("admin@example.com", "HASHED")
    assert fake_conn.committed is True
