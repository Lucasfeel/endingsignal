from flask import Flask

import database


class FakeConnection:
    def __init__(self):
        self.rollback_calls = 0
        self.close_calls = 0

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1


class FakePool:
    def __init__(self, connection):
        self.connection = connection
        self.putconn_calls = []

    def getconn(self):
        return self.connection

    def putconn(self, conn):
        self.putconn_calls.append(conn)


def test_db_pool_enabled_defaults_to_true(monkeypatch):
    monkeypatch.delenv("DB_POOL_ENABLED", raising=False)

    assert database._db_pool_enabled() is True


def test_close_db_returns_connection_to_pool_when_enabled(monkeypatch):
    app = Flask(__name__)
    fake_conn = FakeConnection()
    fake_pool = FakePool(fake_conn)

    monkeypatch.setenv("DB_POOL_ENABLED", "1")
    monkeypatch.setattr(database, "_DB_POOL", fake_pool)
    monkeypatch.setattr(database, "_DB_POOL_CONFIG", (1, 4))
    monkeypatch.setattr(database, "_get_db_pool", lambda: fake_pool)

    with app.app_context():
        conn = database.get_db()
        assert conn is fake_conn
        database.close_db()

    assert fake_conn.rollback_calls == 1
    assert fake_conn.close_calls == 0
    assert fake_pool.putconn_calls == [fake_conn]


def test_close_db_closes_connection_when_pool_disabled(monkeypatch):
    app = Flask(__name__)
    fake_conn = FakeConnection()

    monkeypatch.setenv("DB_POOL_ENABLED", "0")
    monkeypatch.setattr(database, "_create_connection", lambda: fake_conn)

    with app.app_context():
        conn = database.get_db()
        assert conn is fake_conn
        database.close_db()

    assert fake_conn.rollback_calls == 0
    assert fake_conn.close_calls == 1
