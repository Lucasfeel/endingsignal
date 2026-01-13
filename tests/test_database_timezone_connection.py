import database


def test_create_connection_passes_timezone_options_with_database_url(monkeypatch):
    calls = {}
    sentinel = object()

    def fake_connect(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return sentinel

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.delenv("DB_TIMEZONE", raising=False)
    monkeypatch.setattr(database.psycopg2, "connect", fake_connect)

    result = database._create_connection()

    assert result is sentinel
    assert calls["args"][0] == "postgres://example"
    assert calls["kwargs"]["options"] == "-c timezone=Asia/Seoul"


def test_create_connection_passes_timezone_options_with_individual_vars(monkeypatch):
    calls = {}
    sentinel = object()

    def fake_connect(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return sentinel

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_TIMEZONE", raising=False)
    monkeypatch.setenv("DB_NAME", "dbname")
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    monkeypatch.setenv("DB_HOST", "host")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setattr(database.psycopg2, "connect", fake_connect)

    result = database._create_connection()

    assert result is sentinel
    assert calls["args"] == ()
    assert calls["kwargs"]["dbname"] == "dbname"
    assert calls["kwargs"]["user"] == "user"
    assert calls["kwargs"]["password"] == "password"
    assert calls["kwargs"]["host"] == "host"
    assert calls["kwargs"]["port"] == "5432"
    assert calls["kwargs"]["options"] == "-c timezone=Asia/Seoul"


def test_db_timezone_env_override(monkeypatch):
    calls = {}
    sentinel = object()

    def fake_connect(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return sentinel

    monkeypatch.setenv("DATABASE_URL", "postgres://example")
    monkeypatch.setenv("DB_TIMEZONE", "UTC")
    monkeypatch.setattr(database.psycopg2, "connect", fake_connect)

    result = database._create_connection()

    assert result is sentinel
    assert calls["kwargs"]["options"] == "-c timezone=UTC"
