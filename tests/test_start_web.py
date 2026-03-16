import scripts.start_web as start_web


def _clear_db_env(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)


def test_should_run_db_init_false_without_db_config(monkeypatch):
    _clear_db_env(monkeypatch)
    monkeypatch.delenv("RUN_DB_INIT", raising=False)
    monkeypatch.delenv("SKIP_DB_INIT", raising=False)

    assert start_web.should_run_db_init() is False


def test_should_run_db_init_true_with_database_url(monkeypatch):
    _clear_db_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://db")
    monkeypatch.delenv("RUN_DB_INIT", raising=False)
    monkeypatch.delenv("SKIP_DB_INIT", raising=False)

    assert start_web.should_run_db_init() is True


def test_should_run_db_init_respects_skip_flag(monkeypatch):
    _clear_db_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://db")
    monkeypatch.setenv("SKIP_DB_INIT", "1")
    monkeypatch.setenv("RUN_DB_INIT", "1")

    assert start_web.should_run_db_init() is False


def test_build_db_init_env_defaults_backfill_off(monkeypatch):
    monkeypatch.delenv("DB_INIT_ENABLE_BACKFILL", raising=False)
    monkeypatch.delenv("RUN_DB_INIT_WITH_BACKFILL", raising=False)

    env = start_web.build_db_init_env()

    assert env["DB_INIT_ENABLE_BACKFILL"] == "0"


def test_build_db_init_env_allows_backfill_opt_in(monkeypatch):
    monkeypatch.delenv("DB_INIT_ENABLE_BACKFILL", raising=False)
    monkeypatch.setenv("RUN_DB_INIT_WITH_BACKFILL", "1")

    env = start_web.build_db_init_env()

    assert env["DB_INIT_ENABLE_BACKFILL"] == "1"


def test_build_db_init_env_preserves_explicit_backfill_setting(monkeypatch):
    monkeypatch.setenv("DB_INIT_ENABLE_BACKFILL", "1")
    monkeypatch.delenv("RUN_DB_INIT_WITH_BACKFILL", raising=False)

    env = start_web.build_db_init_env()

    assert env["DB_INIT_ENABLE_BACKFILL"] == "1"


def test_build_db_init_env_normalizes_blank_backfill_setting(monkeypatch):
    monkeypatch.setenv("DB_INIT_ENABLE_BACKFILL", "")
    monkeypatch.delenv("RUN_DB_INIT_WITH_BACKFILL", raising=False)

    env = start_web.build_db_init_env()

    assert env["DB_INIT_ENABLE_BACKFILL"] == "0"


def test_build_gunicorn_command_uses_port_default(monkeypatch):
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("GUNICORN_BIND", raising=False)
    monkeypatch.delenv("WEB_CONCURRENCY", raising=False)
    monkeypatch.delenv("GUNICORN_TIMEOUT", raising=False)

    command = start_web.build_gunicorn_command()

    assert command == [
        "gunicorn",
        "app:app",
        "--bind",
        "0.0.0.0:5000",
        "--workers",
        "2",
        "--timeout",
        "120",
    ]
