from database import _read_db_init_settings


def test_db_init_settings_enable_backfill_defaults_false(monkeypatch):
    monkeypatch.delenv("DB_INIT_ENABLE_BACKFILL", raising=False)

    settings = _read_db_init_settings()

    assert settings["enable_backfill"] is False


def test_db_init_settings_enable_backfill_respects_disable(monkeypatch):
    monkeypatch.setenv("DB_INIT_ENABLE_BACKFILL", "0")

    settings = _read_db_init_settings()

    assert settings["enable_backfill"] is False


def test_db_init_settings_enable_backfill_respects_opt_in(monkeypatch):
    monkeypatch.setenv("DB_INIT_ENABLE_BACKFILL", "1")

    settings = _read_db_init_settings()

    assert settings["enable_backfill"] is True
