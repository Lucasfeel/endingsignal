import importlib

import config as config_module


def test_cors_allow_origins_parses_comma_separated(monkeypatch):
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "https://a.com, https://b.com")
    monkeypatch.setenv("CORS_SUPPORTS_CREDENTIALS", "0")

    config = importlib.reload(config_module)

    assert config.CORS_ALLOW_ORIGINS == ["https://a.com", "https://b.com"]
    assert config.CORS_SUPPORTS_CREDENTIALS is False


def test_cors_allow_origins_parses_json_array(monkeypatch):
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", '["https://a.com", "https://b.com"]')
    monkeypatch.setenv("CORS_SUPPORTS_CREDENTIALS", "1")

    config = importlib.reload(config_module)

    assert config.CORS_ALLOW_ORIGINS == ["https://a.com", "https://b.com"]
    assert config.CORS_SUPPORTS_CREDENTIALS is True


def test_cors_allow_origins_empty(monkeypatch):
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "   ")

    config = importlib.reload(config_module)

    assert config.CORS_ALLOW_ORIGINS is None
