import importlib

import views.contents as contents


def test_search_limit_from_env(monkeypatch):
    monkeypatch.setenv("SEARCH_MAX_RESULTS", "50")
    importlib.reload(contents)

    assert contents._get_search_limit() == 50


def test_search_threshold_overrides(monkeypatch):
    monkeypatch.setenv("SEARCH_SIMILARITY_TITLE_THRESHOLD", "0.33")
    monkeypatch.setenv("SEARCH_SIMILARITY_AUTHOR_THRESHOLD", "0.44")
    importlib.reload(contents)

    defaults = (0.12, 0.18)
    assert contents._apply_threshold_overrides(defaults, 5) == (0.33, 0.44)


def test_search_threshold_invalid_env_falls_back(monkeypatch):
    monkeypatch.setenv("SEARCH_SIMILARITY_TITLE_THRESHOLD", "bad")
    monkeypatch.setenv("SEARCH_SIMILARITY_AUTHOR_THRESHOLD", "1.5")
    importlib.reload(contents)

    defaults = (0.12, 0.18)
    assert contents._apply_threshold_overrides(defaults, 5) == defaults
