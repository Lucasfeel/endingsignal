from datetime import datetime

from services.final_state_resolver import resolve_final_state


class RowLike:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]


def test_scheduled_completion_pending():
    now = datetime(2025, 12, 17, 12, 0, 0)
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)
    override = {
        "override_status": "완결",
        "override_completed_at": override_completed_at,
    }

    result = resolve_final_state("연재중", override, now=now)

    assert result["final_status"] == "연재중"
    assert result["resolved_by"] == "crawler"
    assert result["final_completed_at"] is None


def test_scheduled_completion_effective():
    now = datetime(2025, 12, 30, 0, 0, 0)
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)
    override = {
        "override_status": "완결",
        "override_completed_at": override_completed_at,
    }

    result = resolve_final_state("연재중", override, now=now)

    assert result["final_status"] == "완결"
    assert result["resolved_by"] == "override"
    assert result["final_completed_at"] == override_completed_at


def test_immediate_completion_without_date():
    now = datetime(2025, 12, 30, 0, 0, 0)
    override = {
        "override_status": "완결",
        "override_completed_at": None,
    }

    result = resolve_final_state("연재중", override, now=now)

    assert result["final_status"] == "완결"
    assert result["resolved_by"] == "override"
    assert result["final_completed_at"] is None


def test_resolver_uses_kst_default_now(monkeypatch):
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)
    override = {
        "override_status": "완결",
        "override_completed_at": override_completed_at,
    }

    # final_state_resolver imports now_kst_naive into its module namespace
    monkeypatch.setattr(
        "services.final_state_resolver.now_kst_naive",
        lambda: datetime(2025, 12, 29, 23, 59, 59),
    )

    result = resolve_final_state("연재중", override)

    assert result["final_status"] == "연재중"
    assert result["resolved_by"] == "crawler"


def test_row_like_override_is_supported():
    now = datetime(2025, 12, 17, 12, 0, 0)
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)
    override = RowLike(
        {
            "override_status": "완결",
            "override_completed_at": override_completed_at,
        }
    )

    result = resolve_final_state("연재중", override, now=now)

    assert result["final_status"] == "연재중"
    assert result["resolved_by"] == "crawler"
    assert result["final_completed_at"] is None
