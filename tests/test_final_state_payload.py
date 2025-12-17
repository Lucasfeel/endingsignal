from datetime import datetime

from services.final_state_payload import build_final_state_payload


class RowLike:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]


def test_scheduled_override_pending_sets_flags():
    now = datetime(2025, 12, 17, 12, 0, 0)
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)

    payload = build_final_state_payload(
        "연재중",
        {
            "override_status": "완결",
            "override_completed_at": override_completed_at,
        },
        now=now,
    )

    assert payload["is_scheduled_completion"] is True
    assert payload["scheduled_completed_at"] == override_completed_at.isoformat()
    assert payload["final_status"] == "연재중"


def test_effective_scheduled_completion_unsets_flags():
    now = datetime(2025, 12, 30, 0, 0, 0)
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)

    payload = build_final_state_payload(
        "연재중",
        {
            "override_status": "완결",
            "override_completed_at": override_completed_at,
        },
        now=now,
    )

    assert payload["is_scheduled_completion"] is False
    assert payload["scheduled_completed_at"] is None
    assert payload["final_status"] == "완결"


def test_immediate_completion_override_is_not_scheduled():
    now = datetime(2025, 12, 30, 0, 0, 0)

    payload = build_final_state_payload(
        "연재중",
        {
            "override_status": "완결",
            "override_completed_at": None,
        },
        now=now,
    )

    assert payload["is_scheduled_completion"] is False
    assert payload["scheduled_completed_at"] is None
    assert payload["final_status"] == "완결"


def test_row_like_override_builds_payload():
    now = datetime(2025, 12, 17, 12, 0, 0)
    override_completed_at = datetime(2025, 12, 30, 0, 0, 0)

    payload = build_final_state_payload(
        "연재중",
        RowLike(
            {
                "override_status": "완결",
                "override_completed_at": override_completed_at,
            }
        ),
        now=now,
    )

    assert payload["is_scheduled_completion"] is True
    assert payload["scheduled_completed_at"] == override_completed_at.isoformat()
    assert payload["final_status"] == "연재중"
