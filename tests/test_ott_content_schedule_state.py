from datetime import datetime

from services import ott_content_service as service


def test_compute_schedule_state_clears_stale_confirmed_when_entry_returns_unknown():
    release_end_at, release_end_status, resolution_state = service._compute_schedule_state(
        existing_meta={
            "ott": {
                "release_end_at": "2026-01-31T00:00:00",
                "release_end_status": service.RELEASE_END_STATUS_CONFIRMED,
            }
        },
        entry={
            "release_end_at": None,
            "release_end_status": service.RELEASE_END_STATUS_UNKNOWN,
            "resolution_state": service.RESOLUTION_TRACKING,
        },
        now_value=datetime(2026, 3, 12, 0, 0, 0),
    )

    assert release_end_at is None
    assert release_end_status == service.RELEASE_END_STATUS_UNKNOWN
    assert resolution_state == service.RESOLUTION_TRACKING
