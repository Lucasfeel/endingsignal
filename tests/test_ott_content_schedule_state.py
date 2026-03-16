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


def test_compute_watchlist_state_defers_unknown_future_release_until_day_after_premiere():
    next_check_at, fail_count, resolution_state = service._compute_watchlist_state(
        existing_row=None,
        release_start_at=datetime(2026, 3, 14, 0, 0, 0),
        release_end_at=None,
        release_end_status=service.RELEASE_END_STATUS_UNKNOWN,
        resolution_state=service.RESOLUTION_TRACKING,
        status=service.STATUS_ONGOING,
        now_value=datetime(2026, 3, 1, 0, 0, 0),
    )

    assert next_check_at == datetime(2026, 3, 15, 0, 0, 0)
    assert fail_count == 0
    assert resolution_state == service.RESOLUTION_TRACKING
