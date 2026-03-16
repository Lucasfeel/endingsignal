from services import ott_verification_service as service


def test_collect_targets_skips_watchlist_when_current_platform_item_exists():
    write_plan = {
        "verification_candidates": [
            {
                "content_id": "ott_series:2021:base-title:abc123",
                "source_name": "coupangplay",
                "title": "나는 솔로",
                "content_url": "https://www.coupangplay.com/content/aafafdbe-67d6-4335-a456-0997f00364f9",
                "change_kinds": ["force_reverify"],
                "source_item": {
                    "title": "나는 솔로",
                    "platform_content_id": "aafafdbe-67d6-4335-a456-0997f00364f9",
                    "platform_url": "https://www.coupangplay.com/content/aafafdbe-67d6-4335-a456-0997f00364f9",
                    "content_url": "https://www.coupangplay.com/content/aafafdbe-67d6-4335-a456-0997f00364f9",
                },
            }
        ],
        "snapshot_existing_rows": [],
        "platform_links": [
            {
                "canonical_content_id": "ott_series:2026:seasoned-title:def456",
                "platform_source": "coupangplay",
                "platform_content_id": "aafafdbe-67d6-4335-a456-0997f00364f9",
                "platform_url": "https://www.coupangplay.com/content/aafafdbe-67d6-4335-a456-0997f00364f9",
            }
        ],
        "watchlist_rows": [
            {
                "canonical_content_id": "ott_series:2026:seasoned-title:def456",
                "platform_source": "coupangplay",
                "release_end_status": "unknown",
            }
        ],
    }

    targets = service.collect_ott_verification_targets(write_plan)

    assert len(targets) == 1
    assert targets[0]["content_id"] == "ott_series:2021:base-title:abc123"
