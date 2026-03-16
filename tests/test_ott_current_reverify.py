from services import ott_verification_service as service


def test_collect_targets_includes_current_source_entries_for_full_reverify():
    write_plan = {
        "source_name": "coupangplay",
        "all_content_today": {
            "canonical-1": {
                "title": "Honor",
                "platform_content_id": "cp-1",
                "canonical_content_id": "canonical-1",
                "platform_url": "https://www.coupangplay.com/content/cp-1",
                "content_url": "https://www.coupangplay.com/content/cp-1",
            }
        },
        "verification_candidates": [],
        "snapshot_existing_rows": [],
        "platform_links": [],
        "watchlist_rows": [],
    }

    targets = service.collect_ott_verification_targets(write_plan)

    assert len(targets) == 1
    assert targets[0]["content_id"] == "canonical-1"
    assert targets[0]["source_item"]["platform_content_id"] == "cp-1"
    assert targets[0]["change_kinds"] == ["current_reverify"]
