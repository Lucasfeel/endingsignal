from datetime import datetime

from services import ott_verification_service as verification_service


def test_long_running_nonscripted_policy_filters_season_ten_plus():
    candidate = {
        "source_name": "coupangplay",
        "title": "Long Running Variety",
        "source_item": {
            "title": "Long Running Variety",
            "description": "weekly variety season 10",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.coupangplay.com/content/long-running-variety",
            "ok": True,
            "title": "Long Running Variety season 10",
            "payload_titles": ["Long Running Variety Season 10"],
            "body_text": "weekly variety season 10 2026-03-01",
            "description": "weekly variety season 10",
            "cast": [],
            "release_start_at": datetime(2026, 3, 1),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_public_page",
        }
    ]

    metadata = verification_service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["exclude_from_sync"] is True
    assert metadata["exclude_reason"] == "long_running_nonscripted_policy"


def test_long_running_nonscripted_policy_filters_first_airing_older_than_ten_years():
    candidate = {
        "source_name": "tving",
        "title": "Legacy Variety",
        "source_item": {
            "title": "Legacy Variety",
            "description": "variety series",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://namu.wiki/w/Legacy_Variety",
            "ok": True,
            "title": "Legacy Variety - namuwiki",
            "payload_titles": ["Legacy Variety"],
            "body_text": "variety show 2014-03-01 ~ now airing",
            "description": "variety show",
            "cast": [],
            "release_start_at": datetime(2014, 3, 1),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        }
    ]

    metadata = verification_service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["exclude_from_sync"] is True
    assert metadata["exclude_reason"] == "long_running_nonscripted_policy"
