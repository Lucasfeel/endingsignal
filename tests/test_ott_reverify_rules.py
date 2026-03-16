from datetime import datetime, timedelta

from services import ott_verification_service as service


def _canonical_id() -> str:
    return "ott_series:2026:surprise-thursday:abc123def456"


def test_extract_date_signals_handles_single_public_date_and_open_ended_start():
    single = service._extract_date_signals("3월 19일 공개")
    open_ended = service._extract_date_signals("2026년 2월 28일부터 방영 중")

    assert single["release_start_at"] == datetime(2026, 3, 19)
    assert single["release_end_at"] is None
    assert open_ended["release_start_at"] == datetime(2026, 2, 28)
    assert open_ended["release_end_at"] is None


def test_extract_date_signals_handles_labeled_open_range_start():
    signal = service._extract_date_signals("방송 예정 2026년 4월 10일 ~ [2] 12부작")

    assert signal["release_start_at"] == datetime(2026, 4, 10)
    assert signal["release_end_at"] is None
    assert signal["release_end_status"] == "unknown"


def test_extract_date_signals_handles_on_air_open_range():
    signal = service._extract_date_signals("시즌 2 : 2025년 9월 16일 ~ ON AIR", season_label="시즌 2")

    assert signal["release_start_at"] == datetime(2025, 9, 16)
    assert signal["release_end_at"] is None
    assert signal["release_end_status"] == "unknown"


def test_extract_date_signals_handles_same_doc_multiseason_rows_with_on_air_current_season():
    signal = service._extract_date_signals(
        "방송 기간 시즌 1 : 2024년 9월 17일 시즌 2 : 2025년 9월 16일 ~ ON AIR",
        season_label="시즌 2",
    )

    assert signal["release_start_at"] == datetime(2025, 9, 16)
    assert signal["release_end_at"] is None
    assert signal["release_end_status"] == "unknown"


def test_collect_targets_includes_incomplete_snapshot_rows_even_if_watchlist_not_due():
    tomorrow = datetime.now() + timedelta(days=1)
    write_plan = {
        "source_name": "tving",
        "all_content_today": {
            "P001783904": {
                "title": "놀라운 목요일",
                "platform_content_id": "P001783904",
                "platform_url": "https://www.tving.com/contents/P001783904",
                "content_url": "https://www.tving.com/contents/P001783904",
                "release_end_status": "unknown",
            }
        },
        "verification_candidates": [],
        "snapshot_existing_rows": [
            {
                "content_id": _canonical_id(),
                "title": "놀라운 목요일",
                "status": "연재중",
                "meta": {
                    "common": {
                        "authors": [],
                        "content_url": "https://www.tving.com/contents/P001783904",
                        "thumbnail_url": "https://img.example/poster.jpg",
                    },
                    "ott": {
                        "platforms": [
                            {
                                "source": "tving",
                                "platform_content_id": "P001783904",
                                "content_url": "https://www.tving.com/contents/P001783904",
                                "thumbnail_url": "https://img.example/poster.jpg",
                            }
                        ],
                        "release_end_status": "unknown",
                        "needs_end_date_verification": True,
                    },
                },
            }
        ],
        "platform_links": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "platform_content_id": "P001783904",
                "platform_url": "https://www.tving.com/contents/P001783904",
            }
        ],
        "watchlist_rows": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "next_check_at": tomorrow,
                "release_end_status": "unknown",
            }
        ],
    }

    targets = service.collect_ott_verification_targets(write_plan)

    assert len(targets) == 1
    assert targets[0]["content_id"] == _canonical_id()
    assert targets[0]["source_item"]["platform_content_id"] == "P001783904"
    assert targets[0]["change_kinds"] == ["current_reverify"]


def test_verify_ott_write_plan_reverifies_incomplete_rows_from_current_source_entry(monkeypatch):
    tomorrow = datetime.now() + timedelta(days=1)
    current_entry = {
        "title": "놀라운 목요일",
        "platform_content_id": "P001783904",
        "platform_url": "https://www.tving.com/contents/P001783904",
        "content_url": "https://www.tving.com/contents/P001783904",
        "release_end_status": "unknown",
    }
    write_plan = {
        "source_name": "tving",
        "all_content_today": {"P001783904": current_entry},
        "verification_candidates": [],
        "snapshot_existing_rows": [
            {
                "content_id": _canonical_id(),
                "title": "놀라운 목요일",
                "status": "연재중",
                "meta": {
                    "common": {
                        "authors": [],
                        "content_url": "https://www.tving.com/contents/P001783904",
                    },
                    "ott": {
                        "platforms": [
                            {
                                "source": "tving",
                                "platform_content_id": "P001783904",
                                "content_url": "https://www.tving.com/contents/P001783904",
                            }
                        ],
                        "release_end_status": "unknown",
                        "needs_end_date_verification": True,
                    },
                },
            }
        ],
        "platform_links": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "platform_content_id": "P001783904",
                "platform_url": "https://www.tving.com/contents/P001783904",
            }
        ],
        "watchlist_rows": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "next_check_at": tomorrow,
                "release_end_status": "unknown",
            }
        ],
    }

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda *_args, **_kwargs: {
            "url": "https://www.tving.com/contents/P001783904",
            "ok": True,
            "title": "놀라운 목요일",
            "payload_titles": ["놀라운 목요일"],
            "body_text": "놀라운 목요일 3월 19일 공개 출연 붐, 이용진",
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_public_page",
        },
    )
    monkeypatch.setattr(
        service,
        "_fetch_rendered_official_document",
        lambda *_args, **_kwargs: {
            "url": "https://www.tving.com/contents/P001783904",
            "ok": True,
            "title": "놀라운 목요일",
            "payload_titles": ["놀라운 목요일"],
            "body_text": "놀라운 목요일 3월 19일 공개 출연 붐, 이용진, 조째즈",
            "description": "",
            "cast": ["붐", "이용진", "조째즈"],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
    )
    monkeypatch.setattr(service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_collect_public_result_urls", lambda *_args, **_kwargs: [])

    verdict = service.verify_ott_write_plan(write_plan, source_name="tving")

    assert verdict["gate"] == "passed"
    assert verdict["apply_allowed"] is True
    assert verdict["watchlist_rechecked_count"] == 0
    assert verdict["verified_count"] == 1
    assert current_entry["release_start_at"] == datetime(2026, 3, 19)
    assert current_entry["cast"] == ["붐", "이용진", "조째즈"]
