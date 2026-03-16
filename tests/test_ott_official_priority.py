from datetime import datetime

from services import ott_content_service as content_service
from services import ott_verification_service as verification_service


def _canonical_id() -> str:
    return "ott_series:2026:test-series:abc123def456"


def test_merge_verification_metadata_prefers_official_dates_over_namuwiki():
    candidate = {
        "title": "\ub354 \ud53c\ud2b8",
        "source_item": {
            "title": "\ub354 \ud53c\ud2b8",
            "description": "\uc2dc\uc98c 2",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.coupangplay.com/content/the-pitt",
            "ok": True,
            "title": "\ub354 \ud53c\ud2b8 \uc2dc\uc98c 2",
            "payload_titles": ["The Pitt Season 2"],
            "body_text": "\uc2dc\uc98c 2 2026\ub144 1\uc6d4 12\uc77c ~ 2026\ub144 4\uc6d4 20\uc77c",
            "description": "",
            "cast": [],
            "release_start_at": datetime(2026, 1, 12),
            "release_end_at": datetime(2026, 4, 20),
            "release_end_status": "scheduled",
            "source": "official_episode_schedule",
        },
        {
            "url": "https://namu.wiki/w/%EB%8D%94_%ED%94%BC%ED%8A%B8",
            "ok": True,
            "title": "\ub354 \ud53c\ud2b8(\ub4dc\ub77c\ub9c8)",
            "payload_titles": ["The Pitt"],
            "body_text": "\uc2dc\uc98c 2 \uacf5\uac1c \uc911 2026\ub144 1\uc6d4 8\uc77c ~ 2026\ub144 4\uc6d4 16\uc77c (\uc608\uc815)",
            "description": "",
            "cast": [],
            "release_start_at": datetime(2026, 1, 8),
            "release_end_at": datetime(2026, 4, 16),
            "release_end_status": "scheduled",
            "source": "public_web",
        },
    ]

    metadata = verification_service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["resolved_title"] == "\ub354 \ud53c\ud2b8 \uc2dc\uc98c 2"
    assert metadata["release_start_at"] == datetime(2026, 1, 12)
    assert metadata["release_end_at"] == datetime(2026, 4, 20)
    assert metadata["release_end_status"] == "scheduled"


def test_coupang_default_season_marks_latest_season_completed(monkeypatch):
    entry = {
        "title": "\uc778\ub354\uc2a4\ud2b8\ub9ac",
        "platform_content_id": "6ad7d2a3-df61-4cc4-9cb1-8fd367f006ca",
        "platform_url": "https://www.coupangplay.com/content/6ad7d2a3-df61-4cc4-9cb1-8fd367f006ca",
        "content_url": "https://www.coupangplay.com/content/6ad7d2a3-df61-4cc4-9cb1-8fd367f006ca",
        "description": "\uc2dc\ub9ac\uc988",
        "release_end_status": "unknown",
        "cast": [],
    }
    write_plan = {
        "source_name": "coupangplay",
        "all_content_today": {_canonical_id(): entry},
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "coupangplay",
                "title": "\uc778\ub354\uc2a4\ud2b8\ub9ac",
                "content_url": entry["content_url"],
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    monkeypatch.setattr(
        verification_service,
        "_fetch_document",
        lambda *_args, **_kwargs: {
            "url": entry["content_url"],
            "ok": True,
            "title": "\uc778\ub354\uc2a4\ud2b8\ub9ac",
            "payload_titles": ["Industry"],
            "body_text": "\uc2dc\uc98c 4",
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_public_page",
        },
    )
    monkeypatch.setattr(verification_service, "_fetch_rendered_official_document", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        verification_service,
        "_fetch_coupang_episode_schedule_document",
        lambda *_args, **_kwargs: {
            "url": entry["content_url"],
            "ok": True,
            "title": "\uc778\ub354\uc2a4\ud2b8\ub9ac \uc2dc\uc98c 4",
            "payload_titles": ["Industry Season 4"],
            "body_text": "\uc2dc\uc98c 4 8\ud654",
            "description": "",
            "cast": [],
            "episode_total": 8,
            "release_weekdays": [3],
            "release_start_at": datetime(2026, 1, 15),
            "release_end_at": datetime(2026, 3, 5),
            "release_end_status": "confirmed",
            "source": "official_episode_schedule",
        },
    )
    monkeypatch.setattr(verification_service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(verification_service, "_collect_public_result_urls", lambda *_args, **_kwargs: [])

    verdict = verification_service.verify_ott_write_plan(write_plan, source_name="coupangplay")

    assert verdict["gate"] == "passed"
    assert entry["title"] == "\uc778\ub354\uc2a4\ud2b8\ub9ac \uc2dc\uc98c 4"
    assert entry["release_start_at"] == datetime(2026, 1, 15)
    assert entry["release_end_at"] == datetime(2026, 3, 5)
    assert entry["release_end_status"] == "confirmed"


def test_compute_schedule_state_keeps_first_completed_timestamp():
    release_end_at, release_end_status, resolution_state = content_service._compute_schedule_state(
        existing_meta={
            "ott": {
                "completed_at": "2026-03-13T00:00:00",
                "release_end_status": content_service.RELEASE_END_STATUS_CONFIRMED,
            }
        },
        entry={
            "release_end_at": None,
            "release_end_status": content_service.RELEASE_END_STATUS_CONFIRMED,
            "resolution_state": content_service.RESOLUTION_TRACKING,
        },
        now_value=datetime(2026, 3, 20, 0, 0, 0),
    )

    assert release_end_at == datetime(2026, 3, 13, 0, 0, 0)
    assert release_end_status == content_service.RELEASE_END_STATUS_CONFIRMED
    assert resolution_state == content_service.RESOLUTION_TRACKING


def test_compute_schedule_state_prefers_current_verified_end_date():
    release_end_at, release_end_status, resolution_state = content_service._compute_schedule_state(
        existing_meta={
            "ott": {
                "release_end_at": "2026-02-25T00:00:00",
                "release_end_status": content_service.RELEASE_END_STATUS_UNKNOWN,
            }
        },
        entry={
            "release_end_at": datetime(2026, 4, 20, 0, 0, 0),
            "release_end_status": content_service.RELEASE_END_STATUS_SCHEDULED,
            "resolution_state": content_service.RESOLUTION_TRACKING,
        },
        now_value=datetime(2026, 3, 20, 0, 0, 0),
    )

    assert release_end_at == datetime(2026, 4, 20, 0, 0, 0)
    assert release_end_status == content_service.RELEASE_END_STATUS_SCHEDULED
    assert resolution_state == content_service.RESOLUTION_TRACKING


def test_public_docs_do_not_promote_season_without_official_support():
    candidate = {
        "source_name": "coupangplay",
        "title": "나는 솔로",
        "source_item": {
            "title": "나는 솔로",
            "description": "연애 리얼리티 프로그램",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.coupangplay.com/content/solo",
            "ok": True,
            "title": "나는 솔로",
            "payload_titles": ["I Am Solo"],
            "body_text": "연애 리얼리티 프로그램",
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_public_page",
        },
        {
            "url": "https://namu.wiki/w/%EB%82%98%EB%8A%94_%EC%86%94%EB%A1%9C",
            "ok": True,
            "title": "나는 솔로 30기 - 나무위키",
            "payload_titles": ["나는 솔로 30기"],
            "body_text": "나는 솔로 30기 2026년 1월 1일 ~ 2026년 2월 1일",
            "description": "",
            "cast": [],
            "release_start_at": datetime(2026, 1, 1),
            "release_end_at": datetime(2026, 2, 1),
            "release_end_status": "confirmed",
            "source": "public_web",
        },
    ]

    metadata = verification_service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["resolved_title"] == "나는 솔로"
    assert metadata["season_label"] == ""


def test_season_counter_text_is_not_treated_as_season_label():
    assert verification_service._extract_season_label("놀라운 목요일 시즌 1개") == ""
