from datetime import datetime

import requests

from services import ott_verification_service as service


def _canonical_id() -> str:
    return "ott_series:2026:test-series:abc123def456"


def test_extract_date_signals_prefers_requested_season_range():
    text = (
        "\ub354 \ud53c\ud2b8(\ub4dc\ub77c\ub9c8) "
        "\ubc29\uc1a1 \uae30\uac04 "
        "\uc2dc\uc98c 1 2025\ub144 1\uc6d4 9\uc77c ~ 4\uc6d4 10\uc77c "
        "\uc2dc\uc98c 2 \uacf5\uac1c \uc911 2026\ub144 1\uc6d4 8\uc77c ~ 4\uc6d4 16\uc77c (\uc608\uc815)"
    )

    signal = service._extract_date_signals(text, season_label="\uc2dc\uc98c 2")

    assert signal["release_start_at"] == datetime(2026, 1, 8)
    assert signal["release_end_at"] == datetime(2026, 4, 16)
    assert signal["release_end_status"] == "scheduled"


def test_extract_date_signals_ignores_other_season_range_when_requested_season_has_no_dates():
    text = (
        "\ubc29\uc1a1 \uae30\uac04 "
        "\uc2dc\uc98c 1 2025\ub144 1\uc6d4 9\uc77c ~ 4\uc6d4 10\uc77c "
        "\uc2dc\uc98c 2 \uc81c\uc791 \ud655\uc815"
    )

    signal = service._extract_date_signals(text, season_label="\uc2dc\uc98c 2")

    assert signal["release_start_at"] is None
    assert signal["release_end_at"] is None
    assert signal["release_end_status"] == "unknown"


def test_extract_date_signals_parses_labeled_broadcast_range_with_status_prefix():
    text = (
        "\ubc29\uc1a1 \uae30\uac04 "
        "\ubc29\uc1a1 \uc608\uc815 "
        "2026\ub144 3\uc6d4 14\uc77c ~ 2026\ub144 4\uc6d4 19\uc77c (\uc608\uc815)"
    )

    signal = service._extract_date_signals(text)

    assert signal["release_start_at"] == datetime(2026, 3, 14)
    assert signal["release_end_at"] == datetime(2026, 4, 19)
    assert signal["release_end_status"] == "scheduled"


def test_verify_ott_write_plan_promotes_title_and_uses_season_specific_schedule(monkeypatch):
    entry = {
        "title": "\ub354 \ud53c\ud2b8",
        "platform_content_id": "56a158a4-bdfd-4d54-a3d0-e8b0b29d68a8",
        "platform_url": "https://www.coupangplay.com/content/56a158a4-bdfd-4d54-a3d0-e8b0b29d68a8",
        "content_url": "https://www.coupangplay.com/content/56a158a4-bdfd-4d54-a3d0-e8b0b29d68a8",
        "description": "\uc2dc\uc98c 2: \ub9e4\uc8fc \uc6d4\uc694\uc77c \uc624\ud6c4 8\uc2dc \uacf5\uac1c",
        "release_start_at": datetime(2025, 5, 14),
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
                "title": "\ub354 \ud53c\ud2b8",
                "content_url": entry["content_url"],
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    def fake_fetch_document(_session, url):
        if "coupangplay.com" in url:
            return {
                "url": url,
                "ok": True,
                "title": "\ub354 \ud53c\ud2b8",
                "payload_titles": ["The Pitt"],
                "body_text": "\uc2dc\uc98c 2: \ub9e4\uc8fc \uc6d4\uc694\uc77c \uc624\ud6c4 8\uc2dc \uacf5\uac1c",
                "description": "\uc2dc\uc98c 2: \ub9e4\uc8fc \uc6d4\uc694\uc77c \uc624\ud6c4 8\uc2dc \uacf5\uac1c",
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            }
        return {
            "url": url,
            "ok": True,
            "title": "\ub354 \ud53c\ud2b8(\ub4dc\ub77c\ub9c8) - \ub098\ubb34\uc704\ud0a4",
            "payload_titles": ["The Pitt"],
            "body_text": (
                "\ub354 \ud53c\ud2b8(\ub4dc\ub77c\ub9c8) "
                "\ubc29\uc1a1 \uae30\uac04 "
                "\uc2dc\uc98c 1 2025\ub144 1\uc6d4 9\uc77c ~ 4\uc6d4 10\uc77c "
                "\uc2dc\uc98c 2 \uacf5\uac1c \uc911 2026\ub144 1\uc6d4 8\uc77c ~ 4\uc6d4 16\uc77c (\uc608\uc815)"
            ),
            "description": "",
            "cast": ["\ub178\uc544 \uc640\uc77c\ub9ac"],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
        }

    monkeypatch.setattr(service, "_fetch_document", fake_fetch_document)
    monkeypatch.setattr(service, "_fetch_coupang_episode_schedule_document", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_collect_public_result_urls",
        lambda _session, _candidate: ["https://namu.wiki/w/%EB%8D%94%20%ED%94%BC%ED%8A%B8(%EB%93%9C%EB%9D%BC%EB%A7%88)"],
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="coupangplay")

    assert verdict["gate"] == "passed"
    assert entry["title"] == "\ub354 \ud53c\ud2b8 \uc2dc\uc98c 2"
    assert entry["release_start_at"] == datetime(2026, 1, 8)
    assert entry["release_end_at"] == datetime(2026, 4, 16)
    assert entry["release_end_status"] == "scheduled"


def test_verify_ott_write_plan_filters_ambiguous_long_running_nonscripted(monkeypatch):
    entry = {
        "title": "1\ubc15 2\uc77c \uc2dc\uc98c4",
        "platform_content_id": "98a25485-7bc4-4cb5-b0cf-58f25e6412d4",
        "platform_url": "https://www.coupangplay.com/content/98a25485-7bc4-4cb5-b0cf-58f25e6412d4",
        "content_url": "https://www.coupangplay.com/content/98a25485-7bc4-4cb5-b0cf-58f25e6412d4",
        "description": "\ub9e4\uc8fc \uc77c\uc694\uc77c \uacf5\uac1c \uc5ec\ud589 \uc608\ub2a5 \ud504\ub85c\uadf8\ub7a8",
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
                "title": "1\ubc15 2\uc77c \uc2dc\uc98c4",
                "content_url": entry["content_url"],
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    def fake_fetch_document(_session, url):
        if "coupangplay.com" in url:
            return {
                "url": url,
                "ok": True,
                "title": "1\ubc15 2\uc77c \uc2dc\uc98c4",
                "payload_titles": ["2 Days & 1 Night"],
                "body_text": "\ub9e4\uc8fc \uc77c\uc694\uc77c \uacf5\uac1c \uc5ec\ud589 \uc608\ub2a5 \ud504\ub85c\uadf8\ub7a8",
                "description": "\ub9e4\uc8fc \uc77c\uc694\uc77c \uacf5\uac1c \uc5ec\ud589 \uc608\ub2a5 \ud504\ub85c\uadf8\ub7a8",
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            }
        return {
            "url": url,
            "ok": True,
            "title": "1\ubc15 2\uc77c/\uc5ed\uc0ac/\uc2dc\uc98c 4 - \ub098\ubb34\uc704\ud0a4",
            "payload_titles": ["1\ubc15 2\uc77c \uc2dc\uc98c 4"],
            "body_text": (
                "1\ubc15 2\uc77c/\uc5ed\uc0ac/\uc2dc\uc98c 4 "
                "2019.08.29 ~ 2019.12.08 "
                "2026.01.04 ~ ON AIR"
            ),
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": datetime(2019, 12, 8),
            "release_end_status": "confirmed",
        }

    monkeypatch.setattr(service, "_fetch_document", fake_fetch_document)
    monkeypatch.setattr(service, "_fetch_coupang_episode_schedule_document", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_collect_public_result_urls",
        lambda _session, _candidate: ["https://namu.wiki/w/1%EB%B0%95%202%EC%9D%BC/%EC%97%AD%EC%82%AC/%EC%8B%9C%EC%A6%8C%204"],
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="coupangplay")

    assert verdict["apply_allowed"] is True
    assert verdict["items"][0]["reason"] == "filtered_out"
    assert entry["exclude_from_sync"] is True
    assert entry["exclude_reason"] == "nonscripted_requires_finite_verified_season"


def test_fetch_document_reads_og_description(monkeypatch):
    class FakeResponse:
        def __init__(self):
            self.text = """
            <html>
              <head>
                <title></title>
                <meta property="og:title" content="신비한 TV 서프라이즈" />
                <meta property="og:description" content="대한민국의 최장수 예능 프로그램. 시사교양 성격을 가진다." />
              </head>
              <body></body>
            </html>
            """
            self.url = "https://namu.wiki/w/%EC%8B%A0%EB%B9%84%ED%95%9C%20TV%20%EC%84%9C%ED%94%84%EB%9D%BC%EC%9D%B4%EC%A6%88"

        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, *_args, **_kwargs):
            return FakeResponse()

    document = service._fetch_document(FakeSession(), "https://namu.wiki/w/test")

    assert document["ok"] is True
    assert document["title"] == "신비한 TV 서프라이즈"
    assert "최장수 예능 프로그램" in document["description"]


def test_build_direct_public_result_urls_adds_short_drama_variant():
    urls = service._build_direct_public_result_urls(
        {
            "title": "아너: 그녀들의 법정",
            "source_item": {},
        }
    )

    assert "https://namu.wiki/w/%EC%95%84%EB%84%88%28%EB%93%9C%EB%9D%BC%EB%A7%88%29" in urls


def test_season_titles_ignore_non_season_public_dates(monkeypatch):
    entry = {
        "title": "사냥개들 시즌 2",
        "platform_content_id": "81444051",
        "platform_url": "https://www.netflix.com/kr/title/81444051",
        "content_url": "https://www.netflix.com/kr/title/81444051",
        "description": "시즌 2, 2026년 4월 3일 공개",
        "release_start_at": datetime(2026, 4, 3),
        "release_end_status": "unknown",
        "cast": [],
    }
    write_plan = {
        "source_name": "netflix",
        "all_content_today": {_canonical_id(): entry},
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "netflix",
                "title": "사냥개들 시즌 2",
                "content_url": entry["content_url"],
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    def fake_fetch_document(_session, url):
        if "netflix.com" in url:
            return {
                "url": url,
                "ok": True,
                "title": "사냥개들 시즌 2",
                "payload_titles": ["Bloodhounds Season 2"],
                "body_text": "사냥개들 시즌 2 2026년 4월 3일 공개",
                "description": "사냥개들 시즌 2 2026년 4월 3일 공개",
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            }
        return {
            "url": url,
            "ok": True,
            "title": "사냥개들(드라마) - 나무위키",
            "payload_titles": ["사냥개들(드라마)"],
            "body_text": "사냥개들(드라마) 시즌 1 2023년 6월 9일 공개",
            "description": "동명의 웹툰을 원작으로 하는 넷플릭스 오리지널 한국 드라마. 방영 목록 시즌 1.",
            "cast": [],
            "release_start_at": datetime(2023, 6, 9),
            "release_end_at": None,
            "release_end_status": "unknown",
        }

    monkeypatch.setattr(service, "_fetch_document", fake_fetch_document)
    monkeypatch.setattr(service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_collect_public_result_urls",
        lambda _session, _candidate: ["https://namu.wiki/w/%EC%82%AC%EB%83%A5%EA%B0%9C%EB%93%A4(%EB%93%9C%EB%9D%BC%EB%A7%88)"],
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="netflix")

    assert verdict["gate"] == "passed"
    assert entry["title"] == "사냥개들 시즌 2"
    assert entry["release_start_at"] == datetime(2026, 4, 3)
    assert entry.get("release_end_at") is None


def test_season_titles_do_not_fallback_to_other_season_ranges_without_anchor_start():
    candidate = {
        "source_name": "netflix",
        "title": "사냥개들 시즌 2",
        "source_item": {
            "title": "사냥개들 시즌 2",
            "title_alias": ["Bloodhounds Season 2"],
            "description": "시즌 2",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://namu.wiki/w/%EC%82%AC%EB%83%A5%EA%B0%9C%EB%93%A4(%EB%93%9C%EB%9D%BC%EB%A7%88)",
            "ok": True,
            "title": "사냥개들(드라마) - 나무위키",
            "payload_titles": ["사냥개들(드라마)"],
            "body_text": "방송 기간 시즌 1 2023년 6월 9일 공개 시즌 2 제작 확정",
            "description": "동명의 웹툰을 원작으로 하는 넷플릭스 오리지널 한국 드라마.",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        }
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["release_start_at"] is None
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"


def test_infers_end_date_from_episode_total_and_weekly_schedule():
    candidate = {
        "source_name": "coupangplay",
        "title": "DTF 세인트루이스",
        "source_item": {
            "title": "DTF 세인트루이스",
            "description": "매주 금요일 오후 8시 공개",
            "raw_schedule_note": "매주 금요일 오후 8시 공개",
            "release_start_at": datetime(2026, 3, 6),
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.coupangplay.com/content/f905b9bc-51ca-4fa3-870d-87230d6001a1",
            "ok": True,
            "title": "DTF 세인트루이스",
            "payload_titles": ["DTF St. Louis"],
            "body_text": "매주 금요일 오후 8시 공개",
            "description": "매주 금요일 오후 8시 공개",
            "cast": [],
            "episode_total": None,
            "release_weekdays": [4],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_public_page",
        },
        {
            "url": "https://en.wikipedia.org/wiki/DTF_St._Louis",
            "ok": True,
            "title": "DTF St. Louis - Wikipedia",
            "payload_titles": ["DTF St. Louis", "DTF 세인트루이스"],
            "body_text": "American television drama No. of episodes 7",
            "description": "Television series with 7 episodes.",
            "cast": [],
            "episode_total": 7,
            "release_weekdays": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["release_start_at"] == datetime(2026, 3, 6)
    assert metadata["release_end_at"] == datetime(2026, 4, 17)
    assert metadata["release_end_status"] == "scheduled"


def test_finite_nonscripted_with_official_episode_schedule_is_not_filtered(monkeypatch):
    entry = {
        "title": "DTF 세인트루이스",
        "platform_content_id": "f905b9bc-51ca-4fa3-870d-87230d6001a1",
        "platform_url": "https://www.coupangplay.com/content/f905b9bc-51ca-4fa3-870d-87230d6001a1",
        "content_url": "https://www.coupangplay.com/content/f905b9bc-51ca-4fa3-870d-87230d6001a1",
        "description": "매주 금요일 오후 8시 공개 스포츠 다큐멘터리",
        "raw_schedule_note": "매주 금요일 오후 8시 공개",
        "release_start_at": datetime(2026, 3, 6),
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
                "title": "DTF 세인트루이스",
                "content_url": entry["content_url"],
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda *_args, **_kwargs: {
            "url": "https://www.coupangplay.com/content/f905b9bc-51ca-4fa3-870d-87230d6001a1",
            "ok": True,
            "title": "DTF 세인트루이스",
            "payload_titles": ["DTF St. Louis"],
            "body_text": "매주 금요일 오후 8시 공개 스포츠 다큐멘터리",
            "description": "매주 금요일 오후 8시 공개 스포츠 다큐멘터리",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
        },
    )
    monkeypatch.setattr(
        service,
        "_fetch_coupang_episode_schedule_document",
        lambda *_args, **_kwargs: {
            "url": "https://www.coupangplay.com/content/f905b9bc-51ca-4fa3-870d-87230d6001a1",
            "ok": True,
            "title": "DTF 세인트루이스",
            "payload_titles": ["DTF St. Louis"],
            "body_text": "1화 3월 6일 공개 7화 4월 17일 공개",
            "description": "",
            "cast": [],
            "episode_total": 7,
            "release_weekdays": [4],
            "release_start_at": datetime(2026, 3, 6),
            "release_end_at": datetime(2026, 4, 17),
            "release_end_status": "scheduled",
            "source": "official_episode_schedule",
        },
    )
    monkeypatch.setattr(service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(service, "_collect_public_result_urls", lambda *_args, **_kwargs: [])

    verdict = service.verify_ott_write_plan(write_plan, source_name="coupangplay")

    assert verdict["gate"] == "passed"
    assert entry.get("exclude_from_sync") is not True
    assert entry["release_end_at"] == datetime(2026, 4, 17)
    assert entry["release_end_status"] == "scheduled"


def test_history_only_season_page_does_not_whitelist_long_running_nonscripted(monkeypatch):
    entry = {
        "title": "1박 2일 시즌4",
        "platform_content_id": "98a25485-7bc4-4cb5-b0cf-58f25e6412d4",
        "platform_url": "https://www.coupangplay.com/content/98a25485-7bc4-4cb5-b0cf-58f25e6412d4",
        "content_url": "https://www.coupangplay.com/content/98a25485-7bc4-4cb5-b0cf-58f25e6412d4",
        "description": "매주 일요일 공개 여행 예능 프로그램",
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
                "title": "1박 2일 시즌4",
                "content_url": entry["content_url"],
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    def fake_fetch_document(_session, url):
        if "coupangplay.com" in url:
            return {
                "url": url,
                "ok": True,
                "title": "1박 2일 시즌4",
                "payload_titles": ["2 Days & 1 Night"],
                "body_text": "매주 일요일 공개 여행 예능 프로그램",
                "description": "매주 일요일 공개 여행 예능 프로그램",
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            }
        return {
            "url": "https://namu.wiki/w/1%EB%B0%95%202%EC%9D%BC/%EC%97%AD%EC%82%AC/%EC%8B%9C%EC%A6%8C%204",
            "ok": True,
            "title": "1박 2일/역사/시즌 4 - 나무위키",
            "payload_titles": ["1박 2일 시즌 4"],
            "body_text": "1박 2일/역사/시즌 4 2019.08.29 ~ 2019.12.08 2026.01.04 ~ ON AIR",
            "description": "",
            "cast": [],
            "release_start_at": datetime(2019, 8, 29),
            "release_end_at": datetime(2019, 12, 8),
            "release_end_status": "confirmed",
        }

    monkeypatch.setattr(service, "_fetch_document", fake_fetch_document)
    monkeypatch.setattr(service, "_fetch_coupang_episode_schedule_document", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "_collect_public_result_urls",
        lambda _session, _candidate: ["https://namu.wiki/w/1%EB%B0%95%202%EC%9D%BC/%EC%97%AD%EC%82%AC/%EC%8B%9C%EC%A6%8C%204"],
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="coupangplay")

    assert verdict["apply_allowed"] is True
    assert verdict["items"][0]["reason"] == "filtered_out"
    assert entry["exclude_from_sync"] is True
    assert entry["exclude_reason"] == "nonscripted_requires_finite_verified_season"


def test_fetch_public_documents_uses_wikipedia_search_alias(monkeypatch):
    candidate = {
        "title": "DTF 세인트루이스",
        "source_item": {
            "title": "DTF 세인트루이스",
            "platform_url": "https://www.coupangplay.com/content/f905b9bc-51ca-4fa3-870d-87230d6001a1",
        },
    }

    monkeypatch.setattr(service, "_build_direct_public_result_urls", lambda _candidate: [])
    monkeypatch.setattr(service, "_collect_public_result_urls", lambda _session, _candidate: [])
    monkeypatch.setattr(
        service,
        "_search_wikipedia_result_candidates",
        lambda _session, _candidate: [
            {
                "url": "https://en.wikipedia.org/wiki/DTF_St._Louis",
                "search_source": "wikipedia_api",
                "search_query": "DTF 세인트루이스",
                "search_title": "DTF St. Louis",
                "allow_query_alias": True,
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda _session, _url: {
            "url": "https://en.wikipedia.org/wiki/DTF_St._Louis",
            "ok": True,
            "title": "DTF St. Louis - Wikipedia",
            "payload_titles": ["DTF St. Louis"],
        },
    )

    documents = service._fetch_public_documents(object(), candidate)

    assert len(documents) == 1
    assert "DTF 세인트루이스" in documents[0]["payload_titles"]
    assert "DTF St. Louis" in documents[0]["payload_titles"]
