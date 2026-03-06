import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import asyncio
import config
from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


def test_extract_profile_status_completed():
    crawler = KakaoWebtoonCrawler()
    payload = {"data": {"badges": [{"type": "STATUS", "code": "COMPLETED"}]}}

    assert crawler._extract_profile_status_from_payload(payload) == "COMPLETED"


def test_extract_profile_status_pause():
    crawler = KakaoWebtoonCrawler()
    payload = {"badges": [{"badgeType": "STATUS", "code": "pause"}]}

    assert crawler._extract_profile_status_from_payload(payload) == "PAUSE"


def test_extract_profile_status_missing():
    crawler = KakaoWebtoonCrawler()
    payload = {"badges": [{"type": "GENRE", "code": "ROMANCE"}]}

    assert crawler._extract_profile_status_from_payload(payload) is None


def test_needs_profile_lookup_ttl_logic():
    crawler = KakaoWebtoonCrawler()
    now = datetime(2024, 1, 10, 10, 0, 0)

    db_info_recent = {
        "status": "완결",
        "kakao_profile_status": "COMPLETED",
        "kakao_profile_status_checked_at": now - timedelta(days=1),
    }
    assert not crawler._needs_profile_lookup("1", db_info_recent, now, ttl_days=7)

    db_info_expired = {
        "status": "완결",
        "kakao_profile_status": "COMPLETED",
        "kakao_profile_status_checked_at": now - timedelta(days=8),
    }
    assert crawler._needs_profile_lookup("1", db_info_expired, now, ttl_days=7)

    db_info_transition = {
        "status": "연재중",
        "kakao_profile_status": "COMPLETED",
        "kakao_profile_status_checked_at": now - timedelta(days=1),
    }
    assert crawler._needs_profile_lookup("1", db_info_transition, now, ttl_days=7)

    db_info_missing = {
        "status": "완결",
        "kakao_profile_status": None,
        "kakao_profile_status_checked_at": None,
    }
    assert crawler._needs_profile_lookup("1", db_info_missing, now, ttl_days=7)

    assert crawler._needs_profile_lookup("1", None, now, ttl_days=7)


class StubProfileLookupFailureCrawler(KakaoWebtoonCrawler):
    def __init__(self):
        super().__init__()
        self._entry = {
            "content_id": "9001",
            "title": "title-9001",
            "authors": ["author"],
            "thumbnail_url": "https://example.com/thumb.webp",
            "content_url": "https://webtoon.kakao.com/content/test/9001",
            "kakao_ongoing_status": None,
        }

    async def _fetch_placement_entries(self, session, placement, headers):
        entries = [dict(self._entry)] if placement == "timetable_completed" else []
        meta = {
            "http_status": 200,
            "count": len(entries),
            "stopped_reason": None if entries else "no_data",
        }
        return entries, meta, None

    def _load_completed_candidate_db_info(self, content_ids):
        return {}

    async def _fetch_profile_statuses(self, session, content_ids, headers):
        return [(content_id, None, "http_404", False) for content_id in content_ids]


def test_profile_lookup_failures_are_best_effort_and_not_fetch_errors(monkeypatch):
    monkeypatch.setattr(config, "KAKAOWEBTOON_PLACEMENTS_WEEKDAYS", ["timetable_mon"])
    monkeypatch.setattr(config, "KAKAOWEBTOON_PLACEMENT_COMPLETED", "timetable_completed")
    monkeypatch.setattr(config, "KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET", 10)

    crawler = StubProfileLookupFailureCrawler()

    ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta = asyncio.run(
        crawler.fetch_all_data()
    )

    assert ongoing_today == {}
    assert hiatus_today == {}
    assert "9001" in finished_today
    assert all_content_today["9001"]["kakao_unverified_completed_candidate"] is True
    assert fetch_meta["errors"] == []
    assert fetch_meta["profile_lookup_failed"] == 1
    assert fetch_meta["profile_lookup_errors"] == ["profile:9001:http_404"]
    assert fetch_meta["profile_status_counts"]["FETCH_FAILED"] == 1
    assert "profile_lookup_partial_failure" in fetch_meta["health_notes"]
