import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


def _make_entry(content_id, status):
    return {
        "content_id": str(content_id),
        "title": f"title-{content_id}",
        "authors": ["author"],
        "thumbnail_url": "https://example.com/thumb.webp",
        "content_url": f"https://webtoon.kakao.com/content/test/{content_id}",
        "kakao_ongoing_status": status,
    }


class StubKakaoWebtoonCrawler(KakaoWebtoonCrawler):
    def __init__(self, entries_by_placement):
        super().__init__()
        self._entries_by_placement = entries_by_placement

    async def _fetch_placement_entries(self, session, placement, headers):
        entries = [dict(entry) for entry in self._entries_by_placement.get(placement, [])]
        meta = {
            "http_status": 200,
            "count": len(entries),
            "stopped_reason": None if entries else "no_data",
        }
        return entries, meta, None

    def _load_completed_candidate_db_info(self, content_ids):
        return {}


def _patch_kakao_config(monkeypatch):
    monkeypatch.setattr(config, "KAKAOWEBTOON_PLACEMENTS_WEEKDAYS", ["timetable_mon"])
    monkeypatch.setattr(config, "KAKAOWEBTOON_PLACEMENT_COMPLETED", "timetable_completed")
    monkeypatch.setattr(config, "KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET", 0)


def test_completed_placement_completed_status_goes_to_finished_without_profile_lookup(monkeypatch):
    _patch_kakao_config(monkeypatch)
    crawler = StubKakaoWebtoonCrawler(
        {
            "timetable_completed": [_make_entry("9001", "COMPLETED")],
        }
    )

    ongoing_today, hiatus_today, finished_today, _, _ = asyncio.run(crawler.fetch_all_data())

    assert "9001" in finished_today
    assert "9001" not in hiatus_today
    assert "9001" not in ongoing_today


def test_completed_placement_pause_status_goes_to_hiatus_without_profile_lookup(monkeypatch):
    _patch_kakao_config(monkeypatch)
    crawler = StubKakaoWebtoonCrawler(
        {
            "timetable_completed": [_make_entry("9002", "PAUSE")],
        }
    )

    ongoing_today, hiatus_today, finished_today, _, _ = asyncio.run(crawler.fetch_all_data())

    assert "9002" in hiatus_today
    assert "9002" not in finished_today
    assert "9002" not in ongoing_today


def test_completed_placement_season_completed_status_goes_to_hiatus_without_profile_lookup(monkeypatch):
    _patch_kakao_config(monkeypatch)
    crawler = StubKakaoWebtoonCrawler(
        {
            "timetable_completed": [_make_entry("9003", "SEASON_COMPLETED")],
        }
    )

    ongoing_today, hiatus_today, finished_today, _, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert "9003" in hiatus_today
    assert "9003" not in finished_today
    assert "9003" not in ongoing_today
    assert "pause_found_in_completed_placement" in fetch_meta.get("health_notes", [])


def test_weekday_season_completed_status_is_hiatus_like(monkeypatch):
    _patch_kakao_config(monkeypatch)
    crawler = StubKakaoWebtoonCrawler(
        {
            "timetable_mon": [_make_entry("9101", "SEASON_COMPLETED")],
            "timetable_completed": [],
        }
    )

    ongoing_today, hiatus_today, finished_today, _, _ = asyncio.run(crawler.fetch_all_data())

    assert "9101" in hiatus_today
    assert "9101" not in finished_today
    assert "9101" not in ongoing_today
