import asyncio
import sys
from pathlib import Path

import aiohttp

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


class _RecordingSession:
    created_kwargs = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.created_kwargs.append(kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _StubKakaoWebtoonCrawler(KakaoWebtoonCrawler):
    def __init__(self, *, include_completed=False):
        super().__init__()
        self._include_completed = include_completed

    async def _fetch_placement_entries(self, session, placement, headers):
        if self._include_completed and placement == "timetable_completed":
            entries = [
                {
                    "content_id": "4465",
                    "title": "남주의 남자친구가 내게 집착한다 [19세 완전판]",
                    "authors": ["author"],
                    "thumbnail_url": "https://example.com/thumb.webp",
                    "content_url": "https://webtoon.kakao.com/content/test/4465",
                    "kakao_ongoing_status": None,
                }
            ]
        else:
            entries = []
        meta = {
            "http_status": 200,
            "count": len(entries),
            "stopped_reason": None if entries else "no_data",
        }
        return entries, meta, None

    def _load_completed_candidate_db_info(self, content_ids):
        return {}

    async def _fetch_profile_statuses(self, session, content_ids, headers):
        return [(content_id, "COMPLETED", None, True) for content_id in content_ids]


def _patch_proxy_test_config(monkeypatch, *, profile_budget):
    monkeypatch.setattr(config, "CRAWLER_HTTP_TRUST_ENV", True)
    monkeypatch.setattr(config, "KAKAOWEBTOON_PLACEMENTS_WEEKDAYS", ["timetable_mon"])
    monkeypatch.setattr(config, "KAKAOWEBTOON_PLACEMENT_COMPLETED", "timetable_completed")
    monkeypatch.setattr(config, "KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET", profile_budget)
    monkeypatch.setattr(aiohttp, "ClientSession", _RecordingSession)
    monkeypatch.setattr(aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    _RecordingSession.created_kwargs = []


def test_fetch_all_data_uses_proxy_aware_session_for_placement_fetch(monkeypatch):
    _patch_proxy_test_config(monkeypatch, profile_budget=0)

    crawler = _StubKakaoWebtoonCrawler(include_completed=False)

    asyncio.run(crawler.fetch_all_data())

    assert len(_RecordingSession.created_kwargs) == 1
    assert _RecordingSession.created_kwargs[0]["trust_env"] is True


def test_fetch_all_data_uses_proxy_aware_session_for_profile_lookup(monkeypatch):
    _patch_proxy_test_config(monkeypatch, profile_budget=10)

    crawler = _StubKakaoWebtoonCrawler(include_completed=True)

    _, _, finished_today, _, _ = asyncio.run(crawler.fetch_all_data())

    assert "4465" in finished_today
    assert len(_RecordingSession.created_kwargs) == 2
    assert all(kwargs["trust_env"] is True for kwargs in _RecordingSession.created_kwargs)
