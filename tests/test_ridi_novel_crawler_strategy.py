import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.ridi_novel_crawler import RidiNovelCrawler


def test_webnovel_listing_prefers_api_when_api_returns_data(monkeypatch):
    crawler = RidiNovelCrawler()
    next_data_called = {"value": False}

    async def fake_api_listing(_session, *, root_key, category_id, completed_only):
        assert root_key == "webnovel_romance"
        assert category_id == 1650
        assert completed_only is False
        return (
            {"cid-1": {"content_id": "cid-1", "title": "API Title"}},
            {
                "strategy": "api",
                "unique_contents": 1,
                "errors": [],
                "stopped_reason": "no_next_page",
            },
        )

    async def fake_next_data_listing(*_args, **_kwargs):
        next_data_called["value"] = True
        return {}, {"strategy": "next_data", "unique_contents": 0, "errors": [], "stopped_reason": "empty_page"}

    monkeypatch.setattr(crawler, "_fetch_api_category_listing", fake_api_listing)
    monkeypatch.setattr(crawler, "_fetch_webnovel_listing_from_next_data", fake_next_data_listing)

    entries, meta = asyncio.run(
        crawler._fetch_webnovel_listing(
            None,
            root_key="webnovel_romance",
            category_id=1650,
            completed_only=False,
        )
    )

    assert next_data_called["value"] is False
    assert set(entries.keys()) == {"cid-1"}
    assert meta["strategy"] == "api"


def test_webnovel_listing_falls_back_to_next_data_on_api_empty_or_error(monkeypatch):
    crawler = RidiNovelCrawler()
    next_data_called = {"value": False}

    async def fake_api_listing(_session, *, root_key, category_id, completed_only):
        assert root_key == "webnovel_fantasy"
        assert category_id == 1750
        assert completed_only is True
        return (
            {},
            {
                "strategy": "api",
                "unique_contents": 0,
                "errors": ["API_TIMEOUT"],
                "stopped_reason": "exception",
            },
        )

    async def fake_next_data_listing(_session, *, root_key, category_id, completed_only):
        next_data_called["value"] = True
        assert root_key == "webnovel_fantasy"
        assert category_id == 1750
        assert completed_only is True
        return (
            {"cid-2": {"content_id": "cid-2", "title": "NextData Title"}},
            {
                "strategy": "next_data",
                "unique_contents": 1,
                "errors": [],
                "stopped_reason": "no_next_page",
            },
        )

    monkeypatch.setattr(crawler, "_fetch_api_category_listing", fake_api_listing)
    monkeypatch.setattr(crawler, "_fetch_webnovel_listing_from_next_data", fake_next_data_listing)

    entries, meta = asyncio.run(
        crawler._fetch_webnovel_listing(
            None,
            root_key="webnovel_fantasy",
            category_id=1750,
            completed_only=True,
        )
    )

    assert next_data_called["value"] is True
    assert set(entries.keys()) == {"cid-2"}
    assert meta["strategy"] == "next_data"
    assert meta["fallback_from"] == "api"
    assert meta["api_errors"] == ["API_TIMEOUT"]


def test_fetch_all_data_marks_suspicious_empty_when_every_listing_is_empty(monkeypatch):
    crawler = RidiNovelCrawler()

    async def fake_webnovel_listing(_session, *, root_key, category_id, completed_only):
        return (
            {},
            {
                "strategy": "api",
                "root_key": root_key,
                "category_id": category_id,
                "completed_only": completed_only,
                "unique_contents": 0,
                "errors": [],
                "stopped_reason": "no_next_page",
            },
        )

    async def fake_lightnovel_listing(_session, *, completed_only):
        return (
            {},
            {
                "strategy": "api",
                "root_key": "lightnovel",
                "category_id": crawler.LIGHTNOVEL_ROOT[1],
                "completed_only": completed_only,
                "unique_contents": 0,
                "errors": [],
                "stopped_reason": "no_next_page",
            },
        )

    monkeypatch.setattr(crawler, "_fetch_webnovel_listing", fake_webnovel_listing)
    monkeypatch.setattr(crawler, "_fetch_lightnovel_listing", fake_lightnovel_listing)

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert ongoing == {}
    assert hiatus == {}
    assert finished == {}
    assert all_content == {}
    assert fetch_meta["is_suspicious_empty"] is True
    assert "SUSPICIOUS_EMPTY_RESULT:RIDI" in fetch_meta["errors"]
