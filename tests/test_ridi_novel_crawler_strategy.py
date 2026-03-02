import asyncio
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.ridi_novel_crawler import RidiNovelCrawler


def _next_data_item(content_id: str):
    return {
        "book": {
            "bookId": content_id,
            "title": f"Title {content_id}",
            "authors": [{"name": "Writer A", "role": "author"}],
            "categories": [],
        }
    }


def test_webnovel_listing_routes_to_next_data_endpoint(monkeypatch):
    crawler = RidiNovelCrawler()
    captured = {}

    async def fake_next_data_listing(_session, *, endpoint, completed_only):
        captured["key"] = endpoint.key
        captured["category_id"] = endpoint.category_id
        captured["completed_only"] = completed_only
        return (
            {"cid-1": {"content_id": "cid-1", "title": "NextData Title"}},
            {
                "strategy": "next_data",
                "unique_contents": 1,
                "errors": [],
                "stopped_reason": "no_next_page",
            },
        )

    monkeypatch.setattr(crawler, "_fetch_webnovel_listing_from_next_data", fake_next_data_listing)

    entries, meta = asyncio.run(
        crawler._fetch_webnovel_listing(
            None,
            root_key="romance",
            category_id=1650,
            completed_only=False,
        )
    )

    assert captured == {"key": "romance", "category_id": 1650, "completed_only": False}
    assert set(entries.keys()) == {"cid-1"}
    assert meta["strategy"] == "next_data"


def test_webnovel_listing_raises_for_non_next_data_strategy():
    crawler = RidiNovelCrawler()

    with pytest.raises(ValueError, match="RIDI_ENDPOINT_STRATEGY_MISMATCH"):
        asyncio.run(
            crawler._fetch_webnovel_listing(
                None,
                root_key="light_novel",
                category_id=3000,
                completed_only=True,
            )
        )


def test_fetch_all_data_marks_suspicious_empty_when_every_listing_is_empty(monkeypatch):
    crawler = RidiNovelCrawler()

    async def fake_webnovel_listing(_session, *, root_key, category_id, completed_only):
        return (
            {},
            {
                "strategy": "next_data",
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
                "root_key": "light_novel",
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


def test_next_data_listing_stops_on_repeated_page_signature(monkeypatch):
    crawler = RidiNovelCrawler()
    endpoint = crawler._get_endpoint("romance")

    async def fake_fetch_page_items(_session, _endpoint, page, *, completed_only):
        assert completed_only is False
        if page == 1:
            ids = ["1", "2"]
        elif page == 2:
            ids = ["3", "4"]
        else:
            ids = ["3", "4"]
        return ([_next_data_item(content_id) for content_id in ids], True)

    monkeypatch.setattr(crawler, "_fetch_webnovel_page_items", fake_fetch_page_items)
    monkeypatch.setattr(crawler, "_max_pages_per_category", lambda: 10)
    monkeypatch.setattr(crawler, "_no_new_ids_threshold", lambda: 2)

    entries, meta = asyncio.run(
        crawler._fetch_webnovel_listing_from_next_data(
            None,
            endpoint=endpoint,
            completed_only=False,
        )
    )

    assert meta["stopped_reason"] == "repeated_page_signature"
    assert meta["unique_contents"] == 4
    assert meta["pages_fetched"] == 3
    assert set(entries.keys()) == {"1", "2", "3", "4"}


def test_next_data_listing_stops_on_no_new_ids_when_signature_changes(monkeypatch):
    crawler = RidiNovelCrawler()
    endpoint = crawler._get_endpoint("fantasy")

    async def fake_fetch_page_items(_session, _endpoint, page, *, completed_only):
        assert completed_only is True
        if page == 1:
            ids = ["1", "2"]
        elif page == 2:
            ids = ["3", "4"]
        else:
            ids = ["4", "3"]
        return ([_next_data_item(content_id) for content_id in ids], True)

    monkeypatch.setattr(crawler, "_fetch_webnovel_page_items", fake_fetch_page_items)
    monkeypatch.setattr(crawler, "_max_pages_per_category", lambda: 10)
    monkeypatch.setattr(crawler, "_no_new_ids_threshold", lambda: 1)

    entries, meta = asyncio.run(
        crawler._fetch_webnovel_listing_from_next_data(
            None,
            endpoint=endpoint,
            completed_only=True,
        )
    )

    assert meta["stopped_reason"] == "no_new_ids"
    assert meta["unique_contents"] == 4
    assert meta["pages_fetched"] == 3
    assert set(entries.keys()) == {"1", "2", "3", "4"}
