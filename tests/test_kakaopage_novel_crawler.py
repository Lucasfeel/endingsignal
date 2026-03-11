import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import crawlers.kakaopage_novel_crawler as kakao_module
from crawlers.kakaopage_novel_crawler import KakaoPageNovelCrawler
from utils.polite_http import BlockedError


def test_kakaopage_incremental_merges_existing_records_and_fetches_new_details(monkeypatch):
    async def fake_discover(self, *, existing_by_id):
        assert "1" in existing_by_id
        return (
            {
                "1": {
                    "content_id": "1",
                    "title": "",
                    "authors": [],
                    "status": "완결",
                    "content_url": "https://page.kakao.com/content/1",
                    "genres": ["판타지"],
                    "crawl_roots": ["판타지"],
                    "seed_completed": True,
                },
                "2": {
                    "content_id": "2",
                    "title": "신규 작품",
                    "authors": [],
                    "status": "연재중",
                    "content_url": "https://page.kakao.com/content/2",
                    "genres": ["현판"],
                    "crawl_roots": ["현판"],
                    "seed_completed": False,
                },
            },
            {"seeds": {}, "health_notes": ["SEED_OPEN_FAILED:romance"], "failed_seed_count": 1},
        )

    async def fake_fetch_detail(
        *,
        session,
        content_id,
        discovered_entry,
        headers,
        retries,
        retry_base_delay_seconds,
        retry_max_delay_seconds,
        canonical_fallback_enabled=True,
    ):
        assert content_id == "2"
        return {
            "content_id": "2",
            "source": "kakao_page",
            "title": "신규 작품",
            "authors": ["새 작가"],
            "status": "연재중",
            "content_url": "https://page.kakao.com/content/2",
            "genres": ["현판"],
        }

    monkeypatch.setattr(KakaoPageNovelCrawler, "discover_listing_entries", fake_discover)
    monkeypatch.setattr(kakao_module, "fetch_kakao_detail_and_build_record", fake_fetch_detail)

    crawler = KakaoPageNovelCrawler()
    crawler._prefetch_context = {
        "existing_by_id": {
            "1": {
                "title": "기존 작품",
                "authors": ["기존 작가"],
                "content_url": "https://page.kakao.com/content/1",
                "genres": ["판타지"],
                "crawl_roots": ["판타지"],
                "status": "연재중",
            }
        }
    }

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert hiatus == {}
    assert set(all_content.keys()) == {"1", "2"}
    assert all_content["1"]["authors"] == ["기존 작가"]
    assert all_content["1"]["status"] == "완결"
    assert all_content["2"]["authors"] == ["새 작가"]
    assert set(ongoing.keys()) == {"2"}
    assert set(finished.keys()) == {"1"}
    assert fetch_meta["status"] == "warn"
    assert fetch_meta["errors"] == []
    assert fetch_meta["fetched_count"] == 2


def test_kakaopage_incremental_skips_blocked_new_items_without_source_failure(monkeypatch):
    async def fake_discover(self, *, existing_by_id):
        return (
            {
                "1": {
                    "content_id": "1",
                    "title": "기존 작품",
                    "authors": [],
                    "status": "연재중",
                    "content_url": "https://page.kakao.com/content/1",
                    "genres": ["판타지"],
                    "crawl_roots": ["판타지"],
                    "seed_completed": False,
                },
                "2": {
                    "content_id": "2",
                    "title": "차단 작품",
                    "authors": [],
                    "status": "연재중",
                    "content_url": "https://page.kakao.com/content/2",
                    "genres": ["판타지"],
                    "crawl_roots": ["판타지"],
                    "seed_completed": False,
                },
            },
            {"seeds": {}, "health_notes": [], "failed_seed_count": 0},
        )

    async def fake_fetch_detail(**kwargs):
        raise BlockedError(status=200, url="https://page.kakao.com/content/2", diagnostics={})

    monkeypatch.setattr(KakaoPageNovelCrawler, "discover_listing_entries", fake_discover)
    monkeypatch.setattr(kakao_module, "fetch_kakao_detail_and_build_record", fake_fetch_detail)

    crawler = KakaoPageNovelCrawler()
    crawler._prefetch_context = {
        "existing_by_id": {
            "1": {
                "title": "기존 작품",
                "authors": ["기존 작가"],
                "content_url": "https://page.kakao.com/content/1",
                "genres": ["판타지"],
                "crawl_roots": ["판타지"],
                "status": "연재중",
            }
        }
    }

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert hiatus == {}
    assert set(ongoing.keys()) == {"1"}
    assert finished == {}
    assert set(all_content.keys()) == {"1"}
    assert fetch_meta["status"] == "ok"
    assert fetch_meta["errors"] == []
    assert any(note.startswith("DETAIL_BLOCKED:2") for note in fetch_meta["health_notes"])


def test_kakaopage_incremental_marks_zero_discovery_as_suspicious_empty(monkeypatch):
    async def fake_discover(self, *, existing_by_id):
        return ({}, {"seeds": {}, "health_notes": [], "failed_seed_count": 12})

    monkeypatch.setattr(KakaoPageNovelCrawler, "discover_listing_entries", fake_discover)

    crawler = KakaoPageNovelCrawler()
    crawler._prefetch_context = {"existing_by_id": {}}

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert ongoing == {}
    assert hiatus == {}
    assert finished == {}
    assert all_content == {}
    assert fetch_meta["skip_database_sync"] is True
    assert fetch_meta["status"] == "error"


def test_kakaopage_incremental_can_limit_new_detail_fetch(monkeypatch):
    async def fake_discover(self, *, existing_by_id):
        assert "1" in existing_by_id
        return (
            {
                "1": {
                    "content_id": "1",
                    "title": "기존 작품",
                    "authors": [],
                    "status": "연재중",
                    "content_url": "https://page.kakao.com/content/1",
                    "genres": ["판타지"],
                    "crawl_roots": ["판타지"],
                    "seed_completed": False,
                },
                "2": {
                    "content_id": "2",
                    "title": "신규 작품 1",
                    "authors": [],
                    "status": "연재중",
                    "content_url": "https://page.kakao.com/content/2",
                    "genres": ["판타지"],
                    "crawl_roots": ["판타지"],
                    "seed_completed": False,
                },
                "3": {
                    "content_id": "3",
                    "title": "신규 작품 2",
                    "authors": [],
                    "status": "연재중",
                    "content_url": "https://page.kakao.com/content/3",
                    "genres": ["판타지"],
                    "crawl_roots": ["판타지"],
                    "seed_completed": False,
                },
            },
            {"seeds": {}, "health_notes": [], "failed_seed_count": 0},
        )

    async def fake_fetch_detail(**kwargs):
        raise AssertionError("detail fetch should be skipped when limit is 0")

    monkeypatch.setattr(KakaoPageNovelCrawler, "discover_listing_entries", fake_discover)
    monkeypatch.setattr(kakao_module, "fetch_kakao_detail_and_build_record", fake_fetch_detail)
    monkeypatch.setenv("KAKAOPAGE_INCREMENTAL_MAX_NEW_DETAILS", "0")

    crawler = KakaoPageNovelCrawler()
    crawler._prefetch_context = {
        "existing_by_id": {
            "1": {
                "title": "기존 작품",
                "authors": ["기존 작가"],
                "content_url": "https://page.kakao.com/content/1",
                "genres": ["판타지"],
                "crawl_roots": ["판타지"],
                "status": "연재중",
            }
        }
    }

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert hiatus == {}
    assert finished == {}
    assert set(ongoing.keys()) == {"1"}
    assert set(all_content.keys()) == {"1"}
    assert fetch_meta["status"] == "warn"
    assert fetch_meta["new_detail_limit"] == 0
    assert fetch_meta["new_detail_candidate_count"] == 2
    assert fetch_meta["new_detail_fetch_count"] == 0
    assert any(note.startswith("DETAIL_FETCH_LIMIT_APPLIED:kept=0:skipped=2:limit=0") for note in fetch_meta["health_notes"])
