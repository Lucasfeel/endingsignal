import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
import crawlers.naver_series_novel_crawler as naver_module
from crawlers.naver_series_novel_crawler import NaverSeriesNovelCrawler


def test_naver_series_incremental_uses_seed_budgets_and_existing_snapshot(monkeypatch):
    fetch_urls = []

    async def fake_fetch_text_polite(
        _session,
        url,
        *,
        headers,
        retries=2,
        retry_base_delay_seconds=0.5,
        retry_max_delay_seconds=2.0,
        jitter_min_seconds=0.05,
        jitter_max_seconds=0.35,
        sleep_func=asyncio.sleep,
    ):
        fetch_urls.append(url)
        if "genreCode=201" in url:
            return """
            <ul>
              <li>
                <h3><a href="/novel/detail.series?productNo=100" title="기존 작품">기존 작품</a></h3>
              </li>
            </ul>
            """
        if "genreCode=203" in url and "isFinished=true" in url:
            return """
            <ul>
              <li>
                <h3><a href="/novel/detail.series?productNo=200" title="미스터리 완결작">미스터리 완결작</a></h3>
                <p class="info">평점 9.8 | 새 작가 | 2024.01.01</p>
              </li>
            </ul>
            """
        return "<html></html>"

    monkeypatch.setattr(naver_module, "fetch_text_polite", fake_fetch_text_polite)
    monkeypatch.setattr(config, "NAVER_SERIES_INCREMENTAL_ONGOING_MAX_PAGES", 1)
    monkeypatch.setattr(config, "NAVER_SERIES_INCREMENTAL_COMPLETED_MAX_PAGES", 1)

    crawler = NaverSeriesNovelCrawler()
    crawler._prefetch_context = {
        "existing_by_id": {
            "100": {
                "title": "기존 작품",
                "authors": ["기존 작가"],
                "content_url": "https://series.naver.com/novel/detail.series?productNo=100",
                "genres": ["로맨스"],
                "status": "완결",
            }
        }
    }

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert hiatus == {}
    assert ongoing == {}
    assert set(finished.keys()) == {"100", "200"}
    assert all_content["100"]["authors"] == ["기존 작가"]
    assert all_content["100"]["status"] == "완결"
    assert all_content["200"]["status"] == "완결"
    assert fetch_meta["force_no_ratio"] is True
    assert fetch_meta["status"] == "ok"
    assert all("page=2" not in url for url in fetch_urls)
    assert len(fetch_urls) == 16


def test_naver_series_incremental_marks_suspicious_empty(monkeypatch):
    async def fake_fetch_text_polite(
        _session,
        _url,
        *,
        headers,
        retries=2,
        retry_base_delay_seconds=0.5,
        retry_max_delay_seconds=2.0,
        jitter_min_seconds=0.05,
        jitter_max_seconds=0.35,
        sleep_func=asyncio.sleep,
    ):
        return "<html></html>"

    monkeypatch.setattr(naver_module, "fetch_text_polite", fake_fetch_text_polite)
    monkeypatch.setattr(config, "NAVER_SERIES_INCREMENTAL_ONGOING_MAX_PAGES", 1)
    monkeypatch.setattr(config, "NAVER_SERIES_INCREMENTAL_COMPLETED_MAX_PAGES", 1)

    crawler = NaverSeriesNovelCrawler()
    crawler._prefetch_context = {"existing_by_id": {}}

    ongoing, hiatus, finished, all_content, fetch_meta = asyncio.run(crawler.fetch_all_data())

    assert ongoing == {}
    assert hiatus == {}
    assert finished == {}
    assert all_content == {}
    assert fetch_meta["skip_database_sync"] is True
    assert fetch_meta["status"] == "error"
