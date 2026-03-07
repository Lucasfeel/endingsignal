import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_novel_crawlers


def test_novel_runner_executes_only_novel_crawlers(monkeypatch):
    called = []

    async def fake_run_one_crawler(crawler_class):
        called.append(crawler_class.__name__)
        return {"status": "ok", "crawler_name": crawler_class.__name__, "fetched_count": 1}

    monkeypatch.setattr(run_novel_crawlers, "run_one_crawler", fake_run_one_crawler)

    exit_code = asyncio.run(run_novel_crawlers.main())

    assert exit_code == 0
    assert called == ["NaverSeriesNovelCrawler", "KakaoPageNovelCrawler"]


def test_novel_runner_returns_error_when_any_crawler_errors(monkeypatch):
    async def fake_run_one_crawler(crawler_class):
        status = "error" if crawler_class.__name__ == "KakaoPageNovelCrawler" else "ok"
        return {"status": status, "crawler_name": crawler_class.__name__, "fetched_count": 1}

    monkeypatch.setattr(run_novel_crawlers, "run_one_crawler", fake_run_one_crawler)

    exit_code = asyncio.run(run_novel_crawlers.main())

    assert exit_code == 1
