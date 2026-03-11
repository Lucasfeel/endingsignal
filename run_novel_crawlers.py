import sys

from dotenv import load_dotenv

load_dotenv()

from crawlers.kakaopage_novel_crawler import KakaoPageNovelCrawler
from crawlers.naver_series_novel_crawler import NaverSeriesNovelCrawler
from run_all_crawlers import run_cli, run_crawler_suite, run_one_crawler


NOVEL_CRAWLERS = [
    NaverSeriesNovelCrawler,
    KakaoPageNovelCrawler,
]


async def main():
    return await run_crawler_suite(
        NOVEL_CRAWLERS,
        suite_display_name="novel incremental crawler run",
        runner=run_one_crawler,
        emit_json_payload=True,
    )


if __name__ == "__main__":
    sys.exit(run_cli(main, "novel-crawler"))
