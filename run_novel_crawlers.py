import asyncio
import json
import sys
import time
import traceback

from dotenv import load_dotenv

load_dotenv()

from crawlers.kakaopage_novel_crawler import KakaoPageNovelCrawler
from crawlers.naver_series_novel_crawler import NaverSeriesNovelCrawler
from run_all_crawlers import normalize_runtime_status, run_one_crawler


NOVEL_CRAWLERS = [
    NaverSeriesNovelCrawler,
    KakaoPageNovelCrawler,
]


async def main():
    start_time = time.time()
    print("==========================================", flush=True)
    print("   novel incremental crawler run start", flush=True)
    print("==========================================", flush=True)

    results = await asyncio.gather(
        *(run_one_crawler(crawler_class) for crawler_class in NOVEL_CRAWLERS),
        return_exceptions=True,
    )

    has_error = False
    has_warn = False
    summarized_results = []
    for result in results:
        if isinstance(result, Exception):
            has_error = True
            print(f"ERROR: novel crawler gather failure: {result}", file=sys.stderr, flush=True)
            continue
        summarized_results.append(result)
        status = normalize_runtime_status(result.get("status"))
        if status == "error":
            has_error = True
        elif status == "warn":
            has_warn = True

    payload = {
        "results": summarized_results,
        "duration_seconds": round(time.time() - start_time, 3),
    }
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    print("==========================================", flush=True)
    print("   novel incremental crawler run end", flush=True)
    print("==========================================", flush=True)
    return 1 if has_error else 0


if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = asyncio.run(main())
    except Exception:
        print("ERROR: novel crawler execution crashed.", file=sys.stderr, flush=True)
        traceback.print_exc()
        exit_code = 1
    sys.exit(exit_code)
