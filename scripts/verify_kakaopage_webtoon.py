"""Run KakaoPage webtoon crawler in verify mode and print fetch_meta.

Usage:
    python scripts/verify_kakaopage_webtoon.py

Environment:
    KAKAOWEBTOON_WEBID and KAKAOWEBTOON_T_ANO can be set to seed cookies.
"""

import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import asyncio
import json

from crawlers.kakaopage_webtoon_crawler import KakaoPageWebtoonCrawler


async def main():
    crawler = KakaoPageWebtoonCrawler()
    crawler.mode = "verify"
    _, _, _, _, fetch_meta = await crawler.fetch_all_data()
    print(json.dumps(fetch_meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
