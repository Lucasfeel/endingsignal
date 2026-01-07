import asyncio

import aiohttp

import config
from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


async def main():
    crawler = KakaoWebtoonCrawler()
    headers = crawler._build_headers()
    timeout = aiohttp.ClientTimeout(
        total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
        connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
        sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
    )
    connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)
    placements = ["timetable_tue", config.KAKAOWEBTOON_PLACEMENT_COMPLETED]

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for placement in placements:
            entries, meta, error = await crawler._fetch_placement_entries(session, placement, headers)
            label = "weekday" if placement.startswith("timetable_") else "completed"
            print(f"\n[{label}:{placement}] http={meta.get('http_status')} count={meta.get('count')} error={error}")
            for entry in entries[:3]:
                print(f"  - {entry['title']} ({entry['content_id']})")


if __name__ == "__main__":
    asyncio.run(main())
