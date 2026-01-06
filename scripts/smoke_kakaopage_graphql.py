"""Lightweight smoke test for KakaoPage GraphQL staticLandingDayOfWeekSection.

Usage:
    python scripts/smoke_kakaopage_graphql.py

No auth or secrets required; this only performs a public landing request.
"""

import asyncio
import os
import pprint
import sys

import aiohttp

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import config
from services.kakaopage_graphql import (
    build_section_id,
    normalize_kakaopage_param,
    fetch_static_landing_section,
    parse_section_payload,
)

DEFAULT_PARAM = {
    "categoryUid": config.KAKAOPAGE_CATEGORY_UID,
    "bnType": "A",
    "subcategoryUid": "0",
    "dayTabUid": "2",
    "screenUid": config.KAKAOPAGE_DAYOFWEEK_SCREEN_UID,
    "page": 1,
}

HEADERS = {**config.CRAWLER_HEADERS, "Accept": "application/json"}


async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout, headers=HEADERS) as session:
        param = normalize_kakaopage_param(DEFAULT_PARAM)
        section_id = build_section_id(
            param["categoryUid"],
            param["subcategoryUid"],
            param["bnType"],
            param["dayTabUid"],
            param["screenUid"],
        )
        data = await fetch_static_landing_section(session, section_id, param)
        payload = data.get("staticLandingDayOfWeekSection") or {}
        items, meta = parse_section_payload(payload)
        series_ids = [item["series_id"] for item in items if not item.get("isLegacy")]

    print("status: ok")
    print("totalCount:", meta.get("totalCount"))
    print("unique_non_legacy_ids:", len(series_ids))
    print("sample:", series_ids[:10])
    print("params discovered (bm/sub/day):")
    pprint.pprint(
        {
            "businessModelList": meta.get("businessModelList"),
            "subcategoryList": meta.get("subcategoryList"),
            "dayTabList": meta.get("dayTabList"),
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
