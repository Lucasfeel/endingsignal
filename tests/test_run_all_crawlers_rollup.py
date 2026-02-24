import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_all_crawlers


def test_format_final_collection_summary_orders_known_sources_first():
    results = [
        {"summary": {"crawler": "ridi"}, "fetched_count": 20000},
        {"summary": {"crawler": "kakaowebtoon"}, "fetched_count": 2000},
        {"summary": {"crawler": "naver_webtoon"}, "fetched_count": 3000},
    ]

    summary = run_all_crawlers.format_final_collection_summary(results)

    assert summary == "카카오웹툰 2000개 수집, 네이버웹툰 3000개 수집, 리디 20000개 수집, 총 25000개 수집"


def test_format_final_collection_summary_appends_unknown_sources():
    results = [
        {"summary": {"crawler": "laftel"}, "fetched_count": 12},
        {"summary": {"crawler": "ridi"}, "fetched_count": 5},
    ]

    summary = run_all_crawlers.format_final_collection_summary(results)

    assert summary == "리디 5개 수집, laftel 12개 수집, 총 17개 수집"
