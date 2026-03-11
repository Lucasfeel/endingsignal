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

    assert summary == (
        "\ub124\uc774\ubc84 \uc6f9\ud230 3000\uac1c \uc218\uc9d1, "
        "\uce74\uce74\uc624\uc6f9\ud230 2000\uac1c \uc218\uc9d1, "
        "\ub9ac\ub514 20000\uac1c \uc218\uc9d1, "
        "\ucd1d 25000\uac1c \uc218\uc9d1"
    )


def test_format_final_collection_summary_appends_unknown_sources():
    results = [
        {"summary": {"crawler": "laftel"}, "fetched_count": 12},
        {"summary": {"crawler": "ridi"}, "fetched_count": 5},
    ]

    summary = run_all_crawlers.format_final_collection_summary(results)

    assert summary == (
        "\ub9ac\ub514 5\uac1c \uc218\uc9d1, "
        "\ub77c\ud504\ud154 12\uac1c \uc218\uc9d1, "
        "\ucd1d 17\uac1c \uc218\uc9d1"
    )


def test_build_rollup_payload_sums_all_registered_sources(monkeypatch):
    monkeypatch.delenv("ROLLUP_TARGET_TOTAL_UNIQUE", raising=False)
    results = [
        {"source_name": "naver_webtoon", "fetched_count": 3},
        {"source_name": "kakaowebtoon", "fetched_count": 5},
        {"source_name": "naver_series", "fetched_count": 7},
        {"source_name": "kakao_page", "fetched_count": 11},
        {"source_name": "ridi", "fetched_count": 13},
        {"source_name": "laftel", "fetched_count": 17},
    ]

    rollup, target_warning, warning_reasons, kakao_rollup_log = run_all_crawlers.build_rollup_payload(
        results,
        include_target_total_check=True,
        include_kakao_fetch_check=False,
    )

    assert rollup["counts_by_source"] == {
        "naver_webtoon": 3,
        "kakaowebtoon": 5,
        "naver_series": 7,
        "kakao_page": 11,
        "ridi": 13,
        "laftel": 17,
    }
    assert rollup["actual_total_unique"] == 56
    assert rollup["naver_unique"] == 3
    assert rollup["kakao_unique"] == 5
    assert target_warning is None
    assert warning_reasons == []
    assert kakao_rollup_log is None
