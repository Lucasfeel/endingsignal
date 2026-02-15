import asyncio
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_all_crawlers


def test_rollup_total_below_target_does_not_force_kakao_failed():
    kakao_fetch_failed = run_all_crawlers.is_kakao_fetch_failed(
        kakao_status="warn",
        kakao_unique=12345,
        kakao_fetch_meta={
            "errors": [],
            "health_notes": ["finished_count_zero"],
            "health_warnings": [],
        },
    )

    warnings = run_all_crawlers.build_rollup_warning_reasons(
        actual_total_unique=19000,
        target_total_unique=20000,
        kakao_fetch_failed=kakao_fetch_failed,
    )

    assert warnings == ["TOTAL_UNIQUE_BELOW_TARGET"]


def test_rollup_marks_kakao_failed_when_unique_is_zero():
    kakao_fetch_failed = run_all_crawlers.is_kakao_fetch_failed(
        kakao_status="ok",
        kakao_unique=0,
        kakao_fetch_meta={"errors": [], "health_notes": ["finished_count_zero"]},
    )

    warnings = run_all_crawlers.build_rollup_warning_reasons(
        actual_total_unique=20000,
        target_total_unique=20000,
        kakao_fetch_failed=kakao_fetch_failed,
    )

    assert warnings == ["KAKAO_FETCH_FAILED"]


def test_rollup_target_env_unset_disables_total_threshold(monkeypatch):
    monkeypatch.delenv("ROLLUP_TARGET_TOTAL_UNIQUE", raising=False)

    target, warning = run_all_crawlers.get_rollup_target_total_unique()
    warnings = run_all_crawlers.build_rollup_warning_reasons(
        actual_total_unique=1,
        target_total_unique=target,
        kakao_fetch_failed=False,
    )

    assert target is None
    assert warning is None
    assert warnings == []


def test_rollup_target_env_parses_integer(monkeypatch):
    monkeypatch.setenv("ROLLUP_TARGET_TOTAL_UNIQUE", "20000")

    target, warning = run_all_crawlers.get_rollup_target_total_unique()
    warnings = run_all_crawlers.build_rollup_warning_reasons(
        actual_total_unique=19999,
        target_total_unique=target,
        kakao_fetch_failed=False,
    )

    assert target == 20000
    assert warning is None
    assert warnings == ["TOTAL_UNIQUE_BELOW_TARGET"]


def test_rollup_target_env_invalid_disables_threshold(monkeypatch):
    monkeypatch.setenv("ROLLUP_TARGET_TOTAL_UNIQUE", "abc")

    target, warning = run_all_crawlers.get_rollup_target_total_unique()
    warnings = run_all_crawlers.build_rollup_warning_reasons(
        actual_total_unique=10,
        target_total_unique=target,
        kakao_fetch_failed=False,
    )

    assert target is None
    assert warning is not None
    assert warnings == []


def test_kakao_fetch_failed_false_when_only_health_notes():
    failed = run_all_crawlers.is_kakao_fetch_failed(
        kakao_status="ok",
        kakao_unique=123,
        kakao_fetch_meta={"health_notes": ["finished_count_zero"]},
    )

    assert failed is False


def test_kakao_fetch_failed_true_for_health_warning():
    failed = run_all_crawlers.is_kakao_fetch_failed(
        kakao_status="ok",
        kakao_unique=123,
        kakao_fetch_meta={"health_warnings": ["SUSPICIOUS_EMPTY_RESULT"]},
    )

    assert failed is True


def test_main_warn_uses_warning_prefix_and_exit_zero(monkeypatch, capsys):
    class FakeNaver:
        pass

    class FakeKakao:
        pass

    async def fake_run_one_crawler(crawler_class):
        name = crawler_class.__name__.lower()
        if "kakao" in name:
            return {
                "status": "warn",
                "crawler_name": "Kakaowebtoon",
                "fetched_count": 100,
                "summary": {"reason": "degraded", "message": "fetch ratio low"},
                "cdc_info": {"fetch_meta": {"errors": [], "health_warnings": []}},
            }
        return {
            "status": "ok",
            "crawler_name": "Naver Webtoon",
            "fetched_count": 50,
            "summary": {"reason": "ok", "message": "crawler run completed"},
            "cdc_info": {"fetch_meta": {"errors": []}},
        }

    monkeypatch.setattr(run_all_crawlers, "ALL_CRAWLERS", [FakeNaver, FakeKakao])
    monkeypatch.setattr(run_all_crawlers, "run_one_crawler", fake_run_one_crawler)
    monkeypatch.setattr(
        run_all_crawlers, "run_scheduled_completion_cdc", lambda: {"status": "success"}
    )
    monkeypatch.setattr(
        run_all_crawlers, "run_scheduled_publication_cdc", lambda: {"status": "success"}
    )
    monkeypatch.setenv("ROLLUP_TARGET_TOTAL_UNIQUE", "200")

    exit_code = asyncio.run(run_all_crawlers.main())
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "WARNING: TOTAL_UNIQUE_BELOW_TARGET" in captured.out
    assert "ERROR: TOTAL_UNIQUE_BELOW_TARGET" not in captured.out


def test_main_error_uses_error_prefix_and_exit_one(monkeypatch, capsys):
    class FakeKakao:
        pass

    async def fake_run_one_crawler(_crawler_class):
        return {
            "status": "error",
            "crawler_name": "Kakaowebtoon",
            "fetched_count": 0,
            "summary": {"reason": "exception", "message": "fatal"},
            "cdc_info": {
                "fetch_meta": {
                    "errors": [
                        "SECTION_PARSE_ERROR:ongoing:timetable_mon:http_500",
                    ],
                    "health_warnings": [],
                }
            },
        }

    monkeypatch.setattr(run_all_crawlers, "ALL_CRAWLERS", [FakeKakao])
    monkeypatch.setattr(run_all_crawlers, "run_one_crawler", fake_run_one_crawler)
    monkeypatch.setattr(
        run_all_crawlers, "run_scheduled_completion_cdc", lambda: {"status": "success"}
    )
    monkeypatch.setattr(
        run_all_crawlers, "run_scheduled_publication_cdc", lambda: {"status": "success"}
    )
    monkeypatch.delenv("ROLLUP_TARGET_TOTAL_UNIQUE", raising=False)

    exit_code = asyncio.run(run_all_crawlers.main())
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "ERROR: KAKAO_FETCH_FAILED" in captured.out
