import sys

import scripts.backfill_kakao_page_only as kakao_only
import scripts.backfill_naver_series_only as naver_only


def test_strip_sources_args_removes_flag_and_value():
    cleaned = naver_only._strip_sources_args(["--sources", "kakao_page", "--dry-run"])

    assert cleaned == ["--dry-run"]


def test_strip_sources_args_handles_missing_value_without_swallowing_next_flag():
    cleaned = naver_only._strip_sources_args(["--sources", "--dry-run", "--max-pages", "1"])

    assert cleaned == ["--dry-run", "--max-pages", "1"]


def test_strip_sources_args_handles_equals_syntax():
    cleaned = kakao_only._strip_sources_args(["--sources=kakao_page", "--dry-run"])

    assert cleaned == ["--dry-run"]


def test_naver_wrapper_build_exec_argv_forces_naver_series_only():
    argv = naver_only.build_exec_argv(["--sources", "kakao_page", "--dry-run", "--max-pages", "1"])

    assert argv[:4] == [sys.executable, "scripts/backfill_novels_once.py", "--sources", "naver_series"]
    assert "--dry-run" in argv
    assert "--max-pages" in argv
    assert "kakao_page" not in argv


def test_kakao_wrapper_build_exec_argv_uses_default_seed_when_env_unset(monkeypatch):
    monkeypatch.delenv("KAKAOPAGE_SEED_SET", raising=False)
    monkeypatch.delenv("KAKAOPAGE_BACKFILL_PHASE", raising=False)

    argv = kakao_only.build_exec_argv(["--dry-run"])

    assert argv[:4] == [sys.executable, "scripts/backfill_novels_once.py", "--sources", "kakao_page"]
    assert "--kakaopage-seed-set" in argv
    seed_idx = argv.index("--kakaopage-seed-set")
    assert argv[seed_idx + 1] == "webnoveldb"
    assert "--kakaopage-phase" in argv
    phase_idx = argv.index("--kakaopage-phase")
    assert argv[phase_idx + 1] == "all"
    assert "--dry-run" in argv


def test_kakao_wrapper_forwarded_seed_set_wins_over_default():
    argv = kakao_only.build_exec_argv(
        ["--kakaopage-seed-set", "all", "--kakaopage-phase", "detail", "--dry-run"],
        seed_set="webnoveldb",
        phase="all",
    )

    indices = [idx for idx, token in enumerate(argv) if token == "--kakaopage-seed-set"]
    assert len(indices) == 2
    assert argv[indices[0] + 1] == "webnoveldb"
    assert argv[indices[-1] + 1] == "all"
    phase_indices = [idx for idx, token in enumerate(argv) if token == "--kakaopage-phase"]
    assert len(phase_indices) == 2
    assert argv[phase_indices[0] + 1] == "all"
    assert argv[phase_indices[-1] + 1] == "detail"
