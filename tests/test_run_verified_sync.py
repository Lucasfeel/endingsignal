import asyncio
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_verified_sync


class FakeConnection:
    def close(self):
        return None


def test_verified_sync_if_stale_skips_when_fresh(monkeypatch):
    called = {"suite": False}

    monkeypatch.setattr(run_verified_sync, "create_standalone_connection", lambda: FakeConnection())
    monkeypatch.setattr(
        run_verified_sync,
        "get_verified_sync_freshness",
        lambda conn, enabled_sources, pipeline, stale_after_hours: {
            "stale": False,
            "pipeline": pipeline,
            "freshness_cutoff": "2026-03-08T02:00:00",
            "enabled_sources": enabled_sources,
            "missing_sources": [],
            "non_terminal_sources": [],
            "stale_sources": [],
        },
    )
    monkeypatch.setattr(
        run_verified_sync,
        "run_crawler_suite",
        lambda *args, **kwargs: called.__setitem__("suite", True),
    )

    exit_code = asyncio.run(
        run_verified_sync._run_verified_sync(
            run_verified_sync.parse_args(["--if-stale", "--sources", "naver_webtoon"])
        )
    )

    assert exit_code == 0
    assert called["suite"] is False


def test_verified_sync_require_ac_defers_on_battery(monkeypatch):
    called = {"suite": False}

    monkeypatch.setattr(run_verified_sync, "create_standalone_connection", lambda: FakeConnection())
    monkeypatch.setattr(
        run_verified_sync,
        "get_verified_sync_freshness",
        lambda conn, enabled_sources, pipeline, stale_after_hours: {
            "stale": True,
            "pipeline": pipeline,
            "freshness_cutoff": "2026-03-08T02:00:00",
            "enabled_sources": enabled_sources,
            "missing_sources": enabled_sources,
            "non_terminal_sources": [],
            "stale_sources": [],
        },
    )
    monkeypatch.setattr(run_verified_sync, "is_on_ac_power", lambda: False)
    monkeypatch.setattr(
        run_verified_sync,
        "run_crawler_suite",
        lambda *args, **kwargs: called.__setitem__("suite", True),
    )

    exit_code = asyncio.run(
        run_verified_sync._run_verified_sync(
            run_verified_sync.parse_args(["--require-ac", "--sources", "naver_webtoon"])
        )
    )

    assert exit_code == 0
    assert called["suite"] is False


def test_verified_sync_builds_runner_context_and_writes_artifacts(monkeypatch, tmp_path):
    captured = {}

    async def fake_run_crawler_suite(*crawler_args, **crawler_kwargs):
        captured["suite_args"] = crawler_args
        captured["suite_kwargs"] = crawler_kwargs
        crawler_kwargs["result_handler"](
            {
                "results": [
                    {
                        "source_name": "naver_webtoon",
                        "crawler_name": "Naver Webtoon",
                        "status": "ok",
                        "verification_gate": {"status": "passed"},
                        "apply_result": "dry_run",
                    }
                ],
                "rollup": {"actual_total_unique": 1},
                "action_reports": [],
                "final_status": "ok",
                "duration_seconds": 0.5,
            }
        )
        return 0

    def fake_build_crawler_runner(**kwargs):
        captured["runner_kwargs"] = kwargs
        return "runner-token"

    monkeypatch.setattr(run_verified_sync, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(run_verified_sync, "create_standalone_connection", lambda: FakeConnection())
    monkeypatch.setattr(
        run_verified_sync,
        "get_verified_sync_freshness",
        lambda conn, enabled_sources, pipeline, stale_after_hours: {
            "stale": True,
            "pipeline": pipeline,
            "freshness_cutoff": "2026-03-08T02:00:00",
            "enabled_sources": enabled_sources,
            "missing_sources": enabled_sources,
            "non_terminal_sources": [],
            "stale_sources": [],
        },
    )
    monkeypatch.setattr(run_verified_sync, "get_next_attempt_no", lambda conn, pipeline: 3)
    monkeypatch.setattr(run_verified_sync, "build_crawler_runner", fake_build_crawler_runner)
    monkeypatch.setattr(run_verified_sync, "run_crawler_suite", fake_run_crawler_suite)

    exit_code = asyncio.run(
        run_verified_sync._run_verified_sync(
            run_verified_sync.parse_args(["--sources", "naver_webtoon", "--dry-run"])
        )
    )

    assert exit_code == 0
    assert captured["runner_kwargs"]["write_enabled"] is False
    assert captured["runner_kwargs"]["report_context"]["pipeline"] == "verified_local_v1"
    assert captured["suite_kwargs"]["runner"] == "runner-token"
    summary_files = list(tmp_path.rglob("run-summary.json"))
    verification_files = list(tmp_path.rglob("verification.json"))
    assert len(summary_files) == 1
    assert len(verification_files) == 1


def test_parse_args_uses_ten_hour_default_threshold():
    args = run_verified_sync.parse_args([])

    assert args.stale_after_hours == 10


def test_show_latest_run_summary_serializes_datetimes(monkeypatch, capsys):
    monkeypatch.setattr(run_verified_sync, "create_standalone_connection", lambda: FakeConnection())
    monkeypatch.setattr(
        run_verified_sync,
        "build_latest_run_summary",
        lambda conn, pipeline: {
            "pipeline": pipeline,
            "run_id": "verified_local_v1:20260309-120000:deadbeef",
            "items": [
                {
                    "source_name": "naver_webtoon",
                    "created_at": datetime(2026, 3, 9, 12, 0, 0),
                }
            ],
            "retry_sources": [],
        },
    )

    exit_code = run_verified_sync._show_latest_run_summary()

    assert exit_code == 0
    captured = capsys.readouterr()
    assert '"created_at": "2026-03-09T12:00:00"' in captured.out
