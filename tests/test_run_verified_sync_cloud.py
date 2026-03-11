import asyncio
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_verified_sync_cloud


class FakeCrawler:
    source_name = "naver_webtoon"


def test_verified_sync_cloud_commits_reports_and_writes_artifacts(monkeypatch, tmp_path):
    captured = {"commits": []}

    async def fake_run_crawler_suite(*crawler_args, **crawler_kwargs):
        crawler_kwargs["result_handler"](
            {
                "results": [
                    {
                        "run_id": "verified_cloud_v1:20260311-090000:test",
                        "pipeline": "verified_cloud_v1",
                        "source_name": "naver_webtoon",
                        "crawler_name": "Naver Webtoon",
                        "status": "ok",
                        "apply_result": "deferred",
                        "verification_gate": {"status": "passed"},
                        "cdc_info": {"apply_result": "deferred"},
                        "__cloud_apply_payload": {"source_name": "naver_webtoon"},
                    }
                ],
                "action_reports": [],
                "duration_seconds": 0.5,
                "rollup": {"actual_total_unique": 1},
                "final_status": "ok",
            }
        )
        return 0

    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    monkeypatch.setattr(run_verified_sync_cloud, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(run_verified_sync_cloud, "resolve_crawler_classes", lambda sources: [FakeCrawler])
    monkeypatch.setattr(
        run_verified_sync_cloud,
        "build_run_context",
        lambda pipeline, enabled_sources, attempt_no, dry_run: {
            "run_id": "verified_cloud_v1:20260311-090000:test",
            "pipeline": pipeline,
            "attempt_no": attempt_no,
            "enabled_sources": enabled_sources,
            "dry_run": dry_run,
        },
    )
    monkeypatch.setattr(run_verified_sync_cloud, "run_crawler_suite", fake_run_crawler_suite)
    monkeypatch.setattr(
        run_verified_sync_cloud,
        "_commit_source_report",
        lambda base_url, token, report, apply_payload, timeout_seconds: captured["commits"].append(
            {
                "base_url": base_url,
                "report": report,
                "apply_payload": apply_payload,
            }
        )
        or {
            **report,
            "apply_result": "applied",
            "cdc_info": {"apply_result": "applied"},
        },
    )

    exit_code = asyncio.run(
        run_verified_sync_cloud._run_verified_sync_cloud(
            run_verified_sync_cloud.parse_args(["--api-base-url", "https://example.com"])
        )
    )

    assert exit_code == 0
    assert len(captured["commits"]) == 1
    assert captured["commits"][0]["apply_payload"] == {"source_name": "naver_webtoon"}
    summary_files = list(tmp_path.rglob("run-summary.json"))
    apply_files = list(tmp_path.rglob("apply-result.json"))
    assert len(summary_files) == 1
    assert len(apply_files) == 1


def test_verified_sync_cloud_dry_run_skips_remote_commit(monkeypatch, tmp_path):
    captured = {"commits": 0}

    async def fake_run_crawler_suite(*crawler_args, **crawler_kwargs):
        crawler_kwargs["result_handler"](
            {
                "results": [
                    {
                        "run_id": "verified_cloud_v1:20260311-090000:test",
                        "pipeline": "verified_cloud_v1",
                        "source_name": "naver_webtoon",
                        "crawler_name": "Naver Webtoon",
                        "status": "ok",
                        "apply_result": "dry_run",
                        "verification_gate": {"status": "passed"},
                        "cdc_info": {"apply_result": "dry_run"},
                    }
                ],
                "action_reports": [],
                "duration_seconds": 0.5,
                "rollup": {"actual_total_unique": 1},
                "final_status": "ok",
            }
        )
        return 0

    monkeypatch.setenv("VERIFIED_SYNC_INTERNAL_TOKEN", "secret-token")
    monkeypatch.setattr(run_verified_sync_cloud, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(run_verified_sync_cloud, "resolve_crawler_classes", lambda sources: [FakeCrawler])
    monkeypatch.setattr(
        run_verified_sync_cloud,
        "build_run_context",
        lambda pipeline, enabled_sources, attempt_no, dry_run: {
            "run_id": "verified_cloud_v1:20260311-090000:test",
            "pipeline": pipeline,
            "attempt_no": attempt_no,
            "enabled_sources": enabled_sources,
            "dry_run": dry_run,
        },
    )
    monkeypatch.setattr(run_verified_sync_cloud, "run_crawler_suite", fake_run_crawler_suite)
    monkeypatch.setattr(
        run_verified_sync_cloud,
        "_commit_source_report",
        lambda *args, **kwargs: captured.__setitem__("commits", captured["commits"] + 1),
    )

    exit_code = asyncio.run(
        run_verified_sync_cloud._run_verified_sync_cloud(
            run_verified_sync_cloud.parse_args(["--api-base-url", "https://example.com", "--dry-run"])
        )
    )

    assert exit_code == 0
    assert captured["commits"] == 0
