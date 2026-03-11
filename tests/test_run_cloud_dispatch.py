from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_cloud_dispatch


class FakeConnection:
    def close(self):
        return None


def test_cloud_dispatch_runs_all_actions_and_writes_artifact(monkeypatch, tmp_path):
    calls = []

    monkeypatch.setattr(run_cloud_dispatch, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(run_cloud_dispatch, "create_standalone_connection", lambda: FakeConnection())
    monkeypatch.setattr(run_cloud_dispatch, "get_next_attempt_no", lambda conn, pipeline: 4)
    monkeypatch.setattr(
        run_cloud_dispatch,
        "run_scheduled_completion_cdc",
        lambda report_context=None: calls.append(("completion", report_context)) or {"status": "success"},
    )
    monkeypatch.setattr(
        run_cloud_dispatch,
        "run_scheduled_publication_cdc",
        lambda report_context=None: calls.append(("publication", report_context)) or {"status": "success"},
    )
    monkeypatch.setattr(
        run_cloud_dispatch,
        "run_completion_notification_dispatch",
        lambda report_context=None: calls.append(("dispatch", report_context)) or {"status": "success"},
    )

    exit_code = run_cloud_dispatch._run_cloud_dispatch()

    assert exit_code == 0
    assert [name for name, _ in calls] == ["completion", "publication", "dispatch"]
    assert calls[0][1]["pipeline"] == "cloud_dispatch_v1"
    summary_files = list(tmp_path.rglob("dispatch-summary.json"))
    assert len(summary_files) == 1
