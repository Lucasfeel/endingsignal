from pathlib import Path

import scripts.run_kakaopage_backfill_worker as worker


def test_run_worker_once_skips_when_done_marker_exists(monkeypatch, tmp_path):
    done_marker, _failed_marker = worker._marker_paths(tmp_path)
    done_marker.write_text("2026-01-01T00:00:00+00:00\n", encoding="utf-8")

    def _should_not_run(_command):
        raise AssertionError("backfill command should not run when done marker exists")

    monkeypatch.setattr(worker, "_run_backfill_command", _should_not_run)

    exit_code = worker.run_worker_once(tmp_path, log_level="INFO")

    assert exit_code == 0


def test_run_worker_once_success_writes_done_and_clears_failed(monkeypatch, tmp_path):
    done_marker, failed_marker = worker._marker_paths(tmp_path)
    failed_marker.write_text("old-failure", encoding="utf-8")

    monkeypatch.setattr(worker, "_run_backfill_command", lambda _command: 0)
    monkeypatch.setattr(worker, "_utc_iso_timestamp", lambda: "2026-03-04T00:00:00+00:00")

    exit_code = worker.run_worker_once(tmp_path, log_level="DEBUG")

    assert exit_code == 0
    assert done_marker.exists() is True
    assert "2026-03-04T00:00:00+00:00" in done_marker.read_text(encoding="utf-8")
    assert failed_marker.exists() is False


def test_run_worker_once_failure_writes_failed_marker(monkeypatch, tmp_path):
    _done_marker, failed_marker = worker._marker_paths(tmp_path)

    monkeypatch.setattr(worker, "_run_backfill_command", lambda _command: 7)
    monkeypatch.setattr(worker, "_utc_iso_timestamp", lambda: "2026-03-04T12:34:56+00:00")

    exit_code = worker.run_worker_once(tmp_path, log_level="INFO")

    assert exit_code == 7
    assert failed_marker.exists() is True
    content = failed_marker.read_text(encoding="utf-8")
    assert "2026-03-04T12:34:56+00:00" in content
    assert "exit_code=7" in content
