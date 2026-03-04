"""Render-friendly one-time KakaoPage backfill worker entrypoint."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Sequence, Tuple

DEFAULT_STATE_DIR = "/app/.backfill_state"
DONE_MARKER_NAME = "kakaopage_backfill_done"
FAILED_MARKER_NAME = "kakaopage_backfill_failed"
DEFAULT_LOG_LEVEL = "INFO"
IDLE_SECONDS = 10**9
_TRUTHY_VALUES = {"1", "true", "yes", "y", "on"}


def _resolve_state_dir() -> Path:
    raw = (os.getenv("BACKFILL_STATE_DIR") or "").strip()
    if not raw:
        raw = DEFAULT_STATE_DIR
    return Path(raw)


def _resolve_log_level() -> str:
    value = (os.getenv("BACKFILL_LOG_LEVEL") or "").strip().upper()
    if not value:
        return DEFAULT_LOG_LEVEL
    return value


def _resolve_respect_done_marker() -> bool:
    value = (os.getenv("BACKFILL_RESPECT_DONE_MARKER") or "").strip().lower()
    return value in _TRUTHY_VALUES


def _ensure_state_dir(state_dir: Path) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)


def _marker_paths(state_dir: Path) -> Tuple[Path, Path]:
    return state_dir / DONE_MARKER_NAME, state_dir / FAILED_MARKER_NAME


def _build_backfill_command(*, state_dir: Path, log_level: str) -> List[str]:
    return [
        sys.executable,
        "scripts/backfill_kakao_page_only.py",
        "--state-dir",
        str(state_dir),
        "--log-level",
        log_level,
    ]


def _utc_iso_timestamp() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _write_marker(path: Path, content: str) -> None:
    path.write_text(f"{content}\n", encoding="utf-8")


def _idle_forever() -> None:
    print(f"[worker] idling for {IDLE_SECONDS} seconds to avoid restart-loop re-runs.")
    time.sleep(IDLE_SECONDS)


def _run_backfill_command(command: Sequence[str]) -> int:
    try:
        completed = subprocess.run(list(command), check=False)
        return int(completed.returncode)
    except KeyboardInterrupt:
        if os.name == "nt":
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(os.getpid()), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            except Exception:
                pass
        else:
            try:
                subprocess.run(
                    ["pkill", "-TERM", "-P", str(os.getpid())],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            except Exception:
                pass
        raise


def run_worker_once(state_dir: Path, *, log_level: str) -> int:
    _ensure_state_dir(state_dir)
    done_marker, failed_marker = _marker_paths(state_dir)

    if done_marker.exists():
        if _resolve_respect_done_marker():
            print(
                f"[worker] done marker exists at {done_marker}; "
                "BACKFILL_RESPECT_DONE_MARKER is enabled, skipping backfill run."
            )
            return 0
        print(f"[worker] done marker exists at {done_marker}; proceeding to rerun and overwrite marker.")

    command = _build_backfill_command(state_dir=state_dir, log_level=log_level)
    print(f"[worker] starting KakaoPage backfill once. state_dir={state_dir} log_level={log_level}")
    print(f"[worker] exec={' '.join(command)}")
    return_code = _run_backfill_command(command)
    timestamp = _utc_iso_timestamp()

    if return_code == 0:
        _write_marker(done_marker, timestamp)
        if failed_marker.exists():
            try:
                failed_marker.unlink()
            except Exception:
                pass
        print(f"[worker] backfill succeeded; wrote done marker: {done_marker}")
        return 0

    _write_marker(failed_marker, f"{timestamp} exit_code={return_code}")
    print(f"[worker] backfill failed exit_code={return_code}; wrote failed marker: {failed_marker}")
    return return_code


def _install_signal_handlers() -> None:
    def _raise_interrupt(_signum, _frame):
        raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _raise_interrupt)
        except Exception:
            continue


def main() -> int:
    _install_signal_handlers()
    state_dir = _resolve_state_dir()
    log_level = _resolve_log_level()

    try:
        run_worker_once(state_dir, log_level=log_level)
    except KeyboardInterrupt:
        print("[worker] interrupted; exiting.")
        return 130

    try:
        _idle_forever()
    except KeyboardInterrupt:
        print("[worker] interrupted while idling; exiting.")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
