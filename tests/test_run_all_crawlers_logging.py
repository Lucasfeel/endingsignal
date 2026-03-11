import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import run_all_crawlers


def test_run_cli_writes_utf8_bom_log(monkeypatch, tmp_path):
    async def fake_main():
        print("\ud55c\uae00 \ub85c\uadf8")
        print("\ud45c\uc900 \uc5d0\ub7ec \ub85c\uadf8", file=sys.stderr)
        return 0

    monkeypatch.setattr(run_all_crawlers, "OUTPUT_DIR", tmp_path)

    exit_code = run_all_crawlers.run_cli(fake_main, "crawler")

    assert exit_code == 0

    log_files = list(tmp_path.glob("crawler-run-*.log"))
    assert len(log_files) == 1

    raw_bytes = log_files[0].read_bytes()
    assert raw_bytes.startswith(b"\xef\xbb\xbf")

    text = log_files[0].read_text(encoding="utf-8-sig")
    assert "LOG_PATH:" in text
    assert "\ud55c\uae00 \ub85c\uadf8" in text
    assert "\ud45c\uc900 \uc5d0\ub7ec \ub85c\uadf8" in text
