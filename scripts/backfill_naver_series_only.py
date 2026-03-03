"""Run one-time novel backfill for Naver Series only."""

from __future__ import annotations

import os
import sys
from typing import List, Sequence


def _strip_sources_args(args: Sequence[str]) -> List[str]:
    cleaned: List[str] = []
    idx = 0
    while idx < len(args):
        token = str(args[idx])
        if token == "--sources":
            idx += 1
            if idx < len(args) and not str(args[idx]).startswith("--"):
                idx += 1
            continue
        if token.startswith("--sources="):
            idx += 1
            continue
        cleaned.append(token)
        idx += 1
    return cleaned


def build_exec_argv(user_args: Sequence[str] | None = None) -> List[str]:
    forwarded_args = _strip_sources_args(list(user_args or []))
    return [
        sys.executable,
        "scripts/backfill_novels_once.py",
        "--sources",
        "naver_series",
        *forwarded_args,
    ]


def main() -> int:
    argv = build_exec_argv(sys.argv[1:])
    os.execvp(argv[0], argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
