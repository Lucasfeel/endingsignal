"""Run one-time novel backfill for KakaoPage only."""

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


def _seed_set_from_env() -> str:
    seed_set = (os.getenv("KAKAOPAGE_SEED_SET") or "").strip()
    if not seed_set:
        return "webnoveldb"
    return seed_set


def _phase_from_env() -> str:
    phase = (os.getenv("KAKAOPAGE_BACKFILL_PHASE") or "").strip().lower()
    if not phase:
        return "all"
    return phase


def build_exec_argv(
    user_args: Sequence[str] | None = None,
    *,
    seed_set: str | None = None,
    phase: str | None = None,
) -> List[str]:
    forwarded_args = _strip_sources_args(list(user_args or []))
    resolved_seed_set = (seed_set or "").strip() or _seed_set_from_env()
    resolved_phase = (phase or "").strip() or _phase_from_env()
    return [
        sys.executable,
        "scripts/backfill_novels_once.py",
        "--sources",
        "kakao_page",
        "--kakaopage-seed-set",
        resolved_seed_set,
        "--kakaopage-phase",
        resolved_phase,
        *forwarded_args,
    ]


def main() -> int:
    argv = build_exec_argv(sys.argv[1:])
    os.execvp(argv[0], argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
