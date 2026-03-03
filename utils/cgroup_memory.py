"""Helpers for reading container cgroup memory limits/usage."""

from __future__ import annotations

from typing import Any, Dict, Optional

_CGROUP_V2_LIMIT_PATH = "/sys/fs/cgroup/memory.max"
_CGROUP_V1_LIMIT_PATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
_CGROUP_V2_USAGE_PATH = "/sys/fs/cgroup/memory.current"
_CGROUP_V1_USAGE_PATH = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
_UNLIMITED_LIMIT_SENTINEL = 1 << 60


def _read_text_file(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as fp:
            return fp.read().strip()
    except Exception:
        return None


def _parse_memory_bytes(raw_value: Optional[str], *, treat_huge_as_unlimited: bool) -> Optional[int]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    if value.lower() == "max":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    if treat_huge_as_unlimited and parsed > _UNLIMITED_LIMIT_SENTINEL:
        return None
    return parsed


def read_memory_limit_bytes() -> Optional[int]:
    v2 = _parse_memory_bytes(_read_text_file(_CGROUP_V2_LIMIT_PATH), treat_huge_as_unlimited=True)
    if v2 is not None:
        return v2
    raw_v2 = _read_text_file(_CGROUP_V2_LIMIT_PATH)
    if raw_v2 and str(raw_v2).strip().lower() == "max":
        return None
    return _parse_memory_bytes(_read_text_file(_CGROUP_V1_LIMIT_PATH), treat_huge_as_unlimited=True)


def read_memory_usage_bytes() -> Optional[int]:
    v2 = _parse_memory_bytes(_read_text_file(_CGROUP_V2_USAGE_PATH), treat_huge_as_unlimited=False)
    if v2 is not None:
        return v2
    return _parse_memory_bytes(_read_text_file(_CGROUP_V1_USAGE_PATH), treat_huge_as_unlimited=False)


def get_memory_snapshot() -> Dict[str, Any]:
    limit_bytes = read_memory_limit_bytes()
    usage_bytes = read_memory_usage_bytes()
    usage_ratio = None
    if (
        isinstance(limit_bytes, int)
        and limit_bytes > 0
        and isinstance(usage_bytes, int)
        and usage_bytes >= 0
    ):
        usage_ratio = usage_bytes / float(limit_bytes)
    return {
        "limit_bytes": limit_bytes,
        "usage_bytes": usage_bytes,
        "usage_ratio": usage_ratio,
    }
