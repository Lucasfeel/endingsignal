"""Compatibility helpers for archived content URL backfill tooling."""

from __future__ import annotations


def get_field(row, key, default=None):
    if row is None:
        return default
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    try:
        return row[key]
    except Exception:
        return default
