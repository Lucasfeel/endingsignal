from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

from utils.time import now_kst_naive, parse_iso_naive_kst

_KOREAN_DATE_RE = re.compile(r"(?P<year>\d{4})\D+(?P<month>\d{1,2})\D+(?P<day>\d{1,2})")
_MONTH_FIRST_DATE_RE = re.compile(r"(?P<month>\d{1,2})\D+(?P<day>\d{1,2})\D+(?P<year>\d{4})")
_DOT_DATE_RE = re.compile(r"(?P<year>\d{4})[./-](?P<month>\d{1,2})[./-](?P<day>\d{1,2})")
_KOREAN_MONTH_DAY_RE = re.compile(r"(?P<month>\d{1,2})\s*월\s*(?P<day>\d{1,2})\s*일")


def clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def parse_flexible_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        parsed = parse_iso_naive_kst(value)
        if parsed is not None:
            return parsed

        text = clean_text(value)
        dotted = _DOT_DATE_RE.search(text)
        if dotted:
            try:
                return datetime(
                    int(dotted.group("year")),
                    int(dotted.group("month")),
                    int(dotted.group("day")),
                )
            except ValueError:
                return None

        month_first = _MONTH_FIRST_DATE_RE.search(text)
        if month_first:
            try:
                return datetime(
                    int(month_first.group("year")),
                    int(month_first.group("month")),
                    int(month_first.group("day")),
                )
            except ValueError:
                return None

        korean = _KOREAN_DATE_RE.search(text)
        if korean:
            try:
                return datetime(
                    int(korean.group("year")),
                    int(korean.group("month")),
                    int(korean.group("day")),
                )
            except ValueError:
                return None

        month_day = _KOREAN_MONTH_DAY_RE.search(text)
        if month_day:
            today = now_kst_naive()
            try:
                return datetime(
                    today.year,
                    int(month_day.group("month")),
                    int(month_day.group("day")),
                )
            except ValueError:
                return None
    return None
