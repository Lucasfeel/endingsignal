"""Time utilities for KST-naive comparisons.

This module centralizes how the application interprets timestamps that are
stored as naive ``TIMESTAMP`` values in the database. All scheduled completion
comparisons must use naive datetimes that represent Asia/Seoul local time to
avoid host timezone drift.
"""

from datetime import datetime
from zoneinfo import ZoneInfo


_KST = ZoneInfo("Asia/Seoul")


def now_kst_naive() -> datetime:
    """Return the current time as a naive datetime in KST.

    Uses an aware datetime in Asia/Seoul and then drops the timezone info so it
    can be compared directly to naive ``TIMESTAMP`` values from the database.
    """

    return datetime.now(_KST).replace(tzinfo=None)


def parse_iso_naive_kst(value: str | None) -> datetime | None:
    """Parse an ISO8601 string into a naive KST datetime.

    * If the string is naive, treat it as KST-local and return as-is.
    * If the string has a timezone offset, convert it to KST and then drop the
      tzinfo to return a naive datetime.

    Returns ``None`` if the input cannot be parsed.
    """

    if value is None:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed

    return parsed.astimezone(_KST).replace(tzinfo=None)
