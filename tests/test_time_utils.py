from datetime import datetime

from utils.time import now_kst_naive, parse_iso_naive_kst


def test_now_kst_naive_returns_naive_datetime(monkeypatch):
    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2025, 1, 1, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr("utils.time.datetime", FakeDatetime)

    now = now_kst_naive()

    assert now.tzinfo is None
    assert now.year == 2025 and now.month == 1 and now.day == 1
    assert now.hour == 12 and now.minute == 0 and now.second == 0


def test_parse_iso_naive_kst_with_naive_input():
    parsed = parse_iso_naive_kst("2025-12-30T00:00:00")

    assert parsed == datetime(2025, 12, 30, 0, 0, 0)
    assert parsed.tzinfo is None


def test_parse_iso_naive_kst_converts_zulu_to_kst_naive():
    parsed = parse_iso_naive_kst("2025-12-29T15:00:00Z")

    assert parsed == datetime(2025, 12, 30, 0, 0, 0)
    assert parsed.tzinfo is None


def test_parse_iso_naive_kst_preserves_kst_offset_wall_clock():
    parsed = parse_iso_naive_kst("2025-12-30T00:00:00+09:00")

    assert parsed == datetime(2025, 12, 30, 0, 0, 0)
    assert parsed.tzinfo is None


def test_parse_iso_naive_kst_rejects_invalid_string():
    assert parse_iso_naive_kst("not-a-date") is None
