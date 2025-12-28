import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from views.contents import normalize_meta, normalize_weekdays, safe_get_dict


def test_normalize_meta_handles_dict_and_string_json():
    raw_dict = {"attributes": {"weekdays": ["fri"]}}
    assert normalize_meta(raw_dict) == raw_dict

    raw_str = json.dumps(raw_dict)
    assert normalize_meta(raw_str) == raw_dict


def test_normalize_meta_returns_empty_on_invalid():
    assert normalize_meta(None) == {}
    assert normalize_meta("not json") == {}
    assert normalize_meta([("a", 1)]) == {"a": 1}


@pytest.mark.parametrize(
    "value,expected",
    [
        (["mon", "tue"], ["mon", "tue"]),
        ("wed", ["wed"]),
        (json.dumps(["thu", 123, "fri"]), ["thu", "fri"]),
        (None, []),
        (123, []),
    ],
)
def test_normalize_weekdays(value, expected):
    assert normalize_weekdays(value) == expected


def test_safe_get_dict_only_allows_dict():
    assert safe_get_dict({"a": 1}) == {"a": 1}
    assert safe_get_dict([1, 2]) == {}
