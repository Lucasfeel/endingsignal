from utils.record import read_field


class RowLike:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]


def test_read_field_from_dict():
    record = {"key": "value"}
    assert read_field(record, "key") == "value"


def test_read_field_from_row_like():
    record = RowLike({"key": "value"})
    assert read_field(record, "key") == "value"


def test_read_field_missing_returns_default():
    record = RowLike({})
    assert read_field(record, "missing", default=None) is None
    assert read_field(None, "missing", default="fallback") == "fallback"
