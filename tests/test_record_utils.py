from utils.record import read_field


class RowLike:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]


def test_read_field_with_dict():
    record = {"status": "완결"}

    assert read_field(record, "status") == "완결"
    assert read_field(record, "missing") is None
    assert read_field(record, "missing", "default") == "default"


def test_read_field_with_row_like():
    row = RowLike({"override_completed_at": "2025-01-01"})

    assert read_field(row, "override_completed_at") == "2025-01-01"
    assert read_field(row, "missing") is None
    assert read_field(row, "missing", "default") == "default"


def test_read_field_handles_none():
    assert read_field(None, "anything") is None
    assert read_field(None, "anything", "default") == "default"
