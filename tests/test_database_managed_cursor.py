import database


class FakeCursor:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def test_managed_cursor_closes_on_success(monkeypatch):
    cursor = FakeCursor()

    def fake_get_cursor(_conn):
        return cursor

    monkeypatch.setattr(database, "get_cursor", fake_get_cursor)

    with database.managed_cursor(conn=object()) as managed:
        assert managed is cursor

    assert cursor.closed is True


def test_managed_cursor_closes_on_exception(monkeypatch):
    cursor = FakeCursor()

    def fake_get_cursor(_conn):
        return cursor

    monkeypatch.setattr(database, "get_cursor", fake_get_cursor)

    try:
        with database.managed_cursor(conn=object()):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert cursor.closed is True
