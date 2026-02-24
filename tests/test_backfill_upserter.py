import pytest

import utils.backfill as backfill


class FakeCursor:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.commit_count = 0
        self.rollback_count = 0

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


def _record(*, content_id: str, title: str) -> dict:
    return {
        "content_id": content_id,
        "source": "naver_series",
        "title": title,
        "authors": ["Author One"],
        "status": backfill.STATUS_ONGOING,
        "content_url": f"https://series.naver.com/novel/detail.series?productNo={content_id}",
        "genres": ["fantasy"],
    }


def test_backfill_upserter_dedupes_duplicate_keys_within_batch(monkeypatch):
    conn = FakeConnection()
    cursor = FakeCursor()
    captured_rows = {}

    monkeypatch.setattr(backfill, "get_cursor", lambda _conn: cursor)

    def fake_execute_values(_cursor, _sql, rows, template=None, page_size=None, fetch=False):
        captured_rows["rows"] = rows
        captured_rows["page_size"] = page_size
        assert template is None
        assert fetch is True
        return [(True,) for _ in rows]

    monkeypatch.setattr(backfill.psycopg2.extras, "execute_values", fake_execute_values)

    upserter = backfill.BackfillUpserter(conn, batch_size=100, dry_run=False)
    upserter.add_raw(_record(content_id="123", title="First Title"))
    upserter.add_raw(_record(content_id="123", title="Second Title"))

    upserter.flush()

    rows = captured_rows["rows"]
    assert len(rows) == 1
    assert rows[0][0] == "123"
    assert rows[0][1] == "naver_series"
    assert rows[0][3] == "Second Title"
    assert captured_rows["page_size"] == 1
    assert conn.commit_count == 1
    assert conn.rollback_count == 0
    assert upserter.stats.inserted_count == 1
    assert upserter.stats.updated_count == 0
    assert cursor.closed is True


def test_backfill_upserter_restores_buffer_on_execute_values_error(monkeypatch):
    conn = FakeConnection()
    cursor = FakeCursor()

    monkeypatch.setattr(backfill, "get_cursor", lambda _conn: cursor)

    def fake_execute_values(*_args, **_kwargs):
        raise RuntimeError("execute_values failed")

    monkeypatch.setattr(backfill.psycopg2.extras, "execute_values", fake_execute_values)

    upserter = backfill.BackfillUpserter(conn, batch_size=100, dry_run=False)
    upserter.add_raw(_record(content_id="abc", title="Only Title"))

    with pytest.raises(RuntimeError, match="execute_values failed"):
        upserter.flush()

    assert conn.rollback_count == 1
    assert conn.commit_count == 0
    assert len(upserter._buffer) == 1
    assert upserter._buffer[0].content_id == "abc"
    assert upserter._buffer[0].title == "Only Title"
    assert cursor.closed is True
