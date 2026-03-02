import pytest

import database


class FakeCursor:
    def __init__(self, rowcounts):
        self._rowcounts = list(rowcounts)
        self.rowcount = 0
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))
        if self._rowcounts:
            self.rowcount = self._rowcounts.pop(0)
        else:
            self.rowcount = 0


class FakeConn:
    def __init__(self):
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


def _settings(**overrides):
    base = {
        "backfill_batch_size": 100,
        "backfill_max_batches": 0,
        "backfill_max_seconds": 0.0,
        "backfill_progress_every": 0,
        "strict_maintenance": False,
    }
    base.update(overrides)
    return base


def test_backfill_normalized_sql_is_convergent():
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 0])

    database.run_contents_backfill_in_batches(conn, cursor, settings=_settings())

    assert len(cursor.executed) == 5

    normalized_title_sql = str(cursor.executed[3][0])
    assert "normalized_title IS DISTINCT FROM" in normalized_title_sql
    assert "normalized_title IS NULL OR normalized_title = ''" in normalized_title_sql

    normalized_authors_sql = str(cursor.executed[4][0])
    assert "normalized_authors IS DISTINCT FROM" in normalized_authors_sql
    assert "normalized_authors IS NULL OR normalized_authors = ''" in normalized_authors_sql


def test_backfill_max_batches_breaks_infinite_progress_non_strict():
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 1, 1, 1])
    settings = _settings(
        backfill_batch_size=10,
        backfill_max_batches=3,
        strict_maintenance=False,
    )

    database.run_contents_backfill_in_batches(conn, cursor, settings=settings)

    assert len(cursor.executed) == 7
    assert conn.commit_calls == 7


def test_backfill_max_batches_raises_when_strict():
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 1, 1, 1])
    settings = _settings(
        backfill_batch_size=10,
        backfill_max_batches=3,
        strict_maintenance=True,
    )

    with pytest.raises(RuntimeError):
        database.run_contents_backfill_in_batches(conn, cursor, settings=settings)

    assert len(cursor.executed) == 7
    assert conn.commit_calls == 7
