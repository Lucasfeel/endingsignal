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
        "search_document_backfill_batch_size": 25,
        "backfill_max_batches": 0,
        "backfill_max_seconds": 0.0,
        "backfill_progress_every": 0,
        "strict_maintenance": False,
    }
    base.update(overrides)
    return base


def test_backfill_normalized_sql_is_convergent(monkeypatch):
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 0, 0])
    monkeypatch.setattr(database, "_backfill_novel_genre_columns_in_batches", lambda *_args, **_kwargs: None)

    database.run_contents_backfill_in_batches(conn, cursor, settings=_settings())

    assert len(cursor.executed) == 6

    normalized_title_sql = str(cursor.executed[3][0])
    assert "normalized_title IS DISTINCT FROM" in normalized_title_sql
    assert "normalized_title IS NULL OR normalized_title = ''" in normalized_title_sql

    normalized_authors_sql = str(cursor.executed[4][0])
    assert "normalized_authors IS DISTINCT FROM" in normalized_authors_sql
    assert "normalized_authors IS NULL OR normalized_authors = ''" in normalized_authors_sql

    search_document_sql = str(cursor.executed[5][0])
    assert "search_document IS DISTINCT FROM" in search_document_sql
    assert "search_document IS NULL OR search_document = ''" in search_document_sql
    assert cursor.executed[5][1] == (25,)


def test_backfill_uses_general_batch_size_for_non_search_updates(monkeypatch):
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 0, 0])
    monkeypatch.setattr(database, "_backfill_novel_genre_columns_in_batches", lambda *_args, **_kwargs: None)

    database.run_contents_backfill_in_batches(conn, cursor, settings=_settings())

    assert [params for _query, params in cursor.executed[:5]] == [
        (100,),
        (100,),
        (100,),
        (100,),
        (100,),
    ]


def test_backfill_max_batches_breaks_infinite_progress_non_strict(monkeypatch):
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 1, 1, 1])
    monkeypatch.setattr(database, "_backfill_novel_genre_columns_in_batches", lambda *_args, **_kwargs: None)
    settings = _settings(
        backfill_batch_size=10,
        backfill_max_batches=3,
        strict_maintenance=False,
    )

    database.run_contents_backfill_in_batches(conn, cursor, settings=settings)

    assert len(cursor.executed) == 8
    assert conn.commit_calls == 8


def test_backfill_max_batches_raises_when_strict(monkeypatch):
    conn = FakeConn()
    cursor = FakeCursor([0, 0, 0, 0, 1, 1, 1])
    monkeypatch.setattr(database, "_backfill_novel_genre_columns_in_batches", lambda *_args, **_kwargs: None)
    settings = _settings(
        backfill_batch_size=10,
        backfill_max_batches=3,
        strict_maintenance=True,
    )

    with pytest.raises(RuntimeError):
        database.run_contents_backfill_in_batches(conn, cursor, settings=settings)

    assert len(cursor.executed) == 7
    assert conn.commit_calls == 7
