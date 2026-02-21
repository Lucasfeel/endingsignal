import database


class FakeCursor:
    def __init__(self, fetchone_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if not self.fetchone_values:
            return None
        return self.fetchone_values.pop(0)


class DummyPsycopgError(Exception):
    def __init__(self, message, pgcode=None):
        super().__init__(message)
        self.pgcode = pgcode


def test_column_exists_true():
    cursor = FakeCursor(fetchone_values=[(1,)])

    assert database.column_exists(cursor, "contents", "normalized_title") is True
    assert len(cursor.executed) == 1


def test_column_exists_false():
    cursor = FakeCursor(fetchone_values=[None])

    assert database.column_exists(cursor, "contents", "normalized_title") is False
    assert len(cursor.executed) == 1


def test_ensure_column_exists_skips_alter_when_column_present():
    cursor = FakeCursor(fetchone_values=[(1,)])

    changed = database.ensure_column_exists(
        cursor, "contents", "normalized_title", "TEXT"
    )

    assert changed is False
    assert len(cursor.executed) == 1


def test_ensure_column_default_skips_equivalent_now_default():
    cursor = FakeCursor(fetchone_values=[("CURRENT_TIMESTAMP",)])

    changed = database.ensure_column_default(cursor, "contents", "updated_at", "NOW()")

    assert changed is False
    assert len(cursor.executed) == 1


def test_ensure_column_not_null_skips_when_already_not_null():
    cursor = FakeCursor(fetchone_values=[("NO",)])

    changed = database.ensure_column_not_null(cursor, "contents", "updated_at")

    assert changed is False
    assert len(cursor.executed) == 1


def test_is_lock_timeout_error_matches_pgcode_and_message():
    assert database.is_lock_timeout_error(
        DummyPsycopgError("canceling statement due to lock timeout", pgcode="55P03")
    )
    assert database.is_lock_timeout_error(
        DummyPsycopgError("lock timeout while waiting", pgcode=None)
    )
    assert not database.is_lock_timeout_error(
        DummyPsycopgError("some other error", pgcode="23505")
    )


def test_is_statement_timeout_error_matches_pgcode_and_message():
    assert database.is_statement_timeout_error(
        DummyPsycopgError("canceling statement due to statement timeout", pgcode="57014")
    )
    assert database.is_statement_timeout_error(
        DummyPsycopgError("statement timeout", pgcode=None)
    )
    assert not database.is_statement_timeout_error(
        DummyPsycopgError("some other error", pgcode="23505")
    )
