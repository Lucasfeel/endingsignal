import database


class FakeCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        if not self.fetchone_values:
            return None
        return self.fetchone_values.pop(0)

    def fetchall(self):
        if not self.fetchall_values:
            return []
        return self.fetchall_values.pop(0)

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, cursors=None):
        self.cursors = list(cursors or [])
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self, cursor_factory=None):
        if self.cursors:
            return self.cursors.pop(0)
        return FakeCursor()

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


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
        DummyPsycopgError(
            "canceling statement due to statement timeout",
            pgcode="57014",
        )
    )
    assert database.is_statement_timeout_error(
        DummyPsycopgError("statement timeout", pgcode=None)
    )
    assert not database.is_statement_timeout_error(
        DummyPsycopgError("some other error", pgcode="23505")
    )


def test_find_stale_ddl_waiters_returns_rows():
    rows = [
        {
            "pid": 101,
            "application_name": "endingsignal_init_db",
            "query_text": "ALTER TABLE contents ADD COLUMN x TEXT",
        }
    ]
    cursor = FakeCursor(fetchall_values=[rows])
    conn = FakeConn(cursors=[cursor])

    result = database.find_stale_ddl_waiters(conn, max_age_seconds=300)

    assert result == rows
    assert conn.rollback_calls >= 1
    assert len(cursor.executed) == 1
    assert "wait_event_type = 'Lock'" in str(cursor.executed[0][0])


def test_cleanup_stale_ddl_waiters_cancel(monkeypatch):
    stale_waiters = [
        {
            "pid": 2001,
            "application_name": "endingsignal_init_db",
            "state": "active",
            "query_age": "00:10:00",
            "query_text": "ALTER TABLE contents ADD COLUMN y TEXT",
        }
    ]
    action_cursor = FakeCursor(fetchone_values=[(True,)])
    conn = FakeConn(cursors=[action_cursor])

    monkeypatch.setattr(database, "find_stale_ddl_waiters", lambda *_args, **_kwargs: stale_waiters)

    database.cleanup_stale_ddl_waiters(
        conn,
        max_age_seconds=300,
        cleanup_action="cancel",
    )

    assert len(action_cursor.executed) == 1
    assert "pg_cancel_backend" in str(action_cursor.executed[0][0])


def test_cleanup_stale_ddl_waiters_terminate(monkeypatch):
    stale_waiters = [
        {
            "pid": 2002,
            "application_name": "endingsignal_init_db",
            "state": "active",
            "query_age": "00:10:00",
            "query_text": "CREATE INDEX idx_lock_storm ON contents(title)",
        }
    ]
    action_cursor = FakeCursor(fetchone_values=[(True,)])
    conn = FakeConn(cursors=[action_cursor])

    monkeypatch.setattr(database, "find_stale_ddl_waiters", lambda *_args, **_kwargs: stale_waiters)

    database.cleanup_stale_ddl_waiters(
        conn,
        max_age_seconds=300,
        cleanup_action="terminate",
    )

    assert len(action_cursor.executed) == 1
    assert "pg_terminate_backend" in str(action_cursor.executed[0][0])


def test_run_ddl_with_retry_retries_on_lock_timeout_then_succeeds(monkeypatch):
    conn = FakeConn()
    cursor = FakeCursor()
    state = {"calls": 0, "cleanup_calls": 0}

    def fake_execute():
        state["calls"] += 1
        if state["calls"] == 1:
            raise DummyPsycopgError("lock timeout", pgcode="55P03")

    monkeypatch.setattr(database, "print_relation_lock_report", lambda *_a, **_k: None)
    monkeypatch.setattr(database, "print_lock_diagnostics", lambda *_a, **_k: None)
    monkeypatch.setattr(
        database,
        "cleanup_stale_ddl_waiters",
        lambda *_a, **_k: state.__setitem__("cleanup_calls", state["cleanup_calls"] + 1),
    )
    monkeypatch.setattr(database.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(database.random, "uniform", lambda _a, _b: 0.0)

    database.run_ddl_with_retry(
        conn,
        cursor,
        "test ddl",
        fake_execute,
        ddl_retry_attempts=3,
        ddl_retry_base_delay_seconds=0.01,
        stale_ddl_max_age_seconds=300,
        stale_ddl_cleanup_action="cancel",
    )

    assert state["calls"] == 2
    assert state["cleanup_calls"] == 1
    assert conn.rollback_calls == 1
    assert conn.commit_calls == 1


def test_run_ddl_with_retry_raises_after_retry_exhaustion(monkeypatch):
    conn = FakeConn()
    cursor = FakeCursor()
    state = {"cleanup_calls": 0}

    def always_fail():
        raise DummyPsycopgError("lock timeout", pgcode="55P03")

    monkeypatch.setattr(database, "print_relation_lock_report", lambda *_a, **_k: None)
    monkeypatch.setattr(database, "print_lock_diagnostics", lambda *_a, **_k: None)
    monkeypatch.setattr(
        database,
        "cleanup_stale_ddl_waiters",
        lambda *_a, **_k: state.__setitem__("cleanup_calls", state["cleanup_calls"] + 1),
    )
    monkeypatch.setattr(database.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(database.random, "uniform", lambda _a, _b: 0.0)

    try:
        database.run_ddl_with_retry(
            conn,
            cursor,
            "test ddl",
            always_fail,
            ddl_retry_attempts=2,
            ddl_retry_base_delay_seconds=0.01,
            stale_ddl_max_age_seconds=300,
            stale_ddl_cleanup_action="cancel",
        )
        assert False, "Expected lock-timeout exception"
    except DummyPsycopgError:
        pass

    assert state["cleanup_calls"] == 2
    assert conn.rollback_calls == 2
