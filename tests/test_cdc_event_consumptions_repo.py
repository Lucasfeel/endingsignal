from datetime import datetime

import repositories.cdc_event_consumptions_repo as repo


class FakeCursor:
    def __init__(self, fetchone_result=None):
        self.fetchone_result = fetchone_result
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        self.closed = True


def test_mark_consumed_inserts_once(monkeypatch):
    cursor = FakeCursor(fetchone_result={"id": 10})

    monkeypatch.setattr(repo, "get_cursor", lambda conn: cursor)

    inserted = repo.mark_consumed(
        object(),
        consumer="email_worker",
        event_id=33,
        status="processed",
        reason=None,
    )

    assert inserted is True
    assert len(cursor.executed) == 1
    query, params = cursor.executed[0]
    assert "INSERT INTO cdc_event_consumptions" in query
    assert "ON CONFLICT (consumer, event_id) DO NOTHING" in query
    assert params == ("email_worker", 33, "processed", None)
    assert cursor.closed is True


def test_mark_consumed_skips_when_existing(monkeypatch):
    cursor = FakeCursor(fetchone_result=None)

    monkeypatch.setattr(repo, "get_cursor", lambda conn: cursor)

    inserted = repo.mark_consumed(
        object(),
        consumer="email_worker",
        event_id=33,
        status="skipped",
        reason="deleted",
    )

    assert inserted is False
    assert len(cursor.executed) == 1
    query, params = cursor.executed[0]
    assert "ON CONFLICT (consumer, event_id) DO NOTHING" in query
    assert params == ("email_worker", 33, "skipped", "deleted")
    assert cursor.closed is True


def test_get_consumption_returns_row(monkeypatch):
    created_at = datetime(2024, 7, 1, 10, 0, 0)
    cursor = FakeCursor(
        fetchone_result={
            "consumer": "push_worker",
            "event_id": 55,
            "status": "failed",
            "reason": "timeout",
            "created_at": created_at,
        }
    )

    monkeypatch.setattr(repo, "get_cursor", lambda conn: cursor)

    result = repo.get_consumption(object(), consumer="push_worker", event_id=55)

    assert result == {
        "consumer": "push_worker",
        "event_id": 55,
        "status": "failed",
        "reason": "timeout",
        "created_at": created_at,
    }
    assert cursor.closed is True
