from datetime import datetime

from services import cdc_event_service as service
from services.cdc_constants import (
    EVENT_CONTENT_COMPLETED,
    EVENT_CONTENT_PUBLISHED,
    STATUS_COMPLETED,
    STATUS_PUBLISHED,
)


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.row


def test_record_due_scheduled_completions_uses_set_based_insert():
    now = datetime(2025, 1, 2, 0, 0, 0)
    cursor = FakeCursor({"due_count": 7, "inserted_count": 3})

    result = service.record_due_scheduled_completions(object(), cursor, now)

    assert result == {"due_count": 7, "inserted_count": 3}
    query, params = cursor.executed[0]
    assert "WITH due_rows AS" in query
    assert "INSERT INTO cdc_events" in query
    assert params == (
        EVENT_CONTENT_COMPLETED,
        STATUS_COMPLETED,
        now,
        EVENT_CONTENT_COMPLETED,
        STATUS_COMPLETED,
    )


def test_record_due_scheduled_publications_uses_set_based_insert():
    now = datetime(2025, 1, 2, 0, 0, 0)
    cursor = FakeCursor({"due_count": 11, "inserted_count": 2})

    result = service.record_due_scheduled_publications(object(), cursor, now)

    assert result == {
        "scheduled_publication_due_count": 11,
        "scheduled_publication_events_inserted_count": 2,
        "cdc_events_inserted_count": 2,
    }
    query, params = cursor.executed[0]
    assert "WITH due_rows AS" in query
    assert "COALESCE(c.is_deleted, FALSE) = FALSE" in query
    assert "cdc_event_tombstones" in query
    assert "ON CONFLICT (content_id, source, event_type) DO NOTHING" in query
    assert params == (
        EVENT_CONTENT_PUBLISHED,
        now,
        EVENT_CONTENT_PUBLISHED,
        STATUS_PUBLISHED,
    )
