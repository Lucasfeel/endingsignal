from repositories import cdc_events_repo as repo


class FakeCursor:
    def __init__(self, inserted=True):
        self.inserted = inserted
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return {"id": 1} if self.inserted else None

    def close(self):
        self.closed = True


def test_insert_event_checks_tombstones(monkeypatch):
    fake_cursor = FakeCursor(inserted=True)
    monkeypatch.setattr(repo, "get_cursor", lambda conn: fake_cursor)

    inserted = repo.insert_event(
        object(),
        content_id="123",
        source="naver_webtoon",
        event_type="CONTENT_COMPLETED",
        final_status="완결",
        final_completed_at=None,
        resolved_by="crawler",
    )

    assert inserted is True
    query, params = fake_cursor.executed[0]
    assert "FROM cdc_event_tombstones" in query
    assert params[-3:] == ("123", "naver_webtoon", "CONTENT_COMPLETED")
    assert fake_cursor.closed is True
