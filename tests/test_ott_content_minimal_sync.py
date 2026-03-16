from services import ott_content_service as service


class _FakeCursor:
    def __init__(self, results):
        self._results = list(results)

    def execute(self, *_args, **_kwargs):
        return None

    def executemany(self, *_args, **_kwargs):
        return None

    def fetchall(self):
        if not self._results:
            return []
        return self._results.pop(0)

    def close(self):
        return None


def test_upsert_ott_source_entries_skips_seen_platform_ids_and_missing_urls(monkeypatch):
    read_cursor = _FakeCursor(
        [
            [],
            [],
            [
                {
                    "canonical_content_id": "existing-canonical",
                    "platform_source": "netflix",
                    "platform_content_id": "PID-1",
                }
            ],
        ]
    )
    write_cursor = _FakeCursor([])
    cursors = [read_cursor, write_cursor]
    monkeypatch.setattr(service, "get_cursor", lambda _conn: cursors.pop(0))

    executed_rows = {"canonical": 0, "links": 0, "watchlist": 0}

    def fake_execute_values(_cursor, sql, rows, **_kwargs):
        if "INSERT INTO contents" in sql:
            executed_rows["canonical"] = len(rows)
        elif "INSERT INTO content_platform_links" in sql:
            executed_rows["links"] = len(rows)
        elif "INSERT INTO ott_schedule_watchlist" in sql:
            executed_rows["watchlist"] = len(rows)
        return []

    monkeypatch.setattr(service.psycopg2.extras, "execute_values", fake_execute_values)

    all_content_today = {
        "PID-1": service.build_canonical_ott_entry(
            platform_source="netflix",
            title="Seen Series",
            platform_content_id="PID-1",
            platform_url="https://www.netflix.com/kr/title/1",
        ),
        "PID-2": service.build_canonical_ott_entry(
            platform_source="netflix",
            title="New Series",
            platform_content_id="PID-2",
            platform_url="https://www.netflix.com/kr/title/2",
        ),
        "PID-3": service.build_canonical_ott_entry(
            platform_source="netflix",
            title="Missing Url Series",
            platform_content_id="PID-3",
            platform_url="",
        ),
    }

    result = service.upsert_ott_source_entries(
        conn=object(),
        platform_source="netflix",
        all_content_today=all_content_today,
    )

    assert result["inserted_count"] == 1
    assert result["updated_count"] == 0
    assert result["unchanged_count"] == 0
    assert result["write_skipped_count"] == 2
    assert executed_rows == {"canonical": 1, "links": 1, "watchlist": 1}


def test_upsert_ott_source_entries_does_not_delete_preserved_seen_platform_ids(monkeypatch):
    class _TrackingCursor(_FakeCursor):
        def __init__(self, results):
            super().__init__(results)
            self.executemany_calls = []

        def executemany(self, sql, rows):
            self.executemany_calls.append((sql, list(rows)))

    read_cursor = _TrackingCursor(
        [
            [],
            [],
            [
                {
                    "canonical_content_id": "existing-canonical",
                    "platform_source": "netflix",
                    "platform_content_id": "PID-1",
                }
            ],
        ]
    )
    write_cursor = _TrackingCursor([])
    cursors = [read_cursor, write_cursor]
    monkeypatch.setattr(service, "get_cursor", lambda _conn: cursors.pop(0))
    monkeypatch.setattr(service.psycopg2.extras, "execute_values", lambda *_args, **_kwargs: [])

    all_content_today = {
        "PID-1": service.build_canonical_ott_entry(
            platform_source="netflix",
            title="Seen Series",
            platform_content_id="PID-1",
            platform_url="https://www.netflix.com/kr/title/1",
        ),
    }

    result = service.upsert_ott_source_entries(
        conn=object(),
        platform_source="netflix",
        all_content_today=all_content_today,
    )

    assert result["inserted_count"] == 0
    assert result["write_skipped_count"] == 1
    assert write_cursor.executemany_calls == []
