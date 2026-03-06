import asyncio

import crawlers.base_crawler as base_crawler_module
from crawlers.base_crawler import ContentCrawler


class FakeCursor:
    def __init__(self, conn, name, fetchall_results):
        self.conn = conn
        self.name = name
        self.fetchall_results = list(fetchall_results)
        self.closed = False

    def execute(self, query, params=None):
        sql = str(query)
        if "SELECT content_id, status FROM contents" in sql:
            self.conn.events.append("snapshot_select_contents")
            return
        if "FROM admin_content_overrides" in sql:
            self.conn.events.append("snapshot_select_overrides")
            return
        if "INSERT INTO admin_content_metadata" in sql:
            self.conn.events.append("seed_insert")
            return
        self.conn.events.append(f"execute:{self.name}")

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []

    def close(self):
        self.closed = True
        self.conn.events.append(f"{self.name}_close")


class FakeConnection:
    def __init__(self):
        self.events = []
        self.rollback_calls = 0
        self.commit_calls = 0
        self.snapshot_cursor = FakeCursor(self, "snapshot_cursor", [[], []])
        self.write_cursor = FakeCursor(self, "write_cursor", [[]])
        self._cursor_index = 0

    def cursor(self, cursor_factory=None):  # noqa: ARG002 - matches psycopg2 signature
        if self._cursor_index == 0:
            self._cursor_index += 1
            self.events.append("cursor_open_snapshot")
            return self.snapshot_cursor
        self._cursor_index += 1
        self.events.append("cursor_open_write")
        return self.write_cursor

    def rollback(self):
        self.rollback_calls += 1
        self.events.append("rollback")

    def commit(self):
        self.commit_calls += 1
        self.events.append("commit")


class DummyCrawler(ContentCrawler):
    def __init__(self, conn):
        super().__init__("test_source")
        self.conn = conn

    async def fetch_all_data(self):
        # Must be true before any network I/O starts.
        assert self.conn.rollback_calls == 1
        assert self.conn.snapshot_cursor.closed is True
        self.conn.events.append("fetch_all_data")
        return {}, {}, {}, {}, {"force_no_ratio": True}

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        conn.events.append("synchronize_database")
        return 0


class FailingCrawler(DummyCrawler):
    async def fetch_all_data(self):
        assert self.conn.rollback_calls == 1
        assert self.conn.snapshot_cursor.closed is True
        self.conn.events.append("fetch_all_data")
        raise RuntimeError("network failure")


class BestEffortProfileLookupCrawler(DummyCrawler):
    async def fetch_all_data(self):
        assert self.conn.rollback_calls == 1
        assert self.conn.snapshot_cursor.closed is True
        self.conn.events.append("fetch_all_data")
        item = {"title": "title-1"}
        fetch_meta = {
            "force_no_ratio": True,
            "errors": [],
            "health_notes": ["profile_lookup_partial_failure"],
            "profile_lookup_failed": 1,
            "profile_lookup_errors": ["profile:1:http_404"],
        }
        return {}, {}, {"1": item}, {"1": item}, fetch_meta


def test_run_daily_check_ends_snapshot_transaction_before_fetch():
    conn = FakeConnection()
    crawler = DummyCrawler(conn)

    added, newly_completed_items, cdc_info = asyncio.run(crawler.run_daily_check(conn))

    assert added == 0
    assert newly_completed_items == []
    assert cdc_info["default_publication_seeded_count"] == 0
    assert conn.rollback_calls == 1
    assert conn.commit_calls == 1
    assert conn.events == [
        "cursor_open_snapshot",
        "snapshot_select_contents",
        "snapshot_select_overrides",
        "rollback",
        "snapshot_cursor_close",
        "fetch_all_data",
        "cursor_open_write",
        "synchronize_database",
        "seed_insert",
        "commit",
        "write_cursor_close",
    ]


def test_run_daily_check_still_ends_snapshot_transaction_when_fetch_fails():
    conn = FakeConnection()
    crawler = FailingCrawler(conn)

    try:
        asyncio.run(crawler.run_daily_check(conn))
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass

    assert conn.commit_calls == 0
    # 1 rollback before fetch, 1 rollback from exception handler.
    assert conn.rollback_calls == 2
    assert conn.events == [
        "cursor_open_snapshot",
        "snapshot_select_contents",
        "snapshot_select_overrides",
        "rollback",
        "snapshot_cursor_close",
        "fetch_all_data",
        "rollback",
    ]


def test_run_daily_check_does_not_skip_cdc_for_best_effort_profile_lookup_failures(monkeypatch):
    conn = FakeConnection()
    crawler = BestEffortProfileLookupCrawler(conn)
    recorded_events = []

    def _record_content_completed_event(conn, content_id, source, final_completed_at, resolved_by):
        recorded_events.append(
            {
                "content_id": content_id,
                "source": source,
                "final_completed_at": final_completed_at,
                "resolved_by": resolved_by,
            }
        )
        return True

    monkeypatch.setattr(
        base_crawler_module,
        "record_content_completed_event",
        _record_content_completed_event,
    )

    added, newly_completed_items, cdc_info = asyncio.run(crawler.run_daily_check(conn))

    assert added == 0
    assert len(newly_completed_items) == 1
    assert newly_completed_items[0][0] == "1"
    assert cdc_info["cdc_skipped"] is False
    assert cdc_info["cdc_events_inserted_count"] == 1
    assert recorded_events[0]["content_id"] == "1"
    assert cdc_info["fetch_meta"]["profile_lookup_errors"] == ["profile:1:http_404"]
    assert conn.commit_calls == 1
