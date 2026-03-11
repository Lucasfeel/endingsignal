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
        if "INSERT INTO admin_content_metadata" in sql:
            self.conn.events.append("seed_insert")
            return
        if "FROM contents" in sql and "admin_content_overrides" not in sql:
            self.conn.events.append("snapshot_select_contents")
            return
        if "FROM admin_content_overrides" in sql:
            self.conn.events.append("snapshot_select_overrides")
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


class PrefetchContextCrawler(DummyCrawler):
    def build_prefetch_context(self, conn, cursor, db_status_map, override_map, db_state_before_sync):
        conn.events.append("build_prefetch_context")
        assert cursor is conn.snapshot_cursor
        assert db_status_map == {}
        assert override_map == {}
        assert db_state_before_sync == {}
        return {"snapshot_loaded": True}

    async def fetch_all_data(self):
        assert self.get_prefetch_context()["snapshot_loaded"] is True
        assert self.get_prefetch_context()["sync_snapshot"] == {}
        return await super().fetch_all_data()


class SkipDatabaseSyncCrawler(DummyCrawler):
    async def fetch_all_data(self):
        return {}, {}, {}, {}, {"force_no_ratio": True, "skip_database_sync": True}

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        raise AssertionError("synchronize_database should be skipped")


class VerificationCandidateCrawler(DummyCrawler):
    def __init__(self, conn):
        super().__init__(conn)
        self.source_name = "naver_webtoon"

    async def fetch_all_data(self):
        assert self.conn.rollback_calls == 1
        assert self.conn.snapshot_cursor.closed is True
        self.conn.events.append("fetch_all_data")
        ongoing = {"new-1": {"title": "새 작품"}}
        finished = {"existing-1": {"title": "기존 완결작"}}
        all_content = {**ongoing, **finished}
        return ongoing, {}, finished, all_content, {"force_no_ratio": True}


class RemoteVerificationCandidateCrawler(ContentCrawler):
    def __init__(self):
        super().__init__("naver_webtoon")

    async def fetch_all_data(self):
        assert self.get_prefetch_context()["sync_snapshot"]["existing-1"]["status"] == "연재중"
        ongoing = {"new-1": {"title": "신규 작품"}}
        finished = {"existing-1": {"title": "기존 완결"}}
        all_content = {**ongoing, **finished}
        return ongoing, {}, finished, all_content, {"force_no_ratio": True}

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        return 0


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


def test_run_daily_check_exposes_prefetch_context_during_fetch_only():
    conn = FakeConnection()
    crawler = PrefetchContextCrawler(conn)

    asyncio.run(crawler.run_daily_check(conn))

    assert crawler.get_prefetch_context() == {}
    assert "build_prefetch_context" in conn.events


def test_run_daily_check_can_skip_database_sync_via_fetch_meta():
    conn = FakeConnection()
    crawler = SkipDatabaseSyncCrawler(conn)

    added, _, cdc_info = asyncio.run(crawler.run_daily_check(conn))

    assert added == 0
    assert cdc_info["db_sync_skipped"] is True


def test_run_daily_check_can_block_write_phase_via_verification_gate():
    conn = FakeConnection()
    crawler = DummyCrawler(conn)

    added, _, cdc_info = asyncio.run(
        crawler.run_daily_check(
            conn,
            verification_gate=lambda plan: {
                "gate": "blocked",
                "mode": "test",
                "reason": "manual_block",
                "message": f"blocked:{plan['source_name']}",
                "apply_allowed": False,
            },
        )
    )

    assert added == 0
    assert conn.commit_calls == 0
    assert "cursor_open_write" not in conn.events
    assert cdc_info["apply_result"] == "blocked"
    assert cdc_info["verification"]["status"] == "blocked"
    assert cdc_info["skip_reason"] == "verification_blocked"


def test_run_daily_check_can_dry_run_without_write_phase():
    conn = FakeConnection()
    crawler = DummyCrawler(conn)

    added, _, cdc_info = asyncio.run(crawler.run_daily_check(conn, write_enabled=False))

    assert added == 0
    assert conn.commit_calls == 0
    assert "cursor_open_write" not in conn.events
    assert cdc_info["apply_result"] == "dry_run"
    assert cdc_info["skip_reason"] == "dry_run"


def test_run_daily_check_builds_verification_candidates_for_new_and_completed_changes():
    conn = FakeConnection()
    conn.snapshot_cursor.fetchall_results = [[{"content_id": "existing-1", "status": "연재중"}], []]
    crawler = VerificationCandidateCrawler(conn)
    captured = {}

    def _verification_gate(write_plan):
        captured["write_plan"] = write_plan
        return {"gate": "passed", "mode": "test", "reason": "captured", "apply_allowed": True}

    added, newly_completed_items, cdc_info = asyncio.run(
        crawler.run_daily_check(
            conn,
            verification_gate=_verification_gate,
            write_enabled=False,
        )
    )

    assert added == 0
    assert len(newly_completed_items) == 1
    write_plan = captured["write_plan"]
    assert write_plan["new_contents_count"] == 1
    assert {item["content_id"] for item in write_plan["new_content_items"]} == {"new-1"}
    assert {item["content_id"] for item in write_plan["verification_candidates"]} == {"new-1", "existing-1"}
    assert write_plan["verification_candidates"][0]["content_url"].startswith("https://m.comic.naver.com/webtoon/list")
    assert cdc_info["verification"]["reason"] == "captured"


def test_run_daily_check_can_limit_verification_candidates_per_source(monkeypatch):
    conn = FakeConnection()
    conn.snapshot_cursor.fetchall_results = [[{"content_id": "existing-1", "status": "연재중"}], []]
    crawler = VerificationCandidateCrawler(conn)
    captured = {}

    def _verification_gate(write_plan):
        captured["write_plan"] = write_plan
        return {"gate": "passed", "mode": "test", "reason": "captured", "apply_allowed": True}

    monkeypatch.setenv("VERIFIED_SYNC_MAX_CHANGES_NAVER_WEBTOON", "1")

    added, newly_completed_items, cdc_info = asyncio.run(
        crawler.run_daily_check(
            conn,
            verification_gate=_verification_gate,
            write_enabled=False,
        )
    )

    assert added == 0
    assert len(newly_completed_items) == 1
    write_plan = captured["write_plan"]
    assert write_plan["new_contents_count"] == 0
    assert {item["content_id"] for item in write_plan["verification_candidates"]} == {"existing-1"}
    assert [item["content_id"] for item in write_plan["new_content_items"]] == []
    assert [item["content_id"] for item in write_plan["pending_cdc_records"]] == ["existing-1"]
    assert cdc_info["candidate_total_count"] == 2
    assert cdc_info["candidate_selected_count"] == 1
    assert cdc_info["candidate_skipped_count"] == 1
    assert cdc_info["status"] == "warn"
    assert cdc_info["summary"]["reason"] == "candidate_limit_applied"


def test_run_daily_check_can_apply_verified_subset_when_enabled(monkeypatch):
    conn = FakeConnection()
    conn.snapshot_cursor.fetchall_results = [[{"content_id": "existing-1", "status": "연재중"}], []]
    crawler = VerificationCandidateCrawler(conn)

    def _verification_gate(write_plan):
        return {
            "gate": "blocked",
            "mode": "test",
            "reason": "verification_mismatch",
            "message": "partial",
            "apply_allowed": False,
            "items": [
                {"content_id": "existing-1", "ok": True},
                {"content_id": "new-1", "ok": False},
            ],
            "verified_count": 1,
            "failed_count": 1,
        }

    monkeypatch.setenv("VERIFIED_SYNC_APPLY_VERIFIED_SUBSET", "1")

    added, newly_completed_items, cdc_info = asyncio.run(
        crawler.run_daily_check(
            conn,
            verification_gate=_verification_gate,
            write_enabled=False,
        )
    )

    assert added == 0
    assert len(newly_completed_items) == 1
    assert cdc_info["apply_result"] == "dry_run"
    assert cdc_info["verification"]["reason"] == "verified_subset"
    assert cdc_info["verification"]["apply_allowed"] is True
    assert cdc_info["candidate_total_count"] == 2
    assert cdc_info["candidate_selected_count"] == 1
    assert cdc_info["candidate_skipped_count"] == 1
    assert cdc_info["status"] == "warn"
    assert cdc_info["summary"]["reason"] == "verified_subset_applied"


def test_prepare_remote_daily_check_builds_deferred_apply_payload():
    crawler = RemoteVerificationCandidateCrawler()
    snapshot = {
        "existing_rows": [{"content_id": "existing-1", "status": "연재중"}],
        "override_rows": [],
    }

    added, newly_completed_items, cdc_info, apply_payload = asyncio.run(
        crawler.prepare_remote_daily_check(
            snapshot,
            verification_gate=lambda plan: {
                "gate": "passed",
                "mode": "test",
                "reason": "captured",
                "apply_allowed": True,
            },
            write_enabled=True,
        )
    )

    assert added == 0
    assert len(newly_completed_items) == 1
    assert cdc_info["apply_result"] == "deferred"
    assert apply_payload["source_name"] == "naver_webtoon"
    assert apply_payload["pending_cdc_records"][0]["content_id"] == "existing-1"
