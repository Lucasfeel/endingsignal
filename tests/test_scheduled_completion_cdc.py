import asyncio
from datetime import datetime, timedelta

from crawlers.base_crawler import ContentCrawler


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.last_result = []

    def execute(self, query, params=None):
        params = params or []
        if "SELECT content_id, status FROM contents" in query:
            source = params[0]
            self.last_result = [
                {"content_id": cid, "status": status}
                for (cid, src), status in self.db.contents.items()
                if src == source
            ]
        elif "SELECT content_id, override_status, override_completed_at" in query:
            source = params[0]
            self.last_result = [
                {
                    "content_id": cid,
                    "override_status": row["override_status"],
                    "override_completed_at": row.get("override_completed_at"),
                }
                for (cid, src), row in self.db.overrides.items()
                if src == source
            ]
        elif "override_completed_at <=" in query:
            now = params[1]
            self.last_result = [
                {
                    "content_id": cid,
                    "source": src,
                    "override_completed_at": row.get("override_completed_at"),
                }
                for (cid, src), row in self.db.overrides.items()
                if row.get("override_status") == "완결"
                and row.get("override_completed_at") is not None
                and row.get("override_completed_at") <= now
            ]
        elif "SELECT 1 FROM contents WHERE content_id" in query:
            key = (params[0], params[1])
            self.last_result = [(1,)] if key in self.db.contents else []
        else:
            raise NotImplementedError(query)

    def fetchall(self):
        return self.last_result

    def fetchone(self):
        if not self.last_result:
            return None
        return self.last_result[0]

    def close(self):
        pass


class FakeDB:
    def __init__(self, contents, overrides=None):
        self.contents = contents
        self.overrides = overrides or {}
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class DummyCrawler(ContentCrawler):
    async def fetch_all_data(self):
        return set(), set(), set(), {}

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        return 0


def test_scheduled_completion_event_is_recorded(monkeypatch):
    now = datetime(2025, 1, 2, 0, 0, 0)
    completed_at = now - timedelta(days=1)

    db = FakeDB(
        contents={("CID", "SRC"): "연재중"},
        overrides={
            ("CID", "SRC"): {"override_status": "완결", "override_completed_at": completed_at}
        },
    )

    inserted_events = set()

    monkeypatch.setattr("crawlers.base_crawler.get_cursor", lambda conn: FakeCursor(conn))
    monkeypatch.setattr("utils.time.now_kst_naive", lambda: now)

    def fake_record_content_completed_event(conn, *, content_id, source, final_completed_at, resolved_by):
        key = (content_id, source)
        if key in inserted_events:
            return False
        inserted_events.add(key)
        return True

    monkeypatch.setattr(
        "services.cdc_event_service.record_content_completed_event",
        fake_record_content_completed_event,
    )
    monkeypatch.setattr(
        "crawlers.base_crawler.record_content_completed_event",
        fake_record_content_completed_event,
    )

    crawler = DummyCrawler("SRC")

    _, newly_completed_items, cdc_info = asyncio.run(crawler.run_daily_check(db))

    assert newly_completed_items == []
    assert cdc_info["cdc_events_inserted_count"] == 1
    assert cdc_info["scheduled_completion_events_inserted_count"] == 1
    assert inserted_events == {("CID", "SRC")}

    # Re-run to confirm idempotency (no duplicate events)
    _, _, cdc_info_second = asyncio.run(crawler.run_daily_check(db))
    assert cdc_info_second["cdc_events_inserted_count"] == 0
    assert cdc_info_second["scheduled_completion_events_inserted_count"] == 0
    assert inserted_events == {("CID", "SRC")}
