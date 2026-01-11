import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.base_crawler import ContentCrawler
import run_all_crawlers


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.last_result = []

    def execute(self, query, params=None):
        params = params or []
        if "INSERT INTO daily_crawler_reports" in query:
            self.db.state.daily_reports.append(
                {"crawler_name": params[0], "status": params[1], "report_data": params[2]}
            )
            self.last_result = []
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


class SharedState:
    def __init__(self, contents, publications=None):
        self.contents = contents
        self.publications = publications or {}
        self.cdc_events_published = set()
        self.daily_reports = []
        self.last_result = {}


class FakeDB:
    def __init__(self, state):
        self.state = state
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


class FakeConnectionFactory:
    def __init__(self, state):
        self.state = state
        self.created = []

    def __call__(self):
        conn = FakeDB(self.state)
        self.created.append(conn)
        return conn


class DummyCrawler(ContentCrawler):
    async def fetch_all_data(self):
        return {}, {}, {}, {}

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        return 0


def test_scheduled_publication_event_is_recorded(monkeypatch):
    now = datetime(2025, 1, 2, 0, 0, 0)
    public_at = now - timedelta(days=1)

    state = SharedState(
        contents={("CID", "SRC"): "연재중"},
        publications={
            ("CID", "SRC"): {"public_at": public_at}
        },
    )

    factory = FakeConnectionFactory(state)
    monkeypatch.setattr(run_all_crawlers, "create_standalone_connection", factory)
    monkeypatch.setattr(run_all_crawlers, "get_cursor", lambda conn: FakeCursor(conn))
    monkeypatch.setattr(run_all_crawlers, "now_kst_naive", lambda: now)

    def fake_record_due_scheduled_publications(conn, cursor, now_kst):
        inserted = 0
        for (cid, src), row in conn.state.publications.items():
            if row.get("public_at") is not None and row.get("public_at") <= now_kst:
                if (cid, src) not in conn.state.cdc_events_published:
                    conn.state.cdc_events_published.add((cid, src))
                    inserted += 1
        result = {
            "scheduled_publication_events_inserted_count": inserted,
            "cdc_events_inserted_count": inserted,
        }
        conn.state.last_result = result
        return result

    monkeypatch.setattr(
        run_all_crawlers,
        "record_due_scheduled_publications",
        fake_record_due_scheduled_publications,
    )

    run_all_crawlers.run_scheduled_publication_cdc()

    process_conn, report_conn = factory.created[0], factory.created[1]
    assert process_conn.committed is True
    assert process_conn.rolled_back is False
    assert report_conn.committed is True
    assert report_conn.rolled_back is False
    assert state.daily_reports and state.daily_reports[-1]["crawler_name"] == "scheduled publication cdc"
    assert state.last_result["scheduled_publication_events_inserted_count"] == 1
    assert state.cdc_events_published == {("CID", "SRC")}

    run_all_crawlers.run_scheduled_publication_cdc()
    assert state.last_result["scheduled_publication_events_inserted_count"] == 0
    assert len(state.daily_reports) == 2
    assert state.cdc_events_published == {("CID", "SRC")}
