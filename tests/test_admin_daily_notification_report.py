import pytest
from datetime import datetime

from app import app as flask_app
import utils.auth as auth
import views.admin as admin_view
from services.daily_notification_report_service import build_daily_notification_text


class FakeCursor:
    def __init__(self, fetchall_results=None, fetchone_results=None):
        self.fetchall_results = fetchall_results or []
        self.fetchone_results = fetchone_results or []
        self.fetchall_calls = 0
        self.fetchone_calls = 0
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        result = self.fetchall_results[self.fetchall_calls]
        self.fetchall_calls += 1
        return result

    def fetchone(self):
        result = self.fetchone_results[self.fetchone_calls]
        self.fetchone_calls += 1
        return result

    def close(self):
        self.closed = True


class FakeConnection:
    def close(self):
        pass


@pytest.fixture(autouse=True)
def stub_decode_token(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {"uid": 1, "email": "admin@example.com", "role": "admin"},
    )


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer testtoken"}


def test_daily_notification_text_handles_empty_and_zero_subscribers():
    stats = {
        "duration_seconds": None,
        "new_contents_total": 0,
        "total_recipients": 0,
        "completed_total": 0,
        "dispatch_processed_events": 0,
        "dispatch_deferred_events": 0,
        "dispatch_failed_events": 0,
        "dispatch_skipped_events": 0,
        "dispatch_log_sent_total": 0,
        "dispatch_log_failed_total": 0,
        "dispatch_log_pending_total": 0,
        "dispatch_retried_notifications": 0,
        "dispatch_already_sent_notifications": 0,
    }
    text = build_daily_notification_text("2025-01-02T10:00:00", stats, [])
    assert "- (없음)" in text
    assert "디스패치 처리" in text

    text_with_item = build_daily_notification_text(
        "2025-01-02T10:00:00",
        stats,
        [
            {
                "title": "Title",
                "content_id": "CID",
                "source": "SRC",
                "subscriber_count": 0,
                "dispatch_status": "deferred",
                "dispatch_sent_count": 0,
                "dispatch_failed_count": 0,
                "dispatch_pending_count": 1,
            }
        ],
    )
    assert "구독자 없음" in text_with_item
    assert "dispatch=보류" in text_with_item


def test_admin_daily_notification_report_payload(monkeypatch, client, auth_headers):
    created_at = datetime(2025, 1, 2, 9, 0, 0)
    fetchall_results = [
        [
            {"crawler_name": "naver webtoon", "report_data": {"duration": 1.5}},
            {
                "crawler_name": "completion notification dispatch",
                "report_data": {
                    "duration": 2,
                    "processed_events": 1,
                    "deferred_events": 1,
                    "failed_events": 0,
                    "skipped_events": 0,
                    "sent_notifications": 5,
                    "skipped_notifications": 2,
                    "retried_notifications": 1,
                    "already_sent_notifications": 1,
                },
            },
        ],
        [
            {"content_type": "webtoon", "total": 2},
            {"content_type": "novel", "total": 1},
        ],
        [
            {
                "event_id": 101,
                "content_id": "C1",
                "source": "S1",
                "event_created_at": created_at,
                "final_completed_at": created_at,
                "resolved_by": "crawler",
                "title": "Title 1",
                "content_type": "webtoon",
                "is_deleted": False,
            },
            {
                "event_id": 102,
                "content_id": "C2",
                "source": "S2",
                "event_created_at": created_at,
                "final_completed_at": created_at,
                "resolved_by": "crawler",
                "title": "Title 2",
                "content_type": "webtoon",
                "is_deleted": False,
            },
        ],
        [
            (101, 5),
            (102, 2),
        ],
        [
            (101, 5, 5, 0, 0),
            (102, 2, 1, 0, 1),
        ],
        [
            {
                "event_id": 101,
                "status": "processed",
                "reason": "notifications_dispatched",
                "created_at": created_at,
            }
        ],
    ]
    fetchone_results = [
        {"total": 3},
    ]
    fake_cursor = FakeCursor(fetchall_results=fetchall_results, fetchone_results=fetchone_results)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: created_at)
    monkeypatch.setattr(
        admin_view.psycopg2.extras,
        "execute_values",
        lambda cursor, sql, argslist, **kwargs: (cursor.execute(sql, argslist), cursor.fetchall())[1],
    )

    response = client.get(
        "/api/admin/reports/daily-notification",
        query_string={"date": "2025-01-02", "include_deleted": "1"},
        headers=auth_headers,
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["stats"]["duration_seconds"] == pytest.approx(3.5)
    assert payload["stats"]["new_contents_total"] == 3
    assert payload["stats"]["total_recipients"] == 7
    assert payload["stats"]["completed_total"] == 2
    assert payload["stats"]["dispatch_processed_events"] == 1
    assert payload["stats"]["dispatch_deferred_events"] == 1
    assert payload["stats"]["dispatch_sent_notifications"] == 5
    assert payload["stats"]["dispatch_retried_notifications"] == 1
    assert payload["stats"]["dispatch_already_sent_notifications"] == 1
    assert payload["stats"]["dispatch_log_sent_total"] == 6
    assert payload["stats"]["dispatch_log_pending_total"] == 1
    assert payload["completed_items"][0]["dispatch_status"] == "processed"
    assert payload["completed_items"][0]["dispatch_sent_count"] == 5
    assert payload["completed_items"][1]["dispatch_status"] == "deferred"
    assert payload["completed_items"][1]["dispatch_pending_count"] == 1
