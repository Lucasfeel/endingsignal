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
    }
    text = build_daily_notification_text("2025-01-02T10:00:00", stats, [])
    assert "- (없음)" in text

    text_with_item = build_daily_notification_text(
        "2025-01-02T10:00:00",
        stats,
        [
            {
                "title": "Title",
                "content_id": "CID",
                "source": "SRC",
                "subscriber_count": 0,
            }
        ],
    )
    assert "구독자 없음" in text_with_item


def test_admin_daily_notification_report_payload(monkeypatch, client, auth_headers):
    created_at = datetime(2025, 1, 2, 9, 0, 0)
    fetchall_results = [
        [
            {"report_data": {"duration": 1.5}},
            {"report_data": "{\"duration\": 2}"},
        ],
        [
            {"content_type": "webtoon", "total": 2},
            {"content_type": "novel", "total": 1},
        ],
        [
            {
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
                "content_id": "C2",
                "source": "S2",
                "event_created_at": created_at,
                "final_completed_at": created_at,
                "resolved_by": "crawler",
                "title": "Title 2",
                "content_type": "webtoon",
                "is_deleted": True,
            },
        ],
    ]
    fetchone_results = [
        {"total": 3},
        {"subscriber_count": 5},
        {"subscriber_count": 2},
    ]
    fake_cursor = FakeCursor(fetchall_results=fetchall_results, fetchone_results=fetchone_results)
    monkeypatch.setattr(admin_view, "get_db", lambda: FakeConnection())
    monkeypatch.setattr(admin_view, "get_cursor", lambda conn: fake_cursor)
    monkeypatch.setattr(admin_view, "now_kst_naive", lambda: created_at)

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
    assert payload["stats"]["total_recipients"] == 5
    assert payload["stats"]["completed_total"] == 2
    assert payload["completed_items"][1]["notification_excluded"] is True
