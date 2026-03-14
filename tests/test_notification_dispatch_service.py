from datetime import datetime, timedelta

from services import notification_dispatch_service as service


class FakeConnection:
    def __init__(self):
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


def test_dispatch_pending_completion_events_marks_event_processed(monkeypatch):
    marked = []
    flushed_updates = []
    conn = FakeConnection()

    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 14, 12, 0, 0))
    monkeypatch.setattr(
        service,
        "_fetch_pending_events",
        lambda conn, limit: [
            {
                "id": 42,
                "content_id": "abc-123",
                "source": "naver_webtoon",
                "event_type": "CONTENT_COMPLETED",
                "title": "완결 작품",
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_subscriber_keys_for_events",
        lambda conn, events: {("abc-123", "naver_webtoon"): ["443731104", "443731105"]},
    )

    def _claim(conn, **kwargs):
        conn.commit()
        return {
            "claimed": [
                {"log_id": 1, "user_key": "443731104"},
                {"log_id": 2, "user_key": "443731105"},
            ],
            "claimed_count": 2,
            "skipped_count": 0,
        }

    monkeypatch.setattr(service, "_claim_notification_logs", _claim)
    monkeypatch.setattr(
        service,
        "_fetch_event_notification_logs",
        lambda conn, event_id, subscriber_keys: {
            "443731104": {"id": 1, "user_key": "443731104", "result": "pending"},
            "443731105": {"id": 2, "user_key": "443731105", "result": "pending"},
        },
    )
    monkeypatch.setattr(
        service,
        "_flush_notification_log_updates",
        lambda conn, updates: flushed_updates.append(list(updates)),
    )
    monkeypatch.setattr(
        service,
        "send_completion_message",
        lambda **kwargs: {
            "resultType": "SUCCESS",
            "context": kwargs,
        },
    )
    monkeypatch.setattr(
        service,
        "mark_consumed",
        lambda conn, **kwargs: marked.append(kwargs),
    )

    summary = service.dispatch_pending_completion_events(
        conn,
        template_code="completion_template",
        limit=10,
    )

    assert summary["processed_events"] == 1
    assert summary["deferred_events"] == 0
    assert summary["failed_events"] == 0
    assert summary["sent_notifications"] == 2
    assert len(flushed_updates) == 1
    assert len(flushed_updates[0]) == 2
    assert marked[0]["status"] == "processed"
    assert marked[0]["reason"] == "notifications_dispatched"
    assert conn.commit_calls == 2
    assert conn.rollback_calls == 0


def test_dispatch_pending_completion_events_skips_event_when_no_subscribers(monkeypatch):
    marked = []
    conn = FakeConnection()

    monkeypatch.setattr(
        service,
        "_fetch_pending_events",
        lambda conn, limit: [
            {
                "id": 7,
                "content_id": "empty-1",
                "source": "naver_webtoon",
                "event_type": "CONTENT_COMPLETED",
                "title": "No Subs",
            }
        ],
    )
    monkeypatch.setattr(service, "_fetch_subscriber_keys_for_events", lambda conn, events: {})
    monkeypatch.setattr(
        service,
        "mark_consumed",
        lambda conn, **kwargs: marked.append(kwargs),
    )

    summary = service.dispatch_pending_completion_events(
        conn,
        template_code="completion_template",
        limit=10,
    )

    assert summary["skipped_events"] == 1
    assert summary["processed_events"] == 0
    assert marked[0]["status"] == "skipped"
    assert marked[0]["reason"] == "no_subscribers"
    assert conn.commit_calls == 1


def test_dispatch_pending_completion_events_processes_when_logs_are_already_sent(monkeypatch):
    marked = []
    flushed_updates = []
    conn = FakeConnection()

    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 14, 12, 0, 0))
    monkeypatch.setattr(
        service,
        "_fetch_pending_events",
        lambda conn, limit: [
            {
                "id": 42,
                "content_id": "abc-123",
                "source": "naver_webtoon",
                "event_type": "CONTENT_COMPLETED",
                "title": "Already Sent",
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_subscriber_keys_for_events",
        lambda conn, events: {("abc-123", "naver_webtoon"): ["443731104", "443731105"]},
    )

    def _claim(conn, **kwargs):
        conn.commit()
        return {
            "claimed": [],
            "claimed_count": 0,
            "skipped_count": 2,
        }

    monkeypatch.setattr(service, "_claim_notification_logs", _claim)
    monkeypatch.setattr(
        service,
        "_fetch_event_notification_logs",
        lambda conn, event_id, subscriber_keys: {
            "443731104": {"id": 1, "user_key": "443731104", "result": "sent"},
            "443731105": {"id": 2, "user_key": "443731105", "result": "sent"},
        },
    )
    monkeypatch.setattr(
        service,
        "_flush_notification_log_updates",
        lambda conn, updates: flushed_updates.append(list(updates)),
    )
    monkeypatch.setattr(
        service,
        "mark_consumed",
        lambda conn, **kwargs: marked.append(kwargs),
    )

    summary = service.dispatch_pending_completion_events(
        conn,
        template_code="completion_template",
        limit=10,
    )

    assert summary["processed_events"] == 1
    assert summary["already_sent_notifications"] == 2
    assert summary["sent_notifications"] == 0
    assert flushed_updates == []
    assert marked[0]["reason"] == "notifications_already_sent"
    assert conn.commit_calls == 2


def test_dispatch_pending_completion_events_recovers_stale_pending_logs(monkeypatch):
    marked = []
    flushed_updates = []
    leased_ids = []
    conn = FakeConnection()
    now_value = datetime(2026, 3, 14, 12, 0, 0)

    monkeypatch.setattr(service, "now_kst_naive", lambda: now_value)
    monkeypatch.setattr(
        service,
        "_fetch_pending_events",
        lambda conn, limit: [
            {
                "id": 42,
                "content_id": "abc-123",
                "source": "naver_webtoon",
                "event_type": "CONTENT_COMPLETED",
                "title": "Recovered Delivery",
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_subscriber_keys_for_events",
        lambda conn, events: {("abc-123", "naver_webtoon"): ["443731104", "443731105"]},
    )

    def _claim(conn, **kwargs):
        conn.commit()
        return {
            "claimed": [],
            "claimed_count": 0,
            "skipped_count": 2,
        }

    monkeypatch.setattr(service, "_claim_notification_logs", _claim)
    monkeypatch.setattr(
        service,
        "_fetch_event_notification_logs",
        lambda conn, event_id, subscriber_keys: {
            "443731104": {
                "id": 1,
                "user_key": "443731104",
                "result": "pending",
                "updated_at": now_value - timedelta(seconds=service.DEFAULT_PENDING_RETRY_AFTER_SECONDS + 5),
            },
            "443731105": {
                "id": 2,
                "user_key": "443731105",
                "result": "sent",
                "updated_at": now_value,
            },
        },
    )
    monkeypatch.setattr(
        service,
        "_lease_pending_notification_logs",
        lambda conn, log_ids: leased_ids.extend(list(log_ids)) or list(log_ids),
    )
    monkeypatch.setattr(
        service,
        "_flush_notification_log_updates",
        lambda conn, updates: flushed_updates.append(list(updates)),
    )
    monkeypatch.setattr(
        service,
        "send_completion_message",
        lambda **kwargs: {"resultType": "SUCCESS", "user": kwargs["user_key"]},
    )
    monkeypatch.setattr(
        service,
        "mark_consumed",
        lambda conn, **kwargs: marked.append(kwargs),
    )

    summary = service.dispatch_pending_completion_events(
        conn,
        template_code="completion_template",
        limit=10,
    )

    assert summary["processed_events"] == 1
    assert summary["retried_notifications"] == 1
    assert leased_ids == [1]
    assert len(flushed_updates) == 1
    assert flushed_updates[0][0]["log_id"] == 1
    assert flushed_updates[0][0]["result"] == "sent"
    assert marked[0]["status"] == "processed"
    assert conn.commit_calls == 3


def test_dispatch_pending_completion_events_defers_recent_pending_logs(monkeypatch):
    marked = []
    conn = FakeConnection()
    now_value = datetime(2026, 3, 14, 12, 0, 0)

    monkeypatch.setattr(service, "now_kst_naive", lambda: now_value)
    monkeypatch.setattr(
        service,
        "_fetch_pending_events",
        lambda conn, limit: [
            {
                "id": 42,
                "content_id": "abc-123",
                "source": "naver_webtoon",
                "event_type": "CONTENT_COMPLETED",
                "title": "Recent Pending",
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_subscriber_keys_for_events",
        lambda conn, events: {("abc-123", "naver_webtoon"): ["443731104"]},
    )

    def _claim(conn, **kwargs):
        conn.commit()
        return {
            "claimed": [],
            "claimed_count": 0,
            "skipped_count": 1,
        }

    monkeypatch.setattr(service, "_claim_notification_logs", _claim)
    monkeypatch.setattr(
        service,
        "_fetch_event_notification_logs",
        lambda conn, event_id, subscriber_keys: {
            "443731104": {
                "id": 1,
                "user_key": "443731104",
                "result": "pending",
                "updated_at": now_value - timedelta(seconds=30),
            }
        },
    )
    monkeypatch.setattr(
        service,
        "_lease_pending_notification_logs",
        lambda conn, log_ids: list(log_ids),
    )
    monkeypatch.setattr(
        service,
        "mark_consumed",
        lambda conn, **kwargs: marked.append(kwargs),
    )

    summary = service.dispatch_pending_completion_events(
        conn,
        template_code="completion_template",
        limit=10,
    )

    assert summary["deferred_events"] == 1
    assert summary["processed_events"] == 0
    assert summary["failed_events"] == 0
    assert marked == []
    assert conn.commit_calls == 1


def test_dispatch_pending_completion_events_records_failures_without_marking_processed(monkeypatch):
    marked = []
    flushed_updates = []
    conn = FakeConnection()
    now_value = datetime(2026, 3, 14, 12, 0, 0)

    monkeypatch.setattr(service, "now_kst_naive", lambda: now_value)
    monkeypatch.setattr(
        service,
        "_fetch_pending_events",
        lambda conn, limit: [
            {
                "id": 42,
                "content_id": "abc-123",
                "source": "naver_webtoon",
                "event_type": "CONTENT_COMPLETED",
                "title": "Flaky Delivery",
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_fetch_subscriber_keys_for_events",
        lambda conn, events: {("abc-123", "naver_webtoon"): ["443731104", "443731105"]},
    )

    def _claim(conn, **kwargs):
        conn.commit()
        return {
            "claimed": [
                {"log_id": 1, "user_key": "443731104"},
                {"log_id": 2, "user_key": "443731105"},
            ],
            "claimed_count": 2,
            "skipped_count": 0,
        }

    monkeypatch.setattr(service, "_claim_notification_logs", _claim)
    monkeypatch.setattr(
        service,
        "_fetch_event_notification_logs",
        lambda conn, event_id, subscriber_keys: {
            "443731104": {"id": 1, "user_key": "443731104", "result": "pending"},
            "443731105": {"id": 2, "user_key": "443731105", "result": "pending"},
        },
    )
    monkeypatch.setattr(
        service,
        "_flush_notification_log_updates",
        lambda conn, updates: flushed_updates.append(list(updates)),
    )

    def _send_completion_message(**kwargs):
        if kwargs["user_key"] == "443731105":
            raise service.AppsInTossMessageError("SEND_FAILED", "boom", payload={"ok": False})
        return {"resultType": "SUCCESS"}

    monkeypatch.setattr(service, "send_completion_message", _send_completion_message)
    monkeypatch.setattr(
        service,
        "mark_consumed",
        lambda conn, **kwargs: marked.append(kwargs),
    )

    summary = service.dispatch_pending_completion_events(
        conn,
        template_code="completion_template",
        limit=10,
    )

    assert summary["failed_events"] == 1
    assert summary["processed_events"] == 0
    assert summary["deferred_events"] == 0
    assert summary["sent_notifications"] == 1
    assert len(flushed_updates) == 1
    assert {item["result"] for item in flushed_updates[0]} == {"sent", "failed"}
    assert marked == []
    assert conn.commit_calls == 2
