from services import notification_dispatch_service as service


def test_dispatch_pending_completion_events_marks_event_processed(monkeypatch):
    marked = []
    updates = []

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
        "_fetch_subscriber_keys",
        lambda conn, content_id, source: ["443731104", "443731105"],
    )
    monkeypatch.setattr(
        service,
        "_create_notification_log",
        lambda conn, **kwargs: (kwargs["event_id"], True),
    )
    monkeypatch.setattr(
        service,
        "_update_notification_log",
        lambda conn, **kwargs: updates.append(kwargs),
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
        object(),
        template_code="completion_template",
        limit=10,
    )

    assert summary["processed_events"] == 1
    assert summary["sent_notifications"] == 2
    assert len(updates) == 2
    assert marked[0]["status"] == "processed"
