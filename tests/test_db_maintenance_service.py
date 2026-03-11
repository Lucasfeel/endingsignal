from datetime import datetime

from services import db_maintenance_service as service


def test_resolve_maintenance_settings_reads_env(monkeypatch):
    monkeypatch.setenv("DB_MAINTENANCE_BATCH_SIZE", "25")
    monkeypatch.setenv("DB_RETENTION_REPORT_DAYS", "91")
    monkeypatch.setenv("DB_RETENTION_NOTIFICATION_DAYS", "366")
    monkeypatch.setenv("DB_RETENTION_CDC_DAYS", "367")

    settings = service.resolve_maintenance_settings()

    assert settings == {
        "batch_size": 25,
        "report_retention_days": 91,
        "notification_retention_days": 366,
        "cdc_retention_days": 367,
    }


def test_run_db_maintenance_aggregates_batches(monkeypatch):
    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 9, 12, 0, 0))
    monkeypatch.setattr(
        service,
        "resolve_maintenance_settings",
        lambda: {
            "batch_size": 100,
            "report_retention_days": 90,
            "notification_retention_days": 365,
            "cdc_retention_days": 365,
        },
    )

    report_deletes = iter([100, 5, 0])
    notification_deletes = iter([3, 0])
    cdc_batches = iter(
        [
            {
                "eligible_count": 100,
                "tombstones_inserted": 100,
                "notification_logs_deleted": 2,
                "consumptions_deleted": 100,
                "events_deleted": 100,
                "completion_events_deleted": 40,
                "publication_events_deleted": 60,
            },
            {
                "eligible_count": 7,
                "tombstones_inserted": 7,
                "notification_logs_deleted": 1,
                "consumptions_deleted": 7,
                "events_deleted": 7,
                "completion_events_deleted": 7,
                "publication_events_deleted": 0,
            },
            {
                "eligible_count": 0,
                "tombstones_inserted": 0,
                "notification_logs_deleted": 0,
                "consumptions_deleted": 0,
                "events_deleted": 0,
                "completion_events_deleted": 0,
                "publication_events_deleted": 0,
            },
        ]
    )

    monkeypatch.setattr(
        service,
        "_delete_daily_crawler_reports_batch",
        lambda conn, cutoff, batch_size: next(report_deletes),
    )
    monkeypatch.setattr(
        service,
        "_delete_terminal_notification_logs_batch",
        lambda conn, cutoff, batch_size: next(notification_deletes),
    )
    monkeypatch.setattr(
        service,
        "_prune_cdc_batch",
        lambda conn, cdc_cutoff, notification_cutoff, batch_size: next(cdc_batches),
    )

    summary = service.run_db_maintenance(object())

    assert summary["status"] == "ok"
    assert summary["daily_crawler_reports"] == {"deleted_count": 105, "batches": 2}
    assert summary["notification_log"] == {"deleted_count": 3, "batches": 1}
    assert summary["cdc"]["batches"] == 2
    assert summary["cdc"]["events_deleted"] == 107
    assert summary["cdc"]["completion_events_deleted"] == 47
    assert summary["cdc"]["publication_events_deleted"] == 60
