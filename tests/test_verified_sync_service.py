from datetime import datetime

from services import verified_sync_service as service


class FakeCursor:
    def __init__(self, fetchone_results=None, fetchall_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.closed = False

    def execute(self, query, params=None):  # noqa: ARG002
        return None

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []

    def close(self):
        self.closed = True


def test_enrich_report_data_adds_run_metadata_and_verification_gate():
    run_context = {
        "run_id": "verified_local_v1:20260308-120000:abcd1234",
        "pipeline": service.VERIFIED_LOCAL_PIPELINE,
        "host": "test-host",
        "attempt_no": 2,
        "enabled_sources": ["naver_webtoon"],
        "dry_run": True,
        "started_at": "2026-03-08T12:00:00",
    }

    enriched = service.enrich_report_data(
        {
            "source_name": "naver_webtoon",
            "inserted_count": 1,
            "updated_count": 2,
            "unchanged_count": 3,
            "write_skipped_count": 4,
        },
        run_context=run_context,
        verification_gate={"status": "passed", "mode": "source_pluggable", "reason": "pass"},
        apply_result="dry_run",
    )

    assert enriched["run_id"] == run_context["run_id"]
    assert enriched["pipeline"] == service.VERIFIED_LOCAL_PIPELINE
    assert enriched["verification_gate"]["status"] == "passed"
    assert enriched["apply_result"] == "dry_run"
    assert enriched["updated_count"] == 2


def test_normalize_verification_gate_preserves_extra_fields():
    normalized = service.normalize_verification_gate(
        {
            "status": "passed",
            "mode": "playwright_browser",
            "reason": "verified_all_changed_items",
            "message": "ok",
            "verified_count": 2,
            "items": [{"content_id": "A"}],
        }
    )

    assert normalized["verified_count"] == 2
    assert normalized["items"][0]["content_id"] == "A"


def test_get_verified_sync_freshness_marks_missing_sources_stale(monkeypatch):
    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 9, 0, 0, 0))
    rows = [
        {
            "crawler_name": "Naver Webtoon",
            "status": "ok",
            "report_data": {
                "source_name": "naver_webtoon",
                "pipeline": service.VERIFIED_LOCAL_PIPELINE,
                "apply_result": "applied",
                "run_id": "run-1",
            },
            "created_at": datetime(2026, 3, 8, 10, 0, 0),
        }
    ]
    fake_cursor = FakeCursor(fetchall_results=[rows])
    monkeypatch.setattr(service, "get_cursor", lambda conn: fake_cursor)

    freshness = service.get_verified_sync_freshness(
        object(),
        enabled_sources=["naver_webtoon", "kakao_page"],
    )

    assert freshness["stale"] is True
    assert freshness["missing_sources"] == ["kakao_page"]
    assert freshness["latest_by_source"]["naver_webtoon"]["apply_result"] == "applied"
    assert freshness["reason"] == "missing_sources"


def test_get_verified_sync_freshness_marks_old_sources_stale(monkeypatch):
    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 9, 12, 0, 0))
    rows = [
        {
            "crawler_name": "Naver Webtoon",
            "status": "ok",
            "report_data": {
                "source_name": "naver_webtoon",
                "pipeline": service.VERIFIED_LOCAL_PIPELINE,
                "apply_result": "applied",
                "run_id": "run-3",
            },
            "created_at": datetime(2026, 3, 9, 1, 30, 0),
        }
    ]
    fake_cursor = FakeCursor(fetchall_results=[rows])
    monkeypatch.setattr(service, "get_cursor", lambda conn: fake_cursor)

    freshness = service.get_verified_sync_freshness(
        object(),
        enabled_sources=["naver_webtoon"],
        stale_after_hours=10,
    )

    assert freshness["stale"] is True
    assert freshness["stale_sources"] == ["naver_webtoon"]
    assert freshness["reason"] == "stale_sources"


def test_get_verified_sync_freshness_keeps_recent_sources_fresh(monkeypatch):
    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 9, 12, 0, 0))
    rows = [
        {
            "crawler_name": "Naver Webtoon",
            "status": "ok",
            "report_data": {
                "source_name": "naver_webtoon",
                "pipeline": service.VERIFIED_LOCAL_PIPELINE,
                "apply_result": "applied",
                "run_id": "run-4",
            },
            "created_at": datetime(2026, 3, 9, 4, 30, 0),
        }
    ]
    fake_cursor = FakeCursor(fetchall_results=[rows])
    monkeypatch.setattr(service, "get_cursor", lambda conn: fake_cursor)

    freshness = service.get_verified_sync_freshness(
        object(),
        enabled_sources=["naver_webtoon"],
        stale_after_hours=10,
    )

    assert freshness["stale"] is False
    assert freshness["stale_sources"] == []
    assert freshness["reason"] is None


def test_build_latest_run_summary_collects_retry_sources(monkeypatch):
    latest_run_rows = [
        {
            "crawler_name": "Naver Webtoon",
            "status": "warn",
            "report_data": {
                "source_name": "naver_webtoon",
                "pipeline": service.VERIFIED_LOCAL_PIPELINE,
                "apply_result": "blocked",
                "run_id": "run-2",
            },
            "created_at": datetime(2026, 3, 8, 12, 0, 0),
        },
        {
            "crawler_name": "KakaoPage Novel",
            "status": "ok",
            "report_data": {
                "source_name": "kakao_page",
                "pipeline": service.VERIFIED_LOCAL_PIPELINE,
                "apply_result": "applied",
                "run_id": "run-2",
            },
            "created_at": datetime(2026, 3, 8, 12, 1, 0),
        },
    ]
    monkeypatch.setattr(service, "get_latest_run_rows", lambda conn, pipeline: latest_run_rows)

    summary = service.build_latest_run_summary(object(), pipeline=service.VERIFIED_LOCAL_PIPELINE)

    assert summary["run_id"] == "run-2"
    assert summary["retry_sources"] == ["naver_webtoon"]
