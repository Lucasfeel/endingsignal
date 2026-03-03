import asyncio

import pytest

import scripts.backfill_novels_once as backfill


def test_build_kakaopage_content_urls_uses_fetch_and_canonical_hosts():
    urls = backfill._build_kakaopage_content_urls("12345")

    assert urls["fetch_url"] == "https://bff-page.kakao.com/content/12345"
    assert urls["canonical_url"] == "https://page.kakao.com/content/12345"


def test_fetch_kakaopage_detail_build_record_stores_canonical_content_url(monkeypatch):
    captured = {}

    async def fake_fetch_text_polite(
        _session,
        url,
        *,
        headers,
        retries=4,
        retry_base_delay_seconds=1.0,
        retry_max_delay_seconds=60.0,
        jitter_min_seconds=0.05,
        jitter_max_seconds=0.35,
        sleep_func=asyncio.sleep,
    ):
        captured["fetch_url"] = url
        return "<html></html>"

    def fake_parse_kakaopage_detail(_html, *, fallback_genres=None):
        return {
            "title": "Sample Title",
            "authors": ["Author One"],
            "status": backfill.STATUS_ONGOING,
            "genres": fallback_genres or [],
        }

    monkeypatch.setattr(backfill, "fetch_text_polite", fake_fetch_text_polite)
    monkeypatch.setattr(backfill, "parse_kakaopage_detail", fake_parse_kakaopage_detail)

    record = asyncio.run(
        backfill._fetch_kakao_detail_and_build_record(
            session=None,
            content_id="12345",
            discovered_entry={"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
            headers={},
            retries=2,
            retry_base_delay_seconds=0.5,
            retry_max_delay_seconds=2.0,
        )
    )

    assert captured["fetch_url"] == "https://bff-page.kakao.com/content/12345"
    assert record is not None
    assert record["content_url"] == "https://page.kakao.com/content/12345"


def test_normalize_kakao_discovered_entry_backfills_seed_completed_default_false():
    normalized = backfill._normalize_kakao_discovered_entry({"genres": [backfill.GENRE_FANTASY]})

    assert normalized["genres"] == [backfill.GENRE_FANTASY]
    assert normalized["seed_completed"] is False


def test_resolve_kakaopage_status_overrides_to_completed_for_completed_seed():
    status = backfill._resolve_kakaopage_status(
        parsed_status=backfill.STATUS_ONGOING,
        seed_completed=True,
        content_id="12345",
    )

    assert status == backfill.BACKFILL_STATUS_COMPLETED


def test_fetch_kakaopage_detail_raises_blocked_for_likely_gated_page(monkeypatch):
    async def fake_fetch_text_polite(
        _session,
        _url,
        *,
        headers,
        retries=4,
        retry_base_delay_seconds=1.0,
        retry_max_delay_seconds=60.0,
        jitter_min_seconds=0.05,
        jitter_max_seconds=0.35,
        sleep_func=asyncio.sleep,
    ):
        return "<html><title>로그인 필요</title><body>권한이 필요합니다</body></html>"

    def fake_parse_kakaopage_detail(_html, *, fallback_genres=None):
        return {
            "title": "",
            "authors": [],
            "status": backfill.STATUS_ONGOING,
            "genres": fallback_genres or [],
        }

    monkeypatch.setattr(backfill, "fetch_text_polite", fake_fetch_text_polite)
    monkeypatch.setattr(backfill, "parse_kakaopage_detail", fake_parse_kakaopage_detail)

    with pytest.raises(backfill.BlockedError):
        asyncio.run(
            backfill._fetch_kakao_detail_and_build_record(
                session=None,
                content_id="12345",
                discovered_entry={"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
                headers={},
                retries=2,
                retry_base_delay_seconds=0.5,
                retry_max_delay_seconds=2.0,
            )
        )


def test_trip_kakao_circuit_sets_stop_flag_for_blocked():
    stop_event = asyncio.Event()

    tripped = backfill._trip_kakao_circuit_if_needed(
        error_kind="blocked",
        consecutive_rate_limits=1,
        max_consecutive_rate_limits=5,
        stop_event=stop_event,
    )

    assert tripped is True
    assert stop_event.is_set() is True


def test_kakaopage_phase_default_resolved_from_env(monkeypatch):
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_PHASE", "detail")

    parser = backfill._make_arg_parser()
    args = parser.parse_args([])

    assert args.kakaopage_phase == "detail"


class _NoopUpserter:
    def add_raw(self, _record):
        return True


def test_kakaopage_detail_phase_does_not_require_playwright_import(monkeypatch, tmp_path):
    def _raise_if_called():
        raise AssertionError("Playwright import should not be called for detail-only phase")

    monkeypatch.setattr(backfill, "_load_playwright_async_api", _raise_if_called)

    with pytest.raises(RuntimeError, match="No discovered IDs; run discovery phase first"):
        asyncio.run(
            backfill.run_kakaopage_backfill(
                upserter=_NoopUpserter(),
                dry_run=True,
                max_items=5,
                state_dir=tmp_path,
                seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
                phase=backfill.KAKAOPAGE_PHASE_DETAIL,
                allow_low_memory_playwright=False,
            )
        )


def test_kakaopage_low_memory_guard_blocks_discovery_before_playwright(monkeypatch, tmp_path):
    monkeypatch.setattr(backfill, "read_memory_limit_bytes", lambda: 512 * 1024 * 1024)
    monkeypatch.setattr(
        backfill,
        "get_memory_snapshot",
        lambda: {"limit_bytes": 512 * 1024 * 1024, "usage_bytes": 128 * 1024 * 1024, "usage_ratio": 0.25},
    )

    def _raise_if_called():
        raise AssertionError("Playwright import should not run when low-memory guard blocks discovery")

    monkeypatch.setattr(backfill, "_load_playwright_async_api", _raise_if_called)
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_MIN_MEMORY_FOR_PLAYWRIGHT_MB", "1024")

    with pytest.raises(RuntimeError, match="low-memory guard"):
        asyncio.run(
            backfill.run_kakaopage_backfill(
                upserter=_NoopUpserter(),
                dry_run=True,
                max_items=5,
                state_dir=tmp_path,
                seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
                phase=backfill.KAKAOPAGE_PHASE_DISCOVERY,
                allow_low_memory_playwright=False,
            )
        )
