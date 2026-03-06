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


def test_fetch_kakaopage_detail_uses_canonical_fallback_for_suspicious_authors(monkeypatch):
    fetched_urls = []
    parse_calls = {"count": 0}

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
        fetched_urls.append(url)
        return "<html></html>"

    def fake_parse_kakaopage_detail(_html, *, fallback_genres=None):
        parse_calls["count"] += 1
        if parse_calls["count"] == 1:
            return {
                "title": "Primary Title",
                "authors": ["\ub0b4\uc5ed\ubcf4\uae30"],
                "status": backfill.STATUS_ONGOING,
                "genres": fallback_genres or [],
                "_author_source": "meta",
            }
        return {
            "title": "Canonical Title",
            "authors": ["Real Author"],
            "status": backfill.STATUS_ONGOING,
            "genres": fallback_genres or [],
            "_author_source": "next_data",
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

    assert fetched_urls == [
        "https://bff-page.kakao.com/content/12345",
        "https://page.kakao.com/content/12345",
    ]
    assert record is not None
    assert record["title"] == "Canonical Title"
    assert record["authors"] == ["Real Author"]
    assert record["_diagnostics"].get("author_source") == "canonical_fallback"


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


def test_resolve_effective_kakao_discovery_strategy_auto():
    assert (
        backfill._resolve_effective_kakao_discovery_strategy(
            configured_strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_AUTO,
            discovered_count=0,
        )
        == backfill.KAKAOPAGE_DISCOVERY_STRATEGY_FULL
    )
    assert (
        backfill._resolve_effective_kakao_discovery_strategy(
            configured_strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_AUTO,
            discovered_count=10,
        )
        == backfill.KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH
    )


def test_should_stop_kakao_discovery_tab_refresh_prefers_no_global_growth():
    stop_reason = backfill._should_stop_kakao_discovery_tab(
        strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH,
        no_global_growth_rounds=8,
        no_new_ids_rounds=0,
        no_global_growth_threshold=8,
        no_new_ids_threshold=1,
        memory_usage_ratio=None,
        max_memory_usage_ratio=0.85,
    )

    assert stop_reason == "no_global_growth"


def test_resolve_kakao_discovery_no_new_ids_scrolls_prefers_new_env(monkeypatch):
    monkeypatch.delenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS", raising=False)
    monkeypatch.delenv("KAKAOPAGE_BACKFILL_STAGNANT_SCROLLS", raising=False)

    assert backfill._resolve_kakao_discovery_no_new_ids_scrolls() == 1

    monkeypatch.setenv("KAKAOPAGE_BACKFILL_STAGNANT_SCROLLS", "3")
    assert backfill._resolve_kakao_discovery_no_new_ids_scrolls() == 3

    monkeypatch.setenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS", "2")
    assert backfill._resolve_kakao_discovery_no_new_ids_scrolls() == 2


def test_should_stop_kakao_discovery_tab_full_uses_no_new_ids_only():
    stop_reason = backfill._should_stop_kakao_discovery_tab(
        strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_FULL,
        no_global_growth_rounds=999,
        no_new_ids_rounds=0,
        no_global_growth_threshold=8,
        no_new_ids_threshold=1,
        memory_usage_ratio=None,
        max_memory_usage_ratio=0.85,
    )
    assert stop_reason is None

    stop_reason = backfill._should_stop_kakao_discovery_tab(
        strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_FULL,
        no_global_growth_rounds=1,
        no_new_ids_rounds=1,
        no_global_growth_threshold=8,
        no_new_ids_threshold=1,
        memory_usage_ratio=None,
        max_memory_usage_ratio=0.85,
    )
    assert stop_reason == "no_new_ids"


class _NoopUpserter:
    def add_raw(self, _record):
        return True


class _CollectingUpserter:
    def __init__(self):
        self.records = []

    def add_raw(self, record):
        self.records.append(record)
        return True


class _FakeDiscoveryPage:
    async def route(self, *_args, **_kwargs):
        return None

    async def goto(self, *_args, **_kwargs):
        return None

    async def wait_for_timeout(self, *_args, **_kwargs):
        return None

    async def evaluate(self, *_args, **_kwargs):
        return None

    async def content(self):
        return "<html></html>"


def _install_single_webnoveldb_seed(monkeypatch, *, completed=False):
    monkeypatch.setattr(
        backfill,
        "_build_webnoveldb_kakao_seeds",
        lambda: [
            {
                "name": backfill.GENRE_FANTASY,
                "url": (
                    "https://bff-page.kakao.com/landing/genre/11/86?is_complete=true"
                    if completed
                    else "https://bff-page.kakao.com/landing/genre/11/86"
                ),
                "genres": [backfill.GENRE_FANTASY],
                "seed_completed": completed,
                "seed_stat_key": backfill.GENRE_FANTASY,
            }
        ],
    )


def _run_discovery(monkeypatch, tmp_path, *, ids_by_scroll, state, strategy):
    page = _FakeDiscoveryPage()
    extract_calls = {"count": 0}

    async def fake_extract_listing_ids_via_dom(_page):
        idx = extract_calls["count"]
        extract_calls["count"] += 1
        if idx >= len(ids_by_scroll):
            return ids_by_scroll[-1]
        return ids_by_scroll[idx]

    monkeypatch.setattr(backfill, "_extract_listing_ids_via_dom", fake_extract_listing_ids_via_dom)
    monkeypatch.setattr(backfill, "_save_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backfill, "_resolve_kakao_discovery_scroll_delay_ms", lambda: 0)

    result = asyncio.run(
        backfill._discover_kakaopage_ids(
            strategy=strategy,
            page=page,
            state=state,
            dry_run=False,
            state_dir=tmp_path,
            max_items=None,
            seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
            summary=backfill.SourceSummary(source=backfill.SOURCE_KAKAOPAGE),
            stop_event=asyncio.Event(),
        )
    )
    return result, extract_calls["count"]


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


def test_discovery_full_keeps_scrolling_until_first_zero_new_scroll(monkeypatch, tmp_path):
    _install_single_webnoveldb_seed(monkeypatch, completed=False)
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS", "1")

    state = {"discovered": {}, "tabs_done": [], "detail_done": []}
    result, extract_call_count = _run_discovery(
        monkeypatch,
        tmp_path,
        ids_by_scroll=[
            {"1"},
            {"1", "2"},
            {"1", "2", "3"},
            {"1", "2", "3"},
        ],
        state=state,
        strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_FULL,
    )

    assert result["completed"] is True
    assert result["abort_reason"] is None
    assert extract_call_count == 4
    assert set(state["discovered"].keys()) == {"1", "2", "3"}


def test_discovery_no_longer_truncated_by_legacy_max_scroll_cap(monkeypatch, tmp_path):
    _install_single_webnoveldb_seed(monkeypatch, completed=False)
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS", "1")
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_MAX_SCROLLS_PER_TAB", "1")

    state = {"discovered": {}, "tabs_done": [], "detail_done": []}
    _, extract_call_count = _run_discovery(
        monkeypatch,
        tmp_path,
        ids_by_scroll=[
            {"1"},
            {"1", "2"},
            {"1", "2", "3"},
            {"1", "2", "3"},
        ],
        state=state,
        strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_FULL,
    )

    assert extract_call_count == 4
    assert set(state["discovered"].keys()) == {"1", "2", "3"}


def test_discovery_uses_tab_local_no_new_ids_threshold_and_persists_entry_updates(monkeypatch, tmp_path):
    save_calls = []
    page = _FakeDiscoveryPage()
    extract_calls = {"count": 0}
    ids_by_scroll = [{"1"}, {"1"}, {"1", "2"}]

    async def fake_extract_listing_ids_via_dom(_page):
        idx = extract_calls["count"]
        extract_calls["count"] += 1
        if idx >= len(ids_by_scroll):
            return {"1", "2"}
        return ids_by_scroll[idx]

    monkeypatch.setattr(backfill, "_extract_listing_ids_via_dom", fake_extract_listing_ids_via_dom)
    _install_single_webnoveldb_seed(monkeypatch, completed=True)
    monkeypatch.setattr(backfill, "_save_state", lambda *_args, **_kwargs: save_calls.append(True))
    monkeypatch.setattr(backfill, "_resolve_kakao_discovery_scroll_delay_ms", lambda: 0)
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS", "2")

    state = {
        "discovered": {
            "1": {"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
            "2": {"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
        },
        "tabs_done": [],
        "detail_done": [],
    }

    asyncio.run(
        backfill._discover_kakaopage_ids(
            strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_FULL,
            page=page,
            state=state,
            dry_run=False,
            state_dir=tmp_path,
            max_items=None,
            seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
            summary=backfill.SourceSummary(source=backfill.SOURCE_KAKAOPAGE),
            stop_event=asyncio.Event(),
        )
    )

    assert extract_calls["count"] == 3
    assert state["discovered"]["1"]["seed_completed"] is True
    assert state["discovered"]["2"]["seed_completed"] is True
    assert len(save_calls) >= 1


def test_discovery_refresh_stops_on_no_global_growth_even_when_tab_local_new_ids_exist(monkeypatch, tmp_path):
    _install_single_webnoveldb_seed(monkeypatch, completed=False)
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_DISCOVERY_NO_GLOBAL_GROWTH_SCROLLS", "2")
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS", "10")

    state = {
        "discovered": {
            "1": {"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
            "2": {"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
        },
        "tabs_done": [],
        "detail_done": [],
    }

    _, extract_call_count = _run_discovery(
        monkeypatch,
        tmp_path,
        ids_by_scroll=[
            {"1"},
            {"2"},
            {"1", "2"},
        ],
        state=state,
        strategy=backfill.KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH,
    )

    assert extract_call_count == 2


def test_build_kakao_webnoveldb_coverage_rows_uses_expected_thresholds():
    discovered_counts = {
        backfill.GENRE_FANTASY: 11018,
        backfill.GENRE_HYEONPAN: 8441,
        backfill.GENRE_ROMANCE: 19970,
        backfill.GENRE_ROMANCE_FANTASY: 9587,
        backfill.GENRE_WUXIA: 4162,
        backfill.GENRE_BL: 3408,
    }

    rows = backfill._build_kakao_webnoveldb_coverage_rows(
        discovered_counts=discovered_counts,
        unique_discovered_count=54321,
    )
    row_by_label = {row["label"]: row for row in rows}

    assert row_by_label[backfill.GENRE_FANTASY]["target90"] == 11018
    assert row_by_label[backfill.GENRE_FANTASY]["target97"] == 11875
    assert row_by_label[backfill.GENRE_HYEONPAN]["target90"] == 8441
    assert row_by_label[backfill.GENRE_HYEONPAN]["target97"] == 9097
    assert row_by_label[backfill.GENRE_ROMANCE]["target90"] == 19970
    assert row_by_label[backfill.GENRE_ROMANCE]["target97"] == 21523
    assert row_by_label[backfill.GENRE_ROMANCE_FANTASY]["target90"] == 9587
    assert row_by_label[backfill.GENRE_ROMANCE_FANTASY]["target97"] == 10333
    assert row_by_label[backfill.GENRE_WUXIA]["target90"] == 4162
    assert row_by_label[backfill.GENRE_WUXIA]["target97"] == 4486
    assert row_by_label[backfill.GENRE_BL]["target90"] == 3408
    assert row_by_label[backfill.GENRE_BL]["target97"] == 3673
    assert row_by_label["total"]["expected"] == 62870
    assert row_by_label["total"]["target90"] == 56583
    assert row_by_label["total"]["target97"] == 60984
    assert row_by_label["total"]["unique_discovered"] == 54321


def test_render_source_summary_lines_includes_webnoveldb_coverage():
    summary = backfill.SourceSummary(
        source=backfill.SOURCE_KAKAOPAGE,
        seed_stats={
            backfill.GENRE_FANTASY: {"discovered": 11018, "parsed": 0, "skipped": 0, "errors": 0},
            backfill.GENRE_HYEONPAN: {"discovered": 8441, "parsed": 0, "skipped": 0, "errors": 0},
            backfill.GENRE_ROMANCE: {"discovered": 19970, "parsed": 0, "skipped": 0, "errors": 0},
            backfill.GENRE_ROMANCE_FANTASY: {"discovered": 9587, "parsed": 0, "skipped": 0, "errors": 0},
            backfill.GENRE_WUXIA: {"discovered": 4162, "parsed": 0, "skipped": 0, "errors": 0},
            backfill.GENRE_BL: {"discovered": 3408, "parsed": 0, "skipped": 0, "errors": 0},
        },
        final_discovered_count=54321,
    )

    lines = backfill._render_source_summary_lines(
        summary,
        dry_run=False,
        kakaopage_seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
    )

    assert any(
        line
        == (
            f"[{backfill.SOURCE_KAKAOPAGE}][{backfill.GENRE_FANTASY}] discovered=11018 expected=12242 "
            "coverage=90.0% target90=11018 target97=11875 status=meets90"
        )
        for line in lines
    )
    assert any(
        line
        == (
            f"[{backfill.SOURCE_KAKAOPAGE}][total] discovered=56586 expected=62870 "
            "coverage=90.0% target90=56583 target97=60984 status=meets90 "
            "unique_discovered=54321 diagnostic_only=true"
        )
        for line in lines
    )


def test_kakaopage_all_phase_short_circuits_when_already_complete(monkeypatch, tmp_path):
    state_file = tmp_path / f"{backfill.SOURCE_KAKAOPAGE}.json"
    state_file.write_text(
        '{"discovered":{"1":{"genres":["판타지"],"seed_completed":false}},"detail_done":["1"],'
        '"discovery_complete":true,"discovery_seed_set":"webnoveldb"}',
        encoding="utf-8",
    )

    def _raise_if_called():
        raise AssertionError("Playwright should not launch when backfill is already complete")

    monkeypatch.setattr(backfill, "_load_playwright_async_api", _raise_if_called)
    monkeypatch.setenv("KAKAOPAGE_BACKFILL_DISCOVERY_STRATEGY", "auto")

    summary = asyncio.run(
        backfill.run_kakaopage_backfill(
            upserter=_NoopUpserter(),
            dry_run=True,
            max_items=None,
            state_dir=tmp_path,
            seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
            phase=backfill.KAKAOPAGE_PHASE_ALL,
            allow_low_memory_playwright=False,
        )
    )

    assert summary.source == backfill.SOURCE_KAKAOPAGE
    assert summary.parsed_count == 0


def test_kakaopage_force_detail_refresh_ignores_detail_done_and_reprocesses(monkeypatch, tmp_path):
    state_file = tmp_path / f"{backfill.SOURCE_KAKAOPAGE}.json"
    state_file.write_text(
        '{"discovered":{"1":{"genres":["\ud310\ud0c0\uc9c0"],"seed_completed":false}},"detail_done":["1"],'
        '"discovery_complete":true,"discovery_seed_set":"webnoveldb"}',
        encoding="utf-8",
    )

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
        return "<html></html>"

    def fake_parse_kakaopage_detail(_html, *, fallback_genres=None):
        return {
            "title": "Forced Refresh Title",
            "authors": ["Forced Author"],
            "status": backfill.STATUS_ONGOING,
            "genres": fallback_genres or [],
            "_author_source": "jsonld",
        }

    monkeypatch.setattr(backfill, "fetch_text_polite", fake_fetch_text_polite)
    monkeypatch.setattr(backfill, "parse_kakaopage_detail", fake_parse_kakaopage_detail)
    monkeypatch.setattr(backfill, "_resolve_kakao_detail_concurrency", lambda: 1)
    monkeypatch.setattr(backfill, "_resolve_kakao_min_interval_seconds", lambda: 0.001)
    monkeypatch.setattr(backfill, "_resolve_kakao_detail_jitter_bounds", lambda: (0.0, 0.0))
    monkeypatch.setattr(backfill, "_resolve_kakao_http_retry_policy", lambda: (1, 0.1, 0.2))

    upserter = _CollectingUpserter()
    summary = asyncio.run(
        backfill.run_kakaopage_backfill(
            upserter=upserter,
            dry_run=True,
            max_items=None,
            state_dir=tmp_path,
            seed_set=backfill.KAKAOPAGE_SEED_SET_WEBNOVELDB,
            phase=backfill.KAKAOPAGE_PHASE_ALL,
            allow_low_memory_playwright=False,
            force_detail_refresh=True,
        )
    )

    assert summary.parsed_count == 1
    assert len(upserter.records) == 1
    assert upserter.records[0]["content_id"] == "1"
