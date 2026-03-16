from datetime import datetime, timedelta

from services import ott_verification_service as service


def _canonical_id() -> str:
    return "ott_series:2026:example-series:abc123def456"


def _snapshot_row():
    return {
        "content_id": _canonical_id(),
        "title": "Example Series",
        "status": "연재중",
        "meta": {
            "common": {
                "authors": ["Actor A"],
                "title_alias": ["Example Series S2"],
                "content_url": "https://www.tving.com/contents/P001",
                "thumbnail_url": "https://img.example/poster.jpg",
            },
            "ott": {
                "cast": ["Actor A"],
                "release_start_at": "2026-03-14T00:00:00",
                "release_end_status": "unknown",
                "upcoming": True,
                "platforms": [
                    {
                        "source": "tving",
                        "platform_content_id": "P001",
                        "content_url": "https://www.tving.com/contents/P001",
                        "thumbnail_url": "https://img.example/poster.jpg",
                    }
                ],
            },
        },
    }


def test_collect_targets_includes_due_watchlist_snapshot_rows():
    due_at = datetime.now() - timedelta(days=1)
    write_plan = {
        "source_name": "tving",
        "all_content_today": {},
        "watchlist_rows": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "next_check_at": due_at,
                "release_start_at": "2026-03-14T00:00:00",
                "release_end_status": "unknown",
            }
        ],
        "snapshot_existing_rows": [_snapshot_row()],
        "platform_links": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "platform_content_id": "P001",
                "platform_url": "https://www.tving.com/contents/P001",
            }
        ],
    }

    targets = service.collect_ott_verification_targets(write_plan)

    assert len(targets) == 1
    assert targets[0]["watchlist_recheck"] is True
    assert targets[0]["content_url"] == "https://www.tving.com/contents/P001"
    assert write_plan["all_content_today"][_canonical_id()]["platform_content_id"] == "P001"


def test_verify_ott_write_plan_enriches_changed_entry_and_passes(monkeypatch):
    release_start_at = datetime(2026, 3, 14, 0, 0, 0)
    entry = {
        "title": "Example Series 시즌 2",
        "platform_url": "https://www.netflix.com/kr/title/123",
        "content_url": "https://www.netflix.com/kr/title/123",
        "release_start_at": release_start_at,
        "release_end_status": "unknown",
        "cast": [],
    }
    write_plan = {
        "source_name": "netflix",
        "all_content_today": {_canonical_id(): entry},
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "netflix",
                "title": "Example Series 시즌 2",
                "content_url": "https://www.netflix.com/kr/title/123",
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    def fake_fetch_document(_session, url):
        if "netflix.com" in url:
            return {
                "url": url,
                "ok": True,
                "title": "Example Series 시즌 2",
                "payload_titles": ["Example Series Season 2"],
                "body_text": "Example Series 시즌 2 출연: Actor A, Actor B",
                "description": "",
                "cast": ["Actor A", "Actor B"],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            }
        return {
            "url": url,
            "ok": True,
            "title": "Example Series 시즌 2",
            "payload_titles": ["Example Series Season 2"],
            "body_text": "Example Series 시즌 2 출연: Actor A, Actor B",
            "description": "A verified synopsis.",
            "cast": ["Actor A", "Actor B"],
            "release_start_at": release_start_at,
            "release_end_at": None,
            "release_end_status": "unknown",
        }

    monkeypatch.setattr(service, "_fetch_document", fake_fetch_document)

    verdict = service.verify_ott_write_plan(write_plan, source_name="netflix")

    assert verdict["gate"] == "passed"
    assert verdict["apply_allowed"] is True
    assert verdict["verified_count"] == 1
    assert entry["cast"] == ["Actor A", "Actor B"]
    assert entry.get("description", "") == ""
    assert entry["release_start_at"] == release_start_at
    assert entry.get("release_end_at") is None
    assert entry.get("release_end_status") == "unknown"
    assert verdict["items"][0]["evidence_urls"][0] == "https://www.netflix.com/kr/title/123"
    assert verdict["items"][0]["evidence_urls"] == ["https://www.netflix.com/kr/title/123"]


def test_verify_ott_write_plan_blocks_changed_candidate_without_external_evidence(monkeypatch):
    write_plan = {
        "source_name": "wavve",
        "all_content_today": {
            _canonical_id(): {
                "title": "Unverified Series",
                "platform_url": "https://untrusted.example/series/12345",
            }
        },
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "wavve",
                "title": "Unverified Series",
                "content_url": "https://untrusted.example/series/12345",
                "change_kinds": ["new_content"],
                "source_item": {
                    "title": "Unverified Series",
                    "platform_url": "https://untrusted.example/series/12345",
                    "release_end_status": "unknown",
                },
            }
        ],
    }

    verdict = service.verify_ott_write_plan(write_plan, source_name="wavve")

    assert verdict["gate"] == "not_applicable"
    assert verdict["apply_allowed"] is True
    assert verdict["verified_count"] == 0
    assert verdict["items"] == []


def test_verify_ott_write_plan_rechecks_watchlist_without_blocking_apply(monkeypatch):
    due_at = datetime.now() - timedelta(days=1)
    write_plan = {
        "source_name": "tving",
        "all_content_today": {},
        "verification_candidates": [],
        "watchlist_rows": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "next_check_at": due_at,
                "release_start_at": "2026-03-14T00:00:00",
                "release_end_status": "unknown",
            }
        ],
        "snapshot_existing_rows": [_snapshot_row()],
        "platform_links": [
            {
                "canonical_content_id": _canonical_id(),
                "platform_source": "tving",
                "platform_content_id": "P001",
                "platform_url": "https://www.tving.com/contents/P001",
            }
        ],
    }

    verdict = service.verify_ott_write_plan(write_plan, source_name="tving")

    assert verdict["gate"] == "not_applicable"
    assert verdict["apply_allowed"] is True
    assert verdict["watchlist_rechecked_count"] == 0
    assert verdict["items"] == []


def test_verify_ott_write_plan_accepts_official_crawl_metadata_when_public_docs_are_missing(monkeypatch):
    write_plan = {
        "source_name": "disney_plus",
        "all_content_today": {
            _canonical_id(): {
                "title": "High Potential",
                "platform_url": "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce",
                "content_url": "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce",
                "title_alias": ["하이 포텐셜"],
                "release_end_status": "unknown",
            }
        },
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "disney_plus",
                "title": "하이 포텐셜",
                "content_url": "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce",
                "change_kinds": ["new_content"],
                "source_item": {
                    "title": "High Potential",
                    "title_alias": ["하이 포텐셜"],
                    "platform_url": "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce",
                    "content_url": "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce",
                    "release_end_status": "unknown",
                },
            }
        ],
    }

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda _session, _url: {"url": _url, "ok": False, "error": "unreachable"},
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="disney_plus")

    assert verdict["gate"] == "passed"
    assert verdict["apply_allowed"] is True
    assert verdict["verified_count"] == 1
    assert verdict["items"][0]["evidence_urls"] == [
        "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce"
    ]


def test_verify_ott_write_plan_uses_tmdb_cast_only_when_official_cast_is_empty(monkeypatch):
    entry = {
        "title": "Example Series",
        "platform_url": "https://www.netflix.com/kr/title/123",
        "content_url": "https://www.netflix.com/kr/title/123",
        "release_end_status": "unknown",
        "cast": [],
    }
    write_plan = {
        "source_name": "netflix",
        "all_content_today": {_canonical_id(): entry},
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "netflix",
                "title": "Example Series",
                "content_url": "https://www.netflix.com/kr/title/123",
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda _session, url: {
            "url": url,
            "ok": True,
            "title": "Example Series",
            "payload_titles": ["Example Series"],
            "body_text": "Example Series",
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
        },
    )
    monkeypatch.setattr(
        service,
        "_fetch_tmdb_documents",
        lambda _session, _candidate: [
            {
                "url": "https://www.themoviedb.org/tv/123",
                "ok": True,
                "source": "tmdb",
                "title": "Example Series",
                "payload_titles": ["Example Series"],
                "cast": ["Actor A", "Actor B", "Actor C", "Actor D", "Actor E"],
            }
        ],
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="netflix")

    assert verdict["gate"] == "passed"
    assert entry["cast"] == ["Actor A", "Actor B", "Actor C", "Actor D"]
    assert "https://www.themoviedb.org/tv/123" in verdict["items"][0]["evidence_urls"]


def test_verify_ott_write_plan_does_not_call_tmdb_when_official_cast_exists(monkeypatch):
    entry = {
        "title": "Example Series",
        "platform_url": "https://www.netflix.com/kr/title/123",
        "content_url": "https://www.netflix.com/kr/title/123",
        "release_end_status": "unknown",
        "cast": [],
    }
    write_plan = {
        "source_name": "netflix",
        "all_content_today": {_canonical_id(): entry},
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "netflix",
                "title": "Example Series",
                "content_url": "https://www.netflix.com/kr/title/123",
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda _session, url: {
            "url": url,
            "ok": True,
            "title": "Example Series",
            "payload_titles": ["Example Series"],
            "body_text": "Example Series 출연: Actor A, Actor B",
            "description": "",
            "cast": ["Actor A", "Actor B"],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
        },
    )

    called = {"tmdb": 0}

    def fake_tmdb(_session, _candidate):
        called["tmdb"] += 1
        return []

    monkeypatch.setattr(service, "_fetch_tmdb_documents", fake_tmdb)

    verdict = service.verify_ott_write_plan(write_plan, source_name="netflix")

    assert verdict["gate"] == "passed"
    assert entry["cast"] == ["Actor A", "Actor B"]
    assert called["tmdb"] == 0
