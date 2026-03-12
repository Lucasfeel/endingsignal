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
    release_end_at = datetime(2026, 4, 19, 0, 0, 0)
    entry = {
        "title": "Example Series 시즌 2",
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
                "body_text": "Example Series 시즌 2",
                "description": "",
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            }
        return {
            "url": url,
            "ok": True,
            "title": "Example Series 시즌 2",
            "payload_titles": ["Example Series Season 2"],
            "body_text": "Example Series 시즌 2 출연 Actor A, Actor B",
            "description": "A verified synopsis.",
            "cast": ["Actor A", "Actor B"],
            "release_start_at": release_start_at,
            "release_end_at": release_end_at,
            "release_end_status": "scheduled",
        }

    monkeypatch.setattr(service, "_fetch_document", fake_fetch_document)
    monkeypatch.setattr(
        service,
        "_search_public_result_urls",
        lambda _session, _candidate: ["https://namu.wiki/w/Example_Series"],
    )

    verdict = service.verify_ott_write_plan(write_plan, source_name="netflix")

    assert verdict["gate"] == "passed"
    assert verdict["apply_allowed"] is True
    assert verdict["verified_count"] == 1
    assert entry["cast"] == ["Actor A", "Actor B"]
    assert entry["description"] == "A verified synopsis."
    assert entry["release_start_at"] == release_start_at
    assert entry["release_end_at"] == release_end_at
    assert entry["release_end_status"] == "scheduled"
    assert verdict["items"][0]["evidence_urls"] == ["https://www.netflix.com/kr/title/123", "https://namu.wiki/w/Example_Series"]


def test_verify_ott_write_plan_blocks_changed_candidate_without_external_evidence(monkeypatch):
    write_plan = {
        "source_name": "wavve",
        "all_content_today": {
            _canonical_id(): {
                "title": "Unverified Series",
                "platform_url": "https://www.wavve.com/player/contents/12345",
            }
        },
        "verification_candidates": [
            {
                "content_id": _canonical_id(),
                "source_name": "wavve",
                "title": "Unverified Series",
                "content_url": "https://www.wavve.com/player/contents/12345",
                "change_kinds": ["new_content"],
                "source_item": {
                    "title": "Unverified Series",
                    "release_end_status": "unknown",
                },
            }
        ],
    }

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda _session, url: {
            "url": url,
            "ok": False,
            "error": "404",
        },
    )
    monkeypatch.setattr(service, "_search_public_result_urls", lambda *_args, **_kwargs: [])

    verdict = service.verify_ott_write_plan(write_plan, source_name="wavve")

    assert verdict["gate"] == "blocked"
    assert verdict["apply_allowed"] is False
    assert verdict["verified_count"] == 0
    assert verdict["items"][0]["reason"] == "no_web_evidence"


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

    monkeypatch.setattr(
        service,
        "_fetch_document",
        lambda _session, url: {"url": url, "ok": False, "error": "not found"},
    )
    monkeypatch.setattr(service, "_search_public_result_urls", lambda *_args, **_kwargs: [])

    verdict = service.verify_ott_write_plan(write_plan, source_name="tving")

    assert verdict["gate"] == "not_applicable"
    assert verdict["apply_allowed"] is True
    assert verdict["watchlist_rechecked_count"] == 1
    assert verdict["items"][0]["watchlist_recheck"] is True
    assert verdict["items"][0]["ok"] is True
    assert verdict["items"][0]["reason"] == "watchlist_unresolved"
