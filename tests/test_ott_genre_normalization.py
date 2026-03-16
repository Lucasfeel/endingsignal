from datetime import datetime

import views.contents as contents_view
from services import ott_verification_service as verification_service
from services.ott_content_service import normalize_ott_genres, resolve_display_meta


def test_normalize_ott_genres_maps_scripted_terms_to_drama():
    assert normalize_ott_genres(["서스펜스", "범죄", "스릴러"]) == ["drama"]


def test_normalize_ott_genres_does_not_treat_animal_as_anime():
    assert normalize_ott_genres(["애니멀 킹덤"]) == ["etc"]


def test_resolve_row_for_display_forces_laftel_to_anime():
    row = {
        "content_id": "laftel-1",
        "title": "Sample Anime",
        "source": "laftel",
        "content_type": "ott",
        "meta": {
            "attributes": {
                "genre": "variety",
                "genres": ["variety"],
            }
        },
    }

    resolved = contents_view._resolve_row_for_display(row)

    assert resolved["source"] == "laftel"
    assert resolved["meta"]["attributes"]["genre"] == "anime"
    assert resolved["meta"]["attributes"]["genres"] == ["anime"]
    assert resolved["meta"]["common"]["genre"] == "anime"


def test_resolve_display_meta_normalizes_canonical_ott_genre():
    meta = {
        "common": {
            "primary_source": "tving",
        },
        "attributes": {
            "genre": "series",
            "genres": [],
        },
        "ott": {
            "description": "장르 서스펜스, 범죄, 스릴러, 블랙 코미디, 가족",
            "platforms": [
                {
                    "source": "tving",
                    "content_url": "https://www.tving.com/contents/P001783289",
                }
            ],
        },
    }

    resolved_meta, resolved_source = resolve_display_meta(meta, requested_sources=["tving"])

    assert resolved_source == "tving"
    assert resolved_meta["attributes"]["genre"] == "drama"
    assert resolved_meta["attributes"]["genres"] == ["drama"]
    assert resolved_meta["common"]["genre"] == "drama"
    assert resolved_meta["genre"] == "drama"


def test_verify_ott_write_plan_enriches_entry_with_normalized_genre(monkeypatch):
    canonical_id = "ott_series:2026:example-series:abc123def456"
    release_start_at = datetime(2026, 3, 14, 0, 0, 0)
    release_end_at = datetime(2026, 4, 19, 0, 0, 0)
    entry = {
        "title": "대한민국에서 건물주 되는 법",
        "platform_url": "https://www.tving.com/contents/P001783289",
        "content_url": "https://www.tving.com/contents/P001783289",
        "release_end_status": "unknown",
        "cast": [],
    }
    write_plan = {
        "source_name": "tving",
        "all_content_today": {canonical_id: entry},
        "verification_candidates": [
            {
                "content_id": canonical_id,
                "source_name": "tving",
                "title": "대한민국에서 건물주 되는 법",
                "content_url": "https://www.tving.com/contents/P001783289",
                "change_kinds": ["new_content"],
                "source_item": dict(entry),
            }
        ],
    }

    def fake_fetch_document(_session, url):
        return {
            "url": url,
            "ok": True,
            "title": "대한민국에서 건물주 되는 법",
            "payload_titles": ["Mad Concrete Dreams"],
            "body_text": "장르 서스펜스, 범죄, 스릴러, 블랙 코미디, 가족",
            "description": "방송 예정 2026년 3월 14일 ~ 2026년 4월 19일 (예정)",
            "genre_text": "서스펜스, 범죄, 스릴러, 블랙 코미디, 가족",
            "cast": ["하정우", "임수정"],
            "release_start_at": release_start_at,
            "release_end_at": release_end_at,
            "release_end_status": "scheduled",
        }

    monkeypatch.setattr(verification_service, "_fetch_document", fake_fetch_document)
    monkeypatch.setattr(verification_service, "_fetch_rendered_official_document", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(verification_service, "_search_wikipedia_result_candidates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        verification_service,
        "_search_public_result_urls",
        lambda _session, _candidate: ["https://namu.wiki/w/%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%97%90%EC%84%9C_%EA%B1%B4%EB%AC%BC%EC%A3%BC_%EB%90%98%EB%8A%94_%EB%B2%95"],
    )

    verdict = verification_service.verify_ott_write_plan(write_plan, source_name="tving")

    assert verdict["gate"] == "passed"
    assert entry["genres"] == ["drama"]
    assert entry["genre"] == "drama"


def test_resolve_verified_genres_prefers_official_structured_genre_over_noisy_description():
    genres = verification_service._resolve_verified_genres(
        "coupangplay",
        {
            "genre": "",
            "genres": [],
            "category": "",
            "description": "watch now membership content and buttons",
        },
        [
            {
                "url": "https://www.coupangplay.com/content/example",
                "source": "official_coupang_metadata",
                "genre_text": "Legal, office, mystery, thriller",
                "description": "watch now membership button terms",
                "body_text": "Legal, office, mystery, thriller",
                "title": "Honor",
            }
        ],
        "watch now membership button variety movie live",
    )

    assert genres == ["drama"]
