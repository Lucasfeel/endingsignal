from services import ott_verification_service as service


def test_extract_date_signals_prefers_scheduled_range_hint():
    signal = service._extract_date_signals(
        "방송 기간 2026년 3월 14일 ~ 2026년 4월 19일 (예정)"
    )

    assert signal["release_start_at"] is not None
    assert signal["release_end_at"] is not None
    assert signal["release_end_status"] == "scheduled"


def test_build_direct_public_result_urls_prioritizes_namuwiki():
    urls = service._build_direct_public_result_urls(
        {
            "title": "대한민국에서 건물주 되는 법",
            "source_item": {"title_alias": ["Mad Concrete Dreams"]},
        }
    )

    assert urls[0].startswith("https://namu.wiki/w/")
    assert all(url.startswith("https://namu.wiki/w/") for url in urls)


def test_merge_verification_metadata_discards_end_before_start():
    candidate = {
        "title": "Example Series",
        "source_item": {
            "title": "Example Series",
            "release_start_at": "2026-04-10T00:00:00",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://namu.wiki/w/Example",
            "ok": True,
            "title": "Example Series",
            "payload_titles": ["Example Series"],
            "body_text": "Example Series",
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": "2026-01-31T00:00:00",
            "release_end_status": "confirmed",
        }
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["release_start_at"] is not None
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"
