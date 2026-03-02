import asyncio

import scripts.backfill_novels_once as backfill


def test_build_kakaopage_content_urls_uses_fetch_and_canonical_hosts():
    urls = backfill._build_kakaopage_content_urls("12345")

    assert urls["fetch_url"] == "https://bff-page.kakao.com/content/12345"
    assert urls["canonical_url"] == "https://page.kakao.com/content/12345"


def test_fetch_kakaopage_detail_build_record_stores_canonical_content_url(monkeypatch):
    captured = {}

    async def fake_fetch_text_with_retry(_session, url, *, headers, retries=3, retry_base_delay=1.0):
        captured["fetch_url"] = url
        return "<html></html>"

    def fake_parse_kakaopage_detail(_html, *, fallback_genres=None):
        return {
            "title": "Sample Title",
            "authors": ["Author One"],
            "status": backfill.STATUS_ONGOING,
            "genres": fallback_genres or [],
        }

    monkeypatch.setattr(backfill, "_fetch_text_with_retry", fake_fetch_text_with_retry)
    monkeypatch.setattr(backfill, "parse_kakaopage_detail", fake_parse_kakaopage_detail)

    record = asyncio.run(
        backfill._fetch_kakao_detail_and_build_record(
            session=None,
            content_id="12345",
            discovered_entry={"genres": [backfill.GENRE_FANTASY], "seed_completed": False},
            headers={},
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
