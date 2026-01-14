import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


def _build_payload(author_order, content_overrides=None):
    content_overrides = content_overrides or {}
    return {
        "data": [
            {
                "cardGroups": [
                    {
                        "cards": [
                            {
                                "content": {
                                    "id": 1001,
                                    "title": "테스트웹툰",
                                    "seoId": "test-seo",
                                    "authors": author_order,
                                    "backgroundImage": "https://example.com/bg-no-ext",
                                    **content_overrides,
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }


def test_parse_timetable_payload_and_weekday_union():
    crawler = KakaoWebtoonCrawler()
    payload_tue = _build_payload(
        [
            {"name": "작가B", "order": 2},
            {"name": "작가A", "order": 1},
            {"name": "작가A", "order": 3},
        ]
    )
    payload_fri = _build_payload([{"name": "작가B"}, {"name": "작가C"}])

    entries_tue = crawler._parse_timetable_payload(payload_tue)
    entries_fri = crawler._parse_timetable_payload(payload_fri)

    ongoing_map = {}
    crawler._merge_weekday_entries(ongoing_map, entries_tue, "tue")
    crawler._merge_weekday_entries(ongoing_map, entries_fri, "fri")

    entry = ongoing_map["1001"]
    assert entry["content_id"] == "1001"
    assert entry["title"] == "테스트웹툰"
    assert entry["authors"] == ["작가A", "작가B"]
    assert entry["thumbnail_url"] == "https://example.com/bg-no-ext.webp"
    assert entry["weekdays"] == {"tue", "fri"}


def test_parse_completed_payload_shape():
    crawler = KakaoWebtoonCrawler()
    payload = _build_payload([{"name": "작가A"}], {"featuredCharacterImageA": "https://example.com/char.jpg"})
    entries = crawler._parse_timetable_payload(payload)

    assert len(entries) == 1
    assert entries[0]["content_id"] == "1001"


def test_parse_ongoing_status_from_content():
    crawler = KakaoWebtoonCrawler()
    payload = _build_payload([{"name": "작가A"}], {"onGoingStatus": "pause"})
    entries = crawler._parse_timetable_payload(payload)

    assert entries[0]["kakao_ongoing_status"] == "PAUSE"


def test_parse_ongoing_status_from_card():
    crawler = KakaoWebtoonCrawler()
    payload = {
        "data": [
            {
                "cardGroups": [
                    {
                        "cards": [
                            {
                                "onGoingStatus": "COMPLETED",
                                "content": {
                                    "id": 2002,
                                    "title": "테스트웹툰",
                                    "seoId": "test-seo",
                                    "authors": [{"name": "작가A"}],
                                    "backgroundImage": "https://example.com/bg-no-ext",
                                },
                            }
                        ]
                    }
                ]
            }
        ]
    }
    entries = crawler._parse_timetable_payload(payload)

    assert entries[0]["kakao_ongoing_status"] == "COMPLETED"


def test_pause_status_classified_as_hiatus_in_completed():
    crawler = KakaoWebtoonCrawler()
    status = crawler._normalize_status_text("PAUSE")

    classification = "hiatus" if crawler._is_pause_status(status) else "finished"

    assert classification == "hiatus"


def test_thumbnail_prefers_background_image():
    crawler = KakaoWebtoonCrawler()
    payload = _build_payload(
        [{"name": "작가A"}],
        {
            "backgroundImage": "https://example.com/bg-no-ext",
            "featuredCharacterImageA": "https://example.com/char.png",
            "featuredCharacterImageB": "https://example.com/char-b-no-ext",
            "titleImageA": "https://example.com/title.webp",
            "titleImageB": "https://example.com/title-b-no-ext",
        },
    )

    entries = crawler._parse_timetable_payload(payload)

    kakao_assets = entries[0]["kakao_assets"]
    assert entries[0]["thumbnail_url"] == "https://example.com/bg-no-ext.webp"
    assert kakao_assets["bg"]["webp"] == "https://example.com/bg-no-ext.webp"
    assert kakao_assets["bg"]["jpg"] == "https://example.com/bg-no-ext.jpg"
    assert kakao_assets["character_a"]["webp"] == "https://example.com/char.webp"
    assert kakao_assets["character_a"]["png"] == "https://example.com/char.png"
    assert kakao_assets["character_b"]["webp"] == "https://example.com/char-b-no-ext.webp"
    assert kakao_assets["character_b"]["png"] == "https://example.com/char-b-no-ext.png"
    assert kakao_assets["title_a"]["webp"] == "https://example.com/title.webp"
    assert kakao_assets["title_a"]["png"] == "https://example.com/title.png"
    assert kakao_assets["title_b"]["webp"] == "https://example.com/title-b-no-ext.webp"
    assert kakao_assets["title_b"]["png"] == "https://example.com/title-b-no-ext.png"


def test_thumbnail_fallbacks_when_background_missing():
    crawler = KakaoWebtoonCrawler()
    payload = _build_payload(
        [{"name": "작가A"}],
        {
            "backgroundImage": None,
            "featuredCharacterImageA": "https://example.com/char.png",
        },
    )

    entries = crawler._parse_timetable_payload(payload)

    assert entries[0]["thumbnail_url"] == "https://example.com/char.png"


def test_normalize_kakao_asset_bg_webp():
    crawler = KakaoWebtoonCrawler()

    url = "https://example.com/bg/asset"

    assert crawler._normalize_kakao_asset_url(url) == "https://example.com/bg/asset.webp"


def test_normalize_kakao_asset_t1_webp():
    crawler = KakaoWebtoonCrawler()

    url = "https://example.com/t1/asset"

    assert crawler._normalize_kakao_asset_url(url) == "https://example.com/t1/asset.webp"


def test_normalize_kakao_asset_keeps_extension():
    crawler = KakaoWebtoonCrawler()

    url = "https://example.com/bg/asset.jpg"

    assert crawler._normalize_kakao_asset_url(url) == "https://example.com/bg/asset.jpg"


def test_build_asset_variants_strips_extension():
    crawler = KakaoWebtoonCrawler()

    url = "https://example.com/bg/asset.jpeg"

    assert crawler._build_asset_variants(url, "webp", "jpg") == {
        "webp": "https://example.com/bg/asset.webp",
        "jpg": "https://example.com/bg/asset.jpg",
    }
