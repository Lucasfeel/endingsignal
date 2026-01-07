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
                                    "backgroundImage": "https://example.com/bg.jpg",
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
    assert entry["thumbnail_url"] == "https://example.com/bg.jpg"
    assert entry["weekdays"] == {"tue", "fri"}


def test_parse_completed_payload_shape():
    crawler = KakaoWebtoonCrawler()
    payload = _build_payload([{"name": "작가A"}], {"featuredCharacterImageA": "https://example.com/char.jpg"})
    entries = crawler._parse_timetable_payload(payload)

    assert len(entries) == 1
    assert entries[0]["content_id"] == "1001"
