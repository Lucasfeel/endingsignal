from crawlers.wavve_ott_crawler import WavveOttCrawler


def test_wavve_catalog_payload_parser_uses_network_payload_and_skips_lineup():
    crawler = WavveOttCrawler()
    payload = {
        "data": {
            "context_list": [
                {
                    "manualband": {
                        "title1": "웨이브 라인업",
                        "title2": "3월에도 JUST DIVE, Wavve!",
                        "autoplay_description": "3월의 신규 콘텐츠 라인업",
                    },
                    "series": {
                        "refer_id": "C9901_C99000000165",
                        "title": "웨이브 라인업",
                        "actors": "",
                    },
                    "content": {
                        "refer_id": "C9901_C99000000165_01_0016.1",
                        "original_release_date": "2026-02-27",
                        "original_release_year": "2026",
                    },
                    "additional_information": {
                        "info_url": "contentid=C9901_C99000000165_01_0016.1",
                        "play_url": "contentid=C9901_C99000000165_01_0016.1",
                    },
                },
                {
                    "manualband": {
                        "image": "https://image.wavve.com/meta/image/202602/1771988445236947386.jpg",
                        "title1": "대한민국에서 건물주 되는 법",
                        "title2": "3월 14일, 밤 9시 10분 첫 방송",
                        "autoplay_description": "당신도 건물주가 되고 싶습니까?",
                    },
                    "series": {
                        "refer_id": "C3519_C35000000076",
                        "title": "대한민국에서 건물주 되는 법",
                        "synopsis": "빚에 허덕이는 생계형 건물주의 이야기",
                        "actors": "하정우, 임수정, 김준한",
                    },
                    "content": {
                        "refer_id": "C3519_C35000000076_01_0106.1",
                        "original_release_date": "2026-02-26",
                        "original_release_year": "2026",
                    },
                    "additional_information": {
                        "info_url": "contentid=C3519_C35000000076_01_0106.1",
                        "play_url": "contentid=C3519_C35000000076_01_0106.1",
                    },
                },
            ]
        }
    }

    parsed = crawler._parse_catalog_payloads([payload])

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["title"] == "대한민국에서 건물주 되는 법"
    assert item["platform_content_id"] == "C3519_C35000000076"
    assert item["release_start_at"].month == 3
    assert item["release_start_at"].day == 14
    assert item["cast"] == ["하정우", "임수정", "김준한"]
    assert item["platform_url"].endswith("#contentid=C3519_C35000000076")


def test_wavve_raw_item_entry_uses_schedule_note_for_release_date():
    crawler = WavveOttCrawler()

    entry = crawler._entry_from_raw_item(
        {
            "title": "대한민국에서 건물주 되는 법 3월 14일 밤 9시 10분 첫 방송",
            "href": "javascript:void(0)",
            "thumbnail_url": "https://img.example/wavve.jpg",
            "schedule_note": "3월 14일 밤 9시 10분 첫 방송",
        }
    )

    assert entry is not None
    assert entry["title"] == "대한민국에서 건물주 되는 법"
    assert entry["release_start_at"].month == 3
    assert entry["release_start_at"].day == 14
