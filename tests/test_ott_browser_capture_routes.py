from crawlers.wavve_ott_crawler import WavveOttCrawler


def test_wavve_entries_from_visible_rows_capture_schedule_note():
    crawler = WavveOttCrawler()

    parsed = crawler._entries_from_visible_rows(
        [
            {
                "href": "javascript:void(0)",
                "title": "대한민국에서 건물주 되는 법",
                "schedule_note": "3월 14일, 밤 9시 10분 첫 방송",
                "imgAlt": "대한민국에서 건물주 되는 법 3월 14일, 밤 9시 10분 첫 방송",
                "thumbnail_url": "https://img.example/wavve.jpg",
            }
        ]
    )

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["title"] == "대한민국에서 건물주 되는 법"
    assert item["release_start_at"] is not None
    assert item["platform_source"] == "wavve"
