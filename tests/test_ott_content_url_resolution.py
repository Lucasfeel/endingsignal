from crawlers.coupang_play_ott_crawler import CoupangPlayOttCrawler


def test_resolve_verification_content_url_prefers_platform_url_for_ott_entries():
    crawler = CoupangPlayOttCrawler()

    url = crawler.resolve_verification_content_url(
        "ott_series:2025:더피트:5978e0d338a2",
        {
            "platform_url": "https://www.coupangplay.com/content/56a158a4-bdfd-4d54-a3d0-e8b0b29d68a8",
            "title": "더 피트",
        },
    )

    assert url == "https://www.coupangplay.com/content/56a158a4-bdfd-4d54-a3d0-e8b0b29d68a8"
