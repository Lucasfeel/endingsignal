from crawlers.base_crawler import ContentCrawler


class DummyCrawler(ContentCrawler):
    async def fetch_all_data(self):
        return {}, {}, {}, {}

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        return 0


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows


def test_seed_webtoon_publication_dates_returns_inserted_count():
    crawler = DummyCrawler("naver_webtoon")
    cursor = FakeCursor(rows=[{"content_id": "A"}, {"content_id": "B"}])

    inserted_count = crawler.seed_webtoon_publication_dates(cursor)

    assert inserted_count == 2
    query, params = cursor.executed[0]
    assert "INSERT INTO admin_content_metadata" in query
    assert "c.created_at" in query
    assert "c.content_type = 'webtoon'" in query
    assert params == ("naver_webtoon",)


def test_seed_webtoon_publication_dates_returns_zero_for_empty_result():
    crawler = DummyCrawler("kakao_webtoon")
    cursor = FakeCursor(rows=[])

    inserted_count = crawler.seed_webtoon_publication_dates(cursor)

    assert inserted_count == 0
