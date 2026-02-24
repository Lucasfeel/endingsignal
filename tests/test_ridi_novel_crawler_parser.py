import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.ridi_novel_crawler import RidiNovelCrawler


def _item(
    *,
    book_id,
    serial_id=None,
    serial_title=None,
    serial_completion=None,
    book_title="Book Title",
    categories=None,
):
    serial = None
    if serial_id is not None or serial_title is not None or serial_completion is not None:
        serial = {
            "serialId": serial_id,
            "title": serial_title,
            "completion": serial_completion,
        }
    return {
        "book": {
            "bookId": book_id,
            "title": book_title,
            "serial": serial,
            "authors": [
                {"name": "Writer A", "role": "author"},
                {"name": "Artist A", "role": "illustrator"},
            ],
            "categories": categories or [],
        }
    }


def test_parse_item_completion_variants_for_lightnovel_shapes():
    crawler = RidiNovelCrawler()
    categories = [{"categoryId": 3000, "name": "라이트노벨", "genre": "라노벨", "parentId": 0}]

    serial_true = crawler._parse_item(
        _item(
            book_id="1001",
            serial_id="2001",
            serial_title="Series 1",
            serial_completion=True,
            categories=categories,
        ),
        root_key="lightnovel",
    )
    serial_false = crawler._parse_item(
        _item(
            book_id="1002",
            serial_id="2002",
            serial_title="Series 2",
            serial_completion=False,
            categories=categories,
        ),
        root_key="lightnovel",
    )
    standalone = crawler._parse_item(
        _item(
            book_id="1003",
            serial_id=None,
            serial_title=None,
            serial_completion=None,
            categories=categories,
        ),
        root_key="lightnovel",
    )

    assert serial_true is not None and serial_true["completion"] is True
    assert serial_true["content_id"] == "2001"
    assert serial_true["title"] == "Series 1"
    assert serial_true["genres"] == ["라노벨"]

    assert serial_false is not None and serial_false["completion"] is False
    assert serial_false["content_id"] == "2002"

    assert standalone is not None and standalone["completion"] is True
    assert standalone["content_id"] == "1003"
    assert standalone["content_url"] == "https://ridibooks.com/books/1003"


def test_parse_item_accepts_numeric_ids_and_prefers_serial_id():
    crawler = RidiNovelCrawler()

    with_serial = crawler._parse_item(
        {
            "book": {
                "bookId": 12345,
                "title": "Book Title",
                "serial": {
                    "serialId": 67890,
                    "title": "Serial Title",
                    "completion": False,
                },
            }
        }
    )
    without_serial = crawler._parse_item(
        {
            "book": {
                "bookId": 12345,
                "title": "Book Title",
                "serial": {
                    "serialId": None,
                    "title": "",
                    "completion": False,
                },
            }
        }
    )

    assert with_serial is not None
    assert with_serial["book_id"] == "12345"
    assert with_serial["serial_id"] == "67890"
    assert with_serial["content_id"] == "67890"

    assert without_serial is not None
    assert without_serial["book_id"] == "12345"
    assert without_serial["content_id"] == "12345"


def test_extract_next_data_items_from_nested_payload():
    crawler = RidiNovelCrawler()
    payload = {
        "pageProps": {
            "dehydratedState": {
                "queries": [
                    {
                        "state": {
                            "data": {
                                "section": {
                                    "items": [
                                        {"book": {"bookId": "1", "title": "A"}},
                                        {"book": {"bookId": "2", "title": "B"}},
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        }
    }

    items, found = crawler._extract_next_data_items(payload)

    assert found is True
    assert len(items) == 2
    assert items[0]["book"]["bookId"] == "1"
    assert items[1]["book"]["bookId"] == "2"


def test_extract_build_id_avoids_static_media_false_positive():
    crawler = RidiNovelCrawler()
    html = """
    <script src="/_next/static/media/some-font.woff2"></script>
    <script id="__NEXT_DATA__" type="application/json">
      {"buildId":"456f5f4","page":"/category/[tab]/[category]"}
    </script>
    <script src="/_next/static/456f5f4/_buildManifest.js"></script>
    """

    build_id = crawler._extract_build_id_from_html(html)

    assert build_id == "456f5f4"


def test_max_pages_per_category_zero_disables_cap(monkeypatch):
    monkeypatch.setenv("RIDI_MAX_PAGES_PER_CATEGORY", "0")
    crawler = RidiNovelCrawler()

    assert crawler._max_pages_per_category() is None


def test_merge_entries_unions_categories_genres_roots_and_completion():
    crawler = RidiNovelCrawler()

    romance = crawler._parse_item(
        _item(
            book_id="9001",
            serial_id="5001",
            serial_title="Merged Series",
            serial_completion=False,
            categories=[{"categoryId": 1650, "name": "로맨스", "genre": "로맨스", "parentId": 0}],
        ),
        root_key="webnovel_romance",
    )
    fantasy = crawler._parse_item(
        _item(
            book_id="9002",
            serial_id="5001",
            serial_title="Merged Series",
            serial_completion=True,
            categories=[
                {"categoryId": 1750, "name": "판타지", "genre": "판타지", "parentId": 0},
                {"categoryId": 1650, "name": "로맨스", "genre": "로맨스", "parentId": 0},
            ],
        ),
        root_key="webnovel_fantasy",
    )
    light = crawler._parse_item(
        _item(
            book_id="9003",
            serial_id="5001",
            serial_title="Merged Series",
            serial_completion=False,
            categories=[{"categoryId": 3000, "name": "라이트노벨", "genre": "라노벨", "parentId": 0}],
        ),
        root_key="lightnovel",
    )

    assert romance is not None and fantasy is not None and light is not None

    merged = crawler._merge_entries(None, romance)
    merged = crawler._merge_entries(merged, fantasy)
    merged = crawler._merge_entries(merged, light)

    assert merged["completion"] is True
    assert set(merged["genres"]) == {"로맨스", "판타지", "라노벨"}
    assert set(merged["category_names"]) == {"로맨스", "판타지", "라이트노벨"}
    assert set(merged["crawl_roots"]) == {"webnovel_romance", "webnovel_fantasy", "lightnovel"}
    assert len(merged["categories"]) == 3
