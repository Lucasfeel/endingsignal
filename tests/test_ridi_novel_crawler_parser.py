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
    categories = [{"categoryId": 3000, "name": "Light Novel", "genre": "Light Novel", "parentId": 0}]

    serial_true = crawler._parse_item(
        _item(
            book_id="1001",
            serial_id="2001",
            serial_title="Series 1",
            serial_completion=True,
            categories=categories,
        ),
        root_key="light_novel",
        genre_group="light_novel",
        genre_tokens=("light_novel", "Light Novel"),
    )
    serial_false = crawler._parse_item(
        _item(
            book_id="1002",
            serial_id="2002",
            serial_title="Series 2",
            serial_completion=False,
            categories=categories,
        ),
        root_key="light_novel",
        genre_group="light_novel",
        genre_tokens=("light_novel", "Light Novel"),
    )
    standalone = crawler._parse_item(
        _item(
            book_id="1003",
            serial_id=None,
            serial_title=None,
            serial_completion=None,
            categories=categories,
        ),
        root_key="light_novel",
        genre_group="light_novel",
        genre_tokens=("light_novel", "Light Novel"),
    )

    assert serial_true is not None and serial_true["completion"] is True
    assert serial_true["content_id"] == "2001"
    assert serial_true["title"] == "Series 1"
    assert {"light_novel", "Light Novel"} <= set(serial_true["genres"])

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


def test_parse_item_injects_genre_tokens_from_endpoint_even_when_categories_missing():
    crawler = RidiNovelCrawler()

    parsed = crawler._parse_item(
        _item(
            book_id="5001",
            serial_id="6001",
            serial_title="Endpoint Genre Seed",
            serial_completion=False,
            categories=[],
        ),
        root_key="romance",
        genre_group="romance",
        genre_tokens=("romance", "\ub85c\ub9e8\uc2a4"),
    )

    assert parsed is not None
    assert parsed["genre_group"] == "romance"
    assert parsed["genres"] == ["romance", "\ub85c\ub9e8\uc2a4"]


def test_parse_item_force_completed_overrides_payload_completion_flag():
    crawler = RidiNovelCrawler()

    parsed = crawler._parse_item(
        _item(
            book_id="7001",
            serial_id="8001",
            serial_title="Completed Listing",
            serial_completion=False,
        ),
        root_key="fantasy",
        genre_group="fantasy",
        genre_tokens=("fantasy",),
        force_completed=True,
    )

    assert parsed is not None
    assert parsed["completion"] is True


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


def test_merge_entries_unions_genres_roots_and_completion_without_dropping_tokens():
    crawler = RidiNovelCrawler()

    romance = crawler._parse_item(
        _item(
            book_id="9001",
            serial_id="5001",
            serial_title="Merged Series",
            serial_completion=False,
            categories=[{"categoryId": 1650, "name": "Romance", "genre": "Romance", "parentId": 0}],
        ),
        root_key="romance",
        genre_group="romance",
        genre_tokens=("romance", "\ub85c\ub9e8\uc2a4"),
    )
    fantasy = crawler._parse_item(
        _item(
            book_id="9002",
            serial_id="5001",
            serial_title="Merged Series",
            serial_completion=True,
            categories=[{"categoryId": 1750, "name": "Fantasy", "genre": "Fantasy", "parentId": 0}],
        ),
        root_key="fantasy",
        genre_group="fantasy",
        genre_tokens=("fantasy", "\ud310\ud0c0\uc9c0"),
    )
    light = crawler._parse_item(
        _item(
            book_id="9003",
            serial_id="5001",
            serial_title="Merged Series",
            serial_completion=False,
            categories=[{"categoryId": 3000, "name": "Light Novel", "genre": "Light Novel", "parentId": 0}],
        ),
        root_key="light_novel",
        genre_group="light_novel",
        genre_tokens=("light_novel", "\ub77c\uc774\ud2b8\ub178\ubca8"),
    )

    assert romance is not None and fantasy is not None and light is not None

    merged = crawler._merge_entries(None, romance)
    merged = crawler._merge_entries(merged, fantasy)
    merged = crawler._merge_entries(merged, light)

    assert merged["completion"] is True
    assert {"romance", "fantasy", "light_novel"} <= set(merged["genres"])
    assert set(merged["crawl_roots"]) == {"romance", "fantasy", "light_novel"}
