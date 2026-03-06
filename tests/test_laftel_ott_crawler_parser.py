import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import json

import crawlers.laftel_ott_crawler as crawler_module
from crawlers.laftel_ott_crawler import (
    STATUS_ONGOING,
    LaftelOttCrawler,
)


def test_canonical_content_url():
    crawler = LaftelOttCrawler()

    assert crawler._canonical_content_url("16004") == "https://laftel.net/item/16004"


def test_parse_discover_item_maps_required_fields():
    crawler = LaftelOttCrawler()
    item = {
        "id": 16004,
        "name": "Sample Anime",
        "img": {"large": "https://cdn.example.com/poster.jpg"},
        "url": "https://laftel.net/item/16004",
        "is_adult": False,
        "is_ending": True,
        "viewable": True,
        "author": "Writer A",
        "illustrator": "Artist A",
        "genres": ["Fantasy", "Thriller", "Fantasy"],
        "main_tag": [{"name": "Fantasy"}],
    }

    parsed = crawler._parse_discover_item(item, status=STATUS_ONGOING)

    assert parsed is not None
    assert parsed["content_id"] == "16004"
    assert parsed["title"] == "Sample Anime"
    assert parsed["thumbnail_url"] == "https://cdn.example.com/poster.jpg"
    assert parsed["content_url"] == "https://laftel.net/item/16004"
    assert parsed["authors"] == ["Fantasy", "Thriller"]

    attributes = parsed["attributes"]
    assert attributes["is_adult"] is False
    assert attributes["is_ending"] is True
    assert attributes["viewable"] is True
    assert attributes["genre"] == "anime"
    assert "anime" in [token.lower() for token in attributes.get("genres", [])]
    assert "fantasy" in [token.lower() for token in attributes.get("genres", [])]


def test_parse_discover_item_returns_none_for_missing_id_or_title():
    crawler = LaftelOttCrawler()

    assert crawler._parse_discover_item({"name": "Title Only"}, status=STATUS_ONGOING) is None
    assert crawler._parse_discover_item({"id": 999}, status=STATUS_ONGOING) is None


def test_parse_discover_item_leaves_authors_empty_when_genres_missing():
    crawler = LaftelOttCrawler()
    item = {
        "id": 16005,
        "name": "Genreless Anime",
        "genres": [],
        "author": "Writer A",
        "illustrator": "Artist A",
    }

    parsed = crawler._parse_discover_item(item, status=STATUS_ONGOING)

    assert parsed is not None
    assert parsed["authors"] == []


class FakeCursor:
    def __init__(self, existing_rows):
        self._existing_rows = existing_rows
        self._last_query = ""
        self.executemany_calls = []

    def execute(self, query, params=None):
        self._last_query = query

    def fetchall(self):
        if "SELECT content_id FROM contents" in self._last_query:
            return self._existing_rows
        return []

    def executemany(self, query, rows):
        self.executemany_calls.append((query, list(rows)))

    def close(self):
        return None


def test_synchronize_database_persists_display_genres_as_authors(monkeypatch):
    fake_cursor = FakeCursor([])
    monkeypatch.setattr(crawler_module, "get_cursor", lambda conn: fake_cursor)
    crawler = crawler_module.LaftelOttCrawler()

    item = {
        "16004": {
            "title": "Sample Anime",
            "authors": ["Fantasy", "Thriller"],
            "thumbnail_url": "https://cdn.example.com/poster.jpg",
            "content_url": "https://laftel.net/item/16004",
            "attributes": {
                "genres": ["anime", "Fantasy", "Thriller"],
                "genre": "anime",
                "viewable": True,
            },
        }
    }

    crawler.synchronize_database(
        conn=object(),
        all_content_today=item,
        ongoing_today=item,
        hiatus_today={},
        finished_today={},
    )

    insert_rows = []
    for query, rows in fake_cursor.executemany_calls:
        if query.startswith("INSERT INTO contents"):
            insert_rows = rows
            break

    assert len(insert_rows) == 1
    inserted = insert_rows[0]
    assert inserted[5] == "fantasythriller"
    meta = json.loads(inserted[7])
    assert meta["common"]["authors"] == ["Fantasy", "Thriller"]
    assert meta["attributes"]["genres"] == ["anime", "Fantasy", "Thriller"]


def test_adult_filter_excludes_adult_items_by_default(monkeypatch):
    monkeypatch.delenv("LAFTEL_INCLUDE_ADULT", raising=False)
    crawler = LaftelOttCrawler()
    item = {
        "id": 2001,
        "name": "Adult Anime",
        "is_adult": True,
        "viewable": True,
        "type": "animation",
    }

    parsed = crawler._parse_discover_item(item, status=STATUS_ONGOING)

    assert parsed is not None
    assert crawler._should_include_item(item, parsed) is False


def test_adult_filter_can_be_enabled(monkeypatch):
    monkeypatch.setenv("LAFTEL_INCLUDE_ADULT", "true")
    crawler = LaftelOttCrawler()
    item = {
        "id": 2002,
        "name": "Adult Anime",
        "is_adult": True,
        "viewable": True,
        "type": "animation",
    }

    parsed = crawler._parse_discover_item(item, status=STATUS_ONGOING)

    assert parsed is not None
    assert crawler._should_include_item(item, parsed) is True
