import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.ridi_novel_crawler import RidiNovelCrawler


def test_parse_item_prefers_serial_title_and_writer_roles():
    crawler = RidiNovelCrawler()
    item = {
        "book": {
            "bookId": "10001",
            "title": "Volume Title",
            "authors": [
                {"name": "Illustrator A", "role": "illustrator"},
                {"name": "Writer A", "role": "story_writer"},
                {"name": "Translator A", "role": "translator"},
                {"name": "Writer B", "role": "author"},
                {"name": "Writer A", "role": "writer"},
            ],
            "serial": {
                "serialId": "20002",
                "title": " Series\nTitle ",
                "completion": True,
            },
        }
    }

    parsed = crawler._parse_item(item)

    assert parsed is not None
    assert parsed["content_id"] == "20002"
    assert parsed["title"] == "Series Title"
    assert parsed["authors"] == ["Writer A", "Writer B"]
    assert parsed["authors_display"] == "Writer A, Writer B"
    assert parsed["content_url"] == "https://ridibooks.com/books/20002"
    assert parsed["completion"] is True


def test_parse_item_falls_back_to_book_fields_and_defaults_completion_false():
    crawler = RidiNovelCrawler()
    item = {
        "book": {
            "bookId": "30003",
            "title": "  Single   Work ",
            "authors": [
                {"name": "Translator A", "role": "translator"},
                {"name": "Illustrator A", "role": "illustrator"},
                {"name": "Translator A", "role": "translator"},
            ],
            "serial": {
                "serialId": "",
                "title": "",
                "completion": None,
            },
        }
    }

    parsed = crawler._parse_item(item)

    assert parsed is not None
    assert parsed["content_id"] == "30003"
    assert parsed["title"] == "Single Work"
    assert parsed["authors"] == ["Translator A", "Illustrator A"]
    assert parsed["content_url"] == "https://ridibooks.com/books/30003"
    assert parsed["completion"] is False
