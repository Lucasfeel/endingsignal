import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.ridi_novel_crawler import RidiNovelCrawler


EXPECTED_START_URLS = {
    "light_novel": {
        "all": "https://api.ridibooks.com/v2/category/books?category_id=3000&tab=books&limit=60&platform=web&offset=0&order_by=popular",
        "completed": "https://api.ridibooks.com/v2/category/books?category_id=3000&tab=books&limit=60&platform=web&offset=0&order_by=popular&series_completed=1",
    },
    "romance": {
        "all": "https://ridibooks.com/_next/data/456f5f4/category/books/1650.json?tab=books&category=1650&page=1",
        "completed": "https://ridibooks.com/_next/data/456f5f4/category/books/1650.json?tab=books&category=1650&series_completed=y&page=1",
    },
    "romance_fantasy": {
        "all": "https://ridibooks.com/_next/data/456f5f4/category/books/6050.json?tab=books&category=6050",
        "completed": "https://ridibooks.com/_next/data/456f5f4/category/books/6050.json?tab=books&category=6050&series_completed=y&page=1",
    },
    "fantasy": {
        "all": "https://ridibooks.com/_next/data/456f5f4/category/books/1750.json?tab=books&category=1750",
        "completed": "https://ridibooks.com/_next/data/456f5f4/category/books/1750.json?tab=books&category=1750&series_completed=y&page=1",
    },
    "bl": {
        "all": "https://ridibooks.com/_next/data/456f5f4/category/books/4150.json?page=1&tab=books&category=4150",
        "completed": "https://ridibooks.com/_next/data/456f5f4/category/books/4150.json?page=1&tab=books&category=4150&series_completed=y",
    },
}


def test_endpoint_registry_start_urls_match_exact_templates():
    crawler = RidiNovelCrawler()
    endpoints = {endpoint.key: endpoint for endpoint in crawler._iter_endpoints()}

    assert endpoints["light_novel"].start_url_all == EXPECTED_START_URLS["light_novel"]["all"]
    assert endpoints["light_novel"].start_url_completed == EXPECTED_START_URLS["light_novel"]["completed"]
    assert endpoints["romance"].start_url_all == EXPECTED_START_URLS["romance"]["all"]
    assert endpoints["romance"].start_url_completed == EXPECTED_START_URLS["romance"]["completed"]
    assert endpoints["romance_fantasy"].start_url_all == EXPECTED_START_URLS["romance_fantasy"]["all"]
    assert endpoints["romance_fantasy"].start_url_completed == EXPECTED_START_URLS["romance_fantasy"]["completed"]
    assert endpoints["fantasy"].start_url_all == EXPECTED_START_URLS["fantasy"]["all"]
    assert endpoints["fantasy"].start_url_completed == EXPECTED_START_URLS["fantasy"]["completed"]
    assert endpoints["bl"].start_url_all == EXPECTED_START_URLS["bl"]["all"]
    assert endpoints["bl"].start_url_completed == EXPECTED_START_URLS["bl"]["completed"]


def test_next_data_page_url_builder_keeps_exact_page_one_and_mutates_page_two():
    crawler = RidiNovelCrawler()
    romance = crawler._get_endpoint("romance")
    romance_fantasy = crawler._get_endpoint("romance_fantasy")
    bl = crawler._get_endpoint("bl")

    assert (
        crawler._build_webnovel_next_data_url(romance, 1, completed_only=False)
        == EXPECTED_START_URLS["romance"]["all"]
    )
    assert (
        crawler._build_webnovel_next_data_url(romance, 2, completed_only=False)
        == "https://ridibooks.com/_next/data/456f5f4/category/books/1650.json?tab=books&category=1650&page=2"
    )
    assert (
        crawler._build_webnovel_next_data_url(romance_fantasy, 2, completed_only=False)
        == "https://ridibooks.com/_next/data/456f5f4/category/books/6050.json?tab=books&category=6050&page=2"
    )
    assert (
        crawler._build_webnovel_next_data_url(bl, 2, completed_only=False)
        == "https://ridibooks.com/_next/data/456f5f4/category/books/4150.json?page=2&tab=books&category=4150"
    )


def test_build_id_replacement_only_changes_next_data_path_segment():
    crawler = RidiNovelCrawler()
    crawler._next_build_id = "newBuild123"
    endpoint = crawler._get_endpoint("fantasy")

    page_one_seeded = crawler._build_webnovel_next_data_url(endpoint, 1, completed_only=True)
    page_one_refreshed = crawler._build_webnovel_next_data_url(
        endpoint,
        1,
        completed_only=True,
        build_id_override="newBuild123",
    )

    assert page_one_seeded == EXPECTED_START_URLS["fantasy"]["completed"]
    assert (
        page_one_refreshed
        == "https://ridibooks.com/_next/data/newBuild123/category/books/1750.json?tab=books&category=1750&series_completed=y&page=1"
    )
