"""Shared seed definitions for novel backfill and incremental crawlers."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

GENRE_FANTASY = "판타지"
GENRE_HYEONPAN = "현판"
GENRE_ROMANCE = "로맨스"
GENRE_ROMANCE_FANTASY = "로판"
GENRE_MYSTERY = "미스터리"
GENRE_LIGHT_NOVEL = "라이트노벨"
GENRE_WUXIA = "무협"
GENRE_BL = "BL"

NAVER_SERIES_GENRE_SEED_DEFINITIONS = (
    {"key": "romance", "genre": GENRE_ROMANCE, "genre_code": "201"},
    {"key": "romance_fantasy", "genre": GENRE_ROMANCE_FANTASY, "genre_code": "207"},
    {"key": "fantasy", "genre": GENRE_FANTASY, "genre_code": "202"},
    {"key": "hyeonpan", "genre": GENRE_HYEONPAN, "genre_code": "208"},
    {"key": "wuxia", "genre": GENRE_WUXIA, "genre_code": "206"},
    {"key": "mystery", "genre": GENRE_MYSTERY, "genre_code": "203"},
    {"key": "light_novel", "genre": GENRE_LIGHT_NOVEL, "genre_code": "205"},
    {"key": "bl", "genre": GENRE_BL, "genre_code": "209"},
)

KAKAOPAGE_BASE_URL = "https://bff-page.kakao.com"
KAKAOPAGE_PUBLIC_BASE_URL = "https://page.kakao.com"
KAKAOPAGE_GENRE_ROOT_PATH = "/landing/genre/11"
KAKAOPAGE_LIST_URL = f"{KAKAOPAGE_BASE_URL}{KAKAOPAGE_GENRE_ROOT_PATH}"
KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE = f"{KAKAOPAGE_BASE_URL}/content/{{content_id}}"
KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE = f"{KAKAOPAGE_PUBLIC_BASE_URL}/content/{{content_id}}"

KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER = (
    GENRE_FANTASY,
    GENRE_HYEONPAN,
    GENRE_ROMANCE,
    GENRE_ROMANCE_FANTASY,
    GENRE_WUXIA,
    GENRE_BL,
)
KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES = set(KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER)
KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID = {
    "86": GENRE_FANTASY,
    "120": GENRE_HYEONPAN,
    "89": GENRE_ROMANCE,
    "117": GENRE_ROMANCE_FANTASY,
    "87": GENRE_WUXIA,
    "123": GENRE_BL,
}
KAKAOPAGE_WEBNOVELDB_SEED_INPUTS = (
    {"genre_id": "86", "completed": False, "url": "https://page.kakao.com/landing/genre/11/86"},
    {"genre_id": "86", "completed": True, "url": "https://page.kakao.com/landing/genre/11/86?is_complete=true"},
    {"genre_id": "120", "completed": False, "url": "https://page.kakao.com/landing/genre/11/120"},
    {"genre_id": "120", "completed": True, "url": "https://page.kakao.com/landing/genre/11/120?is_complete=true"},
    {"genre_id": "89", "completed": False, "url": "https://page.kakao.com/landing/genre/11/89"},
    {"genre_id": "89", "completed": True, "url": "https://page.kakao.com/landing/genre/11/89?is_complete=true"},
    {"genre_id": "117", "completed": False, "url": "https://page.kakao.com/landing/genre/11/117"},
    {"genre_id": "117", "completed": True, "url": "https://page.kakao.com/landing/genre/11/117?is_complete=true"},
    {"genre_id": "87", "completed": False, "url": "https://page.kakao.com/landing/genre/11/87"},
    {"genre_id": "87", "completed": True, "url": "https://page.kakao.com/landing/genre/11/87?is_complete=true"},
    {"genre_id": "123", "completed": False, "url": "https://page.kakao.com/landing/genre/11/123"},
    {"genre_id": "123", "completed": True, "url": "https://page.kakao.com/landing/genre/11/123?is_complete=true"},
)
KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS = {
    GENRE_FANTASY: 12242,
    GENRE_HYEONPAN: 9378,
    GENRE_ROMANCE: 22188,
    GENRE_ROMANCE_FANTASY: 10652,
    GENRE_WUXIA: 4624,
    GENRE_BL: 3786,
}
KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL = sum(KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS.values())


def build_naver_series_genre_url(genre_code: str, *, completed: bool) -> str:
    base_url = (
        "https://series.naver.com/novel/categoryProductList.series"
        f"?categoryTypeCode=genre&genreCode={genre_code}"
    )
    if completed:
        return f"{base_url}&orderTypeCode=new&is&isFinished=true"
    return base_url


def build_naver_series_seeds() -> Tuple[Dict[str, Any], ...]:
    seeds: List[Dict[str, Any]] = []
    for definition in NAVER_SERIES_GENRE_SEED_DEFINITIONS:
        for completed in (False, True):
            suffix = "completed" if completed else "ongoing"
            seeds.append(
                {
                    "key": f"{definition['key']}_{suffix}",
                    "genre": definition["genre"],
                    "base_url": build_naver_series_genre_url(
                        definition["genre_code"],
                        completed=completed,
                    ),
                    "is_finished_page": completed,
                }
            )
    return tuple(seeds)


NAVER_SERIES_SEEDS = build_naver_series_seeds()


def normalize_kakao_seed_url_to_crawler_host(seed_url: str) -> str:
    parsed_seed = urlparse(seed_url)
    parsed_crawler = urlparse(KAKAOPAGE_BASE_URL)
    return urlunparse(
        parsed_seed._replace(
            scheme=parsed_crawler.scheme,
            netloc=parsed_crawler.netloc,
        )
    )


def build_kakaopage_content_urls(content_id: str) -> Dict[str, str]:
    normalized_content_id = str(content_id or "").strip()
    return {
        "fetch_url": KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE.format(content_id=normalized_content_id),
        "canonical_url": KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE.format(content_id=normalized_content_id),
    }


def build_kakao_tab_url(url: str) -> str:
    parsed = urlparse(url)
    merged = dict(parse_qsl(parsed.query, keep_blank_values=True))
    merged.pop("is_complete", None)
    return urlunparse(parsed._replace(query=urlencode(merged)))


def build_webnoveldb_kakao_seeds() -> List[Dict[str, Any]]:
    seeds: List[Dict[str, Any]] = []
    for item in KAKAOPAGE_WEBNOVELDB_SEED_INPUTS:
        genre_id = str(item.get("genre_id") or "").strip()
        canonical_genre = KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID.get(genre_id)
        if not canonical_genre:
            continue
        input_url = str(item.get("url") or "").strip()
        if not input_url:
            continue
        crawl_url = normalize_kakao_seed_url_to_crawler_host(input_url)
        seeds.append(
            {
                "url": crawl_url,
                "name": canonical_genre,
                "genres": [canonical_genre],
                "seed_completed": bool(item.get("completed")),
                "seed_stat_key": canonical_genre,
            }
        )
    return seeds
