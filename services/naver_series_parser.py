"""HTML parsing helpers for Naver Series novel list pages."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

STATUS_COMPLETED = "완결"
STATUS_ONGOING = "연재중"
NAVER_SERIES_BASE_URL = "https://series.naver.com"
NAVER_SERIES_DETAIL_URL = "https://series.naver.com/novel/detail.series?productNo={product_no}"
DEFAULT_NOVEL_GENRE = "연재 웹소설"

_PRODUCT_NO_RE = re.compile(r"productNo=(\d+)")
_TRAILING_EPISODE_RE = re.compile(
    r"\(\s*\d+\s*화(?:\s*/\s*(?:완결|미완결))?\s*\)\s*$"
)
_MULTISPACE_RE = re.compile(r"\s+")


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _MULTISPACE_RE.sub(" ", value).strip()


def extract_product_no(href: str) -> Optional[str]:
    """Extract product number from a Naver Series detail href."""
    if not href:
        return None
    match = _PRODUCT_NO_RE.search(href)
    if match:
        return match.group(1)
    parsed = urlparse(href)
    if not parsed.query:
        return None
    query = parse_qs(parsed.query)
    product_numbers = query.get("productNo")
    if not product_numbers:
        return None
    candidate = _clean_text(product_numbers[0])
    return candidate if candidate.isdigit() else None


def _clean_title(raw_title: str) -> str:
    title = _clean_text(raw_title)
    if not title:
        return ""
    title = _TRAILING_EPISODE_RE.sub("", title).strip()
    return title


def _split_authors(raw_value: str) -> List[str]:
    if not raw_value:
        return []
    stripped = raw_value.strip()
    if not stripped:
        return []
    # Keep source strings but split common separators.
    split = re.split(r"[,/&·]|(?:\s+and\s+)", stripped, flags=re.IGNORECASE)
    authors: List[str] = []
    seen = set()
    for part in split:
        name = _clean_text(part)
        if not name:
            continue
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        authors.append(name)
    return authors


def _extract_authors_from_info_text(container_text: str) -> List[str]:
    # Typical pattern: "평점 9.8 | 작가명 | 2024.01.01"
    if "|" not in container_text:
        return []
    segments = [_clean_text(part) for part in container_text.split("|")]
    segments = [part for part in segments if part]
    if len(segments) < 2:
        return []

    candidate = ""
    if len(segments) >= 3 and ("평점" in segments[0] or "별점" in segments[0]):
        candidate = segments[1]
    elif len(segments) >= 2:
        candidate = segments[1]

    # Remove source labels like "작가", "저자".
    candidate = re.sub(r"^(작가|저자|글)\s*[:：]?\s*", "", candidate).strip()
    return _split_authors(candidate)


def _extract_authors(container: Tag) -> List[str]:
    # Try structured nodes first.
    selector_candidates = (
        ".author",
        ".writer",
        ".info .ellipsis",
        ".info",
        "p.info",
        "span.info",
    )
    for selector in selector_candidates:
        for node in container.select(selector):
            text = _clean_text(node.get_text(" ", strip=True))
            authors = _extract_authors_from_info_text(text)
            if authors:
                return authors
            label_match = re.search(r"(?:작가|저자|글)\s*[:：]?\s*(.+)$", text)
            if label_match:
                extracted = _split_authors(label_match.group(1))
                if extracted:
                    return extracted

    # Fallback to scanning all text chunks.
    for text in container.stripped_strings:
        parsed = _extract_authors_from_info_text(_clean_text(text))
        if parsed:
            return parsed

    return []


def _extract_title(anchor: Tag, container: Tag) -> str:
    # Current Naver Series markup includes `<h3><a title="...">...</a></h3>`.
    title_link = container.select_one("h3 a[title]")
    if title_link:
        title = _clean_title(title_link.get("title") or "")
        if title:
            return title

    title_link = container.select_one("h3 a")
    if title_link:
        title = _clean_title(title_link.get_text(" ", strip=True))
        if title:
            return title

    image = container.select_one("img[alt]")
    if image:
        title = _clean_title(image.get("alt") or "")
        if title:
            return title

    title_selectors = (
        ".title",
        ".tit",
        ".subject",
        "strong",
        "h3",
        "h4",
    )
    for selector in title_selectors:
        node = container.select_one(selector)
        if node:
            title = _clean_title(node.get_text(" ", strip=True))
            if title:
                return title

    title_attr = _clean_title(anchor.get("title") or "")
    if title_attr:
        return title_attr
    return _clean_title(anchor.get_text(" ", strip=True))


def _extract_status(container: Tag, *, is_finished_page: bool) -> str:
    text = _clean_text(container.get_text(" ", strip=True))
    if re.search(r"(?<!미)완결", text):
        return STATUS_COMPLETED
    if "미완결" in text:
        return STATUS_ONGOING
    return STATUS_COMPLETED if is_finished_page else STATUS_ONGOING


def _extract_item_genres(container: Tag, default_genres: List[str]) -> List[str]:
    genres: List[str] = []
    seen = set()

    for node in container.select(".genre, .tag, .badge, .label"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        genres.append(text)

    for default_genre in default_genres:
        value = _clean_text(default_genre)
        if not value:
            continue
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        genres.append(value)

    return genres


def parse_naver_series_list(
    html: str,
    *,
    is_finished_page: bool,
    default_genres: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Parse a Naver Series list page and return normalized item dictionaries."""
    soup = BeautifulSoup(html or "", "lxml")
    defaults = default_genres[:] if isinstance(default_genres, list) else [DEFAULT_NOVEL_GENRE]

    results: List[Dict[str, Any]] = []
    seen_product_numbers = set()
    for anchor in soup.select("a[href*='detail.series?productNo=']"):
        href = anchor.get("href") or ""
        product_no = extract_product_no(href)
        if not product_no or product_no in seen_product_numbers:
            continue

        container = anchor.find_parent("li") or anchor.find_parent("div") or anchor
        title = _extract_title(anchor, container)
        if not title:
            continue

        authors = _extract_authors(container)
        status = _extract_status(container, is_finished_page=is_finished_page)
        genres = _extract_item_genres(container, defaults)

        seen_product_numbers.add(product_no)
        results.append(
            {
                "content_id": product_no,
                "content_url": NAVER_SERIES_DETAIL_URL.format(product_no=product_no),
                "title": title,
                "authors": authors,
                "status": status,
                "genres": genres,
                "source_url": urljoin(NAVER_SERIES_BASE_URL, href),
            }
        )
    return results
