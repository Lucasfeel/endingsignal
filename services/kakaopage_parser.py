"""HTML parsing helpers for KakaoPage novel listing/detail pages."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

STATUS_COMPLETED = "완결"
STATUS_ONGOING = "연재중"
KAKAOPAGE_BASE_URL = "https://page.kakao.com"
KAKAOPAGE_GENRE_ROOT_PATH = "/landing/genre/11"
KAKAOPAGE_TITLE_SUFFIX_RE = re.compile(r"\s*-\s*웹소설\s*\|\s*카카오페이지\s*$")
KAKAOPAGE_TITLE_SUFFIX_FALLBACK_RE = re.compile(r"\s*-\s*카카오페이지\s*$")
_MULTISPACE_RE = re.compile(r"\s+")
_CONTENT_ID_RE = re.compile(r"/content/(\d+)")


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return _MULTISPACE_RE.sub(" ", value).strip()


def _dedupe_strings(values: Iterable[str]) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for raw in values:
        text = _clean_text(raw)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(text)
    return deduped


def parse_content_id_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    match = _CONTENT_ID_RE.search(href)
    if not match:
        return None
    return match.group(1)


def extract_listing_content_ids(html: str) -> Set[str]:
    """Extract all `/content/<id>` anchors from a KakaoPage listing page."""
    soup = BeautifulSoup(html or "", "lxml")
    content_ids: Set[str] = set()
    for anchor in soup.select("a[href]"):
        href = anchor.get("href") or ""
        content_id = parse_content_id_from_href(href)
        if content_id:
            content_ids.add(content_id)
    return content_ids


def extract_tab_links(html: str, *, base_url: str = KAKAOPAGE_BASE_URL) -> List[Dict[str, str]]:
    """Discover landing tab URLs under genre/11 and keep a display label."""
    soup = BeautifulSoup(html or "", "lxml")
    discovered: List[Dict[str, str]] = []
    seen_paths = set()

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        parsed = urlparse(href)
        path = parsed.path or ""
        if KAKAOPAGE_GENRE_ROOT_PATH not in path:
            continue
        normalized_path = path.rstrip("/") or KAKAOPAGE_GENRE_ROOT_PATH
        if normalized_path in seen_paths:
            continue
        seen_paths.add(normalized_path)

        label = _clean_text(anchor.get_text(" ", strip=True) or anchor.get("aria-label") or "")
        if not label:
            label = "전체" if normalized_path == KAKAOPAGE_GENRE_ROOT_PATH else normalized_path.split("/")[-1]

        discovered.append(
            {
                "name": label,
                "url": urljoin(base_url, normalized_path),
            }
        )

    return discovered


def _strip_kakaopage_title_suffix(title: str) -> str:
    value = _clean_text(title)
    if not value:
        return ""
    value = KAKAOPAGE_TITLE_SUFFIX_RE.sub("", value).strip()
    value = KAKAOPAGE_TITLE_SUFFIX_FALLBACK_RE.sub("", value).strip()
    return value


def parse_detail_title(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    title_node = soup.select_one("title")
    if not title_node:
        return ""
    return _strip_kakaopage_title_suffix(title_node.get_text(" ", strip=True))


def _author_tokens_from_text(raw_value: str) -> List[str]:
    value = _clean_text(raw_value)
    if not value:
        return []
    value = re.sub(r"^(작가|글|저자|원작)\s*[:：]?\s*", "", value).strip()
    value = re.sub(r"\s+외\s*\d+\s*명$", "", value).strip()
    if not value:
        return []
    split = re.split(r"[,/&·|]", value)
    return _dedupe_strings(split)


def _extract_author_by_title_prefix(soup: BeautifulSoup, title: str) -> List[str]:
    if not title:
        return []
    body = soup.body
    if body is None:
        return []
    title_compact = _clean_text(title)
    for text in body.stripped_strings:
        line = _clean_text(text)
        if not line or len(line) <= len(title_compact):
            continue
        if not line.startswith(title_compact):
            continue
        suffix = line[len(title_compact) :].strip()
        suffix = re.sub(r"^[\s\-|:/·]+", "", suffix).strip()
        if not suffix:
            continue
        authors = _author_tokens_from_text(suffix)
        if authors:
            return authors
    return []


def _extract_author_from_label_text(soup: BeautifulSoup) -> List[str]:
    body = soup.body
    if body is None:
        return []
    for text in body.stripped_strings:
        line = _clean_text(text)
        if not line:
            continue
        match = re.search(r"(?:작가|글|저자)\s*[:：]?\s*(.+)$", line)
        if not match:
            continue
        authors = _author_tokens_from_text(match.group(1))
        if authors:
            return authors
    return []


def parse_detail_authors(html: str, *, title: str) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    by_title = _extract_author_by_title_prefix(soup, title)
    if by_title:
        return by_title
    return _extract_author_from_label_text(soup)


def parse_detail_status(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for node in soup.select("h1, h2, h3, strong, span, div"):
        text = _clean_text(node.get_text(" ", strip=True))
        if not text:
            continue
        if text == STATUS_COMPLETED:
            return STATUS_COMPLETED
        if re.search(r"(?<!미)완결", text):
            return STATUS_COMPLETED
    return STATUS_ONGOING


def parse_detail_genres(html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    genres: List[str] = []

    for text in soup.stripped_strings:
        line = _clean_text(text)
        if "웹소설" not in line or "|" not in line:
            continue
        match = re.search(r"웹소설\s*\|\s*(.+)$", line)
        if not match:
            continue
        candidate = match.group(1).strip()
        candidate = re.sub(r"\s*(연재중|완결)\s*$", "", candidate).strip()
        candidate = candidate.split("카카오페이지")[0].strip()
        if not candidate:
            continue
        split = re.split(r"[,/>|·]", candidate)
        genres.extend(_dedupe_strings(split))

    for anchor in soup.select("a[href*='/landing/genre/11']"):
        label = _clean_text(anchor.get_text(" ", strip=True))
        if label and label not in {"웹소설", "전체"}:
            genres.append(label)

    return _dedupe_strings(genres)


def parse_kakaopage_detail(
    html: str,
    *,
    fallback_genres: Optional[List[str]] = None,
) -> Dict[str, Any]:
    title = parse_detail_title(html)
    authors = parse_detail_authors(html, title=title)
    status = parse_detail_status(html)
    genres = parse_detail_genres(html)
    if fallback_genres:
        genres = _dedupe_strings([*genres, *fallback_genres])
    return {
        "title": title,
        "authors": authors,
        "status": status,
        "genres": genres,
    }
