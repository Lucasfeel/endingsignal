"""Shared KakaoPage helpers for backfill and incremental novel crawlers."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from services.kakaopage_parser import STATUS_ONGOING, is_noise_author_token, parse_kakaopage_detail
from services.novel_seed_catalog import build_kakaopage_content_urls
from utils.backfill import STATUS_COMPLETED, coerce_status, merge_genres
from utils.polite_http import BlockedError, extract_html_diagnostics, fetch_text_polite

LOGGER = logging.getLogger(__name__)
SOURCE_KAKAOPAGE = "kakao_page"
KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS = (
    "로그인",
    "연령",
    "권한",
    "접근",
    "차단",
    "captcha",
    "bot",
    "robot",
    "forbidden",
    "denied",
    "verify",
    "age",
)


def resolve_kakaopage_status(
    *,
    parsed_status: Any,
    seed_completed: bool,
    content_id: str,
) -> str:
    status = coerce_status(str(parsed_status or STATUS_ONGOING))
    if seed_completed and status != STATUS_COMPLETED:
        LOGGER.warning(
            "Kakao status override via seed_completed content_id=%s parsed_status=%s final_status=%s",
            content_id,
            status,
            STATUS_COMPLETED,
        )
        return STATUS_COMPLETED
    return status


def is_kakao_suspicious_author_list(authors: Any) -> bool:
    if not isinstance(authors, list):
        return True
    tokens = [str(item).strip() for item in authors if str(item).strip()]
    if not tokens:
        return True
    return any(is_noise_author_token(token) for token in tokens)


def is_probable_kakao_block_page(
    *,
    title: str,
    authors: List[str],
    diagnostics: Dict[str, str],
) -> bool:
    if title and authors:
        return False
    diagnostic_text = " ".join(
        [
            str(diagnostics.get("title") or ""),
            str(diagnostics.get("text_snippet") or ""),
        ]
    ).lower()
    return any(keyword.lower() in diagnostic_text for keyword in KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS)


async def fetch_kakao_detail_and_build_record(
    *,
    session: aiohttp.ClientSession,
    content_id: str,
    discovered_entry: Dict[str, Any],
    headers: Dict[str, str],
    retries: int,
    retry_base_delay_seconds: float,
    retry_max_delay_seconds: float,
    canonical_fallback_enabled: bool = True,
    fetch_text_func=None,
    parse_detail_func=None,
) -> Optional[Dict[str, Any]]:
    if fetch_text_func is None:
        fetch_text_func = fetch_text_polite
    if parse_detail_func is None:
        parse_detail_func = parse_kakaopage_detail
    content_urls = build_kakaopage_content_urls(content_id)
    fetch_url = content_urls["fetch_url"]
    canonical_content_url = content_urls["canonical_url"]

    async def _fetch_and_parse(url: str) -> Tuple[Dict[str, Any], Dict[str, str]]:
        html = await fetch_text_func(
            session,
            url,
            headers=headers,
            retries=retries,
            retry_base_delay_seconds=retry_base_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
        )
        diagnostics = extract_html_diagnostics(html, snippet_size=200)
        parsed = parse_detail_func(
            html,
            fallback_genres=discovered_entry.get("genres", []),
        )
        author_source = str(parsed.get("_author_source") or "").strip()
        if author_source:
            diagnostics["author_source"] = author_source
        return parsed, diagnostics

    parsed, diagnostics = await _fetch_and_parse(fetch_url)
    parsed_authors = parsed.get("authors", []) or []
    parsed_suspicious = is_kakao_suspicious_author_list(parsed_authors)

    canonical_attempted = False
    fallback_diagnostics: Optional[Dict[str, str]] = None
    if canonical_fallback_enabled and (not parsed_authors or parsed_suspicious):
        canonical_attempted = True
        LOGGER.warning(
            "Kakao suspicious/missing authors; attempting canonical fallback content_id=%s fetch_url=%s canonical_url=%s title=%r authors=%s author_source=%s",
            content_id,
            fetch_url,
            canonical_content_url,
            parsed.get("title"),
            parsed_authors,
            diagnostics.get("author_source"),
        )
        fallback_parsed, fallback_diagnostics = await _fetch_and_parse(canonical_content_url)
        fallback_authors = fallback_parsed.get("authors", []) or []
        fallback_suspicious = is_kakao_suspicious_author_list(fallback_authors)
        fallback_author_source = str(fallback_parsed.get("_author_source") or "").strip()

        if fallback_authors and not fallback_suspicious:
            diagnostics = fallback_diagnostics
            diagnostics["author_source"] = "canonical_fallback"
            diagnostics["canonical_author_source"] = fallback_author_source
            parsed = dict(fallback_parsed)
            parsed["_author_source"] = "canonical_fallback"
        else:
            if not parsed.get("title") and fallback_parsed.get("title"):
                parsed["title"] = fallback_parsed.get("title")
            if not parsed.get("status") and fallback_parsed.get("status"):
                parsed["status"] = fallback_parsed.get("status")
            if not parsed.get("genres") and fallback_parsed.get("genres"):
                parsed["genres"] = fallback_parsed.get("genres")
            parsed["authors"] = []
            diagnostics["author_source"] = str(parsed.get("_author_source") or "")
            diagnostics["canonical_fallback_attempted"] = "1"
            diagnostics["canonical_author_source"] = fallback_author_source

    blocked = is_probable_kakao_block_page(
        title=str(parsed.get("title") or ""),
        authors=parsed.get("authors", []) or [],
        diagnostics=diagnostics,
    )
    if (
        not blocked
        and fallback_diagnostics is not None
        and is_probable_kakao_block_page(
            title=str(parsed.get("title") or ""),
            authors=parsed.get("authors", []) or [],
            diagnostics=fallback_diagnostics,
        )
    ):
        blocked = True
    if blocked:
        raise BlockedError(
            status=200,
            url=canonical_content_url if canonical_attempted else fetch_url,
            diagnostics=fallback_diagnostics or diagnostics,
        )

    status = resolve_kakaopage_status(
        parsed_status=parsed.get("status"),
        seed_completed=bool(discovered_entry.get("seed_completed")),
        content_id=content_id,
    )
    genres = merge_genres(parsed.get("genres"), discovered_entry.get("genres"))

    return {
        "content_id": str(content_id or "").strip(),
        "source": SOURCE_KAKAOPAGE,
        "title": parsed.get("title"),
        "authors": parsed.get("authors", []),
        "status": status,
        "content_url": canonical_content_url,
        "genres": genres,
        "_diagnostics": diagnostics,
    }
