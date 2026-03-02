"""One-time novel backfill runner for Naver Series and KakaoPage."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from database import create_standalone_connection
from services.kakaopage_parser import (
    KAKAOPAGE_BASE_URL,
    KAKAOPAGE_GENRE_ROOT_PATH,
    extract_listing_content_ids,
    extract_tab_links,
    parse_kakaopage_detail,
)
from services.naver_series_parser import (
    DEFAULT_NOVEL_GENRE,
    NAVER_SERIES_DETAIL_URL,
    STATUS_ONGOING,
    parse_naver_series_list,
)
from utils.backfill import (
    STATUS_COMPLETED as BACKFILL_STATUS_COMPLETED,
    BackfillUpserter,
    coerce_status,
    dedupe_strings,
    merge_genres,
)

LOGGER = logging.getLogger("backfill_novels_once")

SOURCE_NAVER_SERIES = "naver_series"
SOURCE_KAKAOPAGE = "kakao_page"
SUPPORTED_SOURCES = (SOURCE_NAVER_SERIES, SOURCE_KAKAOPAGE)

NAVER_LIST_URL_ONGOING = (
    "https://series.naver.com/novel/categoryProductList.series"
    "?OSType=pc&categoryTypeCode=series&genreCode=&orderTypeCode=new&is=&isFinished=false"
)
NAVER_LIST_URL_COMPLETED = (
    "https://series.naver.com/novel/categoryProductList.series"
    "?OSType=pc&categoryTypeCode=series&genreCode=&orderTypeCode=new&is=&isFinished=true"
)

KAKAOPAGE_LIST_URL = f"{KAKAOPAGE_BASE_URL}{KAKAOPAGE_GENRE_ROOT_PATH}"
KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE = f"{KAKAOPAGE_BASE_URL}/content/{{content_id}}"
KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE = "https://page.kakao.com/content/{content_id}"

KAKAOPAGE_SEED_SET_ALL = "all"
KAKAOPAGE_SEED_SET_WEBNOVELDB = "webnoveldb"
KAKAOPAGE_SEED_SET_CHOICES = (KAKAOPAGE_SEED_SET_ALL, KAKAOPAGE_SEED_SET_WEBNOVELDB)

GENRE_FANTASY = "\ud310\ud0c0\uc9c0"
GENRE_HYEONPAN = "\ud604\ud310"
GENRE_ROMANCE = "\ub85c\ub9e8\uc2a4"
GENRE_ROMANCE_FANTASY = "\ub85c\ud310"
GENRE_WUXIA = "\ubb34\ud611"
GENRE_BL = "BL"
KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES = {
    GENRE_FANTASY,
    GENRE_HYEONPAN,
    GENRE_ROMANCE,
    GENRE_ROMANCE_FANTASY,
    GENRE_WUXIA,
    GENRE_BL,
}
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
    {
        "genre_id": "86",
        "completed": True,
        "url": "https://page.kakao.com/landing/genre/11/86?is_complete=true",
    },
    {"genre_id": "120", "completed": False, "url": "https://page.kakao.com/landing/genre/11/120"},
    {
        "genre_id": "120",
        "completed": True,
        "url": "https://page.kakao.com/landing/genre/11/120?is_complete=true",
    },
    {"genre_id": "89", "completed": False, "url": "https://page.kakao.com/landing/genre/11/89"},
    {
        "genre_id": "89",
        "completed": True,
        "url": "https://page.kakao.com/landing/genre/11/89?is_complete=true",
    },
    {"genre_id": "117", "completed": False, "url": "https://page.kakao.com/landing/genre/11/117"},
    {
        "genre_id": "117",
        "completed": True,
        "url": "https://page.kakao.com/landing/genre/11/117?is_complete=true",
    },
    {"genre_id": "87", "completed": False, "url": "https://page.kakao.com/landing/genre/11/87"},
    {
        "genre_id": "87",
        "completed": True,
        "url": "https://page.kakao.com/landing/genre/11/87?is_complete=true",
    },
    {"genre_id": "123", "completed": False, "url": "https://page.kakao.com/landing/genre/11/123"},
    {
        "genre_id": "123",
        "completed": True,
        "url": "https://page.kakao.com/landing/genre/11/123?is_complete=true",
    },
)


@dataclass
class SourceSummary:
    source: str
    fetched_count: int = 0
    parsed_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    sample_records: List[Dict[str, Any]] = field(default_factory=list)
    seed_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)


def _setup_logging(level: str) -> None:
    numeric = getattr(logging, (level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _clean_int_limit(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_sources(raw_sources: str) -> List[str]:
    if not raw_sources:
        return list(SUPPORTED_SOURCES)
    parsed = [token.strip() for token in raw_sources.split(",") if token.strip()]
    if not parsed:
        return list(SUPPORTED_SOURCES)
    normalized: List[str] = []
    for source in parsed:
        if source not in SUPPORTED_SOURCES:
            raise ValueError(f"Unsupported source: {source!r}. Supported: {', '.join(SUPPORTED_SOURCES)}")
        if source not in normalized:
            normalized.append(source)
    return normalized


def _state_file_path(state_dir: Path, source: str) -> Path:
    return state_dir / f"{source}.json"


def _reset_state_files(state_dir: Path, sources: List[str]) -> None:
    for source in sources:
        state_path = _state_file_path(state_dir, source)
        if not state_path.exists():
            continue
        try:
            state_path.unlink()
            LOGGER.info("Removed state file for source=%s path=%s", source, state_path)
        except Exception:
            LOGGER.warning(
                "Failed to remove state file for source=%s path=%s",
                source,
                state_path,
                exc_info=True,
            )


def _rewind_naver_state(state: Dict[str, Any], rewind_pages: int) -> bool:
    if rewind_pages <= 0:
        return False

    changed = False
    modes_state = state.setdefault("modes", {})
    for mode_name in ("ongoing", "completed"):
        mode_state = modes_state.setdefault(mode_name, {"next_page": 1, "done": False})
        try:
            next_page = int(mode_state.get("next_page", 1) or 1)
        except (TypeError, ValueError):
            next_page = 1
        next_page = max(1, next_page)
        rewound_page = max(1, next_page - rewind_pages)
        was_done = bool(mode_state.get("done"))
        if rewound_page != next_page or was_done:
            mode_state["next_page"] = rewound_page
            mode_state["done"] = False
            changed = True
            LOGGER.info(
                "Rewound Naver state mode=%s next_page=%s->%s done=%s rewind_pages=%s",
                mode_name,
                next_page,
                rewound_page,
                was_done,
                rewind_pages,
            )
    return changed


def _load_state(state_dir: Path, source: str, default: Dict[str, Any]) -> Dict[str, Any]:
    path = _state_file_path(state_dir, source)
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fp:
            parsed = json.load(fp)
        if isinstance(parsed, dict):
            return parsed
    except Exception as exc:
        LOGGER.warning("Failed to load state file %s: %s", path, exc)
    return default


def _save_state(state_dir: Path, source: str, state: Dict[str, Any], *, dry_run: bool) -> None:
    if dry_run:
        return
    path = _state_file_path(state_dir, source)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as fp:
        json.dump(state, fp, ensure_ascii=False, indent=2, sort_keys=True)
    temp_path.replace(path)


def _build_headers(*, referer: str, cookie_header: Optional[str] = None) -> Dict[str, str]:
    headers = {
        **config.CRAWLER_HEADERS,
        "Referer": referer,
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.6,en;q=0.5",
    }
    if cookie_header:
        headers["Cookie"] = cookie_header
    return headers


def _append_query(url: str, **params: Any) -> str:
    parsed = urlparse(url)
    merged = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        merged[str(key)] = str(value)
    return urlunparse(parsed._replace(query=urlencode(merged)))


async def _fetch_text_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: Dict[str, str],
    retries: int = 3,
    retry_base_delay: float = 1.0,
) -> str:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=headers) as response:
                text = await response.text()
                if response.status >= 500:
                    raise RuntimeError(f"HTTP {response.status} {url}")
                if response.status >= 400:
                    raise RuntimeError(f"HTTP {response.status} {url}")
                return text
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            sleep_s = retry_base_delay * (2 ** (attempt - 1)) + random.uniform(0.05, 0.25)
            LOGGER.warning("Request failed (%s/%s) %s: %s", attempt, retries, url, exc)
            await asyncio.sleep(sleep_s)
    raise RuntimeError(f"Request failed after retries: {url}. last_error={last_exc}")


def _normalize_kakao_discovered_entry(raw_entry: Any) -> Dict[str, Any]:
    if not isinstance(raw_entry, dict):
        return {"genres": [], "seed_completed": False}
    raw_genres = raw_entry.get("genres")
    if isinstance(raw_genres, list):
        genres = dedupe_strings(raw_genres)
    else:
        genres = []
    raw_seed_completed = raw_entry.get("seed_completed")
    if isinstance(raw_seed_completed, bool):
        seed_completed = raw_seed_completed
    elif isinstance(raw_seed_completed, (int, float)):
        seed_completed = bool(raw_seed_completed)
    elif isinstance(raw_seed_completed, str):
        seed_completed = raw_seed_completed.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    else:
        seed_completed = False
    return {
        "genres": genres,
        "seed_completed": seed_completed,
    }


def _kakao_cookie_header() -> Optional[str]:
    value = os.getenv("KAKAOPAGE_COOKIE_HEADER")
    if not value:
        return None
    stripped = value.strip()
    return stripped or None


def _cookies_from_env_for_playwright() -> List[Dict[str, Any]]:
    raw_json = os.getenv("KAKAOPAGE_COOKIES_JSON")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                return parsed
        except Exception as exc:
            LOGGER.warning("Failed to parse KAKAOPAGE_COOKIES_JSON: %s", exc)

    raw_cookie_header = _kakao_cookie_header()
    if not raw_cookie_header:
        return []

    cookies = []
    for part in raw_cookie_header.split(";"):
        item = part.strip()
        if not item or "=" not in item:
            continue
        name, value = item.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": ".kakao.com",
                "path": "/",
                "httpOnly": False,
                "secure": True,
            }
        )
    return cookies


def _raise_kakao_playwright_launch_error(exc: Exception) -> None:
    raw_message = str(exc)
    lowered = raw_message.lower()

    missing_lib_error = (
        "libglib-2.0.so.0" in lowered
        or "error while loading shared libraries" in lowered
        or "exitcode=127" in lowered
    )
    missing_browser_error = (
        "executable doesn't exist" in lowered
        or "please run the following command to download new browsers" in lowered
    )

    if not (missing_lib_error or missing_browser_error):
        return

    raise RuntimeError(
        "Playwright Chromium launch failed for KakaoPage backfill. "
        "Use the dedicated backfill image (`Dockerfile.backfill`) or install browser deps in this runtime with "
        "`python -m playwright install --with-deps chromium`."
    ) from exc


def _build_kakao_tab_url(url: str) -> str:
    parsed = urlparse(url)
    merged = dict(parse_qsl(parsed.query, keep_blank_values=True))
    merged.pop("is_complete", None)
    return urlunparse(parsed._replace(query=urlencode(merged)))


def _normalize_kakao_seed_url_to_crawler_host(seed_url: str) -> str:
    parsed_seed = urlparse(seed_url)
    parsed_crawler = urlparse(KAKAOPAGE_BASE_URL)
    return urlunparse(
        parsed_seed._replace(
            scheme=parsed_crawler.scheme,
            netloc=parsed_crawler.netloc,
        )
    )


def _build_kakaopage_content_urls(content_id: str) -> Dict[str, str]:
    normalized_content_id = str(content_id or "").strip()
    return {
        "fetch_url": KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE.format(content_id=normalized_content_id),
        "canonical_url": KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE.format(content_id=normalized_content_id),
    }


def _build_webnoveldb_kakao_seeds() -> List[Dict[str, Any]]:
    seeds: List[Dict[str, Any]] = []
    for item in KAKAOPAGE_WEBNOVELDB_SEED_INPUTS:
        genre_id = str(item.get("genre_id") or "").strip()
        canonical_genre = KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID.get(genre_id)
        if not canonical_genre:
            continue
        input_url = str(item.get("url") or "").strip()
        if not input_url:
            continue
        crawl_url = _normalize_kakao_seed_url_to_crawler_host(input_url)
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


def _normalize_seed_stat_key(raw_value: Any) -> str:
    if not isinstance(raw_value, str):
        return ""
    return raw_value.strip()


def _init_seed_stats(summary: SourceSummary, seed_key: str) -> None:
    normalized_seed_key = _normalize_seed_stat_key(seed_key)
    if not normalized_seed_key:
        return
    summary.seed_stats.setdefault(
        normalized_seed_key,
        {"discovered": 0, "parsed": 0, "skipped": 0, "errors": 0},
    )


def _bump_seed_stats(summary: SourceSummary, seed_keys: List[str], field: str, delta: int = 1) -> None:
    if delta <= 0:
        return
    for seed_key in dedupe_strings(seed_keys):
        normalized_seed_key = _normalize_seed_stat_key(seed_key)
        if not normalized_seed_key:
            continue
        _init_seed_stats(summary, normalized_seed_key)
        summary.seed_stats[normalized_seed_key][field] += delta


def _seed_stat_keys_from_discovered_entry(discovered_entry: Dict[str, Any]) -> List[str]:
    genres = discovered_entry.get("genres", [])
    if not isinstance(genres, list):
        return []
    return [
        genre
        for genre in dedupe_strings(genres)
        if genre in KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES
    ]


def _filter_webnoveldb_discovered_map(raw_discovered_map: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw_discovered_map, dict):
        return {}
    filtered: Dict[str, Dict[str, Any]] = {}
    for raw_content_id, raw_entry in raw_discovered_map.items():
        content_id = str(raw_content_id or "").strip()
        if not content_id:
            continue
        discovered_entry = _normalize_kakao_discovered_entry(raw_entry)
        if not _seed_stat_keys_from_discovered_entry(discovered_entry):
            continue
        filtered[content_id] = discovered_entry
    return filtered


def _resolve_kakaopage_status(
    *,
    parsed_status: Any,
    seed_completed: bool,
    content_id: str,
) -> str:
    status = coerce_status(str(parsed_status or STATUS_ONGOING))
    if seed_completed and status != BACKFILL_STATUS_COMPLETED:
        LOGGER.warning(
            "Kakao status override via seed_completed content_id=%s parsed_status=%s final_status=%s",
            content_id,
            status,
            BACKFILL_STATUS_COMPLETED,
        )
        return BACKFILL_STATUS_COMPLETED
    return status


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_MULTISPACE_RE = re.compile(r"\s+")


def _extract_html_diagnostics(html: str) -> Dict[str, str]:
    raw_html = str(html or "")
    title = ""
    match = _HTML_TITLE_RE.search(raw_html)
    if match:
        title = _MULTISPACE_RE.sub(" ", _HTML_TAG_RE.sub(" ", match.group(1))).strip()
    text = _MULTISPACE_RE.sub(" ", _HTML_TAG_RE.sub(" ", raw_html)).strip()
    return {
        "title": title,
        "text_snippet": text[:200],
    }


def _record_sample(summary: SourceSummary, record: Dict[str, Any], *, max_samples: int = 5) -> None:
    if len(summary.sample_records) >= max_samples:
        return
    summary.sample_records.append(
        {
            "content_id": record.get("content_id"),
            "title": record.get("title"),
            "authors": record.get("authors"),
            "status": record.get("status"),
            "genres": record.get("genres", []),
            "content_url": record.get("content_url"),
        }
    )


async def run_naver_series_backfill(
    *,
    session: aiohttp.ClientSession,
    upserter: BackfillUpserter,
    dry_run: bool,
    max_pages: Optional[int],
    max_items: Optional[int],
    state_dir: Path,
    rewind_pages: int,
    summary: Optional[SourceSummary] = None,
) -> SourceSummary:
    summary = summary or SourceSummary(source=SOURCE_NAVER_SERIES)
    state = _load_state(
        state_dir,
        SOURCE_NAVER_SERIES,
        default={
            "modes": {
                "ongoing": {"next_page": 1, "done": False},
                "completed": {"next_page": 1, "done": False},
            }
        },
    )
    if _rewind_naver_state(state, rewind_pages):
        _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)

    modes = [
        ("ongoing", NAVER_LIST_URL_ONGOING, False),
        ("completed", NAVER_LIST_URL_COMPLETED, True),
    ]

    headers = _build_headers(referer="https://series.naver.com/novel")
    no_new_pages_threshold = _clean_int_limit(
        os.getenv("NAVER_SERIES_BACKFILL_NO_NEW_PAGES_THRESHOLD")
    ) or 3
    repeat_page_threshold = _clean_int_limit(
        os.getenv("NAVER_SERIES_BACKFILL_REPEAT_PAGE_THRESHOLD")
    ) or 2

    for mode_name, base_url, is_finished_page in modes:
        mode_state = state.setdefault("modes", {}).setdefault(
            mode_name, {"next_page": 1, "done": False}
        )
        if mode_state.get("done"):
            LOGGER.info("Naver mode=%s already marked done in state file; skipping.", mode_name)
            continue

        try:
            page = int(mode_state.get("next_page", 1) or 1)
        except (TypeError, ValueError):
            page = 1
        page = max(1, page)
        pages_processed = 0
        seen_ids: Set[str] = set()
        consecutive_no_new_pages = 0
        repeated_page_run = 0
        previous_page_signature: Optional[str] = None

        while True:
            if max_items is not None and summary.parsed_count >= max_items:
                LOGGER.info("Naver reached max_items=%s; stopping.", max_items)
                break
            if max_pages is not None and pages_processed >= max_pages:
                LOGGER.info("Naver reached max_pages=%s for mode=%s; stopping mode.", max_pages, mode_name)
                break

            page_url = _append_query(base_url, page=page)
            LOGGER.info("Naver fetch mode=%s page=%s url=%s", mode_name, page, page_url)
            try:
                html = await _fetch_text_with_retry(session, page_url, headers=headers)
            except Exception:
                summary.error_count += 1
                LOGGER.error("Naver fetch error mode=%s page=%s", mode_name, page, exc_info=True)
                break

            parsed_items = parse_naver_series_list(
                html,
                is_finished_page=is_finished_page,
                default_genres=[DEFAULT_NOVEL_GENRE],
            )
            summary.fetched_count += len(parsed_items)
            if not parsed_items:
                mode_state["done"] = True
                mode_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                LOGGER.info("Naver mode=%s page=%s produced 0 items; marking done.", mode_name, page)
                break

            page_ids: List[str] = []
            for item in parsed_items:
                content_id = str(item.get("content_id") or "").strip()
                if content_id:
                    page_ids.append(content_id)

            new_ids_count = sum(1 for content_id in page_ids if content_id not in seen_ids)
            seen_ids.update(page_ids)

            if new_ids_count == 0:
                consecutive_no_new_pages += 1
            else:
                consecutive_no_new_pages = 0

            page_signature: Optional[str] = None
            if page_ids:
                page_signature = hashlib.sha1(",".join(sorted(page_ids)).encode("utf-8")).hexdigest()
            if page_signature and page_signature == previous_page_signature:
                repeated_page_run += 1
            else:
                repeated_page_run = 1 if page_signature else 0
            previous_page_signature = page_signature

            if consecutive_no_new_pages >= no_new_pages_threshold:
                mode_state["done"] = True
                mode_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                LOGGER.warning(
                    "Naver stopping mode=%s page=%s due to no-new pages threshold=%s (consecutive_no_new_pages=%s).",
                    mode_name,
                    page,
                    no_new_pages_threshold,
                    consecutive_no_new_pages,
                )
                break
            if repeated_page_run >= repeat_page_threshold:
                mode_state["done"] = True
                mode_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                LOGGER.warning(
                    "Naver stopping mode=%s page=%s due to repeat-page threshold=%s (repeat_page_run=%s).",
                    mode_name,
                    page,
                    repeat_page_threshold,
                    repeated_page_run,
                )
                break

            for item in parsed_items:
                if max_items is not None and summary.parsed_count >= max_items:
                    break
                record = {
                    "content_id": item.get("content_id"),
                    "source": SOURCE_NAVER_SERIES,
                    "title": item.get("title"),
                    "authors": item.get("authors", []),
                    "status": coerce_status(
                        item.get("status")
                        or (STATUS_ONGOING if not is_finished_page else BACKFILL_STATUS_COMPLETED)
                    ),
                    "content_url": item.get("content_url")
                    or NAVER_SERIES_DETAIL_URL.format(product_no=item.get("content_id")),
                    "genres": item.get("genres") or [DEFAULT_NOVEL_GENRE],
                }
                accepted = upserter.add_raw(record)
                summary.parsed_count += 1
                if not accepted:
                    LOGGER.debug("Naver skipped invalid record content_id=%s", record.get("content_id"))
                if dry_run:
                    _record_sample(summary, record)

            page += 1
            pages_processed += 1
            mode_state["next_page"] = page
            _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
            LOGGER.info(
                "Naver progress mode=%s pages=%s fetched=%s parsed=%s skipped=%s",
                mode_name,
                pages_processed,
                summary.fetched_count,
                summary.parsed_count,
                summary.skipped_count,
            )

        if max_items is not None and summary.parsed_count >= max_items:
            break

    return summary


async def _discover_kakaopage_ids(
    *,
    page,
    state: Dict[str, Any],
    dry_run: bool,
    state_dir: Path,
    max_items: Optional[int],
    seed_set: str,
    summary: SourceSummary,
) -> None:
    discovered = state.setdefault("discovered", {})
    if not isinstance(discovered, dict):
        discovered = {}
        state["discovered"] = discovered

    tabs_done_raw = state.setdefault("tabs_done", [])
    done_set: Set[str] = set()
    if isinstance(tabs_done_raw, dict):
        for tab_urls in tabs_done_raw.values():
            if not isinstance(tab_urls, list):
                continue
            done_set.update(str(url) for url in tab_urls if isinstance(url, str))
    elif isinstance(tabs_done_raw, list):
        done_set.update(str(url) for url in tabs_done_raw if isinstance(url, str))
    state["tabs_done"] = sorted(done_set)

    stagnant_threshold = int(os.getenv("KAKAOPAGE_BACKFILL_STAGNANT_SCROLLS", "4"))
    max_scrolls_per_tab = int(os.getenv("KAKAOPAGE_BACKFILL_MAX_SCROLLS_PER_TAB", "120"))
    if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB:
        queue: List[Dict[str, Any]] = _build_webnoveldb_kakao_seeds()
    else:
        queue = [
            {
                "name": "all",
                "url": _build_kakao_tab_url(KAKAOPAGE_LIST_URL),
                "genres": [],
                "seed_completed": False,
                "seed_stat_key": "",
            }
        ]

    queued_urls = set()
    for seed in queue:
        url = str(seed.get("url") or "").strip()
        if url:
            queued_urls.add(url)
        seed_stat_key = str(seed.get("seed_stat_key") or "").strip()
        if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB and seed_stat_key:
            _init_seed_stats(summary, seed_stat_key)

    while queue:
        if max_items is not None and len(discovered) >= max_items:
            LOGGER.info("Kakao discovery reached max_items=%s", max_items)
            break

        tab = queue.pop(0)
        tab_url = str(tab.get("url") or "").strip()
        tab_name = str(tab.get("name") or "all").strip() or "all"
        tab_genres = dedupe_strings(tab.get("genres", []))
        tab_seed_completed = bool(tab.get("seed_completed"))
        tab_seed_stat_key = str(tab.get("seed_stat_key") or "").strip()
        if not tab_url:
            continue
        if tab_url in done_set:
            continue

        LOGGER.info(
            "Kakao discovery open tab=%s genres=%s completed_seed=%s url=%s",
            tab_name,
            tab_genres,
            tab_seed_completed,
            tab_url,
        )
        try:
            await page.goto(tab_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1200)
        except Exception as exc:
            LOGGER.error("Kakao discovery failed to open tab=%s: %s", tab_url, exc)
            done_set.add(tab_url)
            state["tabs_done"] = sorted(done_set)
            _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
            continue

        stagnant_rounds = 0
        tab_discovered_ids: Set[str] = set()
        last_html = ""
        for scroll_idx in range(max_scrolls_per_tab):
            html = await page.content()
            last_html = html
            ids = extract_listing_content_ids(html)
            previous_count = len(discovered)

            for content_id in ids:
                entry = _normalize_kakao_discovered_entry(discovered.get(content_id))
                if tab_genres:
                    entry["genres"] = dedupe_strings([*entry["genres"], *tab_genres])
                elif tab_name and tab_name != "all":
                    entry["genres"] = dedupe_strings([*entry["genres"], tab_name])
                if tab_seed_completed:
                    entry["seed_completed"] = bool(entry.get("seed_completed")) or True
                discovered[content_id] = entry
                tab_discovered_ids.add(content_id)

            new_count = len(discovered) - previous_count
            if seed_set != KAKAOPAGE_SEED_SET_WEBNOVELDB:
                tab_links = extract_tab_links(html, base_url=KAKAOPAGE_BASE_URL)
                for tab_link in tab_links:
                    candidate_url = _build_kakao_tab_url(tab_link["url"])
                    if candidate_url in queued_urls or candidate_url in done_set:
                        continue
                    queued_urls.add(candidate_url)
                    queue.append(
                        {
                            "name": tab_link.get("name") or "tab",
                            "url": candidate_url,
                            "genres": [],
                            "seed_completed": False,
                            "seed_stat_key": "",
                        }
                    )

            if new_count <= 0:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)

            LOGGER.info(
                "Kakao discovery tab=%s scroll=%s total_ids=%s new_ids=%s stagnant=%s",
                tab_name,
                scroll_idx + 1,
                len(discovered),
                new_count,
                stagnant_rounds,
            )

            if max_items is not None and len(discovered) >= max_items:
                break
            if stagnant_rounds >= stagnant_threshold:
                break

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1200)

        if not tab_discovered_ids:
            diagnostics = _extract_html_diagnostics(last_html)
            LOGGER.warning(
                "Kakao discovery tab yielded 0 content IDs tab=%s url=%s title=%r html_snippet=%r",
                tab_name,
                tab_url,
                diagnostics.get("title"),
                diagnostics.get("text_snippet"),
            )
        elif seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB and tab_seed_stat_key:
            _bump_seed_stats(summary, [tab_seed_stat_key], "discovered", len(tab_discovered_ids))

        done_set.add(tab_url)
        state["tabs_done"] = sorted(done_set)
        _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)

        if max_items is not None and len(discovered) >= max_items:
            break


async def _fetch_kakao_detail_and_build_record(
    *,
    session: aiohttp.ClientSession,
    content_id: str,
    discovered_entry: Dict[str, Any],
    headers: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    await asyncio.sleep(random.uniform(0.03, 0.15))
    content_urls = _build_kakaopage_content_urls(content_id)
    fetch_url = content_urls["fetch_url"]
    canonical_content_url = content_urls["canonical_url"]
    html = await _fetch_text_with_retry(session, fetch_url, headers=headers)
    parsed = parse_kakaopage_detail(
        html,
        fallback_genres=discovered_entry.get("genres", []),
    )
    status = _resolve_kakaopage_status(
        parsed_status=parsed.get("status"),
        seed_completed=bool(discovered_entry.get("seed_completed")),
        content_id=content_id,
    )
    genres = merge_genres(parsed.get("genres"), discovered_entry.get("genres"))

    return {
        "content_id": content_id,
        "source": SOURCE_KAKAOPAGE,
        "title": parsed.get("title"),
        "authors": parsed.get("authors", []),
        "status": status,
        "content_url": canonical_content_url,
        "genres": genres,
    }


async def run_kakaopage_backfill(
    *,
    upserter: BackfillUpserter,
    dry_run: bool,
    max_items: Optional[int],
    state_dir: Path,
    seed_set: str,
    summary: Optional[SourceSummary] = None,
) -> SourceSummary:
    summary = summary or SourceSummary(source=SOURCE_KAKAOPAGE)
    if seed_set not in KAKAOPAGE_SEED_SET_CHOICES:
        LOGGER.warning("Unknown KakaoPage seed_set=%s; falling back to %s", seed_set, KAKAOPAGE_SEED_SET_ALL)
        seed_set = KAKAOPAGE_SEED_SET_ALL
    state = _load_state(
        state_dir,
        SOURCE_KAKAOPAGE,
        default={"discovered": {}, "tabs_done": [], "detail_done": []},
    )
    if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB:
        pre_filtered_discovered = _filter_webnoveldb_discovered_map(state.get("discovered", {}))
        dropped_count = len(state.get("discovered", {})) - len(pre_filtered_discovered)
        if dropped_count > 0:
            LOGGER.info(
                "Kakao webnoveldb mode dropped non-seed discovered entries from state kept=%s dropped=%s",
                len(pre_filtered_discovered),
                dropped_count,
            )
            state["discovered"] = pre_filtered_discovered
            _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)

    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is required for KakaoPage backfill. "
            "Install with `pip install -r requirements-backfill.txt` and "
            "`python -m playwright install --with-deps chromium`."
        ) from exc

    cookies = _cookies_from_env_for_playwright()
    cookie_header = _kakao_cookie_header()
    if not cookies and not cookie_header:
        LOGGER.warning(
            "No KakaoPage cookies were provided. If discovery/detail pages are age- or login-gated, "
            "set KAKAOPAGE_COOKIE_HEADER or KAKAOPAGE_COOKIES_JSON."
        )

    async with async_playwright() as playwright:
        try:
            browser = await playwright.chromium.launch(headless=True)
        except Exception as exc:
            _raise_kakao_playwright_launch_error(exc)
            raise
        context = await browser.new_context()
        if cookies:
            try:
                await context.add_cookies(cookies)
            except Exception as exc:
                LOGGER.warning("Failed to inject Kakao cookies into Playwright context: %s", exc)
        page = await context.new_page()
        try:
            await _discover_kakaopage_ids(
                page=page,
                state=state,
                dry_run=dry_run,
                state_dir=state_dir,
                max_items=max_items,
                seed_set=seed_set,
                summary=summary,
            )
        finally:
            await context.close()
            await browser.close()

    discovered_map = state.get("discovered", {})
    if not isinstance(discovered_map, dict):
        discovered_map = {}
    if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB:
        for canonical_genre in sorted(KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES):
            _init_seed_stats(summary, canonical_genre)
            summary.seed_stats[canonical_genre]["discovered"] = 0
        for raw_entry in discovered_map.values():
            discovered_entry = _normalize_kakao_discovered_entry(raw_entry)
            _bump_seed_stats(
                summary,
                _seed_stat_keys_from_discovered_entry(discovered_entry),
                "discovered",
            )
    if not discovered_map:
        LOGGER.warning(
            "Kakao discovery returned 0 ids from listing=%s. Verify page access, SSR HTML shape, and cookies.",
            KAKAOPAGE_LIST_URL,
        )
    if not discovered_map and not cookies and not cookie_header:
        LOGGER.warning(
            "Kakao discovery returned 0 ids without cookies. KakaoPage likely requires authenticated/age-verified "
            "cookies for this environment. Provide KAKAOPAGE_COOKIE_HEADER or KAKAOPAGE_COOKIES_JSON."
        )
    detail_done_list = state.get("detail_done", [])
    if not isinstance(detail_done_list, list):
        detail_done_list = []
    detail_done_set: Set[str] = set(str(item) for item in detail_done_list)

    ids_to_process = sorted(str(content_id) for content_id in discovered_map.keys())
    if max_items is not None:
        ids_to_process = ids_to_process[:max_items]

    headers = _build_headers(referer=KAKAOPAGE_LIST_URL, cookie_header=cookie_header)

    timeout = aiohttp.ClientTimeout(
        total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
        connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
        sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
    )
    connector = aiohttp.TCPConnector(limit=max(4, int(os.getenv("KAKAOPAGE_BACKFILL_HTTP_CONCURRENCY", "10"))))
    semaphore = asyncio.Semaphore(max(1, int(os.getenv("KAKAOPAGE_BACKFILL_HTTP_CONCURRENCY", "10"))))
    detail_done_dirty = False
    skipped_missing_author = 0

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def process_one(content_id: str) -> None:
            nonlocal detail_done_dirty, skipped_missing_author
            if content_id in detail_done_set:
                return
            entry = _normalize_kakao_discovered_entry(discovered_map.get(content_id))
            async with semaphore:
                summary.fetched_count += 1
                seed_stat_keys = (
                    _seed_stat_keys_from_discovered_entry(entry)
                    if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB
                    else []
                )
                try:
                    record = await _fetch_kakao_detail_and_build_record(
                        session=session,
                        content_id=content_id,
                        discovered_entry=entry,
                        headers=headers,
                    )
                except Exception as exc:
                    summary.error_count += 1
                    _bump_seed_stats(summary, seed_stat_keys, "errors")
                    LOGGER.error("Kakao detail fetch failed content_id=%s: %s", content_id, exc)
                    return

                if not record:
                    summary.skipped_count += 1
                    _bump_seed_stats(summary, seed_stat_keys, "skipped")
                    return
                summary.parsed_count += 1
                _bump_seed_stats(summary, seed_stat_keys, "parsed")

                authors = record.get("authors") or []
                if not authors:
                    skipped_missing_author += 1
                    summary.skipped_count += 1
                    _bump_seed_stats(summary, seed_stat_keys, "skipped")
                    LOGGER.warning(
                        "Kakao skip missing author content_id=%s url=%s",
                        content_id,
                        record.get("content_url"),
                    )
                    return

                accepted = upserter.add_raw(record)
                if not accepted:
                    _bump_seed_stats(summary, seed_stat_keys, "skipped")
                    LOGGER.debug("Kakao skipped invalid record content_id=%s", content_id)
                if dry_run:
                    _record_sample(summary, record)
                detail_done_set.add(content_id)
                detail_done_dirty = True

        chunk_size = 100
        for idx in range(0, len(ids_to_process), chunk_size):
            chunk = ids_to_process[idx : idx + chunk_size]
            await asyncio.gather(*(process_one(content_id) for content_id in chunk))
            LOGGER.info(
                "Kakao detail progress processed=%s/%s parsed=%s skipped=%s errors=%s",
                min(idx + len(chunk), len(ids_to_process)),
                len(ids_to_process),
                summary.parsed_count,
                summary.skipped_count,
                summary.error_count,
            )
            if detail_done_dirty:
                state["detail_done"] = sorted(detail_done_set)
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
                detail_done_dirty = False

    if skipped_missing_author:
        LOGGER.warning("Kakao skipped %s items due to missing authors.", skipped_missing_author)
    return summary


def _remaining_limit(global_limit: Optional[int], processed: int) -> Optional[int]:
    if global_limit is None:
        return None
    remain = global_limit - processed
    return max(0, remain)


def _make_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-time backfill for Naver Series and KakaoPage novels.")
    parser.add_argument(
        "--sources",
        default="naver_series,kakao_page",
        help="Comma-separated sources. Supported: naver_series,kakao_page",
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize only; do not write to DB.")
    parser.add_argument("--max-pages", type=int, default=None, help="Max pages per Naver mode.")
    parser.add_argument(
        "--rewind-pages",
        type=int,
        default=0,
        help="For Naver Series only: rewind each mode's next_page by N before running (min page is 1).",
    )
    parser.add_argument("--max-items", type=int, default=None, help="Global hard limit across selected sources.")
    parser.add_argument("--db-batch-size", type=int, default=500, help="Batch size for DB upsert commits.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    parser.add_argument("--state-dir", default=".backfill_state/", help="State directory for resumable runs.")
    parser.add_argument(
        "--kakaopage-seed-set",
        choices=KAKAOPAGE_SEED_SET_CHOICES,
        default=KAKAOPAGE_SEED_SET_ALL,
        help="KakaoPage discovery seed set. 'all' keeps existing tab crawling, 'webnoveldb' uses fixed WebNovelDB seeds.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Delete state files for requested sources before starting.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    requested_sources = _parse_sources(args.sources)
    max_pages = _clean_int_limit(args.max_pages)
    rewind_pages = max(0, int(args.rewind_pages or 0))
    max_items = _clean_int_limit(args.max_items)
    batch_size = _clean_int_limit(args.db_batch_size) or 500
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    if args.reset_state:
        _reset_state_files(state_dir, requested_sources)

    timeout = aiohttp.ClientTimeout(
        total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
        connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
        sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
    )
    connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

    conn = None
    if not args.dry_run:
        conn = create_standalone_connection()

    summaries: List[SourceSummary] = []
    total_processed = 0

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for source in requested_sources:
                remaining = _remaining_limit(max_items, total_processed)
                if remaining == 0:
                    LOGGER.info("Global max_items reached before source=%s", source)
                    break

                summary = SourceSummary(source=source)
                upserter = BackfillUpserter(conn, batch_size=batch_size, dry_run=args.dry_run)
                try:
                    if source == SOURCE_NAVER_SERIES:
                        await run_naver_series_backfill(
                            session=session,
                            upserter=upserter,
                            dry_run=args.dry_run,
                            max_pages=max_pages,
                            max_items=remaining,
                            state_dir=state_dir,
                            rewind_pages=rewind_pages,
                            summary=summary,
                        )
                    elif source == SOURCE_KAKAOPAGE:
                        await run_kakaopage_backfill(
                            upserter=upserter,
                            dry_run=args.dry_run,
                            max_items=remaining,
                            state_dir=state_dir,
                            seed_set=args.kakaopage_seed_set,
                            summary=summary,
                        )
                    else:
                        raise ValueError(f"Unsupported source={source}")
                except Exception:
                    summary.error_count += 1
                    LOGGER.error("Backfill failed for source=%s", source, exc_info=True)
                finally:
                    try:
                        upserter.close()
                    except Exception:
                        summary.error_count += 1
                        LOGGER.error("Backfill upserter close failed for source=%s", source, exc_info=True)
                    summary.inserted_count = upserter.stats.inserted_count
                    summary.updated_count = upserter.stats.updated_count
                    summary.skipped_count += upserter.stats.skipped_count

                summaries.append(summary)
                total_processed += summary.parsed_count

    finally:
        if conn:
            conn.close()

    overall = SourceSummary(source="overall")
    for summary in summaries:
        overall.fetched_count += summary.fetched_count
        overall.parsed_count += summary.parsed_count
        overall.inserted_count += summary.inserted_count
        overall.updated_count += summary.updated_count
        overall.skipped_count += summary.skipped_count
        overall.error_count += summary.error_count

    print("\n=== Backfill Summary ===")
    for summary in summaries:
        print(
            f"[{summary.source}] fetched={summary.fetched_count} parsed={summary.parsed_count} "
            f"inserted={summary.inserted_count} updated={summary.updated_count} "
            f"skipped={summary.skipped_count} errors={summary.error_count}"
        )
        if args.dry_run and summary.sample_records:
            print(f"[{summary.source}] dry-run samples:")
            for sample in summary.sample_records:
                print(json.dumps(sample, ensure_ascii=False))
        if args.dry_run and summary.seed_stats:
            print(f"[{summary.source}] dry-run seed stats:")
            for seed_key in sorted(summary.seed_stats.keys()):
                stat = summary.seed_stats[seed_key]
                print(
                    f"[{summary.source}][{seed_key}] discovered={stat.get('discovered', 0)} "
                    f"parsed={stat.get('parsed', 0)} skipped={stat.get('skipped', 0)} errors={stat.get('errors', 0)}"
                )

    print(
        f"[overall] fetched={overall.fetched_count} parsed={overall.parsed_count} "
        f"inserted={overall.inserted_count} updated={overall.updated_count} "
        f"skipped={overall.skipped_count} errors={overall.error_count}"
    )

    if args.dry_run:
        print("Dry run enabled: no DB writes were committed.")
    return 1 if overall.error_count > 0 else 0


def main() -> int:
    load_dotenv()
    parser = _make_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
