"""One-time novel backfill runner for Naver Series and KakaoPage."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
from math import ceil
import os
import random
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import aiohttp
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))

import config
from database import create_standalone_connection
from services.kakaopage_novel_common import (
    KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS as SHARED_KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS,
    fetch_kakao_detail_and_build_record as shared_fetch_kakao_detail_and_build_record,
    is_kakao_suspicious_author_list as shared_is_kakao_suspicious_author_list,
    is_probable_kakao_block_page as shared_is_probable_kakao_block_page,
    resolve_kakaopage_status as shared_resolve_kakaopage_status,
)
from services.kakaopage_parser import (
    KAKAOPAGE_BASE_URL,
    KAKAOPAGE_GENRE_ROOT_PATH,
    extract_listing_content_ids,
    extract_tab_links,
    is_noise_author_token,
    parse_content_id_from_href,
    parse_kakaopage_detail,
)
from services.naver_series_parser import (
    NAVER_SERIES_DETAIL_URL,
    STATUS_ONGOING,
    parse_naver_series_list,
)
from services.novel_seed_catalog import (
    GENRE_BL as SHARED_GENRE_BL,
    GENRE_FANTASY as SHARED_GENRE_FANTASY,
    GENRE_HYEONPAN as SHARED_GENRE_HYEONPAN,
    GENRE_LIGHT_NOVEL as SHARED_GENRE_LIGHT_NOVEL,
    GENRE_MYSTERY as SHARED_GENRE_MYSTERY,
    GENRE_ROMANCE as SHARED_GENRE_ROMANCE,
    GENRE_ROMANCE_FANTASY as SHARED_GENRE_ROMANCE_FANTASY,
    GENRE_WUXIA as SHARED_GENRE_WUXIA,
    KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE as SHARED_KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE,
    KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE as SHARED_KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE,
    KAKAOPAGE_LIST_URL as SHARED_KAKAOPAGE_LIST_URL,
    KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER as SHARED_KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER,
    KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES as SHARED_KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES,
    KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS as SHARED_KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS,
    KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL as SHARED_KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL,
    KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID as SHARED_KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID,
    KAKAOPAGE_WEBNOVELDB_SEED_INPUTS as SHARED_KAKAOPAGE_WEBNOVELDB_SEED_INPUTS,
    NAVER_SERIES_GENRE_SEED_DEFINITIONS as SHARED_NAVER_SERIES_GENRE_SEED_DEFINITIONS,
    NAVER_SERIES_SEEDS as SHARED_NAVER_SERIES_SEEDS,
    build_kakao_tab_url as shared_build_kakao_tab_url,
    build_kakaopage_content_urls as shared_build_kakaopage_content_urls,
    build_naver_series_genre_url as shared_build_naver_series_genre_url,
    build_naver_series_seeds as shared_build_naver_series_seeds,
    build_webnoveldb_kakao_seeds as shared_build_webnoveldb_kakao_seeds,
    normalize_kakao_seed_url_to_crawler_host as shared_normalize_kakao_seed_url_to_crawler_host,
)
from utils.backfill import (
    STATUS_COMPLETED as BACKFILL_STATUS_COMPLETED,
    BackfillUpserter,
    coerce_status,
    dedupe_strings,
    merge_genres,
)
from utils.polite_http import (
    AsyncRateLimiter,
    BlockedError,
    HttpStatusError,
    RateLimitedError,
    TransientHttpError,
    extract_html_diagnostics,
    fetch_text_polite,
)
from utils.cgroup_memory import get_memory_snapshot, read_memory_limit_bytes

LOGGER = logging.getLogger("backfill_novels_once")

SOURCE_NAVER_SERIES = "naver_series"
SOURCE_KAKAOPAGE = "kakao_page"
SUPPORTED_SOURCES = (SOURCE_NAVER_SERIES, SOURCE_KAKAOPAGE)

KAKAOPAGE_LIST_URL = f"{KAKAOPAGE_BASE_URL}{KAKAOPAGE_GENRE_ROOT_PATH}"
KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE = f"{KAKAOPAGE_BASE_URL}/content/{{content_id}}"
KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE = "https://page.kakao.com/content/{content_id}"

KAKAOPAGE_SEED_SET_ALL = "all"
KAKAOPAGE_SEED_SET_WEBNOVELDB = "webnoveldb"
KAKAOPAGE_SEED_SET_CHOICES = (KAKAOPAGE_SEED_SET_ALL, KAKAOPAGE_SEED_SET_WEBNOVELDB)
KAKAOPAGE_PHASE_ALL = "all"
KAKAOPAGE_PHASE_DISCOVERY = "discovery"
KAKAOPAGE_PHASE_DETAIL = "detail"
KAKAOPAGE_PHASE_CHOICES = (KAKAOPAGE_PHASE_ALL, KAKAOPAGE_PHASE_DISCOVERY, KAKAOPAGE_PHASE_DETAIL)
KAKAOPAGE_DISCOVERY_STRATEGY_AUTO = "auto"
KAKAOPAGE_DISCOVERY_STRATEGY_FULL = "full"
KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH = "refresh"
KAKAOPAGE_DISCOVERY_STRATEGY_SKIP = "skip"
KAKAOPAGE_DISCOVERY_STRATEGY_CHOICES = (
    KAKAOPAGE_DISCOVERY_STRATEGY_AUTO,
    KAKAOPAGE_DISCOVERY_STRATEGY_FULL,
    KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH,
    KAKAOPAGE_DISCOVERY_STRATEGY_SKIP,
)

GENRE_FANTASY = "\ud310\ud0c0\uc9c0"
GENRE_HYEONPAN = "\ud604\ud310"
GENRE_ROMANCE = "\ub85c\ub9e8\uc2a4"
GENRE_ROMANCE_FANTASY = "\ub85c\ud310"
GENRE_MYSTERY = "\ubbf8\uc2a4\ud130\ub9ac"
GENRE_LIGHT_NOVEL = "\ub77c\uc774\ud2b8\ub178\ubca8"
GENRE_WUXIA = "\ubb34\ud611"
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
KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS = {
    GENRE_FANTASY: 12242,
    GENRE_HYEONPAN: 9378,
    GENRE_ROMANCE: 22188,
    GENRE_ROMANCE_FANTASY: 10652,
    GENRE_WUXIA: 4624,
    GENRE_BL: 3786,
}
KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL = sum(KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS.values())

KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS = (
    "로그인",
    "접근",
    "권한",
    "인증",
    "차단",
    "captcha",
    "bot",
    "robot",
    "forbidden",
    "denied",
    "verify",
    "age",
)


def _build_naver_series_genre_url(genre_code: str, *, completed: bool) -> str:
    base_url = (
        "https://series.naver.com/novel/categoryProductList.series"
        f"?categoryTypeCode=genre&genreCode={genre_code}"
    )
    if completed:
        return f"{base_url}&orderTypeCode=new&is&isFinished=true"
    return base_url


def _build_naver_series_seeds() -> Tuple[Dict[str, Any], ...]:
    seeds: List[Dict[str, Any]] = []
    for definition in NAVER_SERIES_GENRE_SEED_DEFINITIONS:
        for completed in (False, True):
            suffix = "completed" if completed else "ongoing"
            seeds.append(
                {
                    "key": f"{definition['key']}_{suffix}",
                    "genre": definition["genre"],
                    "base_url": _build_naver_series_genre_url(
                        definition["genre_code"],
                        completed=completed,
                    ),
                    "is_finished_page": completed,
                }
            )
    return tuple(seeds)


NAVER_SERIES_SEEDS = _build_naver_series_seeds()

# Keep backfill module exports aligned with the shared seed catalog.
KAKAOPAGE_LIST_URL = SHARED_KAKAOPAGE_LIST_URL
KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE = SHARED_KAKAOPAGE_FETCH_CONTENT_URL_TEMPLATE
KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE = SHARED_KAKAOPAGE_CANONICAL_CONTENT_URL_TEMPLATE
GENRE_FANTASY = SHARED_GENRE_FANTASY
GENRE_HYEONPAN = SHARED_GENRE_HYEONPAN
GENRE_ROMANCE = SHARED_GENRE_ROMANCE
GENRE_ROMANCE_FANTASY = SHARED_GENRE_ROMANCE_FANTASY
GENRE_MYSTERY = SHARED_GENRE_MYSTERY
GENRE_LIGHT_NOVEL = SHARED_GENRE_LIGHT_NOVEL
GENRE_WUXIA = SHARED_GENRE_WUXIA
GENRE_BL = SHARED_GENRE_BL
NAVER_SERIES_GENRE_SEED_DEFINITIONS = SHARED_NAVER_SERIES_GENRE_SEED_DEFINITIONS
NAVER_SERIES_SEEDS = SHARED_NAVER_SERIES_SEEDS
KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER = SHARED_KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER
KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES = SHARED_KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRES
KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID = SHARED_KAKAOPAGE_WEBNOVELDB_GENRE_BY_ID
KAKAOPAGE_WEBNOVELDB_SEED_INPUTS = SHARED_KAKAOPAGE_WEBNOVELDB_SEED_INPUTS
KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS = SHARED_KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS
KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL = SHARED_KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL
KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS = SHARED_KAKAOPAGE_BLOCK_DIAGNOSTIC_KEYWORDS


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
    final_discovered_count: int = 0


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


def _clean_float_limit(value: Optional[Any]) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _is_truthy_flag(raw_value: Optional[Any]) -> bool:
    value = str(raw_value or "").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


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

    _ensure_naver_seed_state(state)
    changed = False
    seeds_state = state.setdefault("seeds", {})
    for seed in NAVER_SERIES_SEEDS:
        seed_key = str(seed["key"])
        seed_state = seeds_state.setdefault(seed_key, {"next_page": 1, "done": False})
        try:
            next_page = int(seed_state.get("next_page", 1) or 1)
        except (TypeError, ValueError):
            next_page = 1
        next_page = max(1, next_page)
        rewound_page = max(1, next_page - rewind_pages)
        was_done = bool(seed_state.get("done"))
        if rewound_page != next_page or was_done:
            seed_state["next_page"] = rewound_page
            seed_state["done"] = False
            changed = True
            LOGGER.info(
                "Rewound Naver state seed=%s next_page=%s->%s done=%s rewind_pages=%s",
                seed_key,
                next_page,
                rewound_page,
                was_done,
                rewind_pages,
            )
    return changed


def _build_naver_seed_state_template() -> Dict[str, Any]:
    return {
        "seeds": {
            str(seed["key"]): {"next_page": 1, "done": False}
            for seed in NAVER_SERIES_SEEDS
        }
    }


def _ensure_naver_seed_state(state: Dict[str, Any]) -> bool:
    changed = False
    seeds_state = state.get("seeds")
    if not isinstance(seeds_state, dict):
        seeds_state = {}
        state["seeds"] = seeds_state
        changed = True

    for seed in NAVER_SERIES_SEEDS:
        seed_key = str(seed["key"])
        seed_state = seeds_state.get(seed_key)
        if isinstance(seed_state, dict):
            continue
        seeds_state[seed_key] = {"next_page": 1, "done": False}
        changed = True

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
    return shared_build_kakao_tab_url(url)


def _normalize_kakao_seed_url_to_crawler_host(seed_url: str) -> str:
    return shared_normalize_kakao_seed_url_to_crawler_host(seed_url)


def _build_kakaopage_content_urls(content_id: str) -> Dict[str, str]:
    return shared_build_kakaopage_content_urls(content_id)


def _build_webnoveldb_kakao_seeds() -> List[Dict[str, Any]]:
    return shared_build_webnoveldb_kakao_seeds()


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
    return shared_resolve_kakaopage_status(
        parsed_status=parsed_status,
        seed_completed=seed_completed,
        content_id=content_id,
    )


def _cookie_names_from_header(cookie_header: Optional[str]) -> List[str]:
    raw_header = str(cookie_header or "").strip()
    if not raw_header:
        return []
    names: List[str] = []
    for part in raw_header.split(";"):
        token = part.strip()
        if not token or "=" not in token:
            continue
        name = token.split("=", 1)[0].strip()
        if name:
            names.append(name)
    return dedupe_strings(names)


def _cookie_header_from_playwright_cookies(raw_cookies: Any) -> Optional[str]:
    if not isinstance(raw_cookies, list):
        return None
    parts: List[str] = []
    for item in raw_cookies:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        value = str(item.get("value") or "").strip()
        if not name or not value:
            continue
        parts.append(f"{name}={value}")
    if not parts:
        return None
    return "; ".join(parts)


def _resolve_kakao_detail_concurrency() -> int:
    preferred = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_DETAIL_CONCURRENCY"))
    if preferred is not None:
        return max(1, preferred)
    legacy = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_HTTP_CONCURRENCY"))
    if legacy is not None:
        return max(1, legacy)
    return 2


def _resolve_kakao_min_interval_seconds() -> float:
    configured_min_interval = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_MIN_INTERVAL_SECONDS"))
    if configured_min_interval is not None:
        return configured_min_interval
    configured_rps = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_RPS"))
    if configured_rps is not None:
        return 1.0 / configured_rps
    return 1.0


def _resolve_kakao_detail_jitter_bounds() -> Tuple[float, float]:
    jitter_min = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_DETAIL_JITTER_MIN_SECONDS")) or 0.8
    jitter_max = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_DETAIL_JITTER_MAX_SECONDS")) or 1.8
    if jitter_max < jitter_min:
        jitter_min, jitter_max = jitter_max, jitter_min
    return jitter_min, jitter_max


def _resolve_kakao_http_retry_policy() -> Tuple[int, float, float]:
    retries = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_HTTP_RETRIES")) or 4
    base_delay = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_HTTP_RETRY_BASE_DELAY_SECONDS")) or 1.0
    max_delay = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_HTTP_RETRY_MAX_DELAY_SECONDS")) or 60.0
    if max_delay < base_delay:
        max_delay = base_delay
    return retries, base_delay, max_delay


def _resolve_kakao_discovery_scroll_delay_ms() -> int:
    base_delay_ms = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_DISCOVERY_SCROLL_DELAY_MS")) or 1600
    jitter_ratio = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_DISCOVERY_SCROLL_JITTER_RATIO")) or 0.25
    jitter_ratio = min(max(jitter_ratio, 0.0), 0.9)
    factor = 1.0 + random.uniform(-jitter_ratio, jitter_ratio)
    return max(200, int(base_delay_ms * factor))


def _resolve_kakao_phase_default() -> str:
    raw_phase = (os.getenv("KAKAOPAGE_BACKFILL_PHASE") or "").strip().lower()
    if raw_phase in KAKAOPAGE_PHASE_CHOICES:
        return raw_phase
    return KAKAOPAGE_PHASE_ALL


def _resolve_kakao_force_detail_refresh_from_env() -> bool:
    return _is_truthy_flag(os.getenv("KAKAOPAGE_BACKFILL_FORCE_DETAIL_REFRESH"))


def _resolve_kakao_canonical_fallback_enabled() -> bool:
    raw_value = os.getenv("KAKAOPAGE_BACKFILL_CANONICAL_FALLBACK")
    if raw_value is None:
        return True
    return _is_truthy_flag(raw_value)


def _resolve_kakao_discovery_strategy_default() -> str:
    raw_strategy = (os.getenv("KAKAOPAGE_BACKFILL_DISCOVERY_STRATEGY") or "").strip().lower()
    if raw_strategy in KAKAOPAGE_DISCOVERY_STRATEGY_CHOICES:
        return raw_strategy
    if raw_strategy:
        LOGGER.warning(
            "Unknown KAKAOPAGE_BACKFILL_DISCOVERY_STRATEGY=%s; falling back to %s",
            raw_strategy,
            KAKAOPAGE_DISCOVERY_STRATEGY_AUTO,
        )
    return KAKAOPAGE_DISCOVERY_STRATEGY_AUTO


def _resolve_effective_kakao_discovery_strategy(
    *,
    configured_strategy: str,
    discovered_count: int,
) -> str:
    strategy = str(configured_strategy or "").strip().lower()
    if strategy not in KAKAOPAGE_DISCOVERY_STRATEGY_CHOICES:
        strategy = KAKAOPAGE_DISCOVERY_STRATEGY_AUTO
    if strategy == KAKAOPAGE_DISCOVERY_STRATEGY_AUTO:
        return (
            KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH
            if discovered_count > 0
            else KAKAOPAGE_DISCOVERY_STRATEGY_FULL
        )
    return strategy


def _resolve_kakao_discovery_no_global_growth_scrolls() -> int:
    return _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_DISCOVERY_NO_GLOBAL_GROWTH_SCROLLS")) or 8


def _resolve_kakao_discovery_no_new_ids_scrolls() -> int:
    return (
        _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS"))
        or _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_STAGNANT_SCROLLS"))
        or 1
    )


def _resolve_kakao_discovery_max_memory_usage_ratio() -> float:
    parsed_ratio = _clean_float_limit(os.getenv("KAKAOPAGE_BACKFILL_DISCOVERY_MAX_MEMORY_USAGE_RATIO"))
    if parsed_ratio is None:
        return 0.85
    return min(parsed_ratio, 1.0)


def _should_stop_kakao_discovery_tab(
    *,
    strategy: str,
    no_global_growth_rounds: int,
    no_new_ids_rounds: int,
    no_global_growth_threshold: int,
    no_new_ids_threshold: int,
    memory_usage_ratio: Optional[float],
    max_memory_usage_ratio: float,
) -> Optional[str]:
    if (
        isinstance(memory_usage_ratio, (int, float))
        and memory_usage_ratio > 0
        and memory_usage_ratio >= max_memory_usage_ratio
    ):
        return "memory_guard"
    if (
        strategy == KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH
        and no_global_growth_rounds >= max(1, no_global_growth_threshold)
    ):
        return "no_global_growth"
    if no_new_ids_rounds >= max(1, no_new_ids_threshold):
        return "no_new_ids"
    return None


def _collect_kakao_webnoveldb_discovered_counts(raw_discovered_map: Any) -> Dict[str, int]:
    counts = {
        canonical_genre: 0
        for canonical_genre in KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER
    }
    if not isinstance(raw_discovered_map, dict):
        return counts
    for raw_entry in raw_discovered_map.values():
        discovered_entry = _normalize_kakao_discovered_entry(raw_entry)
        for canonical_genre in _seed_stat_keys_from_discovered_entry(discovered_entry):
            counts[canonical_genre] += 1
    return counts


def _build_kakao_webnoveldb_coverage_metrics(*, discovered: int, expected: int) -> Dict[str, Any]:
    target90 = ceil(expected * 0.90)
    target97 = ceil(expected * 0.97)
    coverage_ratio = (discovered / expected) if expected > 0 else 0.0
    if discovered >= target97:
        status = "meets97"
    elif discovered >= target90:
        status = "meets90"
    else:
        status = "below90"
    return {
        "discovered": discovered,
        "expected": expected,
        "coverage_ratio": coverage_ratio,
        "target90": target90,
        "target97": target97,
        "status": status,
    }


def _build_kakao_webnoveldb_coverage_rows(
    *,
    discovered_counts: Dict[str, int],
    unique_discovered_count: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    discovered_total = 0
    for canonical_genre in KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER:
        discovered = int(discovered_counts.get(canonical_genre, 0) or 0)
        expected = KAKAOPAGE_WEBNOVELDB_EXPECTED_COUNTS[canonical_genre]
        discovered_total += discovered
        rows.append(
            {
                "label": canonical_genre,
                **_build_kakao_webnoveldb_coverage_metrics(
                    discovered=discovered,
                    expected=expected,
                ),
            }
        )
    rows.append(
        {
            "label": "total",
            "diagnostic_only": True,
            "unique_discovered": unique_discovered_count,
            **_build_kakao_webnoveldb_coverage_metrics(
                discovered=discovered_total,
                expected=KAKAOPAGE_WEBNOVELDB_EXPECTED_TOTAL,
            ),
        }
    )
    return rows


def _render_kakao_webnoveldb_coverage_lines(summary: SourceSummary) -> List[str]:
    discovered_counts = {
        canonical_genre: int(summary.seed_stats.get(canonical_genre, {}).get("discovered", 0) or 0)
        for canonical_genre in KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER
    }
    rows = _build_kakao_webnoveldb_coverage_rows(
        discovered_counts=discovered_counts,
        unique_discovered_count=summary.final_discovered_count or None,
    )
    rendered: List[str] = []
    for row in rows:
        line = (
            f"[{summary.source}][{row['label']}] discovered={row['discovered']} expected={row['expected']} "
            f"coverage={row['coverage_ratio'] * 100:.1f}% target90={row['target90']} "
            f"target97={row['target97']} status={row['status']}"
        )
        unique_discovered = row.get("unique_discovered")
        if unique_discovered is not None:
            line += f" unique_discovered={unique_discovered}"
        if row.get("diagnostic_only"):
            line += " diagnostic_only=true"
        rendered.append(line)
    return rendered


def _render_source_summary_lines(
    summary: SourceSummary,
    *,
    dry_run: bool,
    kakaopage_seed_set: str,
) -> List[str]:
    lines = [
        (
            f"[{summary.source}] fetched={summary.fetched_count} parsed={summary.parsed_count} "
            f"inserted={summary.inserted_count} updated={summary.updated_count} "
            f"skipped={summary.skipped_count} errors={summary.error_count}"
        )
    ]
    if dry_run and summary.sample_records:
        lines.append(f"[{summary.source}] dry-run samples:")
        for sample in summary.sample_records:
            lines.append(json.dumps(sample, ensure_ascii=False))
    if dry_run and summary.seed_stats:
        lines.append(f"[{summary.source}] dry-run seed stats:")
        for seed_key in sorted(summary.seed_stats.keys()):
            stat = summary.seed_stats[seed_key]
            lines.append(
                f"[{summary.source}][{seed_key}] discovered={stat.get('discovered', 0)} "
                f"parsed={stat.get('parsed', 0)} skipped={stat.get('skipped', 0)} errors={stat.get('errors', 0)}"
            )
    if summary.source == SOURCE_KAKAOPAGE and kakaopage_seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB:
        lines.extend(_render_kakao_webnoveldb_coverage_lines(summary))
    return lines


def _kakao_discovery_target_ids(discovered_map: Dict[str, Any], max_items: Optional[int]) -> List[str]:
    ids = sorted(str(content_id) for content_id in discovered_map.keys())
    if max_items is not None:
        ids = ids[:max_items]
    return ids


def _is_kakao_detail_already_complete(
    *,
    discovered_map: Dict[str, Any],
    detail_done_set: Set[str],
    max_items: Optional[int],
) -> bool:
    ids_to_process = _kakao_discovery_target_ids(discovered_map, max_items)
    if not ids_to_process:
        return False
    return all(content_id in detail_done_set for content_id in ids_to_process)


def _should_skip_kakao_discovery_from_state(
    *,
    state: Dict[str, Any],
    seed_set: str,
    strategy: str,
) -> bool:
    if strategy == KAKAOPAGE_DISCOVERY_STRATEGY_FULL:
        return False
    if not bool(state.get("discovery_complete")):
        return False
    return str(state.get("discovery_seed_set") or "").strip() == str(seed_set or "").strip()


def _mark_kakao_discovery_complete(
    *,
    state: Dict[str, Any],
    seed_set: str,
    strategy: str,
) -> None:
    state["discovery_complete"] = True
    state["discovery_seed_set"] = seed_set
    state["discovery_strategy_last"] = strategy
    state["discovery_completed_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _mark_kakao_discovery_incomplete(
    *,
    state: Dict[str, Any],
    seed_set: str,
    strategy: str,
) -> None:
    state["discovery_complete"] = False
    state["discovery_seed_set"] = seed_set
    state["discovery_strategy_last"] = strategy
    state["discovery_completed_at"] = None


def _resolve_kakao_allow_low_memory_playwright_from_env() -> bool:
    return _is_truthy_flag(os.getenv("KAKAOPAGE_BACKFILL_ALLOW_LOW_MEMORY_PLAYWRIGHT"))


def _resolve_kakao_playwright_args() -> List[str]:
    default_args = [
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-gpu",
    ]
    raw_json = os.getenv("KAKAOPAGE_BACKFILL_PLAYWRIGHT_ARGS_JSON")
    if not raw_json:
        return default_args
    try:
        parsed = json.loads(raw_json)
    except Exception as exc:
        LOGGER.warning("Invalid KAKAOPAGE_BACKFILL_PLAYWRIGHT_ARGS_JSON, using defaults: %s", exc)
        return default_args
    if not isinstance(parsed, list):
        LOGGER.warning("KAKAOPAGE_BACKFILL_PLAYWRIGHT_ARGS_JSON must be a JSON list, using defaults.")
        return default_args
    normalized = [str(item).strip() for item in parsed if str(item).strip()]
    if not normalized:
        LOGGER.warning("KAKAOPAGE_BACKFILL_PLAYWRIGHT_ARGS_JSON resolved empty args, using defaults.")
        return default_args
    return normalized


def _load_playwright_async_api():
    from playwright.async_api import async_playwright

    return async_playwright


def _guard_kakao_playwright_memory(
    *,
    allow_low_memory_playwright: bool,
) -> None:
    threshold_mb = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_MIN_MEMORY_FOR_PLAYWRIGHT_MB")) or 1024
    limit_bytes = read_memory_limit_bytes()
    snapshot = get_memory_snapshot()
    if limit_bytes is None:
        LOGGER.info(
            "Kakao memory guard: cgroup limit not detected (threshold_mb=%s usage_bytes=%s).",
            threshold_mb,
            snapshot.get("usage_bytes"),
        )
        return
    limit_mb = int(limit_bytes / (1024 * 1024))
    if allow_low_memory_playwright or limit_mb >= threshold_mb:
        LOGGER.info(
            "Kakao memory guard: limit_mb=%s threshold_mb=%s allow_override=%s",
            limit_mb,
            threshold_mb,
            allow_low_memory_playwright,
        )
        return
    raise RuntimeError(
        "KakaoPage discovery blocked by low-memory guard before launching Playwright: "
        f"detected_limit_mb={limit_mb} threshold_mb={threshold_mb}. "
        "Run discovery on a higher-memory worker/local machine, or run with "
        "--kakaopage-phase detail if discovered IDs already exist, or explicitly set "
        "--kakaopage-allow-low-memory-playwright / KAKAOPAGE_BACKFILL_ALLOW_LOW_MEMORY_PLAYWRIGHT=1."
    )


def _is_probable_kakao_block_page(
    *,
    title: str,
    authors: List[str],
    diagnostics: Dict[str, str],
) -> bool:
    return shared_is_probable_kakao_block_page(
        title=title,
        authors=authors,
        diagnostics=diagnostics,
    )


def _trip_kakao_circuit_if_needed(
    *,
    error_kind: str,
    consecutive_rate_limits: int,
    max_consecutive_rate_limits: int,
    stop_event: asyncio.Event,
) -> bool:
    should_trip = False
    if error_kind == "blocked":
        should_trip = True
    elif error_kind == "rate_limited" and consecutive_rate_limits >= max(1, max_consecutive_rate_limits):
        should_trip = True
    if should_trip:
        stop_event.set()
    return should_trip


def _extract_html_diagnostics(html: str) -> Dict[str, str]:
    return extract_html_diagnostics(html, snippet_size=200)


def _normalize_kakao_anchor_href(raw_href: Any) -> str:
    href = str(raw_href or "").strip()
    if not href:
        return ""
    return urljoin(KAKAOPAGE_BASE_URL, href)


async def _extract_listing_ids_via_dom(page) -> Set[str]:
    hrefs = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => e.getAttribute('href'))",
    )
    content_ids: Set[str] = set()
    if not isinstance(hrefs, list):
        return content_ids
    for raw_href in hrefs:
        normalized_href = _normalize_kakao_anchor_href(raw_href)
        content_id = parse_content_id_from_href(normalized_href)
        if content_id:
            content_ids.add(content_id)
    return content_ids


async def _extract_tab_links_via_dom(page) -> List[Dict[str, str]]:
    rows = await page.eval_on_selector_all(
        "a[href]",
        (
            "els => els.map(e => ({"
            "href: e.getAttribute('href') || '',"
            "label: (e.getAttribute('aria-label') || e.textContent || '').trim()"
            "}))"
        ),
    )
    discovered: List[Dict[str, str]] = []
    seen_urls: Set[str] = set()
    if not isinstance(rows, list):
        return discovered
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized_href = _normalize_kakao_anchor_href(row.get("href"))
        if not normalized_href:
            continue
        parsed = urlparse(normalized_href)
        if KAKAOPAGE_GENRE_ROOT_PATH not in (parsed.path or ""):
            continue
        normalized_tab_url = _build_kakao_tab_url(normalized_href)
        if not normalized_tab_url or normalized_tab_url in seen_urls:
            continue
        seen_urls.add(normalized_tab_url)
        label = str(row.get("label") or "").strip()
        if not label:
            label = "tab"
        discovered.append({"name": label, "url": normalized_tab_url})
    return discovered


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
    shutdown_event: Optional[asyncio.Event] = None,
    summary: Optional[SourceSummary] = None,
) -> SourceSummary:
    summary = summary or SourceSummary(source=SOURCE_NAVER_SERIES)
    state = _load_state(
        state_dir,
        SOURCE_NAVER_SERIES,
        default=_build_naver_seed_state_template(),
    )
    state_changed = _ensure_naver_seed_state(state)
    if _rewind_naver_state(state, rewind_pages):
        state_changed = True
    if state_changed:
        _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)

    headers = _build_headers(referer="https://series.naver.com/novel")
    no_new_pages_threshold = _clean_int_limit(
        os.getenv("NAVER_SERIES_BACKFILL_NO_NEW_PAGES_THRESHOLD")
    ) or 3
    repeat_page_threshold = _clean_int_limit(
        os.getenv("NAVER_SERIES_BACKFILL_REPEAT_PAGE_THRESHOLD")
    ) or 2

    for seed in NAVER_SERIES_SEEDS:
        seed_key = str(seed["key"])
        base_url = str(seed["base_url"])
        is_finished_page = bool(seed["is_finished_page"])
        default_genres = [str(seed["genre"])]
        seed_state = state.setdefault("seeds", {}).setdefault(
            seed_key, {"next_page": 1, "done": False}
        )
        if seed_state.get("done"):
            LOGGER.info("Naver seed=%s already marked done in state file; skipping.", seed_key)
            continue

        try:
            page = int(seed_state.get("next_page", 1) or 1)
        except (TypeError, ValueError):
            page = 1
        page = max(1, page)
        pages_processed = 0
        seen_ids: Set[str] = set()
        consecutive_no_new_pages = 0
        repeated_page_run = 0
        previous_page_signature: Optional[str] = None

        while True:
            if shutdown_event is not None and shutdown_event.is_set():
                LOGGER.warning(
                    "Naver backfill interrupted by stop event seed=%s page=%s; state will be saved for resume.",
                    seed_key,
                    page,
                )
                seed_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                break
            if max_items is not None and summary.parsed_count >= max_items:
                LOGGER.info("Naver reached max_items=%s; stopping.", max_items)
                break
            if max_pages is not None and pages_processed >= max_pages:
                LOGGER.info("Naver reached max_pages=%s for seed=%s; stopping seed.", max_pages, seed_key)
                break

            page_url = _append_query(base_url, page=page)
            LOGGER.info("Naver fetch seed=%s page=%s url=%s", seed_key, page, page_url)
            try:
                html = await _fetch_text_with_retry(session, page_url, headers=headers)
            except Exception:
                summary.error_count += 1
                LOGGER.error("Naver fetch error seed=%s page=%s", seed_key, page, exc_info=True)
                break

            parsed_items = parse_naver_series_list(
                html,
                is_finished_page=is_finished_page,
                default_genres=default_genres,
            )
            summary.fetched_count += len(parsed_items)
            if not parsed_items:
                seed_state["done"] = True
                seed_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                LOGGER.info("Naver seed=%s page=%s produced 0 items; marking done.", seed_key, page)
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
                seed_state["done"] = True
                seed_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                LOGGER.warning(
                    "Naver stopping seed=%s page=%s due to no-new pages threshold=%s (consecutive_no_new_pages=%s).",
                    seed_key,
                    page,
                    no_new_pages_threshold,
                    consecutive_no_new_pages,
                )
                break
            if repeated_page_run >= repeat_page_threshold:
                seed_state["done"] = True
                seed_state["next_page"] = page
                _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
                LOGGER.warning(
                    "Naver stopping seed=%s page=%s due to repeat-page threshold=%s (repeat_page_run=%s).",
                    seed_key,
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
                    "genres": item.get("genres") or default_genres,
                }
                accepted = upserter.add_raw(record)
                summary.parsed_count += 1
                if not accepted:
                    LOGGER.debug("Naver skipped invalid record content_id=%s", record.get("content_id"))
                if dry_run:
                    _record_sample(summary, record)

            page += 1
            pages_processed += 1
            seed_state["next_page"] = page
            _save_state(state_dir, SOURCE_NAVER_SERIES, state, dry_run=dry_run)
            LOGGER.info(
                "Naver progress seed=%s pages=%s fetched=%s parsed=%s skipped=%s",
                seed_key,
                pages_processed,
                summary.fetched_count,
                summary.parsed_count,
                summary.skipped_count,
            )

        if max_items is not None and summary.parsed_count >= max_items:
            break
        if shutdown_event is not None and shutdown_event.is_set():
            break

    if shutdown_event is not None and shutdown_event.is_set():
        raise RuntimeError("Naver backfill interrupted by shutdown signal. Progress saved to state file.")
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
    stop_event: asyncio.Event,
    strategy: str,
) -> Dict[str, Any]:
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

    no_new_ids_threshold = _resolve_kakao_discovery_no_new_ids_scrolls()
    no_global_growth_threshold = _resolve_kakao_discovery_no_global_growth_scrolls()
    max_memory_usage_ratio = _resolve_kakao_discovery_max_memory_usage_ratio()
    if strategy not in (KAKAOPAGE_DISCOVERY_STRATEGY_FULL, KAKAOPAGE_DISCOVERY_STRATEGY_REFRESH):
        strategy = KAKAOPAGE_DISCOVERY_STRATEGY_FULL
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

    blocked_resource_types = {"image", "media", "font", "stylesheet"}

    async def _route_handler(route) -> None:
        resource_type = str(route.request.resource_type or "").strip().lower()
        if resource_type in blocked_resource_types:
            await route.abort()
            return
        await route.continue_()

    try:
        await page.route("**/*", _route_handler)
    except Exception as exc:
        LOGGER.debug("Kakao discovery route optimization unavailable: %s", exc)

    discovery_abort_reason: Optional[str] = None
    while queue:
        if stop_event.is_set():
            LOGGER.warning("Kakao discovery interrupted by stop event; saving state before exit.")
            _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
            discovery_abort_reason = "shutdown"
            break
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
            "Kakao discovery open tab=%s strategy=%s genres=%s completed_seed=%s url=%s",
            tab_name,
            strategy,
            tab_genres,
            tab_seed_completed,
            tab_url,
        )
        try:
            await page.goto(tab_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(_resolve_kakao_discovery_scroll_delay_ms())
        except Exception as exc:
            LOGGER.error("Kakao discovery failed to open tab=%s: %s", tab_url, exc)
            done_set.add(tab_url)
            state["tabs_done"] = sorted(done_set)
            _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
            continue

        no_global_growth_rounds = 0
        no_new_ids_rounds = 0
        tab_discovered_ids: Set[str] = set()
        last_html = ""
        last_scroll_number = 0
        last_global_new_count = 0
        last_tab_local_new_count = 0
        tab_entry_dirty = False
        tab_stop_reason: Optional[str] = None
        scroll_idx = 0
        while True:
            scroll_idx += 1
            last_scroll_number = scroll_idx
            if stop_event.is_set():
                LOGGER.warning("Kakao discovery stop event while scrolling tab=%s; persisting state.", tab_name)
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
                discovery_abort_reason = "shutdown"
                break
            memory_snapshot = get_memory_snapshot()
            usage_ratio = memory_snapshot.get("usage_ratio")
            if (
                isinstance(usage_ratio, (int, float))
                and usage_ratio > 0
                and usage_ratio >= max_memory_usage_ratio
            ):
                tab_stop_reason = "memory_guard"
                discovery_abort_reason = tab_stop_reason
                LOGGER.warning(
                    "Kakao discovery memory guard tab=%s scroll=%s strategy=%s usage_ratio=%.3f threshold=%.3f "
                    "usage_bytes=%s limit_bytes=%s stop_reason=%s",
                    tab_name,
                    scroll_idx,
                    strategy,
                    usage_ratio,
                    max_memory_usage_ratio,
                    memory_snapshot.get("usage_bytes"),
                    memory_snapshot.get("limit_bytes"),
                    tab_stop_reason,
                )
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
                break

            used_html_fallback = False
            fallback_tab_links: List[Dict[str, str]] = []
            try:
                ids = await _extract_listing_ids_via_dom(page)
                if seed_set != KAKAOPAGE_SEED_SET_WEBNOVELDB:
                    discovered_tabs = await _extract_tab_links_via_dom(page)
                else:
                    discovered_tabs = []
            except Exception as exc:
                LOGGER.debug("Kakao discovery DOM extraction failed tab=%s scroll=%s: %s", tab_name, scroll_idx, exc)
                html = await page.content()
                last_html = html
                ids = extract_listing_content_ids(html)
                if seed_set != KAKAOPAGE_SEED_SET_WEBNOVELDB:
                    fallback_tab_links = extract_tab_links(html, base_url=KAKAOPAGE_BASE_URL)
                used_html_fallback = True
                discovered_tabs = []
            previous_count = len(discovered)
            tab_local_new_ids = set(ids) - tab_discovered_ids
            tab_local_new_count = len(tab_local_new_ids)
            last_tab_local_new_count = tab_local_new_count
            changed_any_entry = False

            for content_id in ids:
                entry = _normalize_kakao_discovered_entry(discovered.get(content_id))
                previous_genres = list(entry.get("genres", []))
                previous_seed_completed = bool(entry.get("seed_completed"))
                if tab_genres:
                    entry["genres"] = dedupe_strings([*entry["genres"], *tab_genres])
                elif tab_name and tab_name != "all":
                    entry["genres"] = dedupe_strings([*entry["genres"], tab_name])
                if tab_seed_completed:
                    entry["seed_completed"] = bool(entry.get("seed_completed")) or True
                discovered[content_id] = entry
                tab_discovered_ids.add(content_id)
                if previous_genres != entry.get("genres") or previous_seed_completed != bool(entry.get("seed_completed")):
                    changed_any_entry = True

            global_new_count = len(discovered) - previous_count
            last_global_new_count = global_new_count
            if global_new_count <= 0:
                no_global_growth_rounds += 1
            else:
                no_global_growth_rounds = 0
            if tab_local_new_count <= 0:
                no_new_ids_rounds += 1
            else:
                no_new_ids_rounds = 0
            tab_entry_dirty = tab_entry_dirty or changed_any_entry
            if seed_set != KAKAOPAGE_SEED_SET_WEBNOVELDB:
                tab_links_to_use = discovered_tabs
                if used_html_fallback:
                    tab_links_to_use = [
                        {"name": str(item.get("name") or "tab"), "url": str(item.get("url") or "")}
                        for item in fallback_tab_links
                    ]
                for tab_link in tab_links_to_use:
                    candidate_url = _build_kakao_tab_url(tab_link.get("url") or "")
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

            if global_new_count > 0:
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)

            LOGGER.info(
                "Kakao discovery tab=%s scroll=%s strategy=%s global_total_ids=%s global_new_ids=%s "
                "tab_local_seen_ids=%s tab_local_new_ids=%s entry_changed=%s "
                "no_global_growth_rounds=%s no_new_ids_rounds=%s "
                "no_new_ids_threshold=%s no_global_growth_threshold=%s",
                tab_name,
                scroll_idx,
                strategy,
                len(discovered),
                global_new_count,
                len(tab_discovered_ids),
                tab_local_new_count,
                changed_any_entry,
                no_global_growth_rounds,
                no_new_ids_rounds,
                no_new_ids_threshold,
                no_global_growth_threshold,
            )

            if max_items is not None and len(discovered) >= max_items:
                tab_stop_reason = "max_items"
                break
            tab_stop_reason = _should_stop_kakao_discovery_tab(
                strategy=strategy,
                no_global_growth_rounds=no_global_growth_rounds,
                no_new_ids_rounds=no_new_ids_rounds,
                no_global_growth_threshold=no_global_growth_threshold,
                no_new_ids_threshold=no_new_ids_threshold,
                memory_usage_ratio=None,
                max_memory_usage_ratio=max_memory_usage_ratio,
            )
            if tab_stop_reason:
                break

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(_resolve_kakao_discovery_scroll_delay_ms())

        if tab_stop_reason:
            LOGGER.info(
                "Kakao discovery tab stop tab=%s strategy=%s stop_reason=%s scrolls=%s "
                "global_total_ids=%s global_new_ids=%s tab_local_seen_ids=%s tab_local_new_ids=%s "
                "no_global_growth_rounds=%s no_new_ids_rounds=%s",
                tab_name,
                strategy,
                tab_stop_reason,
                last_scroll_number,
                len(discovered),
                last_global_new_count,
                len(tab_discovered_ids),
                last_tab_local_new_count,
                no_global_growth_rounds,
                no_new_ids_rounds,
            )

        if stop_event.is_set() or discovery_abort_reason:
            if tab_entry_dirty:
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
            if stop_event.is_set():
                state["tabs_done"] = sorted(done_set)
                _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
            break

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

    return {
        "completed": discovery_abort_reason is None and not stop_event.is_set(),
        "abort_reason": discovery_abort_reason,
        "strategy": strategy,
    }


def _is_kakao_suspicious_author_list(authors: Any) -> bool:
    return shared_is_kakao_suspicious_author_list(authors)


async def _fetch_kakao_detail_and_build_record(
    *,
    session: aiohttp.ClientSession,
    content_id: str,
    discovered_entry: Dict[str, Any],
    headers: Dict[str, str],
    retries: int,
    retry_base_delay_seconds: float,
    retry_max_delay_seconds: float,
    canonical_fallback_enabled: bool = True,
) -> Optional[Dict[str, Any]]:
    return await shared_fetch_kakao_detail_and_build_record(
        session=session,
        content_id=content_id,
        discovered_entry=discovered_entry,
        headers=headers,
        retries=retries,
        retry_base_delay_seconds=retry_base_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        canonical_fallback_enabled=canonical_fallback_enabled,
        fetch_text_func=fetch_text_polite,
        parse_detail_func=parse_kakaopage_detail,
    )


async def run_kakaopage_backfill(
    *,
    upserter: BackfillUpserter,
    dry_run: bool,
    max_items: Optional[int],
    state_dir: Path,
    seed_set: str,
    phase: str,
    allow_low_memory_playwright: bool,
    force_detail_refresh: bool = False,
    shutdown_event: Optional[asyncio.Event] = None,
    summary: Optional[SourceSummary] = None,
) -> SourceSummary:
    summary = summary or SourceSummary(source=SOURCE_KAKAOPAGE)
    stop_event = shutdown_event or asyncio.Event()
    if seed_set not in KAKAOPAGE_SEED_SET_CHOICES:
        LOGGER.warning("Unknown KakaoPage seed_set=%s; falling back to %s", seed_set, KAKAOPAGE_SEED_SET_ALL)
        seed_set = KAKAOPAGE_SEED_SET_ALL
    if phase not in KAKAOPAGE_PHASE_CHOICES:
        LOGGER.warning("Unknown KakaoPage phase=%s; falling back to %s", phase, KAKAOPAGE_PHASE_ALL)
        phase = KAKAOPAGE_PHASE_ALL

    state = _load_state(
        state_dir,
        SOURCE_KAKAOPAGE,
        default={
            "discovered": {},
            "tabs_done": [],
            "detail_done": [],
            "discovery_complete": False,
            "discovery_seed_set": "",
            "discovery_strategy_last": "",
            "discovery_completed_at": None,
        },
    )
    state.setdefault("discovery_complete", False)
    state.setdefault("discovery_seed_set", "")
    state.setdefault("discovery_strategy_last", "")
    state.setdefault("discovery_completed_at", None)
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

    discovered_map = state.get("discovered", {})
    if not isinstance(discovered_map, dict):
        discovered_map = {}
        state["discovered"] = discovered_map
    detail_done_list = state.get("detail_done", [])
    if not isinstance(detail_done_list, list):
        detail_done_list = []
    detail_done_set: Set[str] = set(str(item) for item in detail_done_list)
    if force_detail_refresh:
        LOGGER.info(
            "Kakao force detail refresh enabled; detail_done state will be ignored for runtime skips "
            "(KAKAOPAGE_BACKFILL_FORCE_DETAIL_REFRESH=1 or --kakaopage-force-detail-refresh)."
        )

    configured_discovery_strategy = _resolve_kakao_discovery_strategy_default()
    effective_discovery_strategy = _resolve_effective_kakao_discovery_strategy(
        configured_strategy=configured_discovery_strategy,
        discovered_count=len(discovered_map),
    )
    LOGGER.info(
        "Kakao discovery strategy configured=%s effective=%s discovered=%s",
        configured_discovery_strategy,
        effective_discovery_strategy,
        len(discovered_map),
    )

    cookies = _cookies_from_env_for_playwright()
    cookie_header = _kakao_cookie_header()
    if not cookies and not cookie_header and phase in (KAKAOPAGE_PHASE_ALL, KAKAOPAGE_PHASE_DISCOVERY):
        LOGGER.warning(
            "No KakaoPage cookies were provided. If discovery/detail pages are age- or login-gated, "
            "set KAKAOPAGE_COOKIE_HEADER or KAKAOPAGE_COOKIES_JSON."
        )
    elif cookie_header:
        cookie_names = _cookie_names_from_header(cookie_header)
        LOGGER.info(
            "Kakao cookie header detected from env cookie_count=%s cookie_names=%s",
            len(cookie_names),
            cookie_names[:10],
        )

    bridged_cookie_header: Optional[str] = None
    should_run_discovery = phase in (KAKAOPAGE_PHASE_ALL, KAKAOPAGE_PHASE_DISCOVERY)
    should_run_detail = phase in (KAKAOPAGE_PHASE_ALL, KAKAOPAGE_PHASE_DETAIL)
    if should_run_discovery and effective_discovery_strategy == KAKAOPAGE_DISCOVERY_STRATEGY_SKIP:
        LOGGER.info(
            "Kakao discovery skipped by strategy=%s (configured=%s).",
            effective_discovery_strategy,
            configured_discovery_strategy,
        )
        should_run_discovery = False

    if (
        should_run_discovery
        and _should_skip_kakao_discovery_from_state(
            state=state,
            seed_set=seed_set,
            strategy=effective_discovery_strategy,
        )
    ):
        LOGGER.info(
            "Kakao discovery already complete in state; skipping Playwright discovery "
            "(seed_set=%s strategy=%s discovered=%s detail_done=%s completed_at=%s).",
            seed_set,
            effective_discovery_strategy,
            len(discovered_map),
            len(detail_done_set),
            state.get("discovery_completed_at"),
        )
        should_run_discovery = False

    if (
        phase == KAKAOPAGE_PHASE_ALL
        and discovered_map
        and _is_kakao_detail_already_complete(
            discovered_map=discovered_map,
            detail_done_set=detail_done_set,
            max_items=max_items,
        )
        and not force_detail_refresh
    ):
        LOGGER.info(
            "Kakao backfill already complete (discovered=%s detail_done=%s). Skipping discovery+detail.",
            len(discovered_map),
            len(detail_done_set),
        )
        return summary

    discovery_result = {
        "completed": False,
        "abort_reason": None,
        "strategy": effective_discovery_strategy,
    }
    if should_run_discovery:
        _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
        _guard_kakao_playwright_memory(
            allow_low_memory_playwright=allow_low_memory_playwright,
        )
        try:
            async_playwright = _load_playwright_async_api()
        except Exception as exc:
            raise RuntimeError(
                "Playwright is required for KakaoPage discovery phase. "
                "Install with `pip install -r requirements-backfill.txt` and "
                "`python -m playwright install --with-deps chromium`."
            ) from exc

        playwright_args = _resolve_kakao_playwright_args()
        LOGGER.info("Kakao Playwright launch args=%s", playwright_args)
        async with async_playwright() as playwright:
            try:
                browser = await playwright.chromium.launch(headless=True, args=playwright_args)
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
                discovery_result = await _discover_kakaopage_ids(
                    strategy=effective_discovery_strategy,
                    page=page,
                    state=state,
                    dry_run=dry_run,
                    state_dir=state_dir,
                    max_items=max_items,
                    seed_set=seed_set,
                    summary=summary,
                    stop_event=stop_event,
                )
            finally:
                if not cookie_header:
                    try:
                        context_cookies = await context.cookies()
                        bridged_cookie_header = _cookie_header_from_playwright_cookies(context_cookies)
                        if bridged_cookie_header:
                            cookie_names = _cookie_names_from_header(bridged_cookie_header)
                            LOGGER.info(
                                "Bridged Kakao cookies from Playwright context cookie_count=%s cookie_names=%s",
                                len(cookie_names),
                                cookie_names[:10],
                            )
                    except Exception as exc:
                        LOGGER.warning("Failed to bridge cookies from Playwright context: %s", exc)
                await context.close()
                await browser.close()
        discovered_map = state.get("discovered", {})
        if not isinstance(discovered_map, dict):
            discovered_map = {}
            state["discovered"] = discovered_map
        if discovery_result.get("completed"):
            _mark_kakao_discovery_complete(
                state=state,
                seed_set=seed_set,
                strategy=effective_discovery_strategy,
            )
        else:
            _mark_kakao_discovery_incomplete(
                state=state,
                seed_set=seed_set,
                strategy=effective_discovery_strategy,
            )
        _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
    else:
        state["discovery_strategy_last"] = effective_discovery_strategy

    if not cookie_header and bridged_cookie_header:
        cookie_header = bridged_cookie_header

    discovered_map = state.get("discovered", {})
    if not isinstance(discovered_map, dict):
        discovered_map = {}
        state["discovered"] = discovered_map
    summary.final_discovered_count = len(discovered_map)
    if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB:
        discovered_counts = _collect_kakao_webnoveldb_discovered_counts(discovered_map)
        for canonical_genre in KAKAOPAGE_WEBNOVELDB_CANONICAL_GENRE_ORDER:
            _init_seed_stats(summary, canonical_genre)
            summary.seed_stats[canonical_genre]["discovered"] = discovered_counts[canonical_genre]
    if phase == KAKAOPAGE_PHASE_DISCOVERY:
        if stop_event.is_set():
            raise RuntimeError("Kakao discovery interrupted by shutdown signal. Progress saved to state file.")
        if discovery_result.get("abort_reason"):
            LOGGER.warning(
                "Kakao discovery ended early stop_reason=%s strategy=%s discovered=%s",
                discovery_result.get("abort_reason"),
                discovery_result.get("strategy"),
                len(discovered_map),
            )
        return summary

    if phase == KAKAOPAGE_PHASE_DETAIL and not discovered_map:
        raise RuntimeError("No discovered IDs; run discovery phase first (--kakaopage-phase discovery).")

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
    if not should_run_detail:
        return summary

    if (
        discovered_map
        and _is_kakao_detail_already_complete(
            discovered_map=discovered_map,
            detail_done_set=detail_done_set,
            max_items=max_items,
        )
        and not force_detail_refresh
    ):
        LOGGER.info(
            "Kakao detail already complete (discovered=%s detail_done=%s). Skipping detail stage.",
            len(discovered_map),
            len(detail_done_set),
        )
        return summary

    ids_to_process = _kakao_discovery_target_ids(discovered_map, max_items)

    headers = _build_headers(referer=KAKAOPAGE_LIST_URL, cookie_header=cookie_header)
    detail_concurrency = _resolve_kakao_detail_concurrency()
    min_interval_seconds = _resolve_kakao_min_interval_seconds()
    detail_jitter_min, detail_jitter_max = _resolve_kakao_detail_jitter_bounds()
    retries, retry_base_delay_seconds, retry_max_delay_seconds = _resolve_kakao_http_retry_policy()
    canonical_fallback_enabled = _resolve_kakao_canonical_fallback_enabled()
    max_consecutive_rate_limits = (
        _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_MAX_CONSECUTIVE_RATE_LIMITS")) or 5
    )
    cooldown_seconds = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_COOLDOWN_SECONDS")) or 900
    save_every = _clean_int_limit(os.getenv("KAKAOPAGE_BACKFILL_SAVE_STATE_EVERY")) or 20

    LOGGER.info(
        "Kakao detail config concurrency=%s min_interval_seconds=%.3f jitter=[%.3f, %.3f] retries=%s "
        "max_rate_limits=%s canonical_fallback=%s force_refresh=%s",
        detail_concurrency,
        min_interval_seconds,
        detail_jitter_min,
        detail_jitter_max,
        retries,
        max_consecutive_rate_limits,
        canonical_fallback_enabled,
        force_detail_refresh,
    )

    timeout = aiohttp.ClientTimeout(
        total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
        connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
        sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
    )
    connector = aiohttp.TCPConnector(limit=max(4, detail_concurrency * 2))
    limiter = AsyncRateLimiter(min_interval_seconds=min_interval_seconds)
    worker_stop_event = asyncio.Event()
    detail_done_dirty = False
    skipped_missing_author = 0
    processed_since_save = 0
    protection_lock = asyncio.Lock()
    consecutive_rate_limits = 0
    abort_reason: Optional[str] = None

    ids_queue: asyncio.Queue[str] = asyncio.Queue()
    for content_id in ids_to_process:
        if not force_detail_refresh and content_id in detail_done_set:
            continue
        ids_queue.put_nowait(content_id)

    async def persist_detail_state(*, force: bool = False) -> None:
        nonlocal detail_done_dirty, processed_since_save
        if not force and not detail_done_dirty:
            return
        state["detail_done"] = sorted(detail_done_set)
        _save_state(state_dir, SOURCE_KAKAOPAGE, state, dry_run=dry_run)
        detail_done_dirty = False
        processed_since_save = 0

    async def mark_protection_event(
        *,
        error_kind: str,
        content_id: str,
        diagnostics: Optional[Dict[str, str]] = None,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        nonlocal abort_reason, consecutive_rate_limits
        async with protection_lock:
            if error_kind == "rate_limited":
                consecutive_rate_limits += 1
            elif error_kind == "blocked":
                consecutive_rate_limits = max_consecutive_rate_limits
            else:
                consecutive_rate_limits = 0

            should_stop = _trip_kakao_circuit_if_needed(
                error_kind=error_kind,
                consecutive_rate_limits=consecutive_rate_limits,
                max_consecutive_rate_limits=max_consecutive_rate_limits,
                stop_event=worker_stop_event,
            )
            if should_stop and not abort_reason:
                title = ""
                snippet = ""
                if diagnostics:
                    title = str(diagnostics.get("title") or "")
                    snippet = str(diagnostics.get("text_snippet") or "")
                if error_kind == "blocked":
                    abort_reason = (
                        "KakaoPage appears blocked or gated; stopping early to avoid repeated access attempts. "
                        f"content_id={content_id} title={title!r} snippet={snippet!r}"
                    )
                else:
                    abort_reason = (
                        "KakaoPage detail fetch is rate-limited repeatedly; cooldown recommended before retry. "
                        f"consecutive_rate_limits={consecutive_rate_limits} suggested_cooldown_seconds={cooldown_seconds} "
                        f"last_retry_after_seconds={retry_after_seconds}"
                    )
                await persist_detail_state(force=True)

    async def reset_rate_limit_streak() -> None:
        nonlocal consecutive_rate_limits
        async with protection_lock:
            consecutive_rate_limits = 0

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def worker(worker_id: int) -> None:
            nonlocal detail_done_dirty, skipped_missing_author, processed_since_save
            while True:
                if stop_event.is_set():
                    worker_stop_event.set()
                if worker_stop_event.is_set():
                    return
                try:
                    content_id = ids_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                try:
                    if not force_detail_refresh and content_id in detail_done_set:
                        continue
                    entry = _normalize_kakao_discovered_entry(discovered_map.get(content_id))
                    seed_stat_keys = (
                        _seed_stat_keys_from_discovered_entry(entry)
                        if seed_set == KAKAOPAGE_SEED_SET_WEBNOVELDB
                        else []
                    )

                    await limiter.wait()
                    if worker_stop_event.is_set() or stop_event.is_set():
                        continue
                    await asyncio.sleep(random.uniform(detail_jitter_min, detail_jitter_max))
                    if worker_stop_event.is_set() or stop_event.is_set():
                        continue

                    summary.fetched_count += 1
                    try:
                        record = await _fetch_kakao_detail_and_build_record(
                            session=session,
                            content_id=content_id,
                            discovered_entry=entry,
                            headers=headers,
                            retries=retries,
                            retry_base_delay_seconds=retry_base_delay_seconds,
                            retry_max_delay_seconds=retry_max_delay_seconds,
                            canonical_fallback_enabled=canonical_fallback_enabled,
                        )
                    except RateLimitedError as exc:
                        summary.error_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "errors")
                        LOGGER.warning(
                            "Kakao detail rate limited content_id=%s status=%s retry_after=%s url=%s",
                            content_id,
                            exc.status,
                            exc.retry_after_seconds,
                            exc.url,
                        )
                        await mark_protection_event(
                            error_kind="rate_limited",
                            content_id=content_id,
                            retry_after_seconds=exc.retry_after_seconds,
                        )
                        continue
                    except BlockedError as exc:
                        summary.error_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "errors")
                        LOGGER.error(
                            "Kakao detail blocked content_id=%s status=%s url=%s title=%r snippet=%r",
                            content_id,
                            exc.status,
                            exc.url,
                            exc.diagnostics.get("title"),
                            exc.diagnostics.get("text_snippet"),
                        )
                        await mark_protection_event(
                            error_kind="blocked",
                            content_id=content_id,
                            diagnostics=exc.diagnostics,
                        )
                        continue
                    except TransientHttpError as exc:
                        summary.error_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "errors")
                        LOGGER.warning(
                            "Kakao detail transient failure content_id=%s status=%s url=%s",
                            content_id,
                            exc.status,
                            exc.url,
                        )
                        await reset_rate_limit_streak()
                        continue
                    except HttpStatusError as exc:
                        summary.error_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "errors")
                        LOGGER.error(
                            "Kakao detail non-retryable HTTP error content_id=%s status=%s url=%s",
                            content_id,
                            exc.status,
                            exc.url,
                        )
                        await reset_rate_limit_streak()
                        continue
                    except Exception as exc:
                        summary.error_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "errors")
                        LOGGER.error("Kakao detail fetch failed content_id=%s worker=%s: %s", content_id, worker_id, exc)
                        await reset_rate_limit_streak()
                        continue

                    if not record:
                        summary.skipped_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "skipped")
                        await reset_rate_limit_streak()
                        continue

                    summary.parsed_count += 1
                    _bump_seed_stats(summary, seed_stat_keys, "parsed")
                    authors = record.get("authors") or []
                    title = str(record.get("title") or "")
                    diagnostics = record.get("_diagnostics") if isinstance(record.get("_diagnostics"), dict) else {}

                    if not title or not authors:
                        if _is_probable_kakao_block_page(
                            title=title,
                            authors=authors,
                            diagnostics=diagnostics,
                        ):
                            summary.error_count += 1
                            _bump_seed_stats(summary, seed_stat_keys, "errors")
                            await mark_protection_event(
                                error_kind="blocked",
                                content_id=content_id,
                                diagnostics=diagnostics,
                            )
                            continue
                        skipped_missing_author += 1
                        summary.skipped_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "skipped")
                        LOGGER.warning(
                            "Kakao skip missing title/authors content_id=%s url=%s title=%r",
                            content_id,
                            record.get("content_url"),
                            title,
                        )
                        await reset_rate_limit_streak()
                        continue

                    if _is_kakao_suspicious_author_list(authors):
                        skipped_missing_author += 1
                        summary.skipped_count += 1
                        _bump_seed_stats(summary, seed_stat_keys, "skipped")
                        LOGGER.warning(
                            "Kakao skip suspicious authors content_id=%s url=%s title=%r authors=%s author_source=%s",
                            content_id,
                            record.get("content_url"),
                            title,
                            authors,
                            diagnostics.get("author_source"),
                        )
                        await reset_rate_limit_streak()
                        continue

                    record.pop("_diagnostics", None)
                    accepted = upserter.add_raw(record)
                    if not accepted:
                        _bump_seed_stats(summary, seed_stat_keys, "skipped")
                        LOGGER.debug("Kakao skipped invalid record content_id=%s", content_id)
                    if dry_run:
                        _record_sample(summary, record)
                    detail_done_set.add(content_id)
                    detail_done_dirty = True
                    processed_since_save += 1
                    await reset_rate_limit_streak()
                    if processed_since_save >= save_every:
                        await persist_detail_state(force=True)
                finally:
                    ids_queue.task_done()

        workers = [asyncio.create_task(worker(index + 1)) for index in range(detail_concurrency)]
        await asyncio.gather(*workers)

    await persist_detail_state(force=True)

    if stop_event.is_set() and not abort_reason:
        abort_reason = (
            "Backfill received shutdown signal (SIGINT/SIGTERM). Progress was saved; resume with same state file."
        )
    if skipped_missing_author:
        LOGGER.warning(
            "Kakao skipped %s items due to missing title/authors or suspicious author tokens.",
            skipped_missing_author,
        )
    if abort_reason:
        raise RuntimeError(abort_reason)
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
        "--kakaopage-phase",
        choices=KAKAOPAGE_PHASE_CHOICES,
        default=_resolve_kakao_phase_default(),
        help="KakaoPage execution phase: all (discovery+detail), discovery-only, or detail-only.",
    )
    parser.add_argument(
        "--kakaopage-allow-low-memory-playwright",
        action="store_true",
        default=_resolve_kakao_allow_low_memory_playwright_from_env(),
        help="Allow running KakaoPage discovery Playwright even under low cgroup memory limits.",
    )
    parser.add_argument(
        "--kakaopage-force-detail-refresh",
        action="store_true",
        default=_resolve_kakao_force_detail_refresh_from_env(),
        help=(
            "Ignore Kakao detail_done completion checks and reprocess discovered IDs "
            "(also configurable via KAKAOPAGE_BACKFILL_FORCE_DETAIL_REFRESH=1)."
        ),
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
    shutdown_event = asyncio.Event()
    interrupted = False

    loop = asyncio.get_running_loop()

    def _request_shutdown(sig_name: str) -> None:
        nonlocal interrupted
        if shutdown_event.is_set():
            return
        interrupted = True
        LOGGER.warning("Received %s; finishing current step, saving state, and exiting.", sig_name)
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig.name)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, lambda *_args, _sig=sig: _request_shutdown(_sig.name))
            except Exception:
                continue

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for source in requested_sources:
                if shutdown_event.is_set():
                    interrupted = True
                    LOGGER.warning("Stop event set before source=%s; ending run early.", source)
                    break
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
                            shutdown_event=shutdown_event,
                            summary=summary,
                        )
                    elif source == SOURCE_KAKAOPAGE:
                        await run_kakaopage_backfill(
                            upserter=upserter,
                            dry_run=args.dry_run,
                            max_items=remaining,
                            state_dir=state_dir,
                            seed_set=args.kakaopage_seed_set,
                            phase=args.kakaopage_phase,
                            allow_low_memory_playwright=args.kakaopage_allow_low_memory_playwright,
                            force_detail_refresh=args.kakaopage_force_detail_refresh,
                            shutdown_event=shutdown_event,
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
                if shutdown_event.is_set():
                    interrupted = True
                    LOGGER.warning("Stop event set after source=%s; ending run early.", source)
                    break

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
        for line in _render_source_summary_lines(
            summary,
            dry_run=args.dry_run,
            kakaopage_seed_set=args.kakaopage_seed_set,
        ):
            print(line)

    print(
        f"[overall] fetched={overall.fetched_count} parsed={overall.parsed_count} "
        f"inserted={overall.inserted_count} updated={overall.updated_count} "
        f"skipped={overall.skipped_count} errors={overall.error_count}"
    )

    if args.dry_run:
        print("Dry run enabled: no DB writes were committed.")
    if interrupted:
        return 1
    return 1 if overall.error_count > 0 else 0


def main() -> int:
    load_dotenv()
    parser = _make_arg_parser()
    args = parser.parse_args()
    _setup_logging(args.log_level)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
