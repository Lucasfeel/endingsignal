from __future__ import annotations

import asyncio
import os
import re
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from services.kakaopage_parser import parse_kakaopage_detail, parse_kakaopage_listing_items
from services.naver_series_parser import parse_naver_series_list
from services.novel_seed_catalog import NAVER_SERIES_SEEDS, build_webnoveldb_kakao_seeds
from utils.text import normalize_search_text

VerificationGate = Callable[[Dict[str, Any]], Dict[str, Any] | Awaitable[Dict[str, Any]]]
VERIFIER_REGISTRY: Dict[str, VerificationGate] = {}

STATUS_COMPLETED = "완결"
STATUS_ONGOING = "연재중"
STATUS_HIATUS = "휴재"

DEFAULT_BROWSER_TIMEOUT_MS = 45_000
DEFAULT_BROWSER_WAIT_MS = 350
DEFAULT_LISTING_FALLBACK_PAGES = 2
DEFAULT_KAKAOPAGE_SCROLLS = 5
DEFAULT_KAKAOWEBTOON_SCROLLS = 40
DEFAULT_BROWSER_CONCURRENCY = 1
DEFAULT_LAUNCH_ARGS = [
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
]
NAVER_LOGIN_HOST = "nid.naver.com"
KAKAOWEBTOON_COMPLETED_URL = "https://webtoon.kakao.com/?tab=complete"

_NAVER_SERIES_DETAIL_COMPLETED_RE = re.compile(r"\b\d+\s*화\s*완결\b")
_NAVER_SERIES_DETAIL_ONGOING_RE = re.compile(r"\b\d+\s*화\s*연재중\b")
_KAKAO_WEBTOON_SCHEDULE_RE = re.compile(
    r"연재\s+(완결|휴재|시즌완결|매일|매주|월|화|수|목|금|토|일)",
    re.IGNORECASE,
)
_RIDI_ONGOING_RE = re.compile(r"연재\s+(?:매주|매일|매월|월|화|수|목|금|토|일|\d)")
_RIDI_COMPLETED_RE = re.compile(r"(?:#\s*연재완결|연재완결|(?<!미)완결)")
_COMPLETED_RE = re.compile(r"(?<!미)완결")
_HIATUS_RE = re.compile(r"(?:휴재|시즌완결|일시중지)")


_NAVER_WEBTOON_WEEK_DAY_RE = re.compile(r'<div class="week_day">.*?<dd>(.*?)</dd>', re.IGNORECASE | re.DOTALL)
_KAKAOWEBTOON_CONTENT_ID_RE = re.compile(r"/content/(?:[^/?#]+/)?(\d+)(?:[/?#]|$)")
_KAKAOWEBTOON_BADGE_ALTS = {"성인", "3다무", "기다무", "연재무료", "up", "휴재", "신작", "새시즌"}


def register_source_verifier(source_name: str, verifier: VerificationGate) -> None:
    VERIFIER_REGISTRY[str(source_name).strip()] = verifier


def normalize_verification_mode(raw_value: Optional[str] = None) -> str:
    value = (raw_value or os.getenv("VERIFIED_SYNC_VERIFICATION_MODE") or "").strip().lower()
    if not value:
        return "source_pluggable"
    return value


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


PLAYWRIGHT_VERIFICATION_SEMAPHORE = asyncio.Semaphore(
    _get_int_env("VERIFIED_SYNC_PLAYWRIGHT_MAX_BROWSERS", DEFAULT_BROWSER_CONCURRENCY)
)


def _resolve_launch_args() -> List[str]:
    return list(DEFAULT_LAUNCH_ARGS)


def _browser_context_kwargs() -> Dict[str, Any]:
    return {
        "locale": "ko-KR",
        # Codex cloud browser traffic can be intercepted by the managed proxy.
        "ignore_https_errors": _get_bool_env(
            "VERIFIED_SYNC_PLAYWRIGHT_IGNORE_HTTPS_ERRORS", True
        ),
    }


def _browser_timeout_ms() -> int:
    return _get_int_env("VERIFIED_SYNC_PLAYWRIGHT_TIMEOUT_MS", DEFAULT_BROWSER_TIMEOUT_MS)


def _page_wait_ms() -> int:
    return _get_int_env("VERIFIED_SYNC_PLAYWRIGHT_WAIT_MS", DEFAULT_BROWSER_WAIT_MS)


def _listing_fallback_pages() -> int:
    return _get_int_env("VERIFIED_SYNC_LISTING_FALLBACK_PAGES", DEFAULT_LISTING_FALLBACK_PAGES)


def _kakaopage_scrolls() -> int:
    return _get_int_env("VERIFIED_SYNC_KAKAOPAGE_SCROLLS", DEFAULT_KAKAOPAGE_SCROLLS)


def _kakaowebtoon_scrolls() -> int:
    return _get_int_env("VERIFIED_SYNC_KAKAOWEBTOON_SCROLLS", DEFAULT_KAKAOWEBTOON_SCROLLS)


def _normalize_status(value: object) -> str:
    text = str(value or "").strip()
    if text == STATUS_COMPLETED:
        return STATUS_COMPLETED
    if text == STATUS_HIATUS:
        return STATUS_HIATUS
    return STATUS_ONGOING


def _status_matches(expected: object, observed: object) -> bool:
    return _normalize_status(expected) == _normalize_status(observed)


def _candidate_items(write_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = write_plan.get("verification_candidates")
    if not isinstance(candidates, list):
        return []
    return [item for item in candidates if isinstance(item, dict)]


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _titles_match(expected: object, observed: object) -> bool:
    expected_normalized = normalize_search_text(expected)
    observed_normalized = normalize_search_text(observed)
    if not expected_normalized or not observed_normalized:
        return False
    return (
        expected_normalized == observed_normalized
        or expected_normalized in observed_normalized
        or observed_normalized in expected_normalized
    )


def _match_listing_item(candidate: Dict[str, Any], items: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    expected_id = str(candidate.get("content_id") or "").strip()
    expected_title = candidate.get("title")
    for item in items:
        if str(item.get("content_id") or "").strip() == expected_id:
            return item
    for item in items:
        if _titles_match(expected_title, item.get("title")):
            return item
    return None


def _status_from_text(text: str) -> str:
    compact = _clean_text(text)
    if not compact:
        return STATUS_ONGOING
    if _RIDI_COMPLETED_RE.search(compact) or _COMPLETED_RE.search(compact):
        return STATUS_COMPLETED
    if _HIATUS_RE.search(compact):
        return STATUS_HIATUS
    return STATUS_ONGOING


def _status_window(text: str, keyword: str) -> str:
    source = _clean_text(text)
    if not source or not keyword:
        return source[:240]
    index = source.find(keyword)
    if index < 0:
        return source[:240]
    start = max(0, index - 80)
    end = min(len(source), index + 160)
    return source[start:end]


def _excerpt_near(text: str, marker: object, *, before: int = 120, after: int = 2400) -> str:
    source = _clean_text(text)
    target = _clean_text(marker)
    if not source:
        return ""
    if not target:
        return source[:after]
    index = source.find(target)
    if index < 0:
        return source[:after]
    start = max(0, index - before)
    end = min(len(source), index + len(target) + after)
    return source[start:end]


def _extract_naver_webtoon_status(body_text: str, html: str = "") -> str:
    section_match = _NAVER_WEBTOON_WEEK_DAY_RE.search(str(html or ""))
    if section_match:
        section_text = _clean_text(re.sub(r"<[^>]+>", " ", section_match.group(1)))
        if section_text:
            if "완결" in section_text:
                return STATUS_COMPLETED
            if "휴재" in section_text:
                return STATUS_HIATUS
            return STATUS_ONGOING

    compact = _clean_text(body_text)
    if _NAVER_SERIES_DETAIL_COMPLETED_RE.search(compact):
        return STATUS_COMPLETED
    if "휴재" in compact:
        return STATUS_HIATUS
    return STATUS_ONGOING


def _extract_kakao_webtoon_status(body_text: str) -> str:
    compact = _clean_text(body_text)
    match = _KAKAO_WEBTOON_SCHEDULE_RE.search(compact)
    if match:
        token = match.group(1)
        if token == "완결":
            return STATUS_COMPLETED
        if token in {"휴재", "시즌완결"}:
            return STATUS_HIATUS
        return STATUS_ONGOING
    return _status_from_text(compact)


def _extract_naver_series_detail_status(body_text: str) -> str:
    compact = _clean_text(body_text)
    if _NAVER_SERIES_DETAIL_COMPLETED_RE.search(compact):
        return STATUS_COMPLETED
    if _NAVER_SERIES_DETAIL_ONGOING_RE.search(compact) or "연재중" in compact or "미완결" in compact:
        return STATUS_ONGOING
    return _status_from_text(compact)


def _is_ridi_completed_bundle(text: str) -> bool:
    compact = _clean_text(text)
    if not compact:
        return False
    if "[완결 세트]" in compact:
        return True
    has_set_marker = "[특별 세트]" in compact or "세트미리보기" in compact or "권 세트" in compact
    has_volume_count = re.search(r"총\s*\d+\s*권", compact) is not None
    return has_set_marker and has_volume_count


def _extract_ridi_status(body_text: str, html: str, page_title: str = "") -> str:
    compact = _clean_text(body_text)
    if "#연재중" in compact or _RIDI_ONGOING_RE.search(compact):
        return STATUS_ONGOING
    if _RIDI_COMPLETED_RE.search(compact):
        return STATUS_COMPLETED
    if "연재완결" in html:
        return STATUS_COMPLETED
    if _is_ridi_completed_bundle(f"{page_title} {compact}"):
        return STATUS_COMPLETED
    return _status_from_text(compact)


def _append_query(url: str, **params: Any) -> str:
    parsed = urlparse(url)
    merged = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        merged[str(key)] = str(value)
    return urlunparse(parsed._replace(query=urlencode(merged)))


def _public_kakaopage_seed_url(seed_url: str) -> str:
    return str(seed_url or "").replace("https://bff-page.kakao.com", "https://page.kakao.com")


def _playwright_cookies_from_cookie_header(raw_cookie_header: object, *, domain: str) -> List[Dict[str, Any]]:
    raw_value = str(raw_cookie_header or "").strip()
    if not raw_value:
        return []

    cookies: List[Dict[str, Any]] = []
    for part in raw_value.split(";"):
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
                "domain": domain,
                "path": "/",
                "httpOnly": False,
                "secure": True,
            }
        )
    return cookies


def _kakaowebtoon_playwright_cookies() -> List[Dict[str, Any]]:
    return _playwright_cookies_from_cookie_header(
        os.getenv("KAKAOWEBTOON_COOKIE"),
        domain=".kakao.com",
    )


def _parse_kakaowebtoon_content_id(href: object) -> str:
    raw_href = str(href or "").strip()
    if not raw_href:
        return ""
    match = _KAKAOWEBTOON_CONTENT_ID_RE.search(raw_href)
    if not match:
        return ""
    return match.group(1)


def _extract_kakaowebtoon_listing_title(anchor: Any) -> str:
    candidates: List[str] = []
    for img in anchor.select("img[alt]"):
        alt = _clean_text(img.get("alt"))
        if not alt:
            continue
        if alt.lower() in _KAKAOWEBTOON_BADGE_ALTS:
            continue
        candidates.append(alt)
    if not candidates:
        return ""
    return max(candidates, key=len)


def _parse_kakaowebtoon_listing_items(html: str, *, seed_completed: bool = True) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    items_by_id: Dict[str, Dict[str, Any]] = {}
    for anchor in soup.select('a[href*="/content/"]'):
        href = _clean_text(anchor.get("href"))
        content_id = _parse_kakaowebtoon_content_id(href)
        if not content_id:
            continue
        title = _extract_kakaowebtoon_listing_title(anchor)
        if not title:
            continue
        items_by_id[content_id] = {
            "content_id": content_id,
            "content_url": urljoin(KAKAOWEBTOON_COMPLETED_URL, href),
            "title": title,
            "status": STATUS_COMPLETED if seed_completed else STATUS_ONGOING,
            "adult": anchor.select_one('img[alt="성인"]') is not None,
        }
    return list(items_by_id.values())


def _select_naver_series_seeds(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    expected_completed = _normalize_status(candidate.get("expected_status")) == STATUS_COMPLETED
    source_item = candidate.get("source_item")
    crawl_roots = []
    if isinstance(source_item, dict):
        raw_roots = source_item.get("crawl_roots")
        if isinstance(raw_roots, list):
            crawl_roots = [str(root).strip() for root in raw_roots if str(root).strip()]

    def _seed_priority(seed: Dict[str, Any]) -> tuple[int, str]:
        key = str(seed.get("key") or "")
        if key in crawl_roots:
            return (0, key)
        base_key = key.rsplit("_", 1)[0]
        if any(root.startswith(base_key) or base_key.startswith(root) for root in crawl_roots):
            return (1, key)
        return (2, key)

    matching = [
        seed
        for seed in NAVER_SERIES_SEEDS
        if bool(seed.get("is_finished_page")) == expected_completed
    ]
    return sorted(matching, key=_seed_priority)


def _select_kakaopage_seeds(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    expected_completed = _normalize_status(candidate.get("expected_status")) == STATUS_COMPLETED
    source_item = candidate.get("source_item")
    crawl_roots = []
    if isinstance(source_item, dict):
        raw_roots = source_item.get("crawl_roots")
        if isinstance(raw_roots, list):
            crawl_roots = [str(root).strip() for root in raw_roots if str(root).strip()]

    def _seed_priority(seed: Dict[str, Any]) -> tuple[int, str]:
        seed_key = str(seed.get("seed_stat_key") or seed.get("name") or "")
        if seed_key in crawl_roots:
            return (0, seed_key)
        return (1, seed_key)

    matching = [
        seed
        for seed in build_webnoveldb_kakao_seeds()
        if bool(seed.get("seed_completed")) == expected_completed
    ]
    return sorted(matching, key=_seed_priority)


@asynccontextmanager
async def _browser_session():
    from playwright.async_api import async_playwright

    async with PLAYWRIGHT_VERIFICATION_SEMAPHORE:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=_get_bool_env("VERIFIED_SYNC_PLAYWRIGHT_HEADLESS", True),
                args=_resolve_launch_args(),
            )
            context = await browser.new_context(**_browser_context_kwargs())
            cookies = _kakaowebtoon_playwright_cookies()
            if cookies:
                try:
                    await context.add_cookies(cookies)
                except Exception:
                    pass

            async def _route_handler(route):
                resource_type = str(route.request.resource_type or "").strip().lower()
                if resource_type in {"image", "media", "font"}:
                    await route.abort()
                    return
                await route.continue_()

            try:
                await context.route("**/*", _route_handler)
            except Exception:
                pass

            try:
                yield context
            finally:
                await context.close()
                await browser.close()


async def _navigate(page, url: str) -> None:
    await page.goto(url, wait_until="domcontentloaded", timeout=_browser_timeout_ms())
    await page.wait_for_timeout(_page_wait_ms())


async def _page_text(page) -> str:
    try:
        return _clean_text(await page.text_content("body") or "")
    except Exception:
        return ""


async def _click_button_by_name(page, name: str) -> bool:
    locators = [
        page.get_by_role("button", name=name, exact=True),
        page.get_by_text(name, exact=True),
    ]
    for locator in locators:
        try:
            if await locator.count() <= 0:
                continue
            await locator.first.click()
            await page.wait_for_timeout(max(_page_wait_ms(), 750))
            return True
        except Exception:
            continue
    return False


async def _wait_for_kakaowebtoon_listing(page) -> None:
    selectors = [
        'a[href*="/content/"] img[alt]',
        'a[href*="/content/"]',
    ]
    for _ in range(3):
        for selector in selectors:
            try:
                await page.wait_for_selector(
                    selector,
                    state="attached",
                    timeout=min(_browser_timeout_ms(), 5_000),
                )
                await page.wait_for_timeout(max(_page_wait_ms(), 750))
                return
            except Exception:
                continue
        try:
            await page.wait_for_load_state("networkidle", timeout=min(_browser_timeout_ms(), 5_000))
        except Exception:
            pass
        await page.wait_for_timeout(max(_page_wait_ms(), 1_000))


async def _prepare_kakao_webtoon_completed_listing(page) -> None:
    await _navigate(page, KAKAOWEBTOON_COMPLETED_URL)
    await _click_button_by_name(page, "전체")
    await _wait_for_kakaowebtoon_listing(page)


async def _find_kakaowebtoon_listing_match(
    page,
    candidate: Dict[str, Any],
    *,
    settle_attempts: int,
) -> Optional[Dict[str, Any]]:
    wait_ms = max(_page_wait_ms(), 1_000)
    for attempt in range(1, max(1, settle_attempts) + 1):
        html = await page.content()
        items = _parse_kakaowebtoon_listing_items(html, seed_completed=True)
        matched = _match_listing_item(candidate, items)
        if matched is not None:
            matched["_settle_attempt"] = attempt
            return matched
        if attempt < settle_attempts:
            try:
                await page.wait_for_load_state(
                    "networkidle",
                    timeout=min(_browser_timeout_ms(), 3_000),
                )
            except Exception:
                pass
            await page.wait_for_timeout(wait_ms)
    return None


async def _verify_naver_webtoon_candidate(page, candidate: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = str(candidate.get("content_url") or "")
    await _navigate(page, detail_url)
    page_title = await page.title()
    body_text = await _page_text(page)
    html = await page.content()
    observed_status = _extract_naver_webtoon_status(body_text, html)
    ok = _titles_match(candidate.get("title"), page_title) and _status_matches(candidate.get("expected_status"), observed_status)
    return {
        "content_id": candidate.get("content_id"),
        "title": candidate.get("title"),
        "expected_status": candidate.get("expected_status"),
        "observed_status": observed_status,
        "ok": ok,
        "verification_method": "detail",
        "detail_url": page.url,
        "evidence": {
            "page_title": page_title,
            "status_window": _status_window(body_text, "완결" if observed_status == STATUS_COMPLETED else "연재"),
        },
    }


async def _verify_kakao_webtoon_candidate(page, candidate: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = str(candidate.get("content_url") or "")
    if "tab=profile" not in detail_url:
        separator = "&" if "?" in detail_url else "?"
        detail_url = f"{detail_url}{separator}tab=profile"
    await _navigate(page, detail_url)
    page_title = await page.title()
    body_text = await _page_text(page)
    observed_status = _extract_kakao_webtoon_status(body_text)
    title_ok = _titles_match(candidate.get("title"), page_title) or _titles_match(candidate.get("title"), body_text)
    ok = title_ok and _status_matches(candidate.get("expected_status"), observed_status)
    if not ok:
        fallback = await _verify_kakao_webtoon_listing_fallback(page, candidate)
        if fallback is not None and (fallback.get("ok") or not title_ok):
            return fallback
    return {
        "content_id": candidate.get("content_id"),
        "title": candidate.get("title"),
        "expected_status": candidate.get("expected_status"),
        "observed_status": observed_status,
        "ok": ok,
        "verification_method": "detail",
        "detail_url": page.url,
        "evidence": {
            "page_title": page_title,
            "status_window": _status_window(body_text, "연재"),
        },
    }


async def _verify_kakao_webtoon_listing_fallback(page, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    await _prepare_kakao_webtoon_completed_listing(page)
    for scroll_count in range(1, _kakaowebtoon_scrolls() + 1):
        matched = await _find_kakaowebtoon_listing_match(
            page,
            candidate,
            settle_attempts=6 if scroll_count == 1 else 2,
        )
        if matched is not None:
            observed_status = matched.get("status") or STATUS_ONGOING
            matched_by = (
                "content_id"
                if str(matched.get("content_id") or "").strip() == str(candidate.get("content_id") or "").strip()
                else "title"
            )
            return {
                "content_id": candidate.get("content_id"),
                "title": candidate.get("title"),
                "expected_status": candidate.get("expected_status"),
                "observed_status": observed_status,
                "ok": _status_matches(candidate.get("expected_status"), observed_status),
                "verification_method": "listing",
                "listing_url": page.url,
                "evidence": {
                    "scrolls": scroll_count,
                    "settle_attempt": int(matched.get("_settle_attempt") or 1),
                    "matched_by": matched_by,
                    "parsed_title": matched.get("title"),
                    "parsed_status": observed_status,
                    "adult": bool(matched.get("adult")),
                },
            }

        if scroll_count < _kakaowebtoon_scrolls():
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(_page_wait_ms())
    return None


async def _verify_naver_series_listing_fallback(page, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for seed in _select_naver_series_seeds(candidate):
        for page_no in range(1, _listing_fallback_pages() + 1):
            await _navigate(page, _append_query(str(seed["base_url"]), page=page_no))
            html = await page.content()
            items = parse_naver_series_list(
                html,
                is_finished_page=bool(seed.get("is_finished_page")),
                default_genres=[str(seed.get("genre") or "").strip()],
            )
            matched = _match_listing_item(candidate, items)
            if matched is None:
                continue
            observed_status = matched.get("status") or STATUS_ONGOING
            return {
                "content_id": candidate.get("content_id"),
                "title": candidate.get("title"),
                "expected_status": candidate.get("expected_status"),
                "observed_status": observed_status,
                "ok": _status_matches(candidate.get("expected_status"), observed_status),
                "verification_method": "listing",
                "listing_url": page.url,
                "evidence": {
                    "seed_key": seed.get("key"),
                    "page_no": page_no,
                    "parsed_title": matched.get("title"),
                    "parsed_status": observed_status,
                },
            }
    return None


def _naver_series_search_queries(candidate: Dict[str, Any]) -> List[str]:
    raw_title = _clean_text(candidate.get("title"))
    queries: List[str] = []
    for value in (raw_title, raw_title.split("[", 1)[0].strip()):
        cleaned = _clean_text(value)
        if cleaned and cleaned not in queries:
            queries.append(cleaned)
    return queries


async def _verify_naver_series_search_fallback(page, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for query in _naver_series_search_queries(candidate):
        search_url = f"https://series.naver.com/search/search.series?fs=novel&q={quote(query)}"
        await _navigate(page, search_url)
        html = await page.content()
        items = parse_naver_series_list(
            html,
            is_finished_page=False,
            default_genres=[],
        )
        matched = _match_listing_item(candidate, items)
        if matched is None:
            continue
        observed_status = matched.get("status") or STATUS_ONGOING
        return {
            "content_id": candidate.get("content_id"),
            "title": candidate.get("title"),
            "expected_status": candidate.get("expected_status"),
            "observed_status": observed_status,
            "ok": _status_matches(candidate.get("expected_status"), observed_status),
            "verification_method": "search",
            "search_url": page.url,
            "evidence": {
                "query": query,
                "parsed_title": matched.get("title"),
                "parsed_status": observed_status,
            },
        }
    return None


async def _verify_naver_series_candidate(page, candidate: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = str(candidate.get("content_url") or "")
    await _navigate(page, detail_url)
    page_title = await page.title()
    body_text = await _page_text(page)

    if NAVER_LOGIN_HOST in page.url or "로그인" in page_title:
        search_fallback = await _verify_naver_series_search_fallback(page, candidate)
        if search_fallback is not None and search_fallback.get("ok"):
            return search_fallback
        fallback = await _verify_naver_series_listing_fallback(page, candidate)
        if fallback is not None and fallback.get("ok"):
            return fallback
        if search_fallback is not None:
            return search_fallback
        if fallback is not None:
            return fallback
        return {
            "content_id": candidate.get("content_id"),
            "title": candidate.get("title"),
            "expected_status": candidate.get("expected_status"),
            "observed_status": None,
            "ok": False,
            "verification_method": "detail",
            "detail_url": page.url,
            "evidence": {"page_title": page_title, "reason": "login_redirect"},
        }

    observed_status = _extract_naver_series_detail_status(body_text)
    title_ok = _titles_match(candidate.get("title"), page_title) or _titles_match(candidate.get("title"), body_text)
    ok = title_ok and _status_matches(candidate.get("expected_status"), observed_status)
    if not ok:
        search_fallback = await _verify_naver_series_search_fallback(page, candidate)
        if search_fallback is not None and search_fallback.get("ok"):
            return search_fallback
        fallback = await _verify_naver_series_listing_fallback(page, candidate)
        if fallback is not None and fallback.get("ok"):
            return fallback

    return {
        "content_id": candidate.get("content_id"),
        "title": candidate.get("title"),
        "expected_status": candidate.get("expected_status"),
        "observed_status": observed_status,
        "ok": ok,
        "verification_method": "detail",
        "detail_url": page.url,
        "evidence": {
            "page_title": page_title,
            "status_window": _status_window(body_text, "완결" if observed_status == STATUS_COMPLETED else "연재중"),
        },
    }


async def _verify_kakaopage_listing_fallback(page, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for seed in _select_kakaopage_seeds(candidate):
        await _navigate(page, _public_kakaopage_seed_url(str(seed.get("url") or "")))
        for scroll_count in range(1, _kakaopage_scrolls() + 1):
            html = await page.content()
            items = parse_kakaopage_listing_items(
                html,
                default_genres=list(seed.get("genres") or []),
                seed_completed=bool(seed.get("seed_completed")),
            )
            matched = _match_listing_item(candidate, items)
            if matched is not None:
                observed_status = matched.get("status") or STATUS_ONGOING
                return {
                    "content_id": candidate.get("content_id"),
                    "title": candidate.get("title"),
                    "expected_status": candidate.get("expected_status"),
                    "observed_status": observed_status,
                    "ok": _status_matches(candidate.get("expected_status"), observed_status),
                    "verification_method": "listing",
                    "listing_url": page.url,
                    "evidence": {
                        "seed_key": seed.get("seed_stat_key") or seed.get("name"),
                        "scrolls": scroll_count,
                        "parsed_title": matched.get("title"),
                        "parsed_status": observed_status,
                    },
                }

            if scroll_count < _kakaopage_scrolls():
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(_page_wait_ms())
    return None


async def _verify_kakaopage_candidate(page, candidate: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = str(candidate.get("content_url") or "")
    await _navigate(page, detail_url)
    page_title = await page.title()
    html = await page.content()
    parsed = parse_kakaopage_detail(html)
    observed_status = parsed.get("status") or STATUS_ONGOING
    title_ok = _titles_match(candidate.get("title"), parsed.get("title")) or _titles_match(candidate.get("title"), page_title)
    ok = title_ok and _status_matches(candidate.get("expected_status"), observed_status)
    if not ok:
        fallback = await _verify_kakaopage_listing_fallback(page, candidate)
        if fallback is not None and fallback.get("ok"):
            return fallback
    return {
        "content_id": candidate.get("content_id"),
        "title": candidate.get("title"),
        "expected_status": candidate.get("expected_status"),
        "observed_status": observed_status,
        "ok": ok,
        "verification_method": "detail",
        "detail_url": page.url,
        "evidence": {
            "page_title": page_title,
            "parsed_title": parsed.get("title"),
            "parsed_authors": parsed.get("authors") or [],
        },
    }


async def _verify_ridi_candidate(page, candidate: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = str(candidate.get("content_url") or "")
    await _navigate(page, detail_url)
    page_title = await page.title()
    body_text = await _page_text(page)
    html = await page.content()
    focused_text = _excerpt_near(body_text, candidate.get("title"))
    observed_status = _extract_ridi_status(focused_text, html, page_title=page_title)
    title_ok = _titles_match(candidate.get("title"), page_title) or _titles_match(candidate.get("title"), body_text)
    ok = title_ok and _status_matches(candidate.get("expected_status"), observed_status)
    return {
        "content_id": candidate.get("content_id"),
        "title": candidate.get("title"),
        "expected_status": candidate.get("expected_status"),
        "observed_status": observed_status,
        "ok": ok,
        "verification_method": "detail",
        "detail_url": page.url,
        "evidence": {
            "page_title": page_title,
            "status_window": _status_window(
                focused_text or html,
                "연재완결" if observed_status == STATUS_COMPLETED else "연재",
            ),
        },
    }


async def _verify_laftel_candidate(page, candidate: Dict[str, Any]) -> Dict[str, Any]:
    detail_url = str(candidate.get("content_url") or "")
    content_id = str(candidate.get("content_id") or "").strip()
    async with page.expect_response(
        lambda response: f"/api/items/v4/{content_id}/" in response.url and response.ok,
        timeout=_browser_timeout_ms(),
    ) as response_info:
        await _navigate(page, detail_url)
    response = await response_info.value
    payload = await response.json()
    observed_status = STATUS_COMPLETED if bool(payload.get("is_ending")) else STATUS_ONGOING
    observed_title = payload.get("name") or await page.title()
    ok = _titles_match(candidate.get("title"), observed_title) and _status_matches(candidate.get("expected_status"), observed_status)
    return {
        "content_id": candidate.get("content_id"),
        "title": candidate.get("title"),
        "expected_status": candidate.get("expected_status"),
        "observed_status": observed_status,
        "ok": ok,
        "verification_method": "detail",
        "detail_url": page.url,
        "evidence": {
            "api_url": response.url,
            "api_title": payload.get("name"),
            "is_ending": bool(payload.get("is_ending")),
        },
    }


async def _run_playwright_verifier(
    write_plan: Dict[str, Any],
    *,
    source_name: str,
    candidate_verifier: Callable[[Any, Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> Dict[str, Any]:
    candidates = _candidate_items(write_plan)
    if not candidates:
        return {
            "gate": "not_applicable",
            "mode": "playwright_browser",
            "reason": "no_candidate_changes",
            "message": f"no new or newly completed items to verify for {source_name}",
            "apply_allowed": True,
            "changed_count": 0,
            "verified_count": 0,
            "items": [],
        }

    print(f"[VERIFY][{source_name}] candidates={len(candidates)}", flush=True)

    try:
        async with _browser_session() as context:
            page = await context.new_page()
            page.set_default_timeout(_browser_timeout_ms())
            results = []
            for index, candidate in enumerate(candidates, start=1):
                content_id = str(candidate.get("content_id") or "").strip()
                title = str(candidate.get("title") or "").strip()
                print(
                    f"[VERIFY][{source_name}] start={index}/{len(candidates)} content_id={content_id} title={title}",
                    flush=True,
                )
                result = await candidate_verifier(page, candidate)
                print(
                    f"[VERIFY][{source_name}] done={index}/{len(candidates)} content_id={content_id} ok={bool(result.get('ok'))}",
                    flush=True,
                )
                results.append(result)
    except Exception as exc:
        return {
            "gate": "blocked",
            "mode": "playwright_browser",
            "reason": "browser_verification_failed",
            "message": f"{source_name} Playwright verification failed: {type(exc).__name__}: {exc}",
            "apply_allowed": False,
            "changed_count": len(candidates),
            "verified_count": 0,
            "items": [],
        }

    failed_items = [item for item in results if not item.get("ok")]
    verified_count = len(results) - len(failed_items)
    if failed_items:
        failed_titles = ", ".join(str(item.get("title") or item.get("content_id")) for item in failed_items[:3])
        return {
            "gate": "blocked",
            "mode": "playwright_browser",
            "reason": "verification_mismatch",
            "message": f"{source_name} verified {verified_count}/{len(results)} changed items; failed: {failed_titles}",
            "apply_allowed": False,
            "changed_count": len(results),
            "verified_count": verified_count,
            "failed_count": len(failed_items),
            "items": results,
        }

    return {
        "gate": "passed",
        "mode": "playwright_browser",
        "reason": "verified_all_changed_items",
        "message": f"{source_name} verified {verified_count}/{len(results)} changed items via Playwright",
        "apply_allowed": True,
        "changed_count": len(results),
        "verified_count": verified_count,
        "items": results,
    }


async def verify_naver_webtoon(write_plan: Dict[str, Any]) -> Dict[str, Any]:
    return await _run_playwright_verifier(
        write_plan,
        source_name="naver_webtoon",
        candidate_verifier=_verify_naver_webtoon_candidate,
    )


async def verify_kakao_webtoon(write_plan: Dict[str, Any]) -> Dict[str, Any]:
    return await _run_playwright_verifier(
        write_plan,
        source_name="kakaowebtoon",
        candidate_verifier=_verify_kakao_webtoon_candidate,
    )


async def verify_naver_series(write_plan: Dict[str, Any]) -> Dict[str, Any]:
    return await _run_playwright_verifier(
        write_plan,
        source_name="naver_series",
        candidate_verifier=_verify_naver_series_candidate,
    )


async def verify_kakaopage(write_plan: Dict[str, Any]) -> Dict[str, Any]:
    return await _run_playwright_verifier(
        write_plan,
        source_name="kakao_page",
        candidate_verifier=_verify_kakaopage_candidate,
    )


async def verify_ridi(write_plan: Dict[str, Any]) -> Dict[str, Any]:
    return await _run_playwright_verifier(
        write_plan,
        source_name="ridi",
        candidate_verifier=_verify_ridi_candidate,
    )


async def verify_laftel(write_plan: Dict[str, Any]) -> Dict[str, Any]:
    return await _run_playwright_verifier(
        write_plan,
        source_name="laftel",
        candidate_verifier=_verify_laftel_candidate,
    )


register_source_verifier("naver_webtoon", verify_naver_webtoon)
register_source_verifier("kakaowebtoon", verify_kakao_webtoon)
register_source_verifier("naver_series", verify_naver_series)
register_source_verifier("kakao_page", verify_kakaopage)
register_source_verifier("ridi", verify_ridi)
register_source_verifier("laftel", verify_laftel)


def build_verification_gate(
    *,
    dry_run: bool = False,
) -> VerificationGate:
    mode = normalize_verification_mode()

    async def gate(write_plan: Dict[str, Any]) -> Dict[str, Any]:
        source_name = str(write_plan.get("source_name") or "").strip()
        source_verifier = VERIFIER_REGISTRY.get(source_name)

        if callable(source_verifier):
            verdict = source_verifier(write_plan)
            if hasattr(verdict, "__await__"):
                verdict = await verdict
            if isinstance(verdict, dict):
                return verdict

        changed_count = len(write_plan.get("verification_candidates") or [])
        if mode == "fail_closed":
            return {
                "gate": "blocked",
                "mode": mode,
                "reason": "no_source_verifier",
                "message": f"no source verifier registered for {source_name}",
                "apply_allowed": False,
                "changed_count": changed_count,
            }

        message = f"no source verifier registered for {source_name}; pass-through gate used"
        if dry_run:
            message = f"{message} (dry-run)"

        return {
            "gate": "passed",
            "mode": mode,
            "reason": "pass_through",
            "message": message,
            "apply_allowed": True,
            "changed_count": changed_count,
        }

    return gate
