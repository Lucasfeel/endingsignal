from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup

import config
from services.ott_content_service import build_canonical_ott_entry

from .canonical_ott_crawler import CanonicalOttCrawler

TVING_PAGE_URL = "https://www.tving.com/more/band/HM257176"
TVING_HOME_URL = "https://www.tving.com/"
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.S,
)
_CONTENT_PATH_RE = re.compile(r"/contents/(?P<code>[A-Z0-9]+)")
_TVING_ERROR_PATH_RE = re.compile(r"/500/?$")
_TVING_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
_TVING_ACCEPT_LANGUAGE = "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"


class TvingOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "TVING OTT"
    SOURCE_NAME = "tving"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _build_content_url(code: str) -> str:
        return f"https://www.tving.com/contents/{code}"

    def _build_entry(self, *, code: str, title: str, thumbnail_url: str | None = None) -> Dict[str, Any] | None:
        clean_code = str(code or "").strip()
        clean_title = " ".join(str(title or "").split()).strip()
        if not clean_code or not clean_title or not clean_code.startswith("P"):
            return None
        return build_canonical_ott_entry(
            platform_source=self.source_name,
            title=clean_title,
            platform_content_id=clean_code,
            platform_url=self._build_content_url(clean_code),
            thumbnail_url=str(thumbnail_url or "").strip() or None,
            upcoming=True,
            availability_status="scheduled",
        )

    def _entries_from_dom_link_items(self, items: list[dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        for item in items or []:
            if not isinstance(item, dict):
                continue
            href = str(item.get("href") or "").strip()
            match = _CONTENT_PATH_RE.search(href)
            if not match:
                continue
            code = str(match.group("code") or "").strip()
            title = str(item.get("titleText") or "").strip() or str(item.get("imgAlt") or "").strip()
            entry = self._build_entry(code=code, title=title)
            if entry is not None:
                entries[entry["canonical_content_id"]] = entry
        return entries

    def _parse_dom_page(self, html: str) -> Dict[str, Dict[str, Any]]:
        soup = BeautifulSoup(html or "", "html.parser")
        entries: Dict[str, Dict[str, Any]] = {}
        for anchor in soup.select('a[href*="/contents/"]'):
            href = str(anchor.get("href") or "").strip()
            match = _CONTENT_PATH_RE.search(href)
            if not match:
                continue
            code = str(match.group("code") or "").strip()
            title = " ".join(anchor.get_text(" ", strip=True).split()).strip()
            if not title:
                img = anchor.select_one("img[alt]")
                title = str(img.get("alt") or "").strip() if img is not None else ""
            entry = self._build_entry(code=code, title=title)
            if entry is not None:
                entries[entry["canonical_content_id"]] = entry
        return entries

    def _parse_page(self, html: str) -> Dict[str, Dict[str, Any]]:
        match = _NEXT_DATA_RE.search(str(html or ""))
        if not match:
            return self._parse_dom_page(html)

        payload = json.loads(match.group(1))
        queries = (
            payload.get("props", {})
            .get("pageProps", {})
            .get("dehydratedState", {})
            .get("queries", [])
        )
        items = []
        for query in queries:
            data = (query.get("state") or {}).get("data") or {}
            for page in data.get("pages") or []:
                band_items = (((page.get("data") or {}).get("band") or {}).get("items") or [])
                if isinstance(band_items, list):
                    items.extend(band_items)

        entries: Dict[str, Dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").strip()
            title = str(item.get("title") or "").strip()
            entry = self._build_entry(
                code=code,
                title=title,
                thumbnail_url=str(item.get("imageUrl") or "").strip() or None,
            )
            if entry is not None:
                entries[entry["canonical_content_id"]] = entry
        return entries or self._parse_dom_page(html)

    def _fetch_with_requests(self):
        request_attempts = [
            ("crawler_headers", config.CRAWLER_HEADERS),
            ("default_headers", None),
        ]
        errors = []

        for attempt_name, headers in request_attempts:
            response = None
            try:
                response = requests.get(
                    TVING_PAGE_URL,
                    headers=headers,
                    timeout=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
                )
                final_url = str(response.url or "").strip()
                response.raise_for_status()
                if final_url and _TVING_ERROR_PATH_RE.search(final_url):
                    raise requests.HTTPError(
                        f"TVING redirected to error page: {final_url}",
                        response=response,
                    )

                html = response.text
                parsed = self._parse_page(html)
                if parsed:
                    return html, parsed, f"requests:{attempt_name}", errors

                errors.append(
                    f"REQUEST_EMPTY:{attempt_name}:status={response.status_code}:final_url={final_url or TVING_PAGE_URL}"
                )
            except Exception as exc:
                final_url = ""
                if response is not None:
                    final_url = str(response.url or "").strip()
                suffix = f":final_url={final_url}" if final_url else ""
                errors.append(f"REQUEST_FETCH_FAILED:{attempt_name}:{type(exc).__name__}:{exc}{suffix}")

        return "", {}, "requests", errors

    async def _warmup_browser_session(self, page, errors: list[str]):
        try:
            await page.goto(
                TVING_HOME_URL,
                wait_until="domcontentloaded",
                timeout=60_000,
            )
            try:
                await page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                errors.append("PLAYWRIGHT_WARMUP_NETWORKIDLE_TIMEOUT")
            await page.wait_for_timeout(2_500)
        except Exception as exc:
            errors.append(f"PLAYWRIGHT_WARMUP_FAILED:{type(exc).__name__}:{exc}")

    async def _parse_playwright_target_page(self, page, errors: list[str]) -> tuple[str, Dict[str, Dict[str, Any]]]:
        try:
            await page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            errors.append("PLAYWRIGHT_NETWORKIDLE_TIMEOUT")

        try:
            await page.wait_for_selector('a[href^="/contents/P"]', state="attached", timeout=20_000)
        except Exception:
            errors.append("PLAYWRIGHT_SELECTOR_TIMEOUT")

        for delay_ms in (1_500, 3_500, 7_000):
            await page.wait_for_timeout(delay_ms)
            dom_items = await page.evaluate(
                """() => Array.from(document.querySelectorAll('a[href*="/contents/"]')).map((anchor) => {
                    const href = anchor.getAttribute('href') || '';
                    const text = (anchor.textContent || '').replace(/\\s+/g, ' ').trim();
                    const img = anchor.querySelector('img[alt]');
                    return {
                        href,
                        titleText: text,
                        imgAlt: img ? (img.getAttribute('alt') || '') : '',
                    };
                })"""
            )
            parsed = self._entries_from_dom_link_items(dom_items)
            if parsed:
                return await page.content(), parsed

            html = await page.content()
            parsed = self._parse_page(html)
            if parsed:
                return html, parsed

        final_url = str(page.url or "").strip()
        title = (await page.title()).strip()
        errors.append(f"PLAYWRIGHT_EMPTY:title={title}:url={final_url or TVING_PAGE_URL}")
        return await page.content(), {}

    async def _fetch_with_playwright(self) -> tuple[str, Dict[str, Dict[str, Any]], list[str]]:
        from playwright.async_api import async_playwright

        errors: list[str] = []

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"],
            )
            context = await browser.new_context(
                locale="ko-KR",
                ignore_https_errors=True,
                viewport={"width": 1440, "height": 2000},
                user_agent=_TVING_BROWSER_USER_AGENT,
                timezone_id="Asia/Seoul",
                extra_http_headers={
                    "Accept-Language": _TVING_ACCEPT_LANGUAGE,
                    "Referer": TVING_HOME_URL,
                },
            )
            page = await context.new_page()
            try:
                await self._warmup_browser_session(page, errors)

                for attempt_name in ("primary", "retry_after_warmup"):
                    await page.goto(
                        TVING_PAGE_URL,
                        wait_until="domcontentloaded",
                        timeout=60_000,
                    )
                    html, parsed = await self._parse_playwright_target_page(page, errors)
                    if parsed:
                        return html, parsed, errors

                    final_url = str(page.url or "").strip()
                    if final_url and _TVING_ERROR_PATH_RE.search(final_url):
                        errors.append(f"PLAYWRIGHT_ERROR_PAGE:{attempt_name}:{final_url}")
                    if attempt_name == "primary":
                        await self._warmup_browser_session(page, errors)

                return await page.content(), {}, errors
            finally:
                await context.close()
                await browser.close()

    async def fetch_all_data(self):
        html, ongoing_today, fetch_method, errors = await asyncio.to_thread(self._fetch_with_requests)
        if not ongoing_today:
            try:
                html, ongoing_today, play_errors = await self._fetch_with_playwright()
                fetch_method = "playwright"
                errors.extend(play_errors)
            except Exception as exc:
                errors.append(f"PLAYWRIGHT_FETCH_FAILED:{type(exc).__name__}:{exc}")

        all_content_today = dict(ongoing_today)
        fetch_meta = {
            "fetched_count": len(all_content_today),
            "force_no_ratio": True,
            "errors": errors,
            "source_page": TVING_PAGE_URL,
            "fetch_method": fetch_method,
        }
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
        return ongoing_today, {}, {}, all_content_today, fetch_meta
