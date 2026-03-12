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
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.S,
)
_CONTENT_PATH_RE = re.compile(r"/contents/(?P<code>[A-Z0-9]+)")


class TvingOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "TVING OTT"
    SOURCE_NAME = "tving"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _build_content_url(code: str) -> str:
        return f"https://www.tving.com/contents/{code}"

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
            if not code or not title or not code.startswith("P"):
                continue
            entry = build_canonical_ott_entry(
                platform_source=self.source_name,
                title=title,
                platform_content_id=code,
                platform_url=self._build_content_url(code),
                thumbnail_url=None,
                upcoming=True,
                availability_status="scheduled",
            )
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
            if not code or not title or not code.startswith("P"):
                continue
            entry = build_canonical_ott_entry(
                platform_source=self.source_name,
                title=title,
                platform_content_id=code,
                platform_url=self._build_content_url(code),
                thumbnail_url=str(item.get("imageUrl") or "").strip() or None,
                upcoming=True,
                availability_status="scheduled",
            )
            entries[entry["canonical_content_id"]] = entry
        return entries or self._parse_dom_page(html)

    async def _fetch_with_playwright(self) -> str:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"],
            )
            context = await browser.new_context(
                locale="ko-KR",
                ignore_https_errors=True,
            )
            page = await context.new_page()
            try:
                await page.goto(
                    TVING_PAGE_URL,
                    wait_until="domcontentloaded",
                    timeout=60_000,
                )
                try:
                    await page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass
                await page.wait_for_timeout(1_500)
                return await page.content()
            finally:
                await context.close()
                await browser.close()

    async def fetch_all_data(self):
        errors = []
        fetch_method = "requests"

        def _get() -> str:
            response = requests.get(
                TVING_PAGE_URL,
                headers=config.CRAWLER_HEADERS,
                timeout=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.text

        html = ""
        try:
            html = await asyncio.to_thread(_get)
        except Exception as exc:
            errors.append(f"REQUEST_FETCH_FAILED:{type(exc).__name__}:{exc}")

        ongoing_today = self._parse_page(html)
        if not ongoing_today:
            try:
                html = await self._fetch_with_playwright()
                fetch_method = "playwright"
                ongoing_today = self._parse_page(html)
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
