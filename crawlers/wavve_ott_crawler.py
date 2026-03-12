from __future__ import annotations

import asyncio
import re
from typing import Any, Dict
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from services.ott_content_service import build_canonical_ott_entry

from .canonical_ott_crawler import CanonicalOttCrawler
from .ott_parser_utils import clean_text, parse_flexible_datetime

WAVVE_VIEW_MORE_URL = "https://www.wavve.com/view-more?code=EN100000----GN51"
_ID_RE = re.compile(r"([A-Z0-9]{8,}|[0-9]{6,})")
_DATE_SPLIT_RE = re.compile(r"^(?P<title>.+?)\s+\d{1,2}월\s+\d{1,2}일")


class WavveOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "Wavve OTT"
    SOURCE_NAME = "wavve"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _extract_content_id(url: str, fallback_title: str) -> str:
        match = _ID_RE.search(str(url or ""))
        if match:
            return match.group(1)
        return fallback_title

    @staticmethod
    def _normalize_title(raw_title: str) -> str:
        title = str(raw_title or "").strip()
        title = re.sub(r"^(exclusive|original|firstrun)\s+", "", title, flags=re.IGNORECASE).strip()
        match = _DATE_SPLIT_RE.search(title)
        if match:
            title = match.group("title").strip()
        return title

    @staticmethod
    def _extract_raw_items_from_html(html: str) -> list[dict[str, str]]:
        soup = BeautifulSoup(html or "", "html.parser")
        rows = []
        seen = set()
        for anchor in soup.select("a.click-area"):
            href = clean_text(anchor.get("href"))
            img = anchor.select_one("img[alt]")
            alt = clean_text(img.get("alt")) if img is not None else ""
            title1_node = anchor.select_one(".title1")
            title2_node = anchor.select_one(".title2")
            title1 = clean_text(title1_node.get_text(" ", strip=True)) if title1_node is not None else ""
            title2 = clean_text(title2_node.get_text(" ", strip=True)) if title2_node is not None else ""
            title = alt or clean_text(" ".join(part for part in [title1, title2] if part))
            if not title or len(title) < 2 or title in seen:
                continue
            seen.add(title)
            rows.append(
                {
                    "title": title,
                    "href": href,
                    "thumbnail_url": clean_text(img.get("src")) if img is not None else "",
                }
            )
        return rows

    async def _collect_raw_items_with_playwright(self) -> list[dict[str, str]]:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"],
            )
            context = await browser.new_context(locale="ko-KR", ignore_https_errors=True)
            page = await context.new_page()
            try:
                await page.goto(WAVVE_VIEW_MORE_URL, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_selector("a.click-area, .click-area", state="attached", timeout=15_000)
                except Exception:
                    pass

                stable_rounds = 0
                previous_count = -1
                for _ in range(8):
                    html = await page.content()
                    current_items = self._extract_raw_items_from_html(html)
                    current_count = len(current_items)
                    if current_count > 0 and current_count == previous_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                    previous_count = current_count
                    if current_count >= 6 and stable_rounds >= 1:
                        return current_items
                    await page.mouse.wheel(0, 1800)
                    await page.wait_for_timeout(700)

                html = await page.content()
                raw_items = self._extract_raw_items_from_html(html)
                if raw_items:
                    return raw_items

                return await page.evaluate(
                    """
                    () => {
                      const rows = [];
                      const seen = new Set();
                      for (const anchor of document.querySelectorAll('a.click-area')) {
                        const img = anchor.querySelector('img[alt]');
                        const href = anchor.getAttribute('href') || '';
                        const title = ((img && img.getAttribute('alt')) || '').replace(/\\s+/g, ' ').trim();
                        if (!title || title.length < 2 || seen.has(title)) {
                          continue;
                        }
                        seen.add(title);
                        rows.push({
                          title,
                          href,
                          thumbnail_url: img ? (img.getAttribute('src') || '') : '',
                        });
                      }
                      return rows;
                    }
                    """
                )
            finally:
                await context.close()
                await browser.close()

    async def fetch_all_data(self):
        try:
            from playwright.async_api import async_playwright  # noqa: F401
        except Exception as exc:  # pragma: no cover - runtime only
            return {}, {}, {}, {}, {
                "fetched_count": 0,
                "force_no_ratio": True,
                "errors": [f"PLAYWRIGHT_IMPORT_FAILED:{exc}"],
                "is_suspicious_empty": True,
                "source_page": WAVVE_VIEW_MORE_URL,
            }

        ongoing_today: Dict[str, Dict[str, Any]] = {}
        errors = []
        try:
            raw_items = await self._collect_raw_items_with_playwright()
        except Exception as exc:  # pragma: no cover - runtime only
            errors.append(f"PLAYWRIGHT_FETCH_FAILED:{exc}")
            raw_items = []

        for item in raw_items or []:
            if not isinstance(item, dict):
                continue
            raw_title = str(item.get("title") or "").strip()
            title = self._normalize_title(raw_title)
            href = str(item.get("href") or "").strip()
            if not title:
                continue
            if "라인업" in title or "챔피언십" in title or "KLPGA" in title.upper():
                continue
            content_id = self._extract_content_id(href, title)
            entry = build_canonical_ott_entry(
                platform_source=self.source_name,
                title=title,
                platform_content_id=content_id,
                platform_url=urljoin("https://www.wavve.com", href) if href and href != "javascript:void(0)" else WAVVE_VIEW_MORE_URL,
                thumbnail_url=str(item.get("thumbnail_url") or "").strip() or None,
                release_start_at=parse_flexible_datetime(raw_title),
                upcoming=True,
                availability_status="scheduled",
                raw_schedule_note=raw_title,
            )
            ongoing_today[entry["canonical_content_id"]] = entry

        all_content_today = dict(ongoing_today)
        fetch_meta = {
            "fetched_count": len(all_content_today),
            "force_no_ratio": True,
            "errors": errors,
            "source_page": WAVVE_VIEW_MORE_URL,
        }
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
        return ongoing_today, {}, {}, all_content_today, fetch_meta
