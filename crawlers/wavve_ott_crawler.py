from __future__ import annotations

import asyncio
import re
from typing import Any, Dict
from urllib.parse import urljoin

from services.ott_content_service import build_canonical_ott_entry

from .canonical_ott_crawler import CanonicalOttCrawler
from .ott_parser_utils import parse_flexible_datetime

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

    async def fetch_all_data(self):
        try:
            from playwright.async_api import async_playwright
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
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--no-sandbox"])
                context = await browser.new_context(locale="ko-KR")
                page = await context.new_page()
                await page.goto(WAVVE_VIEW_MORE_URL, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(2_000)
                for _ in range(5):
                    await page.mouse.wheel(0, 1800)
                    await page.wait_for_timeout(500)

                raw_items = await page.evaluate(
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
                await context.close()
                await browser.close()
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
