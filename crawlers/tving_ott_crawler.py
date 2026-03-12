from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict

import requests

import config
from services.ott_content_service import build_canonical_ott_entry

from .canonical_ott_crawler import CanonicalOttCrawler

TVING_PAGE_URL = "https://www.tving.com/more/band/HM257176"
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.S,
)


class TvingOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "TVING OTT"
    SOURCE_NAME = "tving"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _build_content_url(code: str) -> str:
        return f"https://www.tving.com/contents/{code}"

    def _parse_page(self, html: str) -> Dict[str, Dict[str, Any]]:
        match = _NEXT_DATA_RE.search(str(html or ""))
        if not match:
            return {}

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
        return entries

    async def fetch_all_data(self):
        def _get() -> str:
            response = requests.get(
                TVING_PAGE_URL,
                headers=config.CRAWLER_HEADERS,
                timeout=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.text

        html = await asyncio.to_thread(_get)
        ongoing_today = self._parse_page(html)
        all_content_today = dict(ongoing_today)
        fetch_meta = {
            "fetched_count": len(all_content_today),
            "force_no_ratio": True,
            "errors": [],
            "source_page": TVING_PAGE_URL,
        }
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
        return ongoing_today, {}, {}, all_content_today, fetch_meta
