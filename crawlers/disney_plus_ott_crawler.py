from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict
from urllib.parse import urljoin

import requests

import config
from services.ott_content_service import build_canonical_ott_entry

from .canonical_ott_crawler import CanonicalOttCrawler

DISNEY_UPCOMING_URL = "https://www.disneyplus.com/ko-kr/browse/page-36541dc7-6961-4bbb-a07b-ef97d7da7995"
_NEXT_DATA_RE = re.compile(
    r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    re.S,
)


class DisneyPlusOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "Disney Plus OTT"
    SOURCE_NAME = "disney_plus"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    def _parse_page(self, html: str) -> Dict[str, Dict[str, Any]]:
        match = _NEXT_DATA_RE.search(str(html or ""))
        if not match:
            return {}

        payload = json.loads(match.group(1))
        main_content = (
            payload.get("props", {})
            .get("pageProps", {})
            .get("stitchDocument", {})
            .get("mainContent", [])
        )
        entries: Dict[str, Dict[str, Any]] = {}
        for block in main_content or []:
            if not isinstance(block, dict) or block.get("_type") != "SetGroup":
                continue
            for slider in block.get("items") or []:
                if not isinstance(slider, dict):
                    continue
                slider_title = str(slider.get("title") or "").strip()
                if not slider_title or (
                    "에피소드" not in slider_title
                    and "시리즈" not in slider_title
                    and "TV" not in slider_title.upper()
                ):
                    continue
                for item in slider.get("items") or []:
                    if not isinstance(item, dict):
                        continue
                    title = str(item.get("title") or "").strip()
                    path = str(item.get("url") or "").strip()
                    content_id = str(item.get("_id") or path or "").strip()
                    if not title or not content_id or not path:
                        continue
                    image = item.get("imageVariants") or {}
                    thumbnail_url = ""
                    if isinstance(image, dict):
                        default_image = image.get("defaultImage") or {}
                        if isinstance(default_image, dict):
                            thumbnail_url = str(default_image.get("source") or "").strip()

                    entry = build_canonical_ott_entry(
                        platform_source=self.source_name,
                        title=title,
                        platform_content_id=content_id,
                        platform_url=urljoin("https://www.disneyplus.com", path),
                        thumbnail_url=thumbnail_url or None,
                        upcoming=True,
                        availability_status="scheduled",
                    )
                    entries[entry["canonical_content_id"]] = entry
        return entries

    async def fetch_all_data(self):
        def _get() -> str:
            response = requests.get(
                DISNEY_UPCOMING_URL,
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
            "source_page": DISNEY_UPCOMING_URL,
        }
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
        return ongoing_today, {}, {}, all_content_today, fetch_meta
