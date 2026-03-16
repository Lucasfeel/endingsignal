from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List

import requests

import config
from services.ott_content_service import build_canonical_ott_entry
from utils.time import now_kst_naive

from .canonical_ott_crawler import CanonicalOttCrawler
from .ott_parser_utils import parse_flexible_datetime

COUPANG_CATALOG_URL = "https://www.coupangplay.com/catalog"
COUPANG_WEEKLY_ROW_NAME = "TV프로그램, 매주 새 에피소드"
COUPANG_WEEKLY_ROW_TOKENS = ("tv프로그램", "매주새에피소드")
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    re.S,
)
_ROW_NAME_NORMALIZE_RE = re.compile(r"[\s,./·:]+")


class CoupangPlayOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "Coupang Play OTT"
    SOURCE_NAME = "coupangplay"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _build_content_url(content_id: str) -> str:
        return f"https://www.coupangplay.com/content/{content_id}"

    @staticmethod
    def _normalize_row_name(value: Any) -> str:
        text = str(value or "").strip().lower()
        return _ROW_NAME_NORMALIZE_RE.sub("", text)

    def _is_weekly_tv_row(self, feed: Dict[str, Any]) -> bool:
        normalized_row_name = self._normalize_row_name(feed.get("row_name"))
        if not normalized_row_name:
            return False
        return all(token in normalized_row_name for token in COUPANG_WEEKLY_ROW_TOKENS)

    def _iter_feed_items(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        feeds = payload.get("props", {}).get("pageProps", {}).get("feeds", [])
        results: List[Dict[str, Any]] = []
        for feed in feeds or []:
            if not isinstance(feed, dict):
                continue
            if not self._is_weekly_tv_row(feed):
                continue
            for item in feed.get("data") or []:
                if isinstance(item, dict):
                    results.append(item)
        return results

    def _parse_page(self, html: str) -> Dict[str, Dict[str, Any]]:
        match = _NEXT_DATA_RE.search(str(html or ""))
        if not match:
            return {}

        payload = json.loads(match.group(1))
        items = self._iter_feed_items(payload)
        entries: Dict[str, Dict[str, Any]] = {}
        for item in items:
            if str(item.get("type") or "").strip().upper() != "TITLE":
                continue
            if str(item.get("sub_type") or "").strip().upper() != "TVSHOW":
                continue

            title = str(item.get("title") or item.get("title_canonical") or "").strip()
            content_id = str(item.get("id") or "").strip()
            if not title or not content_id:
                continue

            cast = []
            for key in ("cast", "casts", "actors", "starring"):
                value = item.get(key)
                if isinstance(value, list):
                    cast.extend(str(entry).strip() for entry in value if str(entry).strip())
            description = str(item.get("description") or "").strip()
            raw_schedule_note = ""
            if description:
                raw_schedule_note = description.splitlines()[0].strip()
            if not raw_schedule_note:
                raw_schedule_note = str(item.get("upcoming_text") or "").strip()
            release_start_at = (
                parse_flexible_datetime(item.get("airing_date_friendly"))
                or parse_flexible_datetime(item.get("vodStartAt"))
            )
            is_future_release = bool(release_start_at and release_start_at > now_kst_naive())
            entry = build_canonical_ott_entry(
                platform_source=self.source_name,
                title=title,
                platform_content_id=content_id,
                platform_url=self._build_content_url(content_id),
                thumbnail_url=str(item.get("image_url") or item.get("imageUrl") or "").strip() or None,
                cast=cast,
                release_start_at=release_start_at,
                upcoming=is_future_release,
                availability_status="scheduled" if is_future_release else "available",
                description=description or None,
                representative_year=item.get("releaseYear"),
                raw_schedule_note=raw_schedule_note or None,
                episode_hint=str(item.get("badgeKey") or "").strip() or None,
            )
            entries[entry["canonical_content_id"]] = entry
        return entries

    async def fetch_all_data(self):
        def _get() -> str:
            response = requests.get(
                COUPANG_CATALOG_URL,
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
            "source_page": COUPANG_CATALOG_URL,
        }
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
        return ongoing_today, {}, {}, all_content_today, fetch_meta
