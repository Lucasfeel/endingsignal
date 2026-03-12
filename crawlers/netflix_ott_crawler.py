from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, Optional

import requests
from bs4 import BeautifulSoup

import config
from services.ott_content_service import build_canonical_ott_entry
from utils.time import now_kst_naive

from .canonical_ott_crawler import CanonicalOttCrawler
from .ott_parser_utils import clean_text, parse_flexible_datetime

NETFLIX_CANDIDATE_URL = "https://place202.com/bale/netflix/coming?worktype=SERIES&order=release"
NETFLIX_OFFICIAL_URL = "https://www.netflix.com/kr/title/{netflix_id}?hl=ko"
_BATCH_RELEASE_HINT_RE = re.compile(
    r"(전편|전체\s*에피소드|모든\s*에피소드|모든\s*회차|한\s*번에|일괄\s*공개|all\s+episodes)",
    re.I,
)
_WEEKLY_RELEASE_HINT_RE = re.compile(
    r"(매주|주간|weekly|new\s+episodes?)",
    re.I,
)
_SEASON_SUFFIX_RE = re.compile(
    r"^(?P<base>.+?)\s*[-:]\s*(?P<suffix>(?:시즌|season|파트|part)\s*\d+)$",
    re.I,
)


class NetflixOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "Netflix OTT"
    SOURCE_NAME = "netflix"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _normalize_season_suffix(text: str) -> str:
        match = _SEASON_SUFFIX_RE.match(clean_text(text))
        if not match:
            return ""
        suffix = clean_text(match.group("suffix"))
        normalized = re.sub(r"(?i)^season", "시즌", suffix)
        normalized = re.sub(r"(?i)^part", "파트", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    @classmethod
    def _merge_official_title(cls, candidate_title: str, official_title: str) -> str:
        safe_candidate = clean_text(candidate_title)
        safe_official = clean_text(official_title)
        if not safe_official:
            return safe_candidate
        season_suffix = cls._normalize_season_suffix(safe_candidate)
        if season_suffix:
            base_match = _SEASON_SUFFIX_RE.match(safe_candidate)
            candidate_base = clean_text(base_match.group("base")) if base_match else safe_candidate
            if candidate_base.lower() == safe_official.lower():
                return f"{safe_official} {season_suffix}"
        return safe_candidate

    @staticmethod
    def _extract_card_id(card: Any, action_link: str) -> str:
        if action_link:
            return action_link.rstrip("/").split("/")[-1]
        button = card.select_one("button[data-netflix-id]")
        if button is not None:
            return clean_text(button.get("data-netflix-id"))
        return ""

    def _fetch_official_metadata(self, session: requests.Session, netflix_id: str, action_link: str) -> Dict[str, Any]:
        url = clean_text(action_link) or NETFLIX_OFFICIAL_URL.format(netflix_id=netflix_id)
        try:
            response = session.get(
                url,
                headers={
                    **config.CRAWLER_HEADERS,
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
                },
                timeout=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except Exception:
            return {
                "platform_url": url,
            }

        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find("script", attrs={"type": "application/ld+json"})
        payload: Dict[str, Any] = {}
        if script and script.string:
            try:
                payload = json.loads(script.string)
            except Exception:
                payload = {}

        cast = []
        actors = payload.get("actors")
        if isinstance(actors, list):
            for actor in actors:
                if isinstance(actor, dict):
                    name = clean_text(actor.get("name"))
                    if name:
                        cast.append(name)
        return {
            "official_title": clean_text(payload.get("name")),
            "description": clean_text(payload.get("description")),
            "thumbnail_url": clean_text(payload.get("image")),
            "cast": cast,
            "platform_url": clean_text(payload.get("url")) or url,
            "official_text": clean_text(soup.get_text(" ", strip=True)),
        }

    def _parse_page(
        self,
        html: str,
        *,
        official_metadata_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        entries: Dict[str, Dict[str, Any]] = {}
        grid = soup.select_one("div.grid") or soup
        current_release_start_at = None
        official_metadata_by_id = official_metadata_by_id or {}

        for node in grid.children:
            node_name = getattr(node, "name", None)
            if node_name == "h2":
                current_release_start_at = parse_flexible_datetime(node.get_text(" ", strip=True))
                continue
            if node_name != "div":
                continue
            classes = node.get("class") or []
            if "row" not in classes:
                continue

            for card in node.select(":scope > div.item"):
                title_node = card.select_one("h5 .inline-with-badge")
                if title_node is None:
                    continue

                candidate_title = clean_text(title_node.get_text(" ", strip=True))
                if not candidate_title:
                    continue

                action_link = ""
                for anchor in card.select("a[href]"):
                    href = clean_text(anchor.get("href"))
                    if "netflix.com/title/" in href:
                        action_link = href
                        break

                netflix_id = self._extract_card_id(card, action_link)
                if not netflix_id:
                    continue

                official = official_metadata_by_id.get(netflix_id) or {}
                final_title = self._merge_official_title(candidate_title, official.get("official_title") or "")

                cast_node = card.select_one(".info > p")
                summary_node = card.select_one("blockquote.border-0 p")
                image = card.select_one("figure.overlay img")

                cast = []
                if cast_node is not None:
                    cast = [entry.strip() for entry in cast_node.get_text(",", strip=True).split(",") if entry.strip()]
                if official.get("cast"):
                    cast = list(official["cast"])

                description = clean_text(summary_node.get_text(" ", strip=True) if summary_node is not None else "")
                if official.get("description"):
                    description = clean_text(official["description"])

                combined_text = clean_text(
                    f"{candidate_title} {description} {official.get('official_text') or ''} {card.get_text(' ', strip=True)}"
                )
                inferred_binge = bool(_BATCH_RELEASE_HINT_RE.search(combined_text))
                inferred_weekly = bool(_WEEKLY_RELEASE_HINT_RE.search(combined_text))
                release_end_at = (
                    current_release_start_at
                    if (current_release_start_at and inferred_binge and not inferred_weekly)
                    else None
                )
                release_end_status = "scheduled" if release_end_at else "unknown"
                platform_url = clean_text(official.get("platform_url")) or action_link or NETFLIX_OFFICIAL_URL.format(
                    netflix_id=netflix_id
                )
                thumbnail_url = (
                    clean_text(official.get("thumbnail_url"))
                    or clean_text(image.get("src") if image is not None else "")
                    or None
                )

                entry = build_canonical_ott_entry(
                    platform_source=self.source_name,
                    title=final_title,
                    platform_content_id=netflix_id,
                    platform_url=platform_url,
                    thumbnail_url=thumbnail_url,
                    cast=cast,
                    release_start_at=current_release_start_at,
                    release_end_at=release_end_at,
                    release_end_status=release_end_status,
                    upcoming=bool(current_release_start_at and current_release_start_at > now_kst_naive()),
                    availability_status="scheduled" if current_release_start_at else "available",
                    description=description or None,
                    title_alias=[candidate_title, clean_text(official.get("official_title"))],
                    representative_year=current_release_start_at.year if current_release_start_at else None,
                    raw_schedule_note=clean_text(card.get_text(" ", strip=True)) or None,
                )
                entries[entry["canonical_content_id"]] = entry
        return entries

    async def fetch_all_data(self):
        def _get() -> str:
            response = requests.get(
                NETFLIX_CANDIDATE_URL,
                headers=config.CRAWLER_HEADERS,
                timeout=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.text

        html = await asyncio.to_thread(_get)
        soup = BeautifulSoup(html, "html.parser")
        netflix_ids = []
        for card in soup.select("div.item"):
            action_link = ""
            for anchor in card.select("a[href]"):
                href = clean_text(anchor.get("href"))
                if "netflix.com/title/" in href:
                    action_link = href
                    break
            netflix_id = self._extract_card_id(card, action_link)
            if netflix_id:
                netflix_ids.append((netflix_id, action_link))

        official_metadata_by_id: Dict[str, Dict[str, Any]] = {}
        with requests.Session() as session:
            for netflix_id, action_link in netflix_ids:
                official_metadata_by_id[netflix_id] = self._fetch_official_metadata(session, netflix_id, action_link)

        ongoing_today = self._parse_page(
            html,
            official_metadata_by_id=official_metadata_by_id,
        )
        all_content_today = dict(ongoing_today)
        fetch_meta = {
            "fetched_count": len(all_content_today),
            "force_no_ratio": True,
            "errors": [],
            "source_page": NETFLIX_CANDIDATE_URL,
            "candidate_feed": "place202",
        }
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
        return ongoing_today, {}, {}, all_content_today, fetch_meta
