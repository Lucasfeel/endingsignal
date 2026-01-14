import asyncio
import json
import os
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp

import config
from database import create_standalone_connection, get_cursor
from utils.time import now_kst_naive, parse_iso_naive_kst
from utils.text import normalize_search_text
from .base_crawler import ContentCrawler


HEADERS = {
    **config.CRAWLER_HEADERS,
    "Accept": "application/json",
    "Accept-Language": "ko",
    "Origin": "https://webtoon.kakao.com",
    "Referer": "https://webtoon.kakao.com/",
}

PLACEMENT_WEEKDAY_MAP = {
    "timetable_mon": "mon",
    "timetable_tue": "tue",
    "timetable_wed": "wed",
    "timetable_thu": "thu",
    "timetable_fri": "fri",
    "timetable_sat": "sat",
    "timetable_sun": "sun",
}


class KakaoWebtoonCrawler(ContentCrawler):
    """Kakao Webtoon timetable crawler."""

    DISPLAY_NAME = "Kakao Webtoon"
    PROFILE_BASE_URL = "https://gateway-kw.kakao.com/content/v1/contents"

    def __init__(self):
        super().__init__("kakaowebtoon")

    def _build_headers(self) -> Dict[str, str]:
        headers = dict(HEADERS)
        cookie = os.getenv("KAKAOWEBTOON_COOKIE")
        if cookie:
            headers["Cookie"] = cookie
        return headers

    @staticmethod
    def _normalize_authors(authors: List[Dict]) -> List[str]:
        if not isinstance(authors, list):
            return []
        ordered = []
        for idx, author in enumerate(authors):
            if not isinstance(author, dict):
                continue
            name = (author.get("name") or "").strip()
            if not name:
                continue
            order = author.get("order")
            ordered.append((order, idx, name))
        ordered.sort(key=lambda item: (item[0] is None, item[0] or 0, item[1]))
        seen = set()
        result = []
        for _, _, name in ordered:
            if name in seen:
                continue
            seen.add(name)
            result.append(name)
        return result

    @staticmethod
    def _select_thumbnail_url(content: Dict) -> Optional[str]:
        def _get_trimmed(value: object) -> Optional[str]:
            if isinstance(value, str):
                trimmed = value.strip()
                if trimmed:
                    return trimmed
            return None

        priority_keys = (
            "backgroundImage",
            "featuredCharacterImageA",
            "featuredCharacterImageB",
            "featuredCharacterAnimationFirstFrame",
            "titleImageA",
            "titleImageB",
        )
        for key in priority_keys:
            value = _get_trimmed(content.get(key))
            if value:
                return value

        anchor_clip = content.get("anchorClip")
        if isinstance(anchor_clip, dict):
            clip_value = _get_trimmed(anchor_clip.get("clipFirstFrame"))
            if clip_value:
                return clip_value

        fallback_keys = (
            "thumbnailUrl",
            "thumbnail_url",
            "featuredImageUrl",
            "posterImageUrl",
            "coverImageUrl",
        )
        for key in fallback_keys:
            value = _get_trimmed(content.get(key))
            if value:
                return value
        return None

    @staticmethod
    def _normalize_kakao_asset_url(url: Optional[str]) -> Optional[str]:
        if not isinstance(url, str):
            return url
        trimmed = url.strip()
        if not trimmed:
            return trimmed
        lowered = trimmed.lower()
        if lowered.endswith((".webp", ".png", ".jpg", ".jpeg", ".gif")):
            return trimmed
        return f"{trimmed}.webp"

    @staticmethod
    def _strip_known_extension(url: Optional[str]) -> Optional[str]:
        if not isinstance(url, str):
            return url
        trimmed = url.strip()
        if not trimmed:
            return trimmed
        lowered = trimmed.lower()
        for ext in (".webp", ".png", ".jpg", ".jpeg"):
            if lowered.endswith(ext):
                return trimmed[: -len(ext)]
        return trimmed

    @classmethod
    def _build_asset_variants(
        cls,
        raw_url: Optional[str],
        primary_ext: str,
        fallback_ext: str,
    ) -> Optional[Dict[str, str]]:
        if not isinstance(raw_url, str):
            return None
        stripped = cls._strip_known_extension(raw_url)
        if not stripped:
            return None
        return {
            primary_ext: f"{stripped}.{primary_ext}",
            fallback_ext: f"{stripped}.{fallback_ext}",
        }

    def _build_entry(self, content: Dict) -> Optional[Dict]:
        content_id = str(content.get("id") or "").strip()
        if not content_id:
            return None
        title = (content.get("title") or "").strip()
        if not title:
            return None
        authors = self._normalize_authors(content.get("authors") or [])
        thumbnail_url = self._select_thumbnail_url(content)
        thumbnail_url = self._normalize_kakao_asset_url(thumbnail_url) if thumbnail_url else None
        raw_bg = content.get("backgroundImage")
        kakao_bg = self._build_asset_variants(raw_bg, "webp", "jpg")
        thumbnail_url = (kakao_bg or {}).get("webp") or thumbnail_url
        kakao_assets = {
            "bg_color": (content.get("backgroundColor") or "").strip() or None,
            "bg": kakao_bg,
            "character_a": self._build_asset_variants(
                content.get("featuredCharacterImageA"),
                "webp",
                "png",
            ),
            "character_b": self._build_asset_variants(
                content.get("featuredCharacterImageB"),
                "webp",
                "png",
            ),
            "title_a": self._build_asset_variants(content.get("titleImageA"), "webp", "png"),
            "title_b": self._build_asset_variants(content.get("titleImageB"), "webp", "png"),
        }
        kakao_assets = {
            key: value
            for key, value in kakao_assets.items()
            if value not in (None, "")
        }
        seo_id = content.get("seoId") or content.get("seo_id") or content.get("seoID")
        slug = seo_id or title or content_id
        content_url = (
            f"https://webtoon.kakao.com/content/{urllib.parse.quote(str(slug))}/{content_id}"
        )
        entry = {
            "content_id": content_id,
            "title": title,
            "authors": authors,
            "thumbnail_url": thumbnail_url,
            "content_url": content_url,
        }
        if kakao_assets:
            entry["kakao_assets"] = kakao_assets
        return entry

    @staticmethod
    def _normalize_status_text(value: Optional[str]) -> Optional[str]:
        if not isinstance(value, str):
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        return trimmed.upper()

    def _extract_ongoing_status(self, card: Dict, content: Dict) -> Optional[str]:
        status = None
        if isinstance(content, dict):
            status = content.get("onGoingStatus")
        if status is None and isinstance(card, dict):
            status = card.get("onGoingStatus")
        return self._normalize_status_text(status)

    @staticmethod
    def _is_pause_status(status: Optional[str]) -> bool:
        return status == "PAUSE"

    @staticmethod
    def _is_completed_status(status: Optional[str]) -> bool:
        return status == "COMPLETED"

    @staticmethod
    def _extract_status_badges(payload: Dict) -> List[Dict]:
        if not isinstance(payload, dict):
            return []
        badges: List[Dict] = []

        def _collect(container: Dict) -> None:
            for key in ("badges", "badgeList", "badge_list"):
                value = container.get(key)
                if isinstance(value, list):
                    badges.extend([item for item in value if isinstance(item, dict)])

        _collect(payload)
        for key in ("content", "data", "result"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                _collect(nested)
        return badges

    @classmethod
    def _extract_profile_status_from_payload(cls, payload: Dict) -> Optional[str]:
        badges = cls._extract_status_badges(payload)
        if not badges:
            return None
        status_candidates = []
        for badge in badges:
            badge_type = badge.get("type") or badge.get("badgeType")
            badge_type = cls._normalize_status_text(badge_type)
            if badge_type != "STATUS":
                continue
            code = badge.get("code") or badge.get("status") or badge.get("badgeCode")
            normalized = cls._normalize_status_text(code)
            if normalized:
                status_candidates.append(normalized)
        if not status_candidates:
            return None
        if "COMPLETED" in status_candidates:
            return "COMPLETED"
        if "SEASON_COMPLETED" in status_candidates:
            return "SEASON_COMPLETED"
        if "PAUSE" in status_candidates:
            return "PAUSE"
        return status_candidates[0]

    @staticmethod
    def _is_profile_status_expired(
        checked_at: Optional[datetime],
        now_kst: datetime,
        ttl_days: int,
    ) -> bool:
        if checked_at is None:
            return True
        return checked_at + timedelta(days=ttl_days) < now_kst

    @classmethod
    def _needs_profile_lookup(
        cls,
        content_id: str,
        db_info: Optional[Dict],
        now_kst: datetime,
        ttl_days: int,
    ) -> bool:
        if db_info is None:
            return True
        db_status = db_info.get("status")
        if db_status != "완결":
            return True
        profile_status = db_info.get("kakao_profile_status")
        if not profile_status:
            return True
        checked_at = db_info.get("kakao_profile_status_checked_at")
        return cls._is_profile_status_expired(checked_at, now_kst, ttl_days)

    @classmethod
    def _profile_lookup_priority(
        cls,
        content_id: str,
        db_info: Optional[Dict],
        now_kst: datetime,
        ttl_days: int,
    ) -> int:
        if db_info is None:
            return 0
        db_status = db_info.get("status")
        if db_status in {"연재중", "휴재"}:
            return 1
        profile_status = db_info.get("kakao_profile_status")
        checked_at = db_info.get("kakao_profile_status_checked_at")
        if not profile_status or checked_at is None:
            return 2
        if cls._is_profile_status_expired(checked_at, now_kst, ttl_days):
            return 3
        return 4

    def _load_completed_candidate_db_info(self, content_ids: List[str]) -> Dict[str, Dict]:
        if not content_ids:
            return {}
        conn = None
        cursor = None
        info: Dict[str, Dict] = {}
        try:
            conn = create_standalone_connection()
            cursor = get_cursor(conn)
            cursor.execute(
                "SELECT content_id, status, meta FROM contents WHERE source = %s AND content_id = ANY(%s)",
                (self.source_name, content_ids),
            )
            for row in cursor.fetchall():
                meta = row.get("meta") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except json.JSONDecodeError:
                        meta = {}
                attributes = meta.get("attributes") if isinstance(meta, dict) else {}
                if not isinstance(attributes, dict):
                    attributes = {}
                profile_status = attributes.get("kakao_profile_status")
                profile_status = self._normalize_status_text(profile_status)
                checked_at_raw = attributes.get("kakao_profile_status_checked_at")
                checked_at = parse_iso_naive_kst(checked_at_raw) if checked_at_raw else None
                info[str(row["content_id"])] = {
                    "status": row.get("status"),
                    "kakao_profile_status": profile_status,
                    "kakao_profile_status_checked_at": checked_at,
                }
        except Exception:
            return {}
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        return info

    def _select_profile_lookup_targets(
        self,
        completed_candidate_ids: List[str],
        db_info_by_id: Dict[str, Dict],
        now_kst: datetime,
    ) -> List[str]:
        ttl_days = config.KAKAOWEBTOON_PROFILE_STATUS_TTL_DAYS
        candidates = []
        for content_id in completed_candidate_ids:
            db_info = db_info_by_id.get(content_id)
            if not self._needs_profile_lookup(content_id, db_info, now_kst, ttl_days):
                continue
            priority = self._profile_lookup_priority(content_id, db_info, now_kst, ttl_days)
            checked_at = None
            if db_info:
                checked_at = db_info.get("kakao_profile_status_checked_at")
            candidates.append((priority, checked_at or datetime.min, content_id))
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return [content_id for _, _, content_id in candidates]

    async def _fetch_profile_status(
        self,
        session: aiohttp.ClientSession,
        content_id: str,
        headers: Dict[str, str],
    ) -> Tuple[Optional[str], Optional[str], bool]:
        url = f"{self.PROFILE_BASE_URL}/{content_id}"
        try:
            async with session.get(url, headers=headers) as response:
                text = await response.text()
                if response.status >= 400:
                    return None, f"http_{response.status}", False
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    return None, "json_error", False
        except Exception as exc:
            return None, str(exc), False
        status = self._extract_profile_status_from_payload(payload)
        return status, None, True

    async def _fetch_profile_statuses(
        self,
        session: aiohttp.ClientSession,
        content_ids: List[str],
        headers: Dict[str, str],
    ) -> List[Tuple[str, Optional[str], Optional[str], bool]]:
        semaphore = asyncio.Semaphore(config.KAKAOWEBTOON_PROFILE_CONCURRENCY)

        async def _fetch_one(content_id: str):
            async with semaphore:
                status, error, ok = await self._fetch_profile_status(session, content_id, headers)
                return content_id, status, error, ok

        tasks = [_fetch_one(content_id) for content_id in content_ids]
        return await asyncio.gather(*tasks)

    def _parse_timetable_payload(self, payload: Dict) -> List[Dict]:
        entries: List[Dict] = []
        if not isinstance(payload, dict):
            return entries
        data = payload.get("data")
        if not isinstance(data, list):
            return entries
        for day in data:
            card_groups = day.get("cardGroups") if isinstance(day, dict) else None
            if not isinstance(card_groups, list):
                continue
            for group in card_groups:
                cards = group.get("cards") if isinstance(group, dict) else None
                if not isinstance(cards, list):
                    continue
                for card in cards:
                    content = card.get("content") if isinstance(card, dict) else None
                    if not isinstance(content, dict):
                        continue
                    entry = self._build_entry(content)
                    if entry:
                        entry["kakao_ongoing_status"] = self._extract_ongoing_status(card, content)
                        entries.append(entry)
        return entries

    def _merge_weekday_entries(
        self,
        ongoing_map: Dict[str, Dict],
        entries: List[Dict],
        weekday: str,
    ) -> None:
        for entry in entries:
            content_id = entry["content_id"]
            if content_id not in ongoing_map:
                ongoing_map[content_id] = {
                    **entry,
                    "weekdays": set(),
                }
            else:
                existing_weekdays = ongoing_map[content_id].get("weekdays")
                for key, value in entry.items():
                    if key not in ongoing_map[content_id]:
                        ongoing_map[content_id][key] = value
                if existing_weekdays is not None:
                    ongoing_map[content_id]["weekdays"] = existing_weekdays
                else:
                    ongoing_map[content_id]["weekdays"] = set()
            ongoing_map[content_id]["weekdays"].add(weekday)

    async def _fetch_placement_entries(
        self,
        session: aiohttp.ClientSession,
        placement: str,
        headers: Dict[str, str],
    ) -> Tuple[List[Dict], Dict, Optional[str]]:
        meta = {"http_status": None, "count": 0, "stopped_reason": None}
        error = None
        url = config.KAKAOWEBTOON_TIMETABLE_BASE_URL
        params = {"placement": placement}
        if placement == config.KAKAOWEBTOON_PLACEMENT_COMPLETED:
            completed_genre = config.KAKAOWEBTOON_COMPLETED_GENRE
            if completed_genre:
                params["genre"] = completed_genre
        try:
            async with session.get(url, headers=headers, params=params) as response:
                meta["http_status"] = response.status
                text = await response.text()
                if response.status >= 400:
                    meta["stopped_reason"] = "http_error"
                    error = f"http_{response.status}"
                    return [], meta, error
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    meta["stopped_reason"] = "json_error"
                    error = "json_error"
                    return [], meta, error
        except Exception as exc:
            meta["stopped_reason"] = "exception"
            error = str(exc)
            return [], meta, error

        entries = self._parse_timetable_payload(payload)
        meta["count"] = len(entries)
        if not entries:
            meta["stopped_reason"] = meta["stopped_reason"] or "no_data"
        return entries, meta, error

    async def fetch_all_data(self):
        print("카카오 웹툰 타임테이블 데이터를 수집합니다...")

        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        fetch_meta = {
            "ongoing": {},
            "finished": {},
            "errors": [],
            "status_counts": {},
            "placement_status_counts": {},
            "completed_candidate_total": 0,
            "profile_lookup_total": 0,
            "profile_lookup_ok": 0,
            "profile_lookup_failed": 0,
            "profile_status_counts": {},
            "lookup_skipped_due_to_budget": 0,
        }
        combined_map: Dict[str, Dict] = {}
        hiatus_ids = set()
        finished_ids = set()
        completed_candidate_ids = set()
        total_parsed = 0
        pause_found_in_completed = False

        headers = self._build_headers()
        placements: List[Tuple[str, str]] = [
            ("ongoing", placement) for placement in config.KAKAOWEBTOON_PLACEMENTS_WEEKDAYS
        ]
        placements.append(("finished", config.KAKAOWEBTOON_PLACEMENT_COMPLETED))

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [
                self._fetch_placement_entries(session, placement, headers) for _, placement in placements
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for (category, placement), result in zip(placements, results):
            if isinstance(result, Exception):
                fetch_meta["errors"].append(f"{category}:{placement}:{result}")
                fetch_meta[category][placement] = {
                    "http_status": None,
                    "count": 0,
                    "stopped_reason": "exception",
                }
                continue

            entries, placement_meta, error = result
            fetch_meta[category][placement] = placement_meta
            if error:
                fetch_meta["errors"].append(f"{category}:{placement}:{error}")

            placement_status_counts = fetch_meta["placement_status_counts"].setdefault(placement, {})

            if category == "ongoing":
                weekday = PLACEMENT_WEEKDAY_MAP.get(placement)
                if weekday:
                    self._merge_weekday_entries(combined_map, entries, weekday)
                for entry in entries:
                    status = entry.get("kakao_ongoing_status")
                    status_key = status or "UNKNOWN"
                    fetch_meta["status_counts"][status_key] = (
                        fetch_meta["status_counts"].get(status_key, 0) + 1
                    )
                    placement_status_counts[status_key] = placement_status_counts.get(status_key, 0) + 1
                    if self._is_pause_status(status):
                        hiatus_ids.add(entry["content_id"])
                    elif self._is_completed_status(status):
                        finished_ids.add(entry["content_id"])
            else:
                for entry in entries:
                    content_id = entry["content_id"]
                    completed_candidate_ids.add(content_id)
                    status = entry.get("kakao_ongoing_status")
                    status_key = status or "UNKNOWN"
                    fetch_meta["status_counts"][status_key] = (
                        fetch_meta["status_counts"].get(status_key, 0) + 1
                    )
                    placement_status_counts[status_key] = placement_status_counts.get(status_key, 0) + 1
                    existing = combined_map.get(content_id)
                    if existing:
                        existing_weekdays = existing.get("weekdays")
                        for key, value in entry.items():
                            if key not in existing:
                                existing[key] = value
                        if existing_weekdays is not None:
                            existing["weekdays"] = existing_weekdays
                    else:
                        combined_map[content_id] = dict(entry)
                    if self._is_pause_status(status):
                        pause_found_in_completed = True
                    entry["kakao_completed_candidate"] = True

            total_parsed += len(entries)

        for entry in combined_map.values():
            weekdays = entry.get("weekdays")
            if isinstance(weekdays, set):
                entry["weekdays"] = sorted(weekdays)

        completed_candidate_list = sorted(completed_candidate_ids)
        fetch_meta["completed_candidate_total"] = len(completed_candidate_list)
        profile_status_verified_ids = set()
        if completed_candidate_list:
            now_kst = now_kst_naive()
            ttl_days = config.KAKAOWEBTOON_PROFILE_STATUS_TTL_DAYS
            db_info_by_id = self._load_completed_candidate_db_info(completed_candidate_list)

            for content_id in completed_candidate_list:
                db_info = db_info_by_id.get(content_id)
                if not db_info:
                    continue
                profile_status = db_info.get("kakao_profile_status")
                checked_at = db_info.get("kakao_profile_status_checked_at")
                if not profile_status or self._is_profile_status_expired(checked_at, now_kst, ttl_days):
                    continue
                entry = combined_map.get(content_id)
                if entry:
                    entry["kakao_profile_status"] = profile_status
                    entry["kakao_profile_status_checked_at"] = checked_at.isoformat()
                if profile_status == "COMPLETED":
                    finished_ids.add(content_id)
                    profile_status_verified_ids.add(content_id)
                elif profile_status in {"SEASON_COMPLETED", "PAUSE"}:
                    hiatus_ids.add(content_id)
                    profile_status_verified_ids.add(content_id)

            lookup_candidates = self._select_profile_lookup_targets(
                completed_candidate_list,
                db_info_by_id,
                now_kst,
            )
            budget = config.KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET
            if budget <= 0:
                lookup_targets = []
            else:
                lookup_targets = lookup_candidates[:budget]
            fetch_meta["profile_lookup_total"] = len(lookup_targets)
            fetch_meta["lookup_skipped_due_to_budget"] = max(
                0, len(lookup_candidates) - len(lookup_targets)
            )

            if lookup_targets:
                profile_timeout = aiohttp.ClientTimeout(
                    total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
                    connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
                    sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
                )
                profile_connector = aiohttp.TCPConnector(
                    limit=config.KAKAOWEBTOON_PROFILE_CONCURRENCY, ttl_dns_cache=300
                )
                checked_at_iso = now_kst.isoformat()
                async with aiohttp.ClientSession(
                    timeout=profile_timeout, connector=profile_connector
                ) as profile_session:
                    results = await self._fetch_profile_statuses(
                        profile_session, lookup_targets, headers
                    )
                for content_id, status, error, ok in results:
                    if error:
                        fetch_meta["profile_lookup_failed"] += 1
                        fetch_meta["errors"].append(f"profile:{content_id}:{error}")
                        fetch_meta["profile_status_counts"]["FETCH_FAILED"] = (
                            fetch_meta["profile_status_counts"].get("FETCH_FAILED", 0) + 1
                        )
                        continue
                    fetch_meta["profile_lookup_ok"] += 1
                    status_key = status or "UNKNOWN"
                    fetch_meta["profile_status_counts"][status_key] = (
                        fetch_meta["profile_status_counts"].get(status_key, 0) + 1
                    )
                    if ok:
                        entry = combined_map.get(content_id)
                        if entry:
                            entry["kakao_profile_status"] = status_key
                            entry["kakao_profile_status_checked_at"] = checked_at_iso
                    if status_key == "COMPLETED":
                        finished_ids.add(content_id)
                        profile_status_verified_ids.add(content_id)
                    elif status_key in {"SEASON_COMPLETED", "PAUSE"}:
                        hiatus_ids.add(content_id)
                        profile_status_verified_ids.add(content_id)

            for content_id in completed_candidate_list:
                entry = combined_map.get(content_id)
                if entry:
                    entry["kakao_unverified_completed_candidate"] = (
                        content_id not in profile_status_verified_ids
                    )

        hiatus_map = {
            content_id: combined_map[content_id]
            for content_id in hiatus_ids
            if content_id in combined_map
        }
        finished_map = {
            content_id: combined_map[content_id]
            for content_id in finished_ids
            if content_id in combined_map and content_id not in hiatus_ids
        }
        ongoing_map = {
            content_id: entry
            for content_id, entry in combined_map.items()
            if content_id not in hiatus_ids and content_id not in finished_ids
        }

        all_map = {**ongoing_map, **hiatus_map, **finished_map}
        fetch_meta["fetched_count"] = len(all_map)
        fetch_meta["is_suspicious_empty"] = total_parsed == 0
        if pause_found_in_completed:
            fetch_meta.setdefault("health_notes", []).append("pause_found_in_completed_placement")
        if not finished_map:
            fetch_meta.setdefault("health_notes", []).append("finished_count_zero")

        print(
            "수집 완료: "
            f"ongoing={len(ongoing_map)} hiatus={len(hiatus_map)} "
            f"finished={len(finished_map)} total={len(all_map)}"
        )
        return ongoing_map, hiatus_map, finished_map, all_map, fetch_meta

    def synchronize_database(
        self,
        conn,
        all_content_today,
        ongoing_today,
        hiatus_today,
        finished_today,
    ):
        print("\nDB를 오늘의 최신 상태로 전체 동기화를 시작합니다...")
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id FROM contents WHERE source = %s", (self.source_name,))
        db_existing_ids = {row["content_id"] for row in cursor.fetchall()}
        updates, inserts = [], []

        for content_id, webtoon_data in all_content_today.items():
            status = ""
            if content_id in hiatus_today:
                status = "휴재"
            elif content_id in finished_today:
                status = "완결"
            elif content_id in ongoing_today:
                status = "연재중"
            else:
                continue

            title = webtoon_data.get("title")
            if not title:
                continue

            authors = webtoon_data.get("authors", [])
            normalized_title = normalize_search_text(title)
            normalized_authors = normalize_search_text(" ".join(authors) if authors else "")

            meta_data = {
                "common": {
                    "authors": authors,
                    "thumbnail_url": webtoon_data.get("thumbnail_url"),
                    "content_url": webtoon_data.get("content_url"),
                },
                "attributes": {
                    "weekdays": webtoon_data.get("weekdays", []),
                },
            }
            profile_status = webtoon_data.get("kakao_profile_status")
            profile_checked_at = webtoon_data.get("kakao_profile_status_checked_at")
            if profile_status:
                meta_data["attributes"]["kakao_profile_status"] = profile_status
            if profile_checked_at:
                meta_data["attributes"]["kakao_profile_status_checked_at"] = profile_checked_at
            if "kakao_unverified_completed_candidate" in webtoon_data:
                meta_data["attributes"]["kakao_unverified_completed_candidate"] = bool(
                    webtoon_data.get("kakao_unverified_completed_candidate")
                )
            kakao_assets = webtoon_data.get("kakao_assets")
            if kakao_assets:
                meta_data["common"]["kakao_assets"] = kakao_assets

            if content_id in db_existing_ids:
                record = (
                    "webtoon",
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    json.dumps(meta_data),
                    content_id,
                    self.source_name,
                )
                updates.append(record)
            else:
                record = (
                    content_id,
                    self.source_name,
                    "webtoon",
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    json.dumps(meta_data),
                )
                inserts.append(record)

        if updates:
            cursor.executemany(
                "UPDATE contents SET content_type=%s, title=%s, normalized_title=%s, normalized_authors=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s",
                updates,
            )
            print(f"{len(updates)}개 웹툰 정보 업데이트 완료.")

        if inserts:
            cursor.executemany(
                "INSERT INTO contents (content_id, source, content_type, title, normalized_title, normalized_authors, status, meta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (content_id, source) DO NOTHING",
                inserts,
            )
            print(f"{len(inserts)}개 신규 웹툰 DB 추가 완료.")

        cursor.close()
        print("DB 동기화 완료.")
        return len(inserts)
