import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

import config
from database import get_cursor
from utils.text import normalize_search_text
from .base_crawler import ContentCrawler


STATUS_FINISHED = "\uc644\uacb0"
STATUS_ONGOING = "\uc5f0\uc7ac\uc911"


class LaftelOttCrawler(ContentCrawler):
    DISPLAY_NAME = "Laftel OTT"
    SOURCE_NAME = "laftel"
    CONTENT_TYPE = "ott"
    LAFTEL_WEB_BASE = "https://laftel.net"
    DEFAULT_DISCOVER_ONGOING_URL = (
        "https://api.laftel.net/api/search/v1/discover/"
        "?sort=rank&ending=false&viewable=true&offset=0&size=60"
    )
    DEFAULT_DISCOVER_FINISHED_URL = (
        "https://api.laftel.net/api/search/v1/discover/"
        "?sort=rank&ending=true&viewable=true&offset=0&size=60"
    )
    DEFAULT_HEADER_VALUE = "TeJava"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)
        self.discover_ongoing_url = self._resolve_url_env(
            "LAFTEL_DISCOVER_ONGOING_URL",
            self.DEFAULT_DISCOVER_ONGOING_URL,
        )
        self.discover_finished_url = self._resolve_url_env(
            "LAFTEL_DISCOVER_FINISHED_URL",
            self.DEFAULT_DISCOVER_FINISHED_URL,
        )
        self.laftel_header_value = (
            os.getenv("LAFTEL_HEADER_VALUE", self.DEFAULT_HEADER_VALUE).strip()
            or self.DEFAULT_HEADER_VALUE
        )
        self.max_pages = self._read_positive_int_env("LAFTEL_MAX_PAGES", 50) or 50
        self.max_items = self._read_positive_int_env("LAFTEL_MAX_ITEMS", 0)
        self.include_adult = self._read_bool_env("LAFTEL_INCLUDE_ADULT", False)

    @staticmethod
    def _resolve_url_env(name: str, default_url: str) -> str:
        configured = os.getenv(name)
        if configured is None:
            return default_url
        normalized = str(configured).strip()
        return normalized or default_url

    @staticmethod
    def _read_bool_env(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        normalized = str(raw).strip().lower()
        if normalized in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _read_positive_int_env(name: str, default: int) -> Optional[int]:
        raw = os.getenv(name)
        if raw is None:
            return default if default > 0 else None
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            return default if default > 0 else None
        return value if value > 0 else None

    @staticmethod
    def _sanitize_text(value: object) -> str:
        if not isinstance(value, str):
            return ""
        return value.strip()

    @staticmethod
    def _dedupe_strings(values: List[str]) -> List[str]:
        deduped = []
        seen = set()
        for raw in values:
            text = LaftelOttCrawler._sanitize_text(raw)
            if not text:
                continue
            lowered = text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(text)
        return deduped

    @staticmethod
    def _coerce_bool(value: object) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "f", "no", "n", "off"}:
                return False
        return None

    @staticmethod
    def _coerce_int(value: object) -> Optional[int]:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                return int(stripped)
            except ValueError:
                return None
        return None

    @classmethod
    def _canonical_content_url(cls, content_id: str) -> str:
        return f"{cls.LAFTEL_WEB_BASE}/item/{content_id}"

    @staticmethod
    def _extract_authors(item: Dict[str, Any]) -> List[str]:
        names = []

        def _collect(value: object):
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    names.append(stripped)
                return
            if isinstance(value, dict):
                candidate = value.get("name")
                if isinstance(candidate, str):
                    stripped = candidate.strip()
                    if stripped:
                        names.append(stripped)
                return
            if isinstance(value, list):
                for nested in value:
                    _collect(nested)

        _collect(item.get("author"))
        _collect(item.get("authors"))
        _collect(item.get("illustrator"))
        _collect(item.get("illustrators"))
        return LaftelOttCrawler._dedupe_strings(names)

    @staticmethod
    def _extract_thumbnail_url(raw: object) -> Optional[str]:
        if isinstance(raw, str):
            stripped = raw.strip()
            return stripped or None

        if isinstance(raw, dict):
            for key in (
                "large",
                "medium",
                "small",
                "thumbnail",
                "portrait",
                "landscape",
                "url",
                "src",
            ):
                candidate = raw.get(key)
                if isinstance(candidate, str):
                    stripped = candidate.strip()
                    if stripped:
                        return stripped
        return None

    @staticmethod
    def _extract_source_tags(raw: object) -> List[str]:
        tags = []
        if isinstance(raw, str):
            stripped = raw.strip()
            if stripped:
                tags.append(stripped)
        elif isinstance(raw, dict):
            name = raw.get("name")
            if isinstance(name, str):
                stripped = name.strip()
                if stripped:
                    tags.append(stripped)
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    stripped = item.strip()
                    if stripped:
                        tags.append(stripped)
                elif isinstance(item, dict):
                    name = item.get("name")
                    if isinstance(name, str):
                        stripped = name.strip()
                        if stripped:
                            tags.append(stripped)
        return LaftelOttCrawler._dedupe_strings(tags)

    @classmethod
    def _normalize_genres(cls, *genre_inputs: object) -> List[str]:
        candidates = ["anime"]
        for raw in genre_inputs:
            if isinstance(raw, str):
                stripped = raw.strip()
                if stripped:
                    candidates.append(stripped)
            elif isinstance(raw, list):
                for item in raw:
                    if isinstance(item, str):
                        stripped = item.strip()
                        if stripped:
                            candidates.append(stripped)
        return cls._dedupe_strings(candidates)

    @staticmethod
    def _normalize_content_url(raw_url: object, content_id: str) -> str:
        if isinstance(raw_url, str):
            candidate = raw_url.strip()
            if candidate:
                parsed = urlparse(candidate)
                if parsed.scheme and parsed.netloc:
                    return candidate
        return LaftelOttCrawler._canonical_content_url(content_id)

    def _parse_discover_item(self, item: Dict[str, Any], status: str) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        raw_content_id = item.get("id")
        content_id = str(raw_content_id).strip() if raw_content_id not in (None, "") else ""
        if not content_id:
            return None

        title = self._sanitize_text(item.get("name")) or self._sanitize_text(item.get("title"))
        if not title:
            return None

        authors = self._extract_authors(item)
        thumbnail_url = self._extract_thumbnail_url(item.get("img"))
        content_url = self._normalize_content_url(item.get("url"), content_id)
        source_tags = self._extract_source_tags(item.get("main_tag"))

        attributes: Dict[str, Any] = {
            "source": self.SOURCE_NAME,
            "genre": "anime",
            "genres": self._normalize_genres(item.get("genre"), item.get("genres"), source_tags),
            "source_tags": source_tags,
        }
        for key in ("is_adult", "is_ending", "viewable", "avg_rating", "content_rating"):
            if key in item and item.get(key) is not None:
                attributes[key] = item.get(key)

        return {
            "content_id": content_id,
            "title": title,
            "authors": authors,
            "thumbnail_url": thumbnail_url,
            "content_url": content_url,
            "source_tags": source_tags,
            "attributes": attributes,
            "status": status,
        }

    def _should_include_item(self, item: Dict[str, Any], parsed_item: Dict[str, Any]) -> bool:
        if not isinstance(parsed_item, dict):
            return False

        attributes = parsed_item.get("attributes")
        attributes = attributes if isinstance(attributes, dict) else {}

        viewable = self._coerce_bool(attributes.get("viewable"))
        if viewable is None:
            viewable = self._coerce_bool(item.get("viewable"))
        if viewable is False:
            return False

        item_type = item.get("type")
        if isinstance(item_type, str):
            normalized_type = item_type.strip().lower()
            if normalized_type and normalized_type != "animation":
                return False

        if not self.include_adult:
            is_adult = self._coerce_bool(attributes.get("is_adult"))
            if is_adult is None:
                is_adult = self._coerce_bool(item.get("is_adult"))
            if is_adult is True:
                return False

        return True

    def _build_headers(self) -> Dict[str, str]:
        return {
            **config.CRAWLER_HEADERS,
            "Accept": "application/json",
            "laftel": self.laftel_header_value,
        }

    @staticmethod
    def _resolve_next_url(current_url: str, next_url: str) -> str:
        return urljoin(current_url, next_url)

    @staticmethod
    def _build_offset_url(current_url: str, offset: int, size: int) -> str:
        parsed = urlparse(current_url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        query["offset"] = [str(offset)]
        query["size"] = [str(size)]
        encoded = urlencode(query, doseq=True)
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                encoded,
                parsed.fragment,
            )
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    async def _fetch_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> Dict[str, Any]:
        async with session.get(url, headers=self._build_headers()) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
            if not isinstance(payload, dict):
                raise ValueError("Laftel response is not a JSON object")
            return payload

    async def _fetch_discover_listing(
        self,
        session: aiohttp.ClientSession,
        *,
        label: str,
        status: str,
        initial_url: str,
        start_time: float,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        meta: Dict[str, Any] = {
            "label": label,
            "pages_fetched": 0,
            "items_seen": 0,
            "items_kept": 0,
            "items_skipped": 0,
            "errors": [],
            "stopped_reason": None,
        }

        next_url = initial_url
        visited = set()

        while next_url:
            current_url = next_url

            if self.max_pages and meta["pages_fetched"] >= self.max_pages:
                meta["stopped_reason"] = "max_pages"
                break

            if self.max_items and len(entries) >= self.max_items:
                meta["stopped_reason"] = "max_items"
                break

            if (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                meta["errors"].append("WALL_TIMEOUT_EXCEEDED")
                meta["stopped_reason"] = "wall_timeout"
                break

            if next_url in visited:
                meta["errors"].append(f"REPEATED_NEXT_URL:{next_url}")
                meta["stopped_reason"] = "repeated_next_url"
                break
            visited.add(current_url)

            try:
                payload = await self._fetch_json(session, current_url)
            except Exception as exc:  # pragma: no cover - retry wrapper path
                meta["errors"].append(f"{type(exc).__name__}:{exc}")
                meta["stopped_reason"] = "exception"
                break

            meta["pages_fetched"] += 1
            results = payload.get("results")
            if not isinstance(results, list):
                results = []
            meta["items_seen"] += len(results)

            if not results:
                meta["stopped_reason"] = "empty_results"
                break

            for raw_item in results:
                parsed = self._parse_discover_item(raw_item, status=status)
                if not parsed or not self._should_include_item(raw_item, parsed):
                    meta["items_skipped"] += 1
                    continue
                entries[parsed["content_id"]] = parsed
                meta["items_kept"] = len(entries)
                if self.max_items and len(entries) >= self.max_items:
                    meta["stopped_reason"] = "max_items"
                    break

            if meta["stopped_reason"] == "max_items":
                break

            raw_next = payload.get("next")
            if isinstance(raw_next, str) and raw_next.strip():
                next_url = self._resolve_next_url(current_url, raw_next.strip())
            else:
                next_url = None
                raw_offset = self._coerce_int(payload.get("offset"))
                raw_size = self._coerce_int(payload.get("size"))
                raw_count = self._coerce_int(payload.get("count"))
                if (
                    isinstance(raw_offset, int)
                    and isinstance(raw_size, int)
                    and raw_size > 0
                    and isinstance(raw_count, int)
                    and raw_count >= 0
                ):
                    next_offset = raw_offset + raw_size
                    if next_offset < raw_count:
                        next_url = self._build_offset_url(current_url, next_offset, raw_size)
                        if not meta["stopped_reason"]:
                            meta["stopped_reason"] = "offset_pagination"

            if not next_url:
                if not meta["stopped_reason"]:
                    meta["stopped_reason"] = "no_next"
                break

            await asyncio.sleep(0.05)

        if not meta["stopped_reason"]:
            meta["stopped_reason"] = "completed"
        meta["fetched_count"] = len(entries)
        return entries, meta

    async def fetch_all_data(self):
        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        fetch_meta: Dict[str, Any] = {
            "force_no_ratio": True,
            "errors": [],
            "pages_fetched": {"ongoing": 0, "finished": 0},
        }
        start_time = time.monotonic()

        ongoing_today: Dict[str, Dict[str, Any]] = {}
        finished_today: Dict[str, Dict[str, Any]] = {}
        hiatus_today: Dict[str, Dict[str, Any]] = {}

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [
                self._fetch_discover_listing(
                    session,
                    label="ongoing",
                    status=STATUS_ONGOING,
                    initial_url=self.discover_ongoing_url,
                    start_time=start_time,
                ),
                self._fetch_discover_listing(
                    session,
                    label="finished",
                    status=STATUS_FINISHED,
                    initial_url=self.discover_finished_url,
                    start_time=start_time,
                ),
            ]
            ongoing_result, finished_result = await asyncio.gather(*tasks, return_exceptions=True)

        if isinstance(ongoing_result, Exception):
            fetch_meta["errors"].append(f"ongoing:{type(ongoing_result).__name__}:{ongoing_result}")
            ongoing_meta = {"pages_fetched": 0, "errors": [str(ongoing_result)], "stopped_reason": "exception"}
        else:
            ongoing_today, ongoing_meta = ongoing_result

        if isinstance(finished_result, Exception):
            fetch_meta["errors"].append(f"finished:{type(finished_result).__name__}:{finished_result}")
            finished_meta = {"pages_fetched": 0, "errors": [str(finished_result)], "stopped_reason": "exception"}
        else:
            finished_today, finished_meta = finished_result

        fetch_meta["ongoing"] = ongoing_meta
        fetch_meta["finished"] = finished_meta
        fetch_meta["pages_fetched"]["ongoing"] = int(ongoing_meta.get("pages_fetched") or 0)
        fetch_meta["pages_fetched"]["finished"] = int(finished_meta.get("pages_fetched") or 0)

        for err in ongoing_meta.get("errors", []):
            fetch_meta["errors"].append(f"ongoing:{err}")
        for err in finished_meta.get("errors", []):
            fetch_meta["errors"].append(f"finished:{err}")

        all_content_today = dict(ongoing_today)
        all_content_today.update(finished_today)

        fetch_meta["fetched_count"] = len(all_content_today)
        if len(ongoing_today) == 0 and len(finished_today) == 0:
            fetch_meta["is_suspicious_empty"] = True

        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    def synchronize_database(
        self,
        conn,
        all_content_today,
        ongoing_today,
        hiatus_today,
        finished_today,
    ):
        cursor = get_cursor(conn)
        cursor.execute("SELECT content_id FROM contents WHERE source = %s", (self.source_name,))
        db_existing_ids = {str(row["content_id"]) for row in cursor.fetchall()}

        updates = []
        inserts = []

        for raw_content_id, entry in all_content_today.items():
            content_id = self._sanitize_text(raw_content_id)
            if not content_id:
                continue

            if content_id in finished_today:
                status = STATUS_FINISHED
            elif content_id in ongoing_today:
                status = STATUS_ONGOING
            else:
                continue

            title = self._sanitize_text(entry.get("title"))
            if not title:
                continue

            authors = self._dedupe_strings(
                [self._sanitize_text(author) for author in (entry.get("authors") or []) if isinstance(author, str)]
            )

            normalized_title = normalize_search_text(title)
            normalized_authors = normalize_search_text(" ".join(authors) if authors else "")

            content_url = self._normalize_content_url(entry.get("content_url"), content_id)
            thumbnail_url = self._extract_thumbnail_url(entry.get("thumbnail_url"))

            parsed_attributes = entry.get("attributes")
            parsed_attributes = parsed_attributes if isinstance(parsed_attributes, dict) else {}
            source_tags = self._extract_source_tags(parsed_attributes.get("source_tags"))

            genres = self._normalize_genres(parsed_attributes.get("genres"), parsed_attributes.get("genre"), source_tags)
            attributes: Dict[str, Any] = {
                "source": self.SOURCE_NAME,
                "genre": "anime",
                "genres": genres,
                "source_tags": source_tags,
            }
            for key in ("is_adult", "is_ending", "viewable", "avg_rating", "content_rating"):
                if key in parsed_attributes and parsed_attributes.get(key) is not None:
                    attributes[key] = parsed_attributes.get(key)

            common: Dict[str, Any] = {
                "authors": authors,
                "content_url": content_url,
            }
            if thumbnail_url:
                common["thumbnail_url"] = thumbnail_url

            meta_data = {
                "common": common,
                "attributes": attributes,
            }

            if content_id in db_existing_ids:
                updates.append(
                    (
                        self.CONTENT_TYPE,
                        title,
                        normalized_title,
                        normalized_authors,
                        status,
                        json.dumps(meta_data),
                        content_id,
                        self.source_name,
                    )
                )
            else:
                inserts.append(
                    (
                        content_id,
                        self.source_name,
                        self.CONTENT_TYPE,
                        title,
                        normalized_title,
                        normalized_authors,
                        status,
                        json.dumps(meta_data),
                    )
                )

        if updates:
            cursor.executemany(
                "UPDATE contents SET content_type=%s, title=%s, normalized_title=%s, normalized_authors=%s, status=%s, meta=%s WHERE content_id=%s AND source=%s",
                updates,
            )

        if inserts:
            cursor.executemany(
                "INSERT INTO contents (content_id, source, content_type, title, normalized_title, normalized_authors, status, meta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (content_id, source) DO NOTHING",
                inserts,
            )

        cursor.close()
        return len(inserts)
