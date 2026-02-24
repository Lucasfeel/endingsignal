import copy
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode, urljoin

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

import config
from database import get_cursor
from utils.text import normalize_search_text
from .base_crawler import ContentCrawler


STATUS_FINISHED = "\uc644\uacb0"
STATUS_ONGOING = "\uc5f0\uc7ac\uc911"


class RidiNovelCrawler(ContentCrawler):
    DISPLAY_NAME = "RIDI Novel"
    RIDI_API_BASE = "https://api.ridibooks.com"
    RIDI_WEB_BASE = "https://ridibooks.com"
    RIDI_LIST_PATH = "/v2/category/books"
    LIGHTNOVEL_ROOT = ("lightnovel", 3000)
    WEBNOVEL_ROOTS = (
        ("webnovel_romance", 1650),
        ("webnovel_ropan", 6050),
        ("webnovel_fantasy", 1750),
        ("webnovel_bl", 4150),
    )
    WRITER_ROLE_PRIORITY = ("author", "story_writer", "original_author")
    BUILD_ID_JSON_RE = re.compile(r'"buildId"\s*:\s*"([^"]+)"')
    NEXT_DATA_BUILD_RE = re.compile(r"/_next/data/([^/]+)/")
    NEXT_STATIC_BUILD_MANIFEST_RE = re.compile(r"/_next/static/([^/]+)/_buildManifest\.js")
    NEXT_STATIC_BUILD_RE = re.compile(r"/_next/static/([^/]+)/")
    INVALID_NEXT_STATIC_SEGMENTS = {
        "media",
        "chunks",
        "css",
        "images",
        "runtime",
        "webpack",
    }

    def __init__(self):
        super().__init__("ridi")
        self._next_build_id: Optional[str] = None

    def _build_headers(self) -> Dict[str, str]:
        return {
            **config.CRAWLER_HEADERS,
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
            "Referer": "https://ridibooks.com/",
        }

    def _max_pages_per_category(self) -> Optional[int]:
        raw = os.getenv("RIDI_MAX_PAGES_PER_CATEGORY")
        if raw is not None:
            try:
                parsed = int(raw)
                if parsed <= 0:
                    return None
                return parsed
            except (TypeError, ValueError):
                pass
        configured = getattr(
            config,
            "RIDI_MAX_PAGES_PER_CATEGORY",
            getattr(config, "RIDI_MAX_PAGES", 500),
        )
        try:
            parsed = int(configured)
            if parsed <= 0:
                return None
            return parsed
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sanitize_text(value: object) -> str:
        if not isinstance(value, str):
            return ""
        return re.sub(r"\s+", " ", value).strip()

    @classmethod
    def _merge_unique_strings(cls, first: object, second: object) -> List[str]:
        merged: List[str] = []
        seen = set()
        for candidate in (first, second):
            if not isinstance(candidate, list):
                continue
            for raw in candidate:
                text = cls._sanitize_text(raw)
                if not text:
                    continue
                key = text.lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(text)
        return merged

    def _extract_authors(self, authors_raw: object) -> Tuple[List[str], List[str]]:
        if not isinstance(authors_raw, list):
            return [], []

        all_names: List[str] = []
        all_seen = set()
        role_buckets = {role: [] for role in self.WRITER_ROLE_PRIORITY}
        role_seen = {role: set() for role in self.WRITER_ROLE_PRIORITY}

        for author in authors_raw:
            if not isinstance(author, dict):
                continue
            name = self._sanitize_text(author.get("name"))
            if not name:
                continue

            lowered = name.lower()
            if lowered not in all_seen:
                all_seen.add(lowered)
                all_names.append(name)

            role = self._sanitize_text(author.get("role")).lower()
            if role in role_buckets and lowered not in role_seen[role]:
                role_seen[role].add(lowered)
                role_buckets[role].append(name)

        primary_names: List[str] = []
        primary_seen = set()
        for role in self.WRITER_ROLE_PRIORITY:
            for name in role_buckets[role]:
                lowered = name.lower()
                if lowered in primary_seen:
                    continue
                primary_seen.add(lowered)
                primary_names.append(name)

        if not primary_names:
            primary_names = list(all_names)
        return all_names, primary_names

    @staticmethod
    def _extract_cover_url(raw_value: object) -> Optional[str]:
        if isinstance(raw_value, str):
            stripped = raw_value.strip()
            return stripped or None
        if isinstance(raw_value, dict):
            for key in ("large", "small", "xlarge", "xxlarge", "medium", "url"):
                candidate = raw_value.get(key)
                if isinstance(candidate, str):
                    stripped = candidate.strip()
                    if stripped:
                        return stripped
        return None

    def _extract_thumbnail_url(self, book: Dict[str, Any]) -> Optional[str]:
        cover = self._extract_cover_url(book.get("cover"))
        if cover:
            return cover

        serial = book.get("serial")
        if isinstance(serial, dict):
            serial_cover = self._extract_cover_url(serial.get("cover"))
            if serial_cover:
                return serial_cover
            serial_header = self._extract_cover_url(serial.get("headerImage"))
            if serial_header:
                return serial_header

        for key in ("thumbnail", "headerImage", "image"):
            candidate = self._extract_cover_url(book.get(key))
            if candidate:
                return candidate
        return None

    @classmethod
    def _sanitize_category_obj(cls, raw: object) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        category_id = raw.get("categoryId")
        if category_id in (None, ""):
            category_id = raw.get("id")

        parent_id = raw.get("parentId")
        if parent_id in (None, ""):
            parent_id = raw.get("parent_id")

        name = cls._sanitize_text(raw.get("name"))
        genre = cls._sanitize_text(raw.get("genre"))

        category: Dict[str, Any] = {}
        if category_id not in (None, ""):
            category["categoryId"] = category_id
        if name:
            category["name"] = name
        if genre:
            category["genre"] = genre
        if parent_id not in (None, ""):
            category["parentId"] = parent_id

        return category or None

    def _extract_categories(self, categories_raw: object) -> Tuple[List[str], List[str], List[Dict[str, Any]]]:
        if not isinstance(categories_raw, list):
            return [], [], []

        genres: List[str] = []
        genre_seen = set()
        category_names: List[str] = []
        name_seen = set()
        categories: List[Dict[str, Any]] = []
        category_seen = set()

        for raw in categories_raw:
            category = self._sanitize_category_obj(raw)
            if not category:
                continue

            if "categoryId" in category:
                key = ("id", str(category["categoryId"]).strip())
            else:
                key = (
                    "fallback",
                    category.get("name", ""),
                    category.get("genre", ""),
                    str(category.get("parentId", "")).strip(),
                )
            if key not in category_seen:
                category_seen.add(key)
                categories.append(category)

            genre = category.get("genre")
            if isinstance(genre, str):
                lowered = genre.lower()
                if lowered not in genre_seen:
                    genre_seen.add(lowered)
                    genres.append(genre)

            name = category.get("name")
            if isinstance(name, str):
                lowered = name.lower()
                if lowered not in name_seen:
                    name_seen.add(lowered)
                    category_names.append(name)

        return genres, category_names, categories

    @classmethod
    def _canonical_content_url(cls, content_id: str) -> str:
        return f"{cls.RIDI_WEB_BASE}/books/{content_id}"

    def _parse_item(
        self,
        item: Dict[str, Any],
        *,
        root_key: Optional[str] = None,
        force_completed: bool = False,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None

        book = item.get("book")
        if not isinstance(book, dict):
            if "bookId" in item and "title" in item:
                book = item
            else:
                return None

        serial = book.get("serial")
        serial = serial if isinstance(serial, dict) else None

        serial_id = self._sanitize_text((serial or {}).get("serialId"))
        book_id = self._sanitize_text(book.get("bookId"))
        content_id = serial_id or book_id
        if not content_id:
            return None

        title = self._sanitize_text((serial or {}).get("title")) or self._sanitize_text(book.get("title"))
        if not title:
            return None

        completion_missing = False
        if serial is None:
            completion = True
        else:
            serial_completion = serial.get("completion")
            if isinstance(serial_completion, bool):
                completion = serial_completion
            else:
                completion = False
                completion_missing = True

        if force_completed:
            completion = True

        authors, primary_authors = self._extract_authors(book.get("authors"))
        thumbnail_url = self._extract_thumbnail_url(book)
        genres, category_names, categories = self._extract_categories(book.get("categories"))

        parsed = {
            "content_id": content_id,
            "title": title,
            "authors": authors,
            "primary_authors": primary_authors,
            "content_url": self._canonical_content_url(content_id),
            "thumbnail_url": thumbnail_url,
            "completion": bool(completion),
            "genres": genres,
            "category_names": category_names,
            "categories": categories,
            "crawl_roots": [root_key] if root_key else [],
        }
        if serial_id:
            parsed["serial_id"] = serial_id
        if book_id:
            parsed["book_id"] = book_id
        if completion_missing:
            parsed["completion_missing"] = True
        return parsed

    def _merge_categories(self, first: object, second: object) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen = set()

        for source in (first, second):
            if not isinstance(source, list):
                continue
            for raw in source:
                category = self._sanitize_category_obj(raw)
                if not category:
                    continue

                if "categoryId" in category:
                    key = ("id", str(category["categoryId"]).strip())
                else:
                    key = (
                        "fallback",
                        category.get("name", ""),
                        category.get("genre", ""),
                        str(category.get("parentId", "")).strip(),
                    )
                if key in seen:
                    continue
                seen.add(key)
                merged.append(category)

        return merged

    def _merge_entries(self, existing: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(existing, dict):
            return copy.deepcopy(incoming)

        merged = existing
        merged["completion"] = bool(merged.get("completion")) or bool(incoming.get("completion"))

        for key in ("title", "content_url", "thumbnail_url", "serial_id", "book_id"):
            if (not merged.get(key)) and incoming.get(key):
                merged[key] = incoming[key]

        merged["authors"] = self._merge_unique_strings(merged.get("authors"), incoming.get("authors"))
        merged["primary_authors"] = self._merge_unique_strings(
            merged.get("primary_authors"),
            incoming.get("primary_authors"),
        )
        merged["genres"] = self._merge_unique_strings(merged.get("genres"), incoming.get("genres"))
        merged["category_names"] = self._merge_unique_strings(
            merged.get("category_names"),
            incoming.get("category_names"),
        )
        merged["crawl_roots"] = self._merge_unique_strings(merged.get("crawl_roots"), incoming.get("crawl_roots"))
        merged["categories"] = self._merge_categories(merged.get("categories"), incoming.get("categories"))

        if incoming.get("completion_missing"):
            merged["completion_missing"] = True
        return merged

    @staticmethod
    def _is_next_data_item(candidate: object) -> bool:
        if not isinstance(candidate, dict):
            return False
        book = candidate.get("book")
        if not isinstance(book, dict):
            return False
        if book.get("bookId") not in (None, ""):
            return True
        serial = book.get("serial")
        return isinstance(serial, dict) and serial.get("serialId") not in (None, "")

    def _search_next_data_items(
        self,
        node: object,
        *,
        key_hint: Optional[str] = None,
    ) -> Tuple[Optional[List[Dict[str, Any]]], bool]:
        if isinstance(node, list):
            if node:
                dict_items = [item for item in node if isinstance(item, dict)]
                if dict_items and all(self._is_next_data_item(item) for item in dict_items):
                    return dict_items, True
            elif key_hint in {"items", "books", "list", "bookList"}:
                return [], True

            for child in node:
                found_items, found = self._search_next_data_items(child)
                if found:
                    return found_items, True
            return None, False

        if isinstance(node, dict):
            for prioritized_key in ("items", "books", "list", "bookList"):
                if prioritized_key in node:
                    found_items, found = self._search_next_data_items(
                        node.get(prioritized_key),
                        key_hint=prioritized_key,
                    )
                    if found:
                        return found_items, True
            for key, value in node.items():
                if key in {"items", "books", "list", "bookList"}:
                    continue
                found_items, found = self._search_next_data_items(value, key_hint=key)
                if found:
                    return found_items, True
        return None, False

    def _extract_next_data_items(self, payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], bool]:
        found_items, found = self._search_next_data_items(payload)
        if not found or not isinstance(found_items, list):
            return [], False
        return [item for item in found_items if isinstance(item, dict)], True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    async def _fetch_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        async with session.get(url, headers=self._build_headers(), params=params) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
            if not isinstance(payload, dict):
                raise ValueError("RIDI response is not a JSON object")
            return payload

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    async def _fetch_text(self, session: aiohttp.ClientSession, url: str) -> str:
        async with session.get(url, headers=self._build_headers()) as response:
            response.raise_for_status()
            return await response.text()

    async def _discover_next_build_id(
        self,
        category_id: int,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> str:
        owns_session = session is None
        active_session = session

        if owns_session:
            timeout = aiohttp.ClientTimeout(
                total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
                connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
                sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
            )
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
            active_session = aiohttp.ClientSession(timeout=timeout, connector=connector)

        try:
            url = f"{self.RIDI_WEB_BASE}/category/books/{category_id}"
            html = await self._fetch_text(active_session, url)
        finally:
            if owns_session and active_session:
                await active_session.close()

        build_id = self._extract_build_id_from_html(html)
        if build_id:
            self._next_build_id = build_id
            return build_id
        raise ValueError(f"NEXT_BUILD_ID_NOT_FOUND:{category_id}")

    def _extract_build_id_from_html(self, html: object) -> Optional[str]:
        if not isinstance(html, str) or not html:
            return None

        for pattern in (
            self.BUILD_ID_JSON_RE,
            self.NEXT_DATA_BUILD_RE,
            self.NEXT_STATIC_BUILD_MANIFEST_RE,
        ):
            match = pattern.search(html)
            if not match:
                continue
            build_id = self._sanitize_text(match.group(1))
            if build_id:
                return build_id

        for match in self.NEXT_STATIC_BUILD_RE.finditer(html):
            candidate = self._sanitize_text(match.group(1))
            if not candidate:
                continue
            lowered = candidate.lower()
            if lowered in self.INVALID_NEXT_STATIC_SEGMENTS:
                continue
            if lowered.startswith("chunks"):
                continue
            return candidate
        return None

    async def _get_next_build_id(
        self,
        session: aiohttp.ClientSession,
        category_id: int,
        *,
        force_refresh: bool = False,
    ) -> str:
        if self._next_build_id and not force_refresh:
            return self._next_build_id
        return await self._discover_next_build_id(category_id, session=session)

    def _build_lightnovel_start_url(self, completed_only: bool) -> str:
        return self._build_api_start_url(
            category_id=self.LIGHTNOVEL_ROOT[1],
            completed_only=completed_only,
        )

    def _build_api_start_url(self, category_id: int, completed_only: bool) -> str:
        params = {
            "category_id": int(category_id),
            "tab": "books",
            "limit": int(config.RIDI_LIMIT),
            "platform": str(config.RIDI_PLATFORM),
            "offset": 0,
            "order_by": str(config.RIDI_ORDER_BY),
        }
        if completed_only:
            params["series_completed"] = 1
        return f"{self.RIDI_API_BASE}{self.RIDI_LIST_PATH}?{urlencode(params)}"

    def _build_webnovel_next_data_url(
        self,
        build_id: str,
        category_id: int,
        page: int,
        *,
        completed_only: bool,
    ) -> str:
        params = {"tab": "books", "category": category_id, "page": page}
        if completed_only:
            params["series_completed"] = "y"
        return (
            f"{self.RIDI_WEB_BASE}/_next/data/{build_id}/category/books/{category_id}.json?"
            f"{urlencode(params)}"
        )

    async def _fetch_api_category_listing(
        self,
        session: aiohttp.ClientSession,
        *,
        root_key: str,
        category_id: int,
        completed_only: bool,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        entries_by_id: Dict[str, Dict[str, Any]] = {}
        meta = {
            "strategy": "api",
            "root_key": root_key,
            "category_id": int(category_id),
            "completed_only": bool(completed_only),
            "pages_fetched": 0,
            "items_seen": 0,
            "unique_contents": 0,
            "completion_missing": 0,
            "errors": [],
            "stopped_reason": None,
        }

        url = self._build_api_start_url(category_id=category_id, completed_only=completed_only)
        visited_urls: Set[str] = set()
        max_pages = self._max_pages_per_category()
        while url:
            if max_pages is not None and meta["pages_fetched"] >= max_pages:
                meta["stopped_reason"] = "max_pages"
                break
            if url in visited_urls:
                meta["errors"].append(f"REPEATED_NEXT_PAGE:{url}")
                meta["stopped_reason"] = "repeated_next_page"
                break
            visited_urls.add(url)

            try:
                payload = await self._fetch_json(session, url)
            except Exception as exc:
                meta["errors"].append(f"{type(exc).__name__}:{exc}")
                meta["stopped_reason"] = "exception"
                break

            data = payload.get("data")
            data = data if isinstance(data, dict) else {}
            items = data.get("items")
            if not isinstance(items, list):
                items = []

            meta["pages_fetched"] += 1
            meta["items_seen"] += len(items)

            for item in items:
                parsed = self._parse_item(
                    item,
                    root_key=root_key,
                    force_completed=completed_only,
                )
                if not parsed:
                    continue
                if parsed.get("completion_missing"):
                    meta["completion_missing"] += 1
                cid = parsed["content_id"]
                entries_by_id[cid] = self._merge_entries(entries_by_id.get(cid), parsed)

            pagination = data.get("pagination")
            pagination = pagination if isinstance(pagination, dict) else {}
            next_page = pagination.get("nextPage")
            if isinstance(next_page, str) and next_page.strip():
                url = urljoin(self.RIDI_API_BASE, next_page.strip())
            else:
                meta["stopped_reason"] = meta["stopped_reason"] or "no_next_page"
                url = None

        if meta["stopped_reason"] is None:
            meta["stopped_reason"] = "completed"
        meta["unique_contents"] = len(entries_by_id)
        meta["fetched_count"] = len(entries_by_id)
        return entries_by_id, meta

    async def _fetch_webnovel_page_items(
        self,
        session: aiohttp.ClientSession,
        category_id: int,
        page: int,
        *,
        completed_only: bool,
        allow_refresh_retry: bool = True,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        build_id = await self._get_next_build_id(session, category_id)
        url = self._build_webnovel_next_data_url(
            build_id,
            category_id,
            page,
            completed_only=completed_only,
        )
        try:
            payload = await self._fetch_json(session, url)
        except aiohttp.ClientResponseError as exc:
            if exc.status == 404 and allow_refresh_retry:
                await self._get_next_build_id(session, category_id, force_refresh=True)
                return await self._fetch_webnovel_page_items(
                    session,
                    category_id,
                    page,
                    completed_only=completed_only,
                    allow_refresh_retry=False,
                )
            raise

        items, structure_found = self._extract_next_data_items(payload)
        if not structure_found and allow_refresh_retry:
            await self._get_next_build_id(session, category_id, force_refresh=True)
            return await self._fetch_webnovel_page_items(
                session,
                category_id,
                page,
                completed_only=completed_only,
                allow_refresh_retry=False,
            )
        return items, structure_found

    async def _fetch_webnovel_listing_from_next_data(
        self,
        session: aiohttp.ClientSession,
        *,
        root_key: str,
        category_id: int,
        completed_only: bool,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        entries_by_id: Dict[str, Dict[str, Any]] = {}
        meta = {
            "strategy": "next_data",
            "root_key": root_key,
            "category_id": int(category_id),
            "completed_only": bool(completed_only),
            "pages_fetched": 0,
            "items_seen": 0,
            "unique_contents": 0,
            "completion_missing": 0,
            "errors": [],
            "stopped_reason": None,
        }
        max_pages = self._max_pages_per_category()
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                meta["stopped_reason"] = "max_pages"
                break
            try:
                items, structure_found = await self._fetch_webnovel_page_items(
                    session,
                    category_id,
                    page,
                    completed_only=completed_only,
                )
            except Exception as exc:
                meta["errors"].append(f"{type(exc).__name__}:{exc}")
                meta["stopped_reason"] = "exception"
                break

            meta["pages_fetched"] += 1
            if not structure_found:
                meta["errors"].append(f"INVALID_NEXT_DATA_STRUCTURE:page={page}")
                meta["stopped_reason"] = "invalid_structure"
                break

            meta["items_seen"] += len(items)
            if not items:
                meta["stopped_reason"] = "empty_page"
                break

            for item in items:
                parsed = self._parse_item(item, root_key=root_key, force_completed=completed_only)
                if not parsed:
                    continue
                if parsed.get("completion_missing"):
                    meta["completion_missing"] += 1
                cid = parsed["content_id"]
                entries_by_id[cid] = self._merge_entries(entries_by_id.get(cid), parsed)

            page += 1

        if meta["stopped_reason"] is None:
            meta["stopped_reason"] = "completed"
        meta["unique_contents"] = len(entries_by_id)
        meta["fetched_count"] = len(entries_by_id)
        return entries_by_id, meta

    async def _fetch_webnovel_listing(
        self,
        session: aiohttp.ClientSession,
        *,
        root_key: str,
        category_id: int,
        completed_only: bool,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        api_entries, api_meta = await self._fetch_api_category_listing(
            session,
            root_key=root_key,
            category_id=category_id,
            completed_only=completed_only,
        )
        api_unique_contents = int(api_meta.get("unique_contents") or 0)
        api_errors = list(api_meta.get("errors") or [])
        api_stopped_reason = api_meta.get("stopped_reason")
        should_fallback = (
            api_unique_contents <= 0
            or api_stopped_reason == "exception"
            or bool(api_errors)
        )
        if not should_fallback:
            return api_entries, api_meta

        next_data_entries, next_data_meta = await self._fetch_webnovel_listing_from_next_data(
            session,
            root_key=root_key,
            category_id=category_id,
            completed_only=completed_only,
        )
        next_data_meta["api_errors"] = api_errors
        next_data_meta["api_stopped_reason"] = api_stopped_reason
        next_data_meta["api_unique_contents"] = api_unique_contents
        next_data_meta["fallback_from"] = "api"
        return next_data_entries, next_data_meta

    def _merge_discovered_entries(
        self,
        merged_by_id: Dict[str, Dict[str, Any]],
        discovered: Dict[str, Dict[str, Any]],
    ) -> None:
        for content_id, incoming in discovered.items():
            merged_by_id[content_id] = self._merge_entries(merged_by_id.get(content_id), incoming)

    @staticmethod
    def _build_per_category_meta(
        *,
        category_id: int,
        all_meta: Dict[str, Any],
        completed_meta: Dict[str, Any],
        discovered_unique: int,
    ) -> Dict[str, Any]:
        return {
            "category_id": category_id,
            "all": all_meta,
            "completed": completed_meta,
            "discovered_unique": discovered_unique,
        }

    async def fetch_all_data(self):
        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        all_content_today: Dict[str, Dict[str, Any]] = {}
        fetch_meta: Dict[str, Any] = {
            "errors": [],
            "health_notes": [],
            "health_warnings": [],
            "category_counts": {},
            "totals": {},
        }

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for root_key, category_id in self.WEBNOVEL_ROOTS:
                all_entries, all_meta = await self._fetch_webnovel_listing(
                    session,
                    root_key=root_key,
                    category_id=category_id,
                    completed_only=False,
                )
                completed_entries, completed_meta = await self._fetch_webnovel_listing(
                    session,
                    root_key=root_key,
                    category_id=category_id,
                    completed_only=True,
                )

                self._merge_discovered_entries(all_content_today, all_entries)
                self._merge_discovered_entries(all_content_today, completed_entries)

                discovered_unique = len(set(all_entries.keys()) | set(completed_entries.keys()))
                fetch_meta["category_counts"][root_key] = self._build_per_category_meta(
                    category_id=category_id,
                    all_meta=all_meta,
                    completed_meta=completed_meta,
                    discovered_unique=discovered_unique,
                )

                for label, meta in (("all", all_meta), ("completed", completed_meta)):
                    for error in meta.get("errors", []):
                        fetch_meta["errors"].append(f"{root_key}:{label}:{error}")
                    for api_error in meta.get("api_errors", []):
                        fetch_meta["errors"].append(f"{root_key}:{label}:api:{api_error}")
                    if meta.get("stopped_reason") == "max_pages":
                        fetch_meta["health_warnings"].append(f"MAX_PAGES_REACHED:{root_key}:{label}")
                    if meta.get("strategy") == "next_data" and meta.get("fallback_from") == "api":
                        fetch_meta["health_warnings"].append(f"WEBNOVEL_API_FALLBACK:{root_key}:{label}")
                        fetch_meta["health_notes"].append(
                            f"WEBNOVEL_API_FALLBACK_USED:{root_key}:{label}:"
                            f"api_stopped_reason={meta.get('api_stopped_reason')}"
                        )

            light_root, light_category_id = self.LIGHTNOVEL_ROOT
            light_all_entries, light_all_meta = await self._fetch_lightnovel_listing(
                session,
                completed_only=False,
            )
            light_completed_entries, light_completed_meta = await self._fetch_lightnovel_listing(
                session,
                completed_only=True,
            )

            self._merge_discovered_entries(all_content_today, light_all_entries)
            self._merge_discovered_entries(all_content_today, light_completed_entries)

            light_discovered_unique = len(set(light_all_entries.keys()) | set(light_completed_entries.keys()))
            fetch_meta["category_counts"][light_root] = self._build_per_category_meta(
                category_id=light_category_id,
                all_meta=light_all_meta,
                completed_meta=light_completed_meta,
                discovered_unique=light_discovered_unique,
            )

            for label, meta in (("all", light_all_meta), ("completed", light_completed_meta)):
                for error in meta.get("errors", []):
                    fetch_meta["errors"].append(f"{light_root}:{label}:{error}")
                if meta.get("stopped_reason") == "max_pages":
                    fetch_meta["health_warnings"].append(f"MAX_PAGES_REACHED:{light_root}:{label}")

        if self._next_build_id:
            fetch_meta["health_notes"].append(f"NEXT_BUILD_ID:{self._next_build_id}")

        ongoing_today: Dict[str, Dict[str, Any]] = {}
        finished_today: Dict[str, Dict[str, Any]] = {}
        hiatus_today: Dict[str, Dict[str, Any]] = {}

        for content_id, entry in all_content_today.items():
            if bool(entry.get("completion")):
                finished_today[content_id] = entry
            else:
                ongoing_today[content_id] = entry

        fetch_meta["fetched_count"] = len(all_content_today)
        fetch_meta["totals"] = {
            "total_unique_contents": len(all_content_today),
            "ongoing": len(ongoing_today),
            "finished": len(finished_today),
            "hiatus": 0,
        }
        if len(all_content_today) == 0:
            fetch_meta["is_suspicious_empty"] = True
            fetch_meta["errors"].append("SUSPICIOUS_EMPTY_RESULT:RIDI")
        return ongoing_today, hiatus_today, finished_today, dict(all_content_today), fetch_meta

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

        for content_id, entry in all_content_today.items():
            cid = self._sanitize_text(content_id)
            if not cid:
                continue

            if cid in finished_today:
                status = STATUS_FINISHED
            elif cid in ongoing_today:
                status = STATUS_ONGOING
            else:
                continue

            title = self._sanitize_text(entry.get("title"))
            if not title:
                continue

            authors = []
            seen_authors = set()
            for raw_name in entry.get("authors", []):
                name = self._sanitize_text(raw_name)
                if not name:
                    continue
                lowered = name.lower()
                if lowered in seen_authors:
                    continue
                seen_authors.add(lowered)
                authors.append(name)

            normalized_title = normalize_search_text(title)
            normalized_authors = normalize_search_text(" ".join(authors))

            content_url = entry.get("content_url")
            if not isinstance(content_url, str) or not content_url.strip():
                content_url = self._canonical_content_url(cid)
            else:
                content_url = content_url.strip()

            thumbnail_url = entry.get("thumbnail_url")
            if isinstance(thumbnail_url, str):
                thumbnail_url = thumbnail_url.strip() or None
            else:
                thumbnail_url = None

            attributes = {
                "weekdays": ["daily"],
                "genres": self._merge_unique_strings(entry.get("genres"), []),
                "category_names": self._merge_unique_strings(entry.get("category_names"), []),
                "categories": self._merge_categories(entry.get("categories"), []),
                "crawl_roots": self._merge_unique_strings(entry.get("crawl_roots"), []),
            }
            if isinstance(entry.get("completion"), bool):
                attributes["completion"] = entry["completion"]
            if entry.get("serial_id"):
                attributes["serial_id"] = str(entry["serial_id"])
            if entry.get("book_id"):
                attributes["book_id"] = str(entry["book_id"])
            if entry.get("primary_authors"):
                attributes["primary_authors"] = self._merge_unique_strings(
                    entry.get("primary_authors"),
                    [],
                )

            meta_data = {
                "common": {
                    "authors": authors,
                    "thumbnail_url": thumbnail_url,
                    "content_url": content_url,
                },
                "attributes": attributes,
            }

            if cid in db_existing_ids:
                updates.append(
                    (
                        "novel",
                        title,
                        normalized_title,
                        normalized_authors,
                        status,
                        json.dumps(meta_data),
                        cid,
                        self.source_name,
                    )
                )
            else:
                inserts.append(
                    (
                        cid,
                        self.source_name,
                        "novel",
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

    async def _fetch_lightnovel_listing(
        self,
        session: aiohttp.ClientSession,
        *,
        completed_only: bool,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        return await self._fetch_api_category_listing(
            session,
            root_key=self.LIGHTNOVEL_ROOT[0],
            category_id=self.LIGHTNOVEL_ROOT[1],
            completed_only=completed_only,
        )
