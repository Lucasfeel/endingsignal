import json
import re
import time
from typing import Dict, List, Optional, Tuple
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
    """RIDI web novel crawler (category_id=3000 by default)."""

    DISPLAY_NAME = "RIDI Novel"
    RIDI_API_BASE = "https://api.ridibooks.com"
    RIDI_LIST_PATH = "/v2/category/books"
    WRITER_ROLES = {"author", "story_writer", "writer"}

    def __init__(self):
        super().__init__("ridi")

    def _build_headers(self) -> Dict[str, str]:
        return {
            **config.CRAWLER_HEADERS,
            "Accept": "application/json",
        }

    def _build_start_url(self) -> str:
        params = {
            "category_id": str(config.RIDI_CATEGORY_ID),
            "tab": "books",
            "limit": int(config.RIDI_LIMIT),
            "platform": str(config.RIDI_PLATFORM),
            "offset": 0,
            "order_by": str(config.RIDI_ORDER_BY),
        }
        return f"{self.RIDI_API_BASE}{self.RIDI_LIST_PATH}?{urlencode(params)}"

    def _build_completed_url(self) -> str:
        params = {
            "category_id": str(config.RIDI_CATEGORY_ID),
            "tab": "books",
            "limit": int(config.RIDI_LIMIT),
            "platform": str(config.RIDI_PLATFORM),
            "offset": 0,
            "order_by": str(config.RIDI_ORDER_BY),
            "series_completed": 1,
        }
        return f"{self.RIDI_API_BASE}{self.RIDI_LIST_PATH}?{urlencode(params)}"

    @staticmethod
    def _sanitize_title(text: object) -> str:
        if not isinstance(text, str):
            return ""
        return re.sub(r"\s+", " ", text).strip()

    def _extract_authors(self, authors_raw: object) -> List[str]:
        if not isinstance(authors_raw, list):
            return []

        all_names: List[str] = []
        writer_names: List[str] = []
        all_seen = set()
        writer_seen = set()

        for author in authors_raw:
            if not isinstance(author, dict):
                continue
            name = self._sanitize_title(author.get("name"))
            if not name:
                continue

            if name not in all_seen:
                all_seen.add(name)
                all_names.append(name)

            role = self._sanitize_title(author.get("role")).lower()
            if role in self.WRITER_ROLES and name not in writer_seen:
                writer_seen.add(name)
                writer_names.append(name)

        return writer_names or all_names

    @staticmethod
    def _extract_cover_url(raw_value: object) -> Optional[str]:
        if isinstance(raw_value, str):
            trimmed = raw_value.strip()
            return trimmed or None
        if isinstance(raw_value, dict):
            for key in ("xxlarge", "xlarge", "large", "medium", "small", "url"):
                value = raw_value.get(key)
                if isinstance(value, str):
                    trimmed = value.strip()
                    if trimmed:
                        return trimmed
        return None

    def _extract_thumbnail_url(self, book: Dict) -> Optional[str]:
        serial = book.get("serial")
        if isinstance(serial, dict):
            serial_cover = self._extract_cover_url(serial.get("cover"))
            if serial_cover:
                return serial_cover
            header_image = self._extract_cover_url(serial.get("headerImage"))
            if header_image:
                return header_image

        for key in ("cover", "thumbnail", "headerImage", "image"):
            selected = self._extract_cover_url(book.get(key))
            if selected:
                return selected
        return None

    def _parse_item(self, item: Dict) -> Optional[Dict]:
        if not isinstance(item, dict):
            return None

        book = item.get("book")
        if not isinstance(book, dict):
            return None

        serial = book.get("serial")
        serial = serial if isinstance(serial, dict) else {}

        serial_id = str(serial.get("serialId") or "").strip()
        book_id = str(book.get("bookId") or "").strip()
        content_id = serial_id or book_id
        if not content_id:
            return None

        title = self._sanitize_title(serial.get("title")) or self._sanitize_title(book.get("title"))
        if not title:
            return None

        completion_raw = serial.get("completion")
        completion = completion_raw if isinstance(completion_raw, bool) else False
        authors = self._extract_authors(book.get("authors"))
        thumbnail_url = self._extract_thumbnail_url(book)

        entry = {
            "content_id": content_id,
            "serial_id": serial_id or None,
            "book_id": book_id or None,
            "title": title,
            "authors": authors,
            "authors_display": ", ".join(authors),
            "content_url": f"https://ridibooks.com/books/{content_id}",
            "thumbnail_url": thumbnail_url,
            "completion": completion,
        }

        if "adultsOnly" in book:
            entry["adults_only"] = book.get("adultsOnly")

        return entry

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True)
    async def _fetch_json(self, session: aiohttp.ClientSession, url: str) -> Dict:
        async with session.get(url, headers=self._build_headers()) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
            if not isinstance(payload, dict):
                raise ValueError("RIDI API response is not a JSON object")
            return payload

    async def _fetch_paginated(
        self, session: aiohttp.ClientSession, start_url: str
    ) -> Tuple[Dict[str, Dict], Dict]:
        entries_by_id: Dict[str, Dict] = {}
        fetch_meta = {
            "pages_fetched": 0,
            "items_seen": 0,
            "unique_contents": 0,
            "errors": [],
            "stopped_reason": None,
        }

        max_pages = max(1, int(config.RIDI_MAX_PAGES))
        started = time.monotonic()
        url = start_url
        visited_urls = set()

        while url:
            if fetch_meta["pages_fetched"] >= max_pages:
                fetch_meta["stopped_reason"] = "max_pages"
                break

            if (time.monotonic() - started) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                fetch_meta["errors"].append("WALL_TIMEOUT_EXCEEDED")
                fetch_meta["stopped_reason"] = "wall_timeout"
                break

            if url in visited_urls:
                fetch_meta["errors"].append(f"REPEATED_NEXT_PAGE:{url}")
                fetch_meta["stopped_reason"] = "repeated_next_page"
                break
            visited_urls.add(url)

            try:
                payload = await self._fetch_json(session, url)
            except Exception as exc:
                fetch_meta["errors"].append(f"{type(exc).__name__}:{exc}")
                fetch_meta["stopped_reason"] = "exception"
                break

            data = payload.get("data")
            data = data if isinstance(data, dict) else {}
            items = data.get("items")
            if not isinstance(items, list):
                items = []

            fetch_meta["pages_fetched"] += 1
            fetch_meta["items_seen"] += len(items)

            for item in items:
                parsed = self._parse_item(item)
                if not parsed:
                    continue
                entries_by_id[parsed["content_id"]] = parsed

            pagination = data.get("pagination")
            pagination = pagination if isinstance(pagination, dict) else {}
            next_page = pagination.get("nextPage")
            if isinstance(next_page, str) and next_page.strip():
                url = urljoin(self.RIDI_API_BASE, next_page.strip())
            else:
                fetch_meta["stopped_reason"] = fetch_meta["stopped_reason"] or "no_next_page"
                url = None

        if fetch_meta["stopped_reason"] is None:
            fetch_meta["stopped_reason"] = "completed"

        fetch_meta["unique_contents"] = len(entries_by_id)
        fetch_meta["fetched_count"] = len(entries_by_id)
        return entries_by_id, fetch_meta

    async def fetch_all_data(self):
        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)

        start_url = self._build_start_url()
        completed_url = self._build_completed_url()

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            all_content_today, fetch_meta = await self._fetch_paginated(session, start_url)

        ongoing_today: Dict[str, Dict] = {}
        finished_today: Dict[str, Dict] = {}
        hiatus_today: Dict[str, Dict] = {}

        for content_id, entry in all_content_today.items():
            if bool(entry.get("completion")):
                finished_today[content_id] = entry
            else:
                ongoing_today[content_id] = entry

        fetch_meta["completed_listing_url"] = completed_url
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
            cid = str(content_id or "").strip()
            if not cid:
                continue

            if cid in finished_today:
                status = STATUS_FINISHED
            elif cid in ongoing_today:
                status = STATUS_ONGOING
            else:
                continue

            title = self._sanitize_title(entry.get("title"))
            if not title:
                continue

            authors_raw = entry.get("authors")
            authors_raw = authors_raw if isinstance(authors_raw, list) else []
            authors = []
            seen_authors = set()
            for author in authors_raw:
                if not isinstance(author, str):
                    continue
                name = self._sanitize_title(author)
                if not name or name in seen_authors:
                    continue
                seen_authors.add(name)
                authors.append(name)

            authors_display = ", ".join(authors)
            normalized_title = normalize_search_text(title)
            normalized_authors = normalize_search_text(authors_display)

            content_url = entry.get("content_url")
            if not isinstance(content_url, str) or not content_url.strip():
                content_url = f"https://ridibooks.com/books/{cid}"
            else:
                content_url = content_url.strip()

            thumbnail_url = entry.get("thumbnail_url")
            if isinstance(thumbnail_url, str):
                thumbnail_url = thumbnail_url.strip() or None
            else:
                thumbnail_url = None

            meta_common = {
                "authors": authors,
                "content_url": content_url,
            }
            if thumbnail_url:
                meta_common["thumbnail_url"] = thumbnail_url

            ridi_meta = {}
            if entry.get("serial_id"):
                ridi_meta["serial_id"] = str(entry["serial_id"])
            if entry.get("book_id"):
                ridi_meta["book_id"] = str(entry["book_id"])
            if isinstance(entry.get("completion"), bool):
                ridi_meta["completion"] = entry["completion"]
            if "adults_only" in entry:
                ridi_meta["adults_only"] = entry["adults_only"]

            meta_attributes = {"weekdays": ["daily"]}
            if ridi_meta:
                meta_attributes["ridi"] = ridi_meta

            meta_data = {
                "common": meta_common,
                "attributes": meta_attributes,
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
