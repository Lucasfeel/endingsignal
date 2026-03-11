from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import aiohttp

import config
from utils.backfill import STATUS_COMPLETED, STATUS_ONGOING, dedupe_strings, merge_genres
from utils.polite_http import fetch_text_polite
from services.naver_series_parser import parse_naver_series_list
from services.novel_seed_catalog import NAVER_SERIES_SEEDS

from .base_crawler import ContentCrawler
from .novel_sync import synchronize_novel_contents


class NaverSeriesNovelCrawler(ContentCrawler):
    DISPLAY_NAME = "Naver Series Novel"

    def __init__(self):
        super().__init__("naver_series")

    @staticmethod
    def _append_query(url: str, **params: Any) -> str:
        parsed = urlparse(url)
        merged = dict(parse_qsl(parsed.query, keep_blank_values=True))
        for key, value in params.items():
            merged[str(key)] = str(value)
        return urlunparse(parsed._replace(query=urlencode(merged)))

    @staticmethod
    def _clean_text(value: object) -> str:
        if not isinstance(value, str):
            return ""
        return " ".join(value.split()).strip()

    @classmethod
    def _extract_existing_entry(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        meta = row.get("meta")
        common = meta.get("common") if isinstance(meta, dict) else {}
        attributes = meta.get("attributes") if isinstance(meta, dict) else {}
        authors = common.get("authors") if isinstance(common, dict) else []
        genres = attributes.get("genres") if isinstance(attributes, dict) else []
        return {
            "title": cls._clean_text(row.get("title")),
            "authors": dedupe_strings(authors if isinstance(authors, list) else []),
            "content_url": cls._clean_text(common.get("content_url") if isinstance(common, dict) else ""),
            "genres": dedupe_strings(genres if isinstance(genres, list) else []),
            "status": cls._clean_text(row.get("status")),
        }

    def build_prefetch_context(self, conn, cursor, db_status_map, override_map, db_state_before_sync):
        cursor.execute(
            "SELECT content_id, title, status, meta FROM contents WHERE source = %s",
            (self.source_name,),
        )
        existing_by_id = {}
        for row in cursor.fetchall():
            existing_by_id[str(row["content_id"])] = self._extract_existing_entry(row)
        return {"existing_by_id": existing_by_id}

    def build_prefetch_context_from_snapshot(self, snapshot):
        context = super().build_prefetch_context_from_snapshot(snapshot)
        existing_by_id = {}
        for row in (snapshot or {}).get("existing_rows") or []:
            if not isinstance(row, dict):
                continue
            content_id = str(row.get("content_id") or "").strip()
            if not content_id:
                continue
            existing_by_id[content_id] = self._extract_existing_entry(row)
        context["existing_by_id"] = existing_by_id
        return context

    def _headers_for(self, *, referer: str) -> Dict[str, str]:
        return {
            **config.CRAWLER_HEADERS,
            "Referer": referer,
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.6,en;q=0.5",
        }

    @staticmethod
    def _merge_entry(
        *,
        existing: Optional[Dict[str, Any]],
        incoming: Dict[str, Any],
        seed: Dict[str, Any],
    ) -> Dict[str, Any]:
        existing = existing or {}
        incoming_authors = dedupe_strings(incoming.get("authors") if isinstance(incoming.get("authors"), list) else [])
        existing_authors = dedupe_strings(existing.get("authors") if isinstance(existing.get("authors"), list) else [])
        status = STATUS_COMPLETED if (
            bool(seed.get("is_finished_page"))
            or str(incoming.get("status")) == STATUS_COMPLETED
            or str(existing.get("status")) == STATUS_COMPLETED
        ) else STATUS_ONGOING
        return {
            "content_id": str(incoming.get("content_id") or "").strip(),
            "title": str(incoming.get("title") or existing.get("title") or "").strip(),
            "authors": incoming_authors or existing_authors,
            "status": status,
            "content_url": str(incoming.get("content_url") or existing.get("content_url") or "").strip(),
            "genres": merge_genres(incoming.get("genres"), [seed["genre"]], existing.get("genres")),
            "crawl_roots": dedupe_strings([seed["key"], *(existing.get("crawl_roots") or [])]),
            "genre": str(seed.get("genre") or "").strip(),
        }

    async def fetch_all_data(self):
        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)
        existing_by_id = dict(self.get_prefetch_context().get("existing_by_id") or {})

        all_content_today: Dict[str, Dict[str, Any]] = {}
        fetch_meta: Dict[str, Any] = {
            "errors": [],
            "health_notes": [],
            "seeds": {},
            "force_no_ratio": True,
        }

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for seed in NAVER_SERIES_SEEDS:
                seed_key = str(seed["key"])
                max_pages = (
                    config.NAVER_SERIES_INCREMENTAL_COMPLETED_MAX_PAGES
                    if seed.get("is_finished_page")
                    else config.NAVER_SERIES_INCREMENTAL_ONGOING_MAX_PAGES
                )
                seed_meta = {
                    "pages_fetched": 0,
                    "items_seen": 0,
                    "discovered": 0,
                    "stopped_reason": "budget",
                }
                seen_on_seed = set()
                for page in range(1, max_pages + 1):
                    url = self._append_query(str(seed["base_url"]), page=page)
                    try:
                        html = await fetch_text_polite(
                            session,
                            url,
                            headers=self._headers_for(referer=str(seed["base_url"])),
                            retries=2,
                            retry_base_delay_seconds=0.5,
                            retry_max_delay_seconds=2.0,
                        )
                    except Exception as exc:
                        fetch_meta["health_notes"].append(
                            f"SEED_FETCH_FAILED:{seed_key}:page={page}:{type(exc).__name__}:{exc}"
                        )
                        seed_meta["stopped_reason"] = "error"
                        break

                    items = parse_naver_series_list(
                        html,
                        is_finished_page=bool(seed.get("is_finished_page")),
                        default_genres=[str(seed["genre"])],
                    )
                    seed_meta["pages_fetched"] += 1
                    if not items:
                        seed_meta["stopped_reason"] = "empty_page"
                        break

                    seed_meta["items_seen"] += len(items)
                    for item in items:
                        content_id = str(item.get("content_id") or "").strip()
                        if not content_id:
                            continue
                        all_content_today[content_id] = self._merge_entry(
                            existing=all_content_today.get(content_id) or existing_by_id.get(content_id),
                            incoming=item,
                            seed=seed,
                        )
                        seen_on_seed.add(content_id)

                if seed_meta["stopped_reason"] == "budget":
                    fetch_meta["health_notes"].append(f"MAX_PAGES_REACHED:{seed_key}:{max_pages}")
                seed_meta["discovered"] = len(seen_on_seed)
                fetch_meta["seeds"][seed_key] = seed_meta

        ongoing_today: Dict[str, Dict[str, Any]] = {}
        finished_today: Dict[str, Dict[str, Any]] = {}
        hiatus_today: Dict[str, Dict[str, Any]] = {}
        for content_id, entry in all_content_today.items():
            if str(entry.get("status")) == STATUS_COMPLETED:
                finished_today[content_id] = entry
            else:
                ongoing_today[content_id] = entry

        fetched_count = len(all_content_today)
        fetch_meta["fetched_count"] = fetched_count
        if fetched_count == 0:
            fetch_meta["errors"] = ["SUSPICIOUS_EMPTY_RESULT:NAVER_SERIES_INCREMENTAL"]
            fetch_meta["is_suspicious_empty"] = True
            fetch_meta["skip_database_sync"] = True
            fetch_meta["status"] = "error"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "suspicious_empty",
                "message": "naver series incremental crawl returned 0 items",
            }
        elif any(seed_meta.get("stopped_reason") == "error" for seed_meta in fetch_meta["seeds"].values()):
            fetch_meta["status"] = "warn"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "partial_seed_failures",
                "message": "naver series incremental crawl completed with seed errors",
            }
        else:
            fetch_meta["status"] = "ok"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "ok",
                "message": "naver series incremental crawl completed",
            }

        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        return synchronize_novel_contents(
            conn,
            source_name=self.source_name,
            all_content_today=all_content_today,
            ongoing_today=ongoing_today,
            finished_today=finished_today,
            existing_snapshot=(self.get_prefetch_context() or {}).get("sync_snapshot"),
        )
