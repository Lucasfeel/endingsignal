from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import aiohttp

import config
from services.kakaopage_novel_common import (
    fetch_kakao_detail_and_build_record,
    is_kakao_suspicious_author_list,
)
from services.kakaopage_parser import STATUS_COMPLETED, STATUS_ONGOING, parse_kakaopage_listing_items
from services.novel_seed_catalog import (
    KAKAOPAGE_BASE_URL,
    build_kakaopage_content_urls,
    build_webnoveldb_kakao_seeds,
)
from utils.backfill import dedupe_strings, merge_genres
from utils.polite_http import BlockedError

from .base_crawler import ContentCrawler
from .novel_sync import synchronize_novel_contents


class KakaoPageNovelCrawler(ContentCrawler):
    DISPLAY_NAME = "KakaoPage Novel"

    def __init__(self):
        super().__init__("kakao_page")

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
        crawl_roots = attributes.get("crawl_roots") if isinstance(attributes, dict) else []
        return {
            "title": cls._clean_text(row.get("title")),
            "authors": dedupe_strings(authors if isinstance(authors, list) else []),
            "content_url": cls._clean_text(common.get("content_url") if isinstance(common, dict) else ""),
            "genres": dedupe_strings(genres if isinstance(genres, list) else []),
            "crawl_roots": dedupe_strings(crawl_roots if isinstance(crawl_roots, list) else []),
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

    @staticmethod
    def _load_playwright_async_api():
        from playwright.async_api import async_playwright

        return async_playwright

    @staticmethod
    async def _extract_listing_ids_via_dom(page) -> Set[str]:
        hrefs = await page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href'))",
        )
        content_ids = set()
        if not isinstance(hrefs, list):
            return content_ids
        for href in hrefs:
            content_id = str(href or "").strip().split("/content/")[-1].split("?")[0]
            if content_id.isdigit():
                content_ids.add(content_id)
        return content_ids

    def _headers_for(self, *, referer: str) -> Dict[str, str]:
        return {
            **config.CRAWLER_HEADERS,
            "Referer": referer,
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.6,en;q=0.5",
        }

    @staticmethod
    def _new_detail_fetch_limit() -> Optional[int]:
        raw_value = str(os.getenv("KAKAOPAGE_INCREMENTAL_MAX_NEW_DETAILS", "")).strip()
        if not raw_value:
            return None
        try:
            return max(0, int(raw_value))
        except ValueError:
            return None

    @staticmethod
    def _merge_discovered_entry(
        *,
        existing: Optional[Dict[str, Any]],
        incoming: Dict[str, Any],
        seed: Dict[str, Any],
        existing_db: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        existing = existing or {}
        existing_db = existing_db or {}
        incoming_genres = incoming.get("genres") if isinstance(incoming.get("genres"), list) else []
        existing_genres = existing.get("genres") if isinstance(existing.get("genres"), list) else []
        db_genres = existing_db.get("genres") if isinstance(existing_db.get("genres"), list) else []
        incoming_authors = incoming.get("authors") if isinstance(incoming.get("authors"), list) else []
        existing_authors = existing.get("authors") if isinstance(existing.get("authors"), list) else []
        db_authors = existing_db.get("authors") if isinstance(existing_db.get("authors"), list) else []
        status = STATUS_COMPLETED if (
            bool(seed.get("seed_completed"))
            or str(incoming.get("status")) == STATUS_COMPLETED
            or str(existing.get("status")) == STATUS_COMPLETED
            or str(existing_db.get("status")) == STATUS_COMPLETED
        ) else STATUS_ONGOING
        merged = {
            "content_id": str(incoming.get("content_id") or existing.get("content_id") or "").strip(),
            "title": str(
                incoming.get("title") or existing.get("title") or existing_db.get("title") or ""
            ).strip(),
            "authors": dedupe_strings([*incoming_authors, *existing_authors, *db_authors]),
            "status": status,
            "content_url": str(
                incoming.get("content_url") or existing.get("content_url") or existing_db.get("content_url") or ""
            ).strip(),
            "genres": merge_genres(incoming_genres, existing_genres, seed.get("genres"), db_genres),
            "crawl_roots": dedupe_strings(
                [
                    *(existing.get("crawl_roots") or []),
                    *(existing_db.get("crawl_roots") or []),
                    str(seed.get("seed_stat_key") or seed.get("name") or "").strip(),
                ]
            ),
            "seed_completed": bool(existing.get("seed_completed")) or bool(seed.get("seed_completed")),
        }
        if not merged["content_url"] and merged["content_id"]:
            merged["content_url"] = build_kakaopage_content_urls(merged["content_id"])["canonical_url"]
        return merged

    async def discover_listing_entries(
        self,
        *,
        existing_by_id: Dict[str, Dict[str, Any]],
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        async_playwright = self._load_playwright_async_api()
        seeds = build_webnoveldb_kakao_seeds()
        discovered_by_id: Dict[str, Dict[str, Any]] = {}
        discovery_meta: Dict[str, Any] = {"seeds": {}, "health_notes": [], "failed_seed_count": 0}

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"],
            )
            context = await browser.new_context()
            page = await context.new_page()

            async def _route_handler(route):
                resource_type = str(route.request.resource_type or "").strip().lower()
                if resource_type in {"image", "media", "font", "stylesheet"}:
                    await route.abort()
                    return
                await route.continue_()

            try:
                await page.route("**/*", _route_handler)
            except Exception:
                pass

            try:
                for seed in seeds:
                    seed_key = str(seed.get("seed_stat_key") or seed.get("name") or "").strip()
                    budget = (
                        config.KAKAOPAGE_INCREMENTAL_COMPLETED_MAX_SCROLLS
                        if seed.get("seed_completed")
                        else config.KAKAOPAGE_INCREMENTAL_ONGOING_MAX_SCROLLS
                    )
                    seed_meta = {
                        "scrolls": 0,
                        "discovered": 0,
                        "stopped_reason": "budget",
                    }
                    seen_on_seed = set()
                    try:
                        await page.goto(str(seed["url"]), wait_until="domcontentloaded", timeout=60000)
                        await page.wait_for_timeout(250)
                    except Exception as exc:
                        discovery_meta["failed_seed_count"] += 1
                        discovery_meta["health_notes"].append(f"SEED_OPEN_FAILED:{seed_key}:{type(exc).__name__}:{exc}")
                        seed_meta["stopped_reason"] = "open_failed"
                        discovery_meta["seeds"][seed_key] = seed_meta
                        continue

                    for scroll_idx in range(1, budget + 1):
                        if scroll_idx > 1:
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await page.wait_for_timeout(250)

                        html = await page.content()
                        parsed_items = parse_kakaopage_listing_items(
                            html,
                            default_genres=seed.get("genres") or [],
                            seed_completed=bool(seed.get("seed_completed")),
                        )
                        dom_ids = await self._extract_listing_ids_via_dom(page)
                        new_ids_this_scroll = 0

                        for content_id in dom_ids:
                            if not any(item.get("content_id") == content_id for item in parsed_items):
                                parsed_items.append(
                                    {
                                        "content_id": content_id,
                                        "content_url": urljoin("https://page.kakao.com", f"/content/{content_id}"),
                                        "title": "",
                                        "authors": [],
                                        "status": STATUS_COMPLETED if seed.get("seed_completed") else STATUS_ONGOING,
                                        "genres": list(seed.get("genres") or []),
                                    }
                                )

                        for item in parsed_items:
                            content_id = str(item.get("content_id") or "").strip()
                            if not content_id:
                                continue
                            before_exists = content_id in discovered_by_id
                            merged = self._merge_discovered_entry(
                                existing=discovered_by_id.get(content_id),
                                incoming=item,
                                seed=seed,
                                existing_db=existing_by_id.get(content_id),
                            )
                            discovered_by_id[content_id] = merged
                            seen_on_seed.add(content_id)
                            if not before_exists:
                                new_ids_this_scroll += 1

                        seed_meta["scrolls"] = scroll_idx
                        if new_ids_this_scroll == 0:
                            seed_meta["stopped_reason"] = "no_new_ids"
                            break
                    seed_meta["discovered"] = len(seen_on_seed)
                    discovery_meta["seeds"][seed_key] = seed_meta
            finally:
                await context.close()
                await browser.close()

        return discovered_by_id, discovery_meta

    @staticmethod
    def _build_existing_observed_record(
        *,
        content_id: str,
        existing_entry: Dict[str, Any],
        discovered_entry: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        title = str(discovered_entry.get("title") or existing_entry.get("title") or "").strip()
        authors = dedupe_strings([*(discovered_entry.get("authors") or []), *(existing_entry.get("authors") or [])])
        content_url = str(discovered_entry.get("content_url") or existing_entry.get("content_url") or "").strip()
        if not title or not authors or not content_url:
            return None
        return {
            "content_id": content_id,
            "source": "kakao_page",
            "title": title,
            "authors": authors,
            "status": STATUS_COMPLETED
            if str(discovered_entry.get("status")) == STATUS_COMPLETED or str(existing_entry.get("status")) == STATUS_COMPLETED
            else STATUS_ONGOING,
            "content_url": content_url,
            "genres": merge_genres(discovered_entry.get("genres"), existing_entry.get("genres")),
            "crawl_roots": dedupe_strings(
                [*(discovered_entry.get("crawl_roots") or []), *(existing_entry.get("crawl_roots") or [])]
            ),
        }

    async def fetch_all_data(self):
        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300)
        existing_by_id = dict(self.get_prefetch_context().get("existing_by_id") or {})

        fetch_meta: Dict[str, Any] = {
            "force_no_ratio": True,
            "health_notes": [],
            "errors": [],
        }

        try:
            discovered_by_id, discovery_meta = await self.discover_listing_entries(existing_by_id=existing_by_id)
        except Exception as exc:
            fetch_meta["errors"] = [f"DISCOVERY_FAILED:{type(exc).__name__}:{exc}"]
            fetch_meta["is_suspicious_empty"] = True
            fetch_meta["skip_database_sync"] = True
            fetch_meta["status"] = "error"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "discovery_failed",
                "message": str(exc),
            }
            return {}, {}, {}, {}, fetch_meta

        fetch_meta["discovery"] = discovery_meta
        fetch_meta["health_notes"].extend(discovery_meta.get("health_notes") or [])

        if not discovered_by_id:
            fetch_meta["errors"] = ["SUSPICIOUS_EMPTY_RESULT:KAKAOPAGE_INCREMENTAL"]
            fetch_meta["is_suspicious_empty"] = True
            fetch_meta["skip_database_sync"] = True
            fetch_meta["status"] = "error"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "suspicious_empty",
                "message": "kakaopage incremental crawl returned 0 discovered items",
            }
            return {}, {}, {}, {}, fetch_meta

        all_content_today: Dict[str, Dict[str, Any]] = {}
        new_ids: List[str] = []
        for content_id, discovered_entry in discovered_by_id.items():
            existing_entry = existing_by_id.get(content_id)
            if existing_entry:
                observed = self._build_existing_observed_record(
                    content_id=content_id,
                    existing_entry=existing_entry,
                    discovered_entry=discovered_entry,
                )
                if observed is not None:
                    all_content_today[content_id] = observed
                continue
            new_ids.append(content_id)

        detail_fetch_limit = self._new_detail_fetch_limit()
        new_ids_to_fetch = new_ids
        skipped_new_detail_count = 0
        if detail_fetch_limit is not None:
            new_ids_to_fetch = new_ids[:detail_fetch_limit]
            skipped_new_detail_count = max(0, len(new_ids) - len(new_ids_to_fetch))
            fetch_meta["new_detail_limit"] = detail_fetch_limit
        fetch_meta["new_detail_candidate_count"] = len(new_ids)
        fetch_meta["new_detail_fetch_count"] = len(new_ids_to_fetch)

        detail_notes: List[str] = []
        if skipped_new_detail_count > 0:
            detail_notes.append(
                "DETAIL_FETCH_LIMIT_APPLIED:"
                f"kept={len(new_ids_to_fetch)}:"
                f"skipped={skipped_new_detail_count}:"
                f"limit={detail_fetch_limit}"
            )
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for content_id in new_ids_to_fetch:
                discovered_entry = discovered_by_id[content_id]
                try:
                    record = await fetch_kakao_detail_and_build_record(
                        session=session,
                        content_id=content_id,
                        discovered_entry=discovered_entry,
                        headers=self._headers_for(referer=str(discovered_entry.get("content_url") or KAKAOPAGE_BASE_URL)),
                        retries=2,
                        retry_base_delay_seconds=0.5,
                        retry_max_delay_seconds=2.0,
                    )
                except BlockedError as exc:
                    detail_notes.append(f"DETAIL_BLOCKED:{content_id}:{exc.url}")
                    continue
                except Exception as exc:
                    detail_notes.append(f"DETAIL_FAILED:{content_id}:{type(exc).__name__}:{exc}")
                    continue

                if not record:
                    continue
                title = str(record.get("title") or "").strip()
                authors = record.get("authors") if isinstance(record.get("authors"), list) else []
                content_url = str(record.get("content_url") or "").strip()
                if not title or not authors or not content_url or is_kakao_suspicious_author_list(authors):
                    detail_notes.append(f"DETAIL_SKIPPED_INCOMPLETE:{content_id}")
                    continue

                record["crawl_roots"] = dedupe_strings(discovered_entry.get("crawl_roots") or [])
                all_content_today[content_id] = record

        if detail_notes:
            fetch_meta["health_notes"].extend(detail_notes[:50])

        ongoing_today: Dict[str, Dict[str, Any]] = {}
        finished_today: Dict[str, Dict[str, Any]] = {}
        hiatus_today: Dict[str, Dict[str, Any]] = {}
        for content_id, entry in all_content_today.items():
            if str(entry.get("status")) == STATUS_COMPLETED:
                finished_today[content_id] = entry
            else:
                ongoing_today[content_id] = entry

        fetch_meta["fetched_count"] = len(discovered_by_id)
        if not all_content_today:
            fetch_meta["errors"] = ["NO_VALID_OBSERVED_RECORDS:KAKAOPAGE_INCREMENTAL"]
            fetch_meta["is_suspicious_empty"] = True
            fetch_meta["skip_database_sync"] = True
            fetch_meta["status"] = "error"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "no_valid_records",
                "message": "kakaopage incremental crawl discovered items but could not build valid records",
            }
        elif discovery_meta.get("failed_seed_count"):
            fetch_meta["status"] = "warn"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "partial_seed_failures",
                "message": "kakaopage incremental crawl completed with seed failures",
            }
        elif skipped_new_detail_count > 0:
            fetch_meta["status"] = "warn"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "detail_fetch_limited",
                "message": "kakaopage incremental crawl skipped some new detail fetches due to configured limit",
            }
        else:
            fetch_meta["status"] = "ok"
            fetch_meta["summary"] = {
                "crawler": self.source_name,
                "reason": "ok",
                "message": "kakaopage incremental crawl completed",
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
