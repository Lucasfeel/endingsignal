from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from utils.time import now_kst_naive

from services.ott_content_service import build_canonical_ott_entry

from .canonical_ott_crawler import CanonicalOttCrawler
from .ott_parser_utils import clean_text, parse_flexible_datetime

WAVVE_VIEW_MORE_URL = "https://www.wavve.com/view-more?code=EN100000----GN51"
WAVVE_CATALOG_URL_MARKER = "apis.wavve.com/v1/catalog"
WAVVE_CODE_MARKER = "code=EN100000----GN51"
WAVVE_NOISE_TITLES = {"웨이브 라인업"}
WAVVE_NOISE_KEYWORDS = ("라인업", "챔피언십", "KLPGA")
WAVVE_REQUEST_HEADERS = {
    "Referer": WAVVE_VIEW_MORE_URL,
    "Origin": "https://www.wavve.com",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "User-Agent": "Mozilla/5.0",
}
WAVVE_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
WAVVE_CATALOG_QUERY = {
    "broadcastid": "EN100000",
    "catalogType": "manualband",
    "code": "EN100000----GN51",
    "limit": "40",
    "manualbandId": "100000",
    "orderby": "default",
    "uicode": "EN100000",
    "uiparent": "GN51-EN100000",
    "uirank": "0",
    "uitype": "band_82",
    "apikey": "E5F3E0D30947AA5440556471321BB6D9",
    "device": "pc",
    "partner": "pooq",
    "region": "kor",
    "targetage": "all",
    "pooqzone": "none",
    "guid": "none",
    "drm": "wm",
    "client_version": "7.1.40",
}
_CONTENT_ID_RE = re.compile(r"(?:contentid=)?(?P<id>[A-Z0-9_]{8,}(?:\.\d+)?)", re.IGNORECASE)
_DATE_SPLIT_RE = re.compile(r"^(?P<title>.+?)\s+\d{1,2}\s*월\s*\d{1,2}\s*일")


class WavveOttCrawler(CanonicalOttCrawler):
    DISPLAY_NAME = "Wavve OTT"
    SOURCE_NAME = "wavve"

    def __init__(self):
        super().__init__(self.SOURCE_NAME)

    @staticmethod
    def _extract_content_id(raw_value: Any, fallback_title: str) -> str:
        match = _CONTENT_ID_RE.search(str(raw_value or ""))
        if match:
            return clean_text(match.group("id"))
        return clean_text(fallback_title)

    @staticmethod
    def _normalize_title(raw_title: str) -> str:
        title = clean_text(raw_title)
        title = re.sub(r"^(exclusive|original|firstrun)\s+", "", title, flags=re.IGNORECASE).strip()
        match = _DATE_SPLIT_RE.search(title)
        if match:
            title = clean_text(match.group("title"))
        return title

    @staticmethod
    def _looks_like_noise_title(*values: Any) -> bool:
        merged = " ".join(clean_text(value) for value in values if clean_text(value))
        if not merged:
            return True
        normalized = clean_text(merged)
        if normalized in WAVVE_NOISE_TITLES:
            return True
        return any(keyword in normalized for keyword in WAVVE_NOISE_KEYWORDS)

    @staticmethod
    def _extract_cast(values: Any) -> List[str]:
        if not isinstance(values, str):
            return []
        cast: List[str] = []
        seen = set()
        for raw_value in re.split(r"[,/|·]\s*", values):
            cleaned = clean_text(raw_value)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            cast.append(cleaned)
        return cast

    @staticmethod
    def _extract_raw_items_from_html(html: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html or "", "html.parser")
        rows = []
        seen = set()
        for anchor in soup.select("a.click-area"):
            href = clean_text(anchor.get("href"))
            img = anchor.select_one("img[alt]")
            alt = clean_text(img.get("alt")) if img is not None else ""
            title1_node = anchor.select_one(".title1")
            title2_node = anchor.select_one(".title2")
            title1 = clean_text(title1_node.get_text(" ", strip=True)) if title1_node is not None else ""
            title2 = clean_text(title2_node.get_text(" ", strip=True)) if title2_node is not None else ""
            title = alt or clean_text(" ".join(part for part in [title1, title2] if part))
            if not title or len(title) < 2 or title in seen:
                continue
            seen.add(title)
            rows.append(
                {
                    "title": title,
                    "href": href,
                    "thumbnail_url": clean_text(img.get("src")) if img is not None else "",
                    "schedule_note": title2,
                }
            )
        return rows

    @staticmethod
    def _build_platform_url(raw_value: Any, content_id: str) -> str:
        cleaned = clean_text(raw_value)
        if cleaned.startswith("http://") or cleaned.startswith("https://"):
            return cleaned
        if content_id:
            return f"{WAVVE_VIEW_MORE_URL}#contentid={content_id}"
        return WAVVE_VIEW_MORE_URL

    def _entry_from_raw_item(self, item: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        raw_title = clean_text(item.get("title"))
        schedule_note = clean_text(item.get("schedule_note"))
        title = self._normalize_title(raw_title)
        if not title or self._looks_like_noise_title(title, schedule_note):
            return None
        href = clean_text(item.get("href"))
        content_id = self._extract_content_id(href, title)
        release_start_at = parse_flexible_datetime(schedule_note or raw_title)
        return build_canonical_ott_entry(
            platform_source=self.source_name,
            title=title,
            platform_content_id=content_id,
            platform_url=self._build_platform_url(href, content_id),
            thumbnail_url=clean_text(item.get("thumbnail_url")) or None,
            release_start_at=release_start_at,
            upcoming=bool(release_start_at and release_start_at > now_kst_naive()),
            availability_status="scheduled" if release_start_at and release_start_at > now_kst_naive() else "available",
            raw_schedule_note=schedule_note or raw_title or None,
        )

    def _entries_from_visible_rows(self, rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            raw_title = clean_text(row.get("title") or row.get("imgAlt") or row.get("text"))
            schedule_note = clean_text(row.get("schedule_note"))
            entry = self._entry_from_raw_item(
                {
                    "title": raw_title,
                    "schedule_note": schedule_note,
                    "href": row.get("href"),
                    "thumbnail_url": row.get("thumbnail_url"),
                }
            )
            if entry is None:
                continue
            entries[entry["canonical_content_id"]] = entry
        return entries

    def _entry_from_catalog_row(self, row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        manualband = dict(row.get("manualband") or {})
        series = dict(row.get("series") or {})
        content = dict(row.get("content") or {})
        additional = dict(row.get("additional_information") or {})

        title = self._normalize_title(
            clean_text(manualband.get("title1")) or clean_text(series.get("title"))
        )
        schedule_note = clean_text(manualband.get("title2")) or clean_text(manualband.get("autoplay_release_date"))
        description = clean_text(series.get("synopsis")) or clean_text(manualband.get("autoplay_description"))
        if not title or self._looks_like_noise_title(title, schedule_note, description):
            return None

        platform_content_id = self._extract_content_id(
            series.get("refer_id")
            or content.get("refer_id")
            or additional.get("info_url")
            or additional.get("play_url"),
            title,
        )
        release_start_at = (
            parse_flexible_datetime(schedule_note)
            or parse_flexible_datetime(manualband.get("autoplay_release_date"))
            or parse_flexible_datetime(content.get("original_release_date"))
        )
        upcoming = bool(release_start_at and release_start_at > now_kst_naive())
        alias_values = [
            clean_text(series.get("title")),
            clean_text(manualband.get("title1")),
        ]
        return build_canonical_ott_entry(
            platform_source=self.source_name,
            title=title,
            platform_content_id=platform_content_id,
            platform_url=self._build_platform_url(
                additional.get("info_url") or additional.get("play_url"),
                platform_content_id,
            ),
            thumbnail_url=clean_text(manualband.get("image")) or clean_text(content.get("content_image")) or None,
            cast=self._extract_cast(series.get("actors")),
            release_start_at=release_start_at,
            upcoming=upcoming,
            availability_status="scheduled" if upcoming else "available",
            description=description or None,
            title_alias=[value for value in alias_values if clean_text(value) and clean_text(value) != title],
            representative_year=content.get("original_release_year") or (release_start_at.year if release_start_at else None),
            raw_schedule_note=schedule_note or description or None,
        )

    def _parse_catalog_payloads(self, payloads: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
        entries: Dict[str, Dict[str, Any]] = {}
        for payload in payloads:
            data = payload.get("data") if isinstance(payload, Mapping) else None
            if not isinstance(data, Mapping):
                continue
            for row in data.get("context_list") or []:
                if not isinstance(row, Mapping):
                    continue
                entry = self._entry_from_catalog_row(row)
                if entry is None:
                    continue
                entries[entry["canonical_content_id"]] = entry
        return entries

    @staticmethod
    def _build_catalog_request_url(offset: int) -> str:
        params = {
            **WAVVE_CATALOG_QUERY,
            "offset": str(max(0, int(offset))),
        }
        return f"https://apis.wavve.com/v1/catalog?{urlencode(params)}"

    async def _collect_payloads_from_request_context(
        self,
        context,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []
        response_urls: List[str] = []
        errors: List[str] = []
        offset = 0
        limit = int(WAVVE_CATALOG_QUERY["limit"])

        for _ in range(4):
            url = self._build_catalog_request_url(offset)
            try:
                response = await context.request.get(
                    url,
                    headers=WAVVE_REQUEST_HEADERS,
                    timeout=60_000,
                )
            except Exception as exc:  # pragma: no cover - runtime only
                errors.append(f"REQUEST_CONTEXT_FETCH_FAILED:{type(exc).__name__}:{exc}")
                break

            response_urls.append(clean_text(getattr(response, "url", url)) or url)
            if not response.ok:
                errors.append(f"REQUEST_CONTEXT_HTTP_{response.status}")
                break

            try:
                payload = await response.json()
            except Exception as exc:  # pragma: no cover - runtime only
                errors.append(f"REQUEST_CONTEXT_JSON_PARSE_FAILED:{type(exc).__name__}:{exc}")
                break

            if not isinstance(payload, dict):
                errors.append("REQUEST_CONTEXT_INVALID_PAYLOAD")
                break

            payloads.append(payload)
            data = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
            rows = data.get("context_list") or []
            total_count = int(str(data.get("count") or 0) or 0)
            offset += limit
            if not rows or total_count <= offset:
                break

        return payloads, {
            "errors": errors,
            "response_urls": response_urls,
        }

    async def _collect_payloads_with_playwright(
        self,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, str]]]:
        from playwright.async_api import async_playwright

        payloads: List[Dict[str, Any]] = []
        response_urls: List[str] = []
        errors: List[str] = []

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu"],
            )
            context = await browser.new_context(
                locale="ko-KR",
                ignore_https_errors=True,
                viewport={"width": 1440, "height": 2000},
                user_agent=WAVVE_BROWSER_USER_AGENT,
                timezone_id="Asia/Seoul",
                extra_http_headers={
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
                    "Referer": "https://www.wavve.com/",
                },
            )
            page = await context.new_page()
            visible_rows: List[Dict[str, str]] = []

            async def _handle_response(response) -> None:
                url = clean_text(getattr(response, "url", ""))
                if WAVVE_CATALOG_URL_MARKER not in url or WAVVE_CODE_MARKER not in url:
                    return
                response_urls.append(url)
                try:
                    payload = await response.json()
                except Exception as exc:  # pragma: no cover - runtime only
                    errors.append(f"CATALOG_JSON_PARSE_FAILED:{type(exc).__name__}:{exc}")
                    return
                if isinstance(payload, dict):
                    payloads.append(payload)

            page.on("response", _handle_response)

            try:
                await page.goto(WAVVE_VIEW_MORE_URL, wait_until="domcontentloaded", timeout=60_000)
                for wait_ms in (1_500, 3_500, 6_000):
                    await page.wait_for_timeout(wait_ms)
                    try:
                        visible_rows = await page.locator("main a").evaluate_all(
                            """(anchors) => anchors.map((anchor) => {
                                const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                                const title1 = clean(anchor.querySelector('.title1')?.textContent || '');
                                const title2 = clean(anchor.querySelector('.title2')?.textContent || '');
                                const img = anchor.querySelector('img');
                                return {
                                    href: anchor.getAttribute('href') || '',
                                    text: clean(anchor.textContent || ''),
                                    title: title1,
                                    schedule_note: title2,
                                    imgAlt: clean(img?.getAttribute('alt') || ''),
                                    thumbnail_url: clean(img?.getAttribute('src') || ''),
                                };
                            }).filter((row) => row.title || row.imgAlt || row.text)"""
                        )
                    except Exception as exc:  # pragma: no cover - runtime only
                        errors.append(f"VISIBLE_DOM_CAPTURE_FAILED:{type(exc).__name__}:{exc}")
                        visible_rows = []
                    if payloads or visible_rows:
                        break

                if not payloads and not visible_rows:
                    request_payloads, request_meta = await self._collect_payloads_from_request_context(context)
                    payloads.extend(request_payloads)
                    response_urls.extend(request_meta.get("response_urls") or [])
                    errors.extend(request_meta.get("errors") or [])

                html = await page.content()
                body_text = clean_text(await page.text_content("body") or "")
                raw_items = visible_rows or self._extract_raw_items_from_html(html)
                meta = {
                    "errors": errors,
                    "response_urls": response_urls,
                    "final_url": page.url,
                    "body_excerpt": body_text[:240],
                }
                return payloads, meta, raw_items
            finally:
                await context.close()
                await browser.close()

    async def fetch_all_data(self):
        try:
            from playwright.async_api import async_playwright  # noqa: F401
        except Exception as exc:  # pragma: no cover - runtime only
            return {}, {}, {}, {}, {
                "fetched_count": 0,
                "force_no_ratio": True,
                "errors": [f"PLAYWRIGHT_IMPORT_FAILED:{exc}"],
                "is_suspicious_empty": True,
                "source_page": WAVVE_VIEW_MORE_URL,
            }

        errors: List[str] = []
        fetch_method = "playwright:catalog_api"
        response_urls: List[str] = []
        final_url = ""
        body_excerpt = ""

        try:
            payloads, meta, raw_items = await self._collect_payloads_with_playwright()
            errors.extend(meta.get("errors") or [])
            response_urls = list(meta.get("response_urls") or [])
            final_url = clean_text(meta.get("final_url"))
            body_excerpt = clean_text(meta.get("body_excerpt"))
        except Exception as exc:  # pragma: no cover - runtime only
            errors.append(f"PLAYWRIGHT_FETCH_FAILED:{type(exc).__name__}:{exc}")
            payloads = []
            raw_items = []

        ongoing_today = self._parse_catalog_payloads(payloads)
        if not ongoing_today and raw_items:
            fetch_method = "playwright:dom_fallback"
            for raw_item in raw_items:
                entry = self._entry_from_raw_item(raw_item)
                if entry is None:
                    continue
                ongoing_today[entry["canonical_content_id"]] = entry

        all_content_today = dict(ongoing_today)
        fetch_meta = {
            "fetched_count": len(all_content_today),
            "force_no_ratio": True,
            "errors": errors,
            "source_page": WAVVE_VIEW_MORE_URL,
            "fetch_method": fetch_method,
            "response_count": len(response_urls),
            "response_urls": response_urls[:8],
        }
        if final_url:
            fetch_meta["final_url"] = final_url
        if body_excerpt:
            fetch_meta["body_excerpt"] = body_excerpt
        if not all_content_today:
            fetch_meta["is_suspicious_empty"] = True
            if not response_urls:
                errors.append("CATALOG_RESPONSE_MISSING")
        return ongoing_today, {}, {}, all_content_today, fetch_meta
