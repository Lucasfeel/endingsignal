# DEPRECATED / ARCHIVED: do not import or execute. Kept only for reference.

import asyncio
import aiohttp
import json
import os
import random
import re
import time
import urllib.parse
import uuid
from http.cookies import SimpleCookie

from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

import config
from .base_crawler import ContentCrawler
from database import get_cursor
from utils.text import normalize_search_text

# --- KakaoWebtoon API Configuration ---
API_BASE_URL = "https://gateway-kw.kakao.com/section/v1/pages"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "Referer": "https://webtoon.kakao.com/",
    "Accept-Language": "ko",
    "Origin": "https://webtoon.kakao.com",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}


class KakaowebtoonCrawler(ContentCrawler):
    """webtoon.kakao.com에서 웹툰 정보를 수집하는 크롤러입니다."""

    # (run_all_crawlers.py에서 인스턴스 생성 전에 스킵 판단용)
    DISPLAY_NAME = "Kakao Webtoon"
    REQUIRED_ENV_VARS = []

    @classmethod
    def get_missing_env_vars(cls):
        return []

    def __init__(self):
        super().__init__("kakaowebtoon")
        self.cookies = self._get_cookies_from_env()
        self.cookie_source = "env" if self.cookies else "none"

    def _get_cookies_from_env(self):
        """환경 변수에서 쿠키 값을 로드합니다. 없으면 None 반환(익명 부트스트랩 시도)."""
        header_cookie = os.getenv("KAKAOWEBTOON_COOKIE_HEADER")
        if header_cookie:
            jar = SimpleCookie()
            jar.load(header_cookie)
            parsed = {k: morsel.value for k, morsel in jar.items() if morsel.value}
            return parsed or None

        webid = os.getenv("KAKAOWEBTOON_WEBID")
        t_ano = os.getenv("KAKAOWEBTOON_T_ANO")

        if webid and t_ano:
            return {"webid": webid, "_T_ANO": t_ano}
        return None

    def _extract_cookie_pair(self, resp, session):
        """응답/세션에서 webid와 _T_ANO를 추출합니다 (둘 다 있어야 반환)."""
        cookies = resp.cookies or {}
        webid = cookies.get("webid")
        t_ano = cookies.get("_T_ANO")

        if not (webid and t_ano):
            filtered = session.cookie_jar.filter_cookies(str(resp.url))
            webid = webid or filtered.get("webid")
            t_ano = t_ano or filtered.get("_T_ANO")

        if webid and t_ano:
            return {
                "webid": getattr(webid, "value", webid),
                "_T_ANO": getattr(t_ano, "value", t_ano),
            }
        return None

    async def _bootstrap_anonymous_cookies(self, session, fetch_meta=None):
        """로그인 없이 발급되는 쿠키를 요청을 통해 받아옵니다(베스트 에포트)."""
        meta = fetch_meta if fetch_meta is not None else {}
        attempts = [
            "https://gateway-kw.kakao.com/section/v1/pages/general-weekdays",
            "https://webtoon.kakao.com/",
        ]

        for attempt_url in attempts:
            try:
                async with session.get(
                    attempt_url,
                    headers=HEADERS,
                    allow_redirects=True,
                    raise_for_status=False,
                ) as resp:
                    await resp.text()
                    pair = self._extract_cookie_pair(resp, session)
                    if pair:
                        session.cookie_jar.update_cookies(pair, response_url=resp.url)
                        self.cookies = self._capture_cookie_dict(session, resp.url)
                        self.cookie_source = "bootstrap"
                        print("부트스트랩된 쿠키 키: ['webid', '_T_ANO']")
                        self._log_cookie_state(session, prefix="kakao.cookies:bootstrap")
                        return
            except Exception as e:
                meta.setdefault("errors", []).append(
                    f"cookies:bootstrap:{attempt_url}:{e}"
                )

        meta.setdefault("errors", []).append("cookies:anonymous_bootstrap_missing")

    async def _ensure_cookies(self, session, fetch_meta=None):
        if self.cookies:
            session.cookie_jar.update_cookies(
                self.cookies, response_url="https://webtoon.kakao.com/"
            )
        else:
            await self._bootstrap_anonymous_cookies(session, fetch_meta=fetch_meta)
            self.cookies = self._capture_cookie_dict(
                session, "https://webtoon.kakao.com/"
            )

        filtered = session.cookie_jar.filter_cookies("https://webtoon.kakao.com/")
        has_webid = "webid" in filtered
        has_t_ano = "_T_ANO" in filtered

        if not (has_webid and has_t_ano):
            synth = {
                "webid": uuid.uuid4().hex,
                "_T_ANO": uuid.uuid4().hex,
            }
            session.cookie_jar.update_cookies(
                synth, response_url="https://webtoon.kakao.com/"
            )
            self.cookies = synth
            self.cookie_source = "synth"
        else:
            self.cookies = self._capture_cookie_dict(
                session, "https://webtoon.kakao.com/"
            )
            if self.cookie_source == "none":
                self.cookie_source = "bootstrap"

        self._log_cookie_state(session)

    def _capture_cookie_dict(self, session, response_url):
        filtered = session.cookie_jar.filter_cookies(str(response_url))
        return {k: v.value for k, v in filtered.items() if getattr(v, "value", None)}

    def _log_cookie_state(self, session, prefix="kakao.cookies"):
        filtered = session.cookie_jar.filter_cookies("https://webtoon.kakao.com/")
        jar_size = len(filtered)
        has_webid = "webid" in filtered
        has_t_ano = "_T_ANO" in filtered
        keys = list(filtered.keys())
        print(
            f"{prefix}: jar_size={jar_size} has_webid={has_webid} has_T_ANO={has_t_ano} "
            f"keys={keys} source={self.cookie_source}"
        )

    def _iter_cards_from_sections(self, sections):
        """gateway-kw 응답의 sections/cardGroups/cards 전체를 안전하게 순회."""
        for section in sections or []:
            for card_group in (section.get("cardGroups", []) or []):
                for card in (card_group.get("cards", []) or []):
                    yield card

    async def _request_json(self, session, url, *, params=None, fetch_meta=None, tag="api"):
        async with session.get(
            url,
            headers=HEADERS,
            params=params,
            allow_redirects=True,
            raise_for_status=False,
            cookies=self.cookies or None,
        ) as response:
            status = response.status
            try:
                body_text = await response.text()
            except Exception:
                body_text = ""
            snippet = body_text[:300]

            parsed_json = None
            content_type = response.headers.get("Content-Type", "")
            if "json" in content_type.lower() or body_text.startswith("{"):
                try:
                    parsed_json = json.loads(body_text)
                except Exception:
                    parsed_json = None

            if 200 <= status < 300:
                if parsed_json is not None:
                    return parsed_json
                try:
                    return await response.json()
                except Exception:
                    return {}

            if fetch_meta is not None:
                parsed = urllib.parse.urlparse(str(response.url))
                path = parsed.path
                code = f"kakao:http_{response.status}:{path}"
                fetch_meta.setdefault("errors", []).append(code)
                fetch_meta.setdefault("errors", []).append(
                    f"{tag}:http_{status}:{snippet}"
                )
            print(
                f"kakao.http_error tag={tag} status={status} url={response.url} body={snippet}"
            )
            raise aiohttp.ClientResponseError(
                response.request_info,
                response.history,
                status=status,
                message=f"{tag} request failed",
                headers=response.headers,
            )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _fetch_from_api(
        self,
        session,
        url,
        params=None,
        *,
        fetch_meta=None,
        tag="api",
        auth_retried=False,
        rate_retried=False,
    ):
        try:
            return await self._request_json(
                session, url, params=params, fetch_meta=fetch_meta, tag=tag
            )
        except aiohttp.ClientResponseError as e:
            status = getattr(e, "status", None)
            if status in {401, 403} and not auth_retried:
                await self._bootstrap_anonymous_cookies(session, fetch_meta=fetch_meta)
                return await self._fetch_from_api(
                    session,
                    url,
                    params=params,
                    fetch_meta=fetch_meta,
                    tag=tag,
                    auth_retried=True,
                    rate_retried=rate_retried,
                )

            if status == 429 and not rate_retried:
                retry_after = e.headers.get("Retry-After") if e.headers else None
                try:
                    delay = min(30, float(retry_after)) if retry_after else 1
                except Exception:
                    delay = 1
                await asyncio.sleep(delay)
                return await self._fetch_from_api(
                    session,
                    url,
                    params=params,
                    fetch_meta=fetch_meta,
                    tag=tag,
                    auth_retried=auth_retried,
                    rate_retried=True,
                )
            raise

    async def _fetch_completed_cards(
        self, session, discovered_slugs, start_time, fetch_meta, seen_ids
    ):
        completion_keywords = ("complete", "completed", "finish", "end")
        fallback_slugs = getattr(config, "KAKAO_DISCOVERY_FALLBACK_SLUGS", [])
        completion_slugs = []

        for slug in sorted(discovered_slugs):
            low = slug.lower()
            if any(key in low for key in completion_keywords):
                completion_slugs.append(slug)

        for slug in fallback_slugs:
            low = str(slug).lower()
            if slug not in completion_slugs and any(
                key in low for key in completion_keywords
            ):
                completion_slugs.append(slug)

        if not completion_slugs:
            completion_slugs = list(discovered_slugs)
            max_general = int(
                getattr(config, "KAKAO_COMPLETED_FALLBACK_LIMIT", 5)
            )
            completion_slugs = completion_slugs[:max_general]

        collected = []
        used_slugs = []
        for slug in completion_slugs:
            if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                fetch_meta.setdefault("errors", []).append("completed:WALL_TIMEOUT_EXCEEDED")
                break
            try:
                cards = await self._fetch_official_section_cards(
                    session,
                    slug,
                    start_time=start_time,
                    fetch_meta=fetch_meta,
                    seen_ids=seen_ids,
                )
                if cards:
                    collected.extend(cards)
                    used_slugs.append(slug)
            except Exception as e:
                fetch_meta.setdefault("errors", []).append(f"completed:{slug}:{e}")
                continue

        if used_slugs:
            fetch_meta["completed_slugs"] = used_slugs
        else:
            fetch_meta.setdefault("errors", []).append("completed:none_collected")

        return collected, set(used_slugs)

    async def _discover_official_slugs(self, session, *, start_time=None, fetch_meta=None):
        """
        webtoon.kakao.com 메인 HTML + JS 번들에서 section/v1/pages/<slug> 패턴을 찾아 slug 목록을 수집합니다.
        (베스트 에포트; 실패해도 크롤러 전체 실패로 이어지지 않도록 errors에만 기록)
        """
        slugs = set()
        meta = fetch_meta if fetch_meta is not None else {}
        bundle_urls = []
        fetched_bundles = 0
        excluded_exact = {"general-weekdays", "completed", "complete"}
        slug_patterns = [
            r"/section/v1/pages/([A-Za-z0-9_-]+)",
            r"\\/section\\/v1\\/pages\\/([A-Za-z0-9_-]+)",
        ]

        max_bundles = int(
            getattr(
                config,
                "KAKAO_DISCOVERY_MAX_BUNDLES",
                getattr(config, "KAKAOWEBTOON_MAX_JS_BUNDLES", 10),
            )
        )
        exclude_regex = getattr(config, "KAKAO_DISCOVERY_EXCLUDE_SLUG_REGEX", "") or None

        try:
            if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
                return slugs

            async with session.get(
                "https://webtoon.kakao.com/",
                headers=HEADERS,
                allow_redirects=True,
            ) as resp:
                html = await resp.text()

            # 신기능 우선: HTML 자체에 노출된 slug도 우선 수집
            for pattern in slug_patterns:
                for slug in re.findall(pattern, html):
                    if not slug:
                        continue
                    if slug in excluded_exact:
                        continue
                    if exclude_regex and re.search(exclude_regex, slug):
                        continue
                    slugs.add(slug)

            script_srcs = re.findall(
                r'src=["\']([^"\']+\.js(?:\?[^"\']*)?)["\']', html
            )
            bundle_urls = []
            for src in script_srcs:
                if not src:
                    continue
                abs_url = urllib.parse.urljoin("https://webtoon.kakao.com/", src)
                bundle_urls.append(abs_url)

            if not bundle_urls:
                bundle_urls = list(
                    {
                        url
                        for url in re.findall(
                            r"https?://[^'\"\s]+\.js(?:\?[^'\"\s]*)?", html
                        )
                    }
                )

            bundle_urls = list(dict.fromkeys(bundle_urls))
            fetched_bundles = 0

            print(
                f"[kakao] discovery scripts_found={len(script_srcs)} bundles_found={len(bundle_urls)}"
            )

            for bundle_url in bundle_urls[:max_bundles]:
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
                    break

                try:
                    async with session.get(bundle_url, headers=HEADERS) as resp:
                        bundle_text = await resp.text()
                    fetched_bundles += 1

                    for pattern in slug_patterns:
                        for slug in re.findall(pattern, bundle_text):
                            if not slug:
                                continue
                            if slug in excluded_exact:
                                continue
                            if exclude_regex and re.search(exclude_regex, slug):
                                continue
                            slugs.add(slug)

                except Exception as e:
                    meta.setdefault("errors", []).append(
                        f"discover:bundle_fetch_failed:{bundle_url}:{e}"
                    )

        except Exception as e:
            meta.setdefault("errors", []).append(f"discover:bootstrap_failed:{e}")

        summary_sample = list(sorted(slugs))[:10]
        print(
            f"[kakao] discovery summary scripts_found={len(script_srcs)} bundles_found={len(bundle_urls)} "
            f"fetched={fetched_bundles} slugs={len(slugs)} sample={summary_sample}"
        )

        return slugs

    async def _fetch_official_section_cards(
        self, session, slug, *, start_time=None, fetch_meta=None, seen_ids=None
    ):
        """
        발견된 slug 페이지를 offset/limit 기반으로 최대한 순회하며 cards 수집.
        (베스트 에포트: slug별 오류는 errors에 기록 후 해당 slug만 중단)
        """
        collected = []
        offset = 0
        limit = 100
        page = 0

        if seen_ids is None:
            seen_ids = set()

        meta = fetch_meta if fetch_meta is not None else {}

        max_pages_per_slug = int(
            getattr(
                config,
                "KAKAO_DISCOVERY_MAX_PAGES_PER_SLUG",
                getattr(config, "KAKAOWEBTOON_MAX_PAGES_PER_SLUG", 400),
            )
        )
        soft_cap = int(
            getattr(
                config,
                "KAKAO_DISCOVERY_SOFT_CAP",
                getattr(config, "KAKAOWEBTOON_TARGET_UNIQUE_TITLES", 20000),
            )
        )

        while page < max_pages_per_slug:
            try:
                if start_time and (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                    meta.setdefault("errors", []).append(f"discover:{slug}:WALL_TIMEOUT_EXCEEDED")
                    break

                url = f"{API_BASE_URL}/{slug}"
                data = await self._fetch_from_api(
                    session,
                    url,
                    params={"offset": offset, "limit": limit},
                    fetch_meta=fetch_meta,
                    tag=f"discover:{slug}",
                )
                cards = list(
                    self._iter_cards_from_sections(data.get("data", {}).get("sections", []))
                )

                if not cards:
                    break

                new_cards = 0
                for card in cards:
                    cid = str(card.get("id") or "").strip()
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        collected.append(card)
                        new_cards += 1

                offset += len(cards)
                page += 1

                if len(cards) < limit or new_cards == 0:
                    break

                await asyncio.sleep(0.1)

                if len(seen_ids) >= soft_cap:
                    meta.setdefault("errors", []).append("discover:soft_cap_reached")
                    break

            except RetryError as e:
                root_exc = e.last_attempt.exception() if e.last_attempt else None
                status = getattr(root_exc, "status", None)
                if status:
                    print(
                        f"[kakao] discover slug={slug} failed with status={status} offset={offset}"
                    )
                meta.setdefault("errors", []).append(f"discover:{slug}:{e}")
                break
            except Exception as e:
                meta.setdefault("errors", []).append(f"discover:{slug}:{e}")
                break

        return collected

    def _merge_weekday(self, card, weekday_eng):
        if weekday_eng:
            groups = card.get("weekdayDisplayGroups")
            if not isinstance(groups, list):
                card["weekdayDisplayGroups"] = []
            if weekday_eng not in card["weekdayDisplayGroups"]:
                card["weekdayDisplayGroups"].append(weekday_eng)

    def _determine_status(self, card, *, from_completed=False):
        content_payload = card.get("content", {}) or {}
        status_text = content_payload.get("onGoingStatus") or card.get("onGoingStatus")

        if from_completed or status_text == "COMPLETED":
            return "완결"
        if status_text == "PAUSE":
            return "휴재"
        return "연재중"

    def _normalize_authors(self, card):
        content_payload = card.get("content", {}) or {}
        authors_field = content_payload.get("authors") or card.get("authors")
        authors = []

        if isinstance(authors_field, list):
            for author in authors_field:
                if isinstance(author, dict):
                    name = author.get("name") or author.get("displayName")
                    if name:
                        authors.append(name)
                elif isinstance(author, str) and author.strip():
                    authors.append(author.strip())
        elif isinstance(authors_field, str) and authors_field.strip():
            authors.append(authors_field.strip())

        display_authors = content_payload.get("displayAuthors")
        if isinstance(display_authors, list):
            for name in display_authors:
                if isinstance(name, str) and name.strip():
                    authors.append(name.strip())

        return authors

    def _select_thumbnail_url(self, card):
        content_payload = card.get("content", {}) or {}
        thumb = (
            content_payload.get("squareThumbnailUrl")
            or content_payload.get("thumbnailUrl")
            or card.get("squareThumbnailUrl")
            or card.get("thumbnailUrl")
        )
        return thumb

    async def fetch_all_data(self):
        """카카오웹툰의 '요일별'과 '완결' API에서 모든 웹툰 데이터를 비동기적으로 가져옵니다."""
        print("카카오웹툰 서버에서 최신 데이터를 가져옵니다...")

        start_time = time.monotonic()

        timeout = aiohttp.ClientTimeout(
            total=config.CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=config.CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=config.CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS,
        )
        connector = aiohttp.TCPConnector(
            limit=config.CRAWLER_HTTP_CONCURRENCY_LIMIT, ttl_dns_cache=300
        )

        fetch_meta = {"errors": []}
        discovered_cards = []
        weekday_cards = []
        completed_data = []
        discovered_slugs = set()

        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            cookie_jar=aiohttp.CookieJar(unsafe=True),
        ) as session:
            await self._ensure_cookies(session, fetch_meta=fetch_meta)
            weekday_url = f"{API_BASE_URL}/general-weekdays"

            if (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                fetch_meta["errors"].append("weekday:WALL_TIMEOUT_EXCEEDED")
                weekday_data, discovered_slugs = {}, set()
            else:
                weekday_data, discovered_slugs = await asyncio.gather(
                    self._fetch_from_api(
                        session,
                        weekday_url,
                        fetch_meta=fetch_meta,
                        tag="weekday",
                    ),
                    self._discover_official_slugs(
                        session, start_time=start_time, fetch_meta=fetch_meta
                    ),
                    return_exceptions=True,
                )

            discovered_cards = []
            seen_ids = set()
            weekday_cards = []
            errors_count = len(fetch_meta.get("errors", []))

            if isinstance(weekday_data, dict):
                for card in self._iter_cards_from_sections(
                    weekday_data.get("data", {}).get("sections", [])
                ):
                    weekday_cards.append(card)
                    cid = str(card.get("id") or "").strip()
                    if cid:
                        seen_ids.add(cid)
            errors_count = len(fetch_meta.get("errors", []))
            print(f"kakao.weekday: cards={len(weekday_cards)} errors={errors_count}")

            if isinstance(discovered_slugs, Exception):
                fetch_meta.setdefault("errors", []).append(
                    f"discover:failed:{discovered_slugs}"
                )
                discovered_slugs = set()

            if not discovered_slugs:
                fallback_slugs = getattr(config, "KAKAO_DISCOVERY_FALLBACK_SLUGS", [])
                if fallback_slugs:
                    print(
                        f"[kakao] discovery empty, using fallbacks: {fallback_slugs}"
                    )
                    discovered_slugs = set(fallback_slugs)

            try:
                completed_data, used_completion_slugs = await self._fetch_completed_cards(
                    session,
                    discovered_slugs,
                    start_time=start_time,
                    fetch_meta=fetch_meta,
                    seen_ids=seen_ids,
                )
            except Exception as e:
                fetch_meta.setdefault("errors", []).append(f"completed:failed:{e}")
                completed_data, used_completion_slugs = [], set()
            if isinstance(completed_data, list):
                for card in completed_data:
                    cid = str(card.get("id") or "").strip()
                    if cid:
                        seen_ids.add(cid)
            completed_raw_count = len(completed_data) if isinstance(completed_data, list) else 0
            errors_count = len(fetch_meta.get("errors", []))
            print(
                f"kakao.completed: slugs_used={len(used_completion_slugs)} cards={completed_raw_count} errors={errors_count}"
            )

            if (time.monotonic() - start_time) > config.CRAWLER_RUN_WALL_TIMEOUT_SECONDS:
                fetch_meta.setdefault("errors", []).append("discover:WALL_TIMEOUT_EXCEEDED")
            else:
                try:
                    remaining_slugs = set(discovered_slugs) - set(used_completion_slugs)
                    soft_cap = int(
                        getattr(
                            config,
                            "KAKAO_DISCOVERY_SOFT_CAP",
                            getattr(config, "KAKAOWEBTOON_TARGET_UNIQUE_TITLES", 20000),
                        )
                    )

                    for slug in remaining_slugs:
                        if len(seen_ids) >= soft_cap:
                            fetch_meta.setdefault("errors", []).append(
                                "discover:soft_cap_reached"
                            )
                            break

                        cards = await self._fetch_official_section_cards(
                            session,
                            slug,
                            start_time=start_time,
                            fetch_meta=fetch_meta,
                            seen_ids=seen_ids,
                        )
                        discovered_cards.extend(cards)
                    errors_count = len(fetch_meta.get("errors", []))
                    print(
                        f"kakao.discover: slugs={len(remaining_slugs)} cards={len(discovered_cards)} errors={errors_count}"
                    )

                except Exception as e:
                    fetch_meta.setdefault("errors", []).append(f"discover:failed:{e}")

        if isinstance(weekday_data, Exception):
            print(f"❌ 요일별 데이터 수집 실패: {weekday_data}")
            fetch_meta["errors"].append(f"weekday:{weekday_data}")
            weekday_data = {}

        if isinstance(completed_data, Exception):
            print(f"❌ 완결 데이터 수집 실패: {completed_data}")
            fetch_meta["errors"].append(f"completed:{completed_data}")
            completed_data = []

        print("\n--- 데이터 정규화 시작 ---")
        ongoing_today, hiatus_today, finished_today = {}, {}, {}
        all_content_today = {}

        day_map = {
            "월": "mon",
            "화": "tue",
            "수": "wed",
            "목": "thu",
            "금": "fri",
            "토": "sat",
            "일": "sun",
        }

        if weekday_data.get("data", {}).get("sections"):
            for section in weekday_data["data"]["sections"]:
                weekday_kor = section.get("title", "").replace("요일", "")
                weekday_eng = day_map.get(weekday_kor)
                for webtoon in self._iter_cards_from_sections([section]):
                    content_id = str(webtoon.get("id") or "").strip()
                    if not content_id:
                        continue

                    self._merge_weekday(webtoon, weekday_eng)

                    if "title" not in webtoon:
                        content_payload = webtoon.get("content", {}) or {}
                        webtoon["title"] = content_payload.get("title")

                    status = self._determine_status(webtoon)
                    all_content_today.setdefault(content_id, webtoon)

                    if status == "휴재":
                        hiatus_today[content_id] = all_content_today[content_id]
                    else:
                        ongoing_today[content_id] = all_content_today[content_id]

        for webtoon in completed_data or []:
            content_id = str(webtoon.get("id") or "").strip()
            if not content_id:
                continue

            all_content_today.setdefault(content_id, webtoon)
            finished_today[content_id] = all_content_today[content_id]

        for webtoon in discovered_cards:
            content_id = str(webtoon.get("id") or "").strip()
            if not content_id:
                continue

            status = self._determine_status(webtoon)
            all_content_today.setdefault(content_id, webtoon)

            if status == "완결":
                finished_today[content_id] = all_content_today[content_id]
            elif status == "휴재":
                hiatus_today[content_id] = all_content_today[content_id]
            else:
                ongoing_today[content_id] = all_content_today[content_id]

        print("데이터 정규화 완료.")

        # 신기능 우선: 수집 품질/오류 가시성 로그
        total_unique = len(all_content_today)
        weekday_count = len(weekday_cards)
        completed_count = len(completed_data) if isinstance(completed_data, list) else 0
        discovered_count = len(discovered_cards)
        has_webid = bool(self.cookies and self.cookies.get("webid"))
        has_t_ano = bool(self.cookies and self.cookies.get("_T_ANO"))
        print(
            f"kakao.totals -> weekday:{weekday_count} completed:{completed_count} "
            f"discovered:{discovered_count} unique_total:{total_unique} "
            f"cookie_source:{self.cookie_source} has_webid:{has_webid} has_T_ANO:{has_t_ano}"
        )
        print(
            f"Kakao breakdown -> ongoing:{len(ongoing_today)} hiatus:{len(hiatus_today)} "
            f"finished:{len(finished_today)}"
        )
        fetch_meta["counts"] = {
            "weekday": weekday_count,
            "completed": completed_count,
            "discovered": discovered_count,
            "total_unique": total_unique,
        }
        if fetch_meta.get("errors"):
            preview = fetch_meta["errors"][:5]
            print(f"Kakao errors (count={len(fetch_meta['errors'])}): {preview}")

        return ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta

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
            if content_id in finished_today:
                status = "완결"
            elif content_id in hiatus_today:
                status = "휴재"
            elif content_id in ongoing_today:
                status = "연재중"
            else:
                continue

            content_payload = webtoon_data.get("content", {}) or {}
            title = webtoon_data.get("title") or content_payload.get("title")
            if not title:
                continue

            authors = self._normalize_authors(webtoon_data)
            thumbnail_url = self._select_thumbnail_url(webtoon_data)
            content_url = f"https://webtoon.kakao.com/content/{urllib.parse.quote(str(content_id))}"
            normalized_title = normalize_search_text(title)
            normalized_authors = normalize_search_text(" ".join(authors) if authors else "")

            meta_data = {
                "common": {
                    "authors": authors,
                    "thumbnail_url": thumbnail_url,
                    "content_url": content_url,
                },
                "attributes": {
                    "weekdays": webtoon_data.get("weekdayDisplayGroups", []),
                },
            }

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


if __name__ == "__main__":
    import traceback
    import sys
    from dotenv import load_dotenv
    from database import create_standalone_connection

    load_dotenv()

    print("==========================================")
    print("  CRAWLER SCRIPT STARTED (STANDALONE)")
    print("==========================================")

    start_time = time.time()
    report = {"status": "성공"}
    db_conn = None
    CRAWLER_DISPLAY_NAME = "카카오 웹툰"

    try:
        print("LOG: Calling create_standalone_connection()...")
        db_conn = create_standalone_connection()
        print("LOG: create_standalone_connection() finished.")

        crawler = KakaowebtoonCrawler()
        print("LOG: KakaowebtoonCrawler instance created.")

        print("LOG: Calling asyncio.run(crawler.run_daily_check())...")
        new_contents, newly_completed_items, cdc_info = asyncio.run(
            crawler.run_daily_check(db_conn)
        )
        print("LOG: asyncio.run(crawler.run_daily_check()) finished.")

        report.update(
            {
                "new_webtoons": new_contents,
                "newly_completed_items": newly_completed_items,
                "cdc_info": cdc_info,
            }
        )

    except Exception as e:
        print(f"치명적 오류 발생: {e}")
        report["status"] = "실패"
        report["error_message"] = traceback.format_exc()

    finally:
        if db_conn:
            print("LOG: Closing database connection.")
            db_conn.close()

        report["duration"] = time.time() - start_time

        report_conn = None
        try:
            report_conn = create_standalone_connection()
            report_cursor = get_cursor(report_conn)
            print("LOG: Saving report to 'daily_crawler_reports' table...")
            report_cursor.execute(
                """
                INSERT INTO daily_crawler_reports (crawler_name, status, report_data)
                VALUES (%s, %s, %s)
                """,
                (CRAWLER_DISPLAY_NAME, report["status"], json.dumps(report)),
            )
            report_conn.commit()
            report_cursor.close()
            print("LOG: Report saved successfully.")
        except Exception as report_e:
            print(f"FATAL: [실패] 보고서 DB 저장 실패: {report_e}", file=sys.stderr)
        finally:
            if report_conn:
                report_conn.close()

        print("==========================================")
        print("  CRAWLER SCRIPT FINISHED")
        print("==========================================")

        if report["status"] == "실패":
            sys.exit(1)
