"""Polite async HTTP helpers for rate-limited crawlers."""

from __future__ import annotations

import asyncio
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Awaitable, Callable, Dict, Optional

import aiohttp

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_MULTISPACE_RE = re.compile(r"\s+")


@dataclass
class RateLimitedError(Exception):
    retry_after_seconds: Optional[float]
    status: int
    url: str

    def __str__(self) -> str:
        return (
            f"Rate limited: status={self.status} url={self.url} "
            f"retry_after_seconds={self.retry_after_seconds}"
        )


@dataclass
class BlockedError(Exception):
    status: int
    url: str
    diagnostics: Dict[str, str]

    def __str__(self) -> str:
        title = self.diagnostics.get("title", "")
        return f"Blocked response: status={self.status} url={self.url} title={title!r}"


@dataclass
class TransientHttpError(Exception):
    status: int
    url: str

    def __str__(self) -> str:
        return f"Transient HTTP failure: status={self.status} url={self.url}"


@dataclass
class HttpStatusError(Exception):
    status: int
    url: str

    def __str__(self) -> str:
        return f"HTTP failure: status={self.status} url={self.url}"


class AsyncRateLimiter:
    """Global async min-interval limiter shared across workers."""

    def __init__(
        self,
        *,
        min_interval_seconds: float,
        now_func: Callable[[], float] = time.monotonic,
        sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._now = now_func
        self._sleep = sleep_func
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        async with self._lock:
            now = float(self._now())
            if now < self._next_allowed_at:
                await self._sleep(self._next_allowed_at - now)
                now = float(self._now())
            self._next_allowed_at = now + self.min_interval_seconds


def parse_retry_after_seconds(raw_value: Optional[str]) -> Optional[float]:
    value = str(raw_value or "").strip()
    if not value:
        return None

    try:
        seconds = float(value)
        return max(0.0, seconds)
    except (TypeError, ValueError):
        pass

    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        delta = (parsed - now).total_seconds()
        return max(0.0, float(delta))
    except Exception:
        return None


def extract_html_diagnostics(html: str, *, snippet_size: int = 200) -> Dict[str, str]:
    raw_html = str(html or "")
    title = ""
    match = _HTML_TITLE_RE.search(raw_html)
    if match:
        title = _MULTISPACE_RE.sub(" ", _HTML_TAG_RE.sub(" ", match.group(1))).strip()
    text = _MULTISPACE_RE.sub(" ", _HTML_TAG_RE.sub(" ", raw_html)).strip()
    return {
        "title": title,
        "text_snippet": text[: max(32, int(snippet_size))],
    }


def _compute_backoff_seconds(
    *,
    attempt: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
    jitter_min_seconds: float,
    jitter_max_seconds: float,
) -> float:
    capped_attempt = max(1, int(attempt))
    backoff = float(base_delay_seconds) * (2 ** (capped_attempt - 1))
    backoff = min(float(max_delay_seconds), backoff)
    jitter_low = min(float(jitter_min_seconds), float(jitter_max_seconds))
    jitter_high = max(float(jitter_min_seconds), float(jitter_max_seconds))
    jitter = random.uniform(jitter_low, jitter_high)
    return max(0.0, backoff + jitter)


async def fetch_text_polite(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: Dict[str, str],
    retries: int = 4,
    retry_base_delay_seconds: float = 1.0,
    retry_max_delay_seconds: float = 60.0,
    jitter_min_seconds: float = 0.05,
    jitter_max_seconds: float = 0.35,
    sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> str:
    last_error: Optional[Exception] = None
    max_attempts = max(1, int(retries))

    for attempt in range(1, max_attempts + 1):
        try:
            async with session.get(url, headers=headers) as response:
                text = await response.text()
                status = int(response.status)

                if 200 <= status < 300:
                    return text
                if status == 429:
                    retry_after = parse_retry_after_seconds(response.headers.get("Retry-After"))
                    if attempt >= max_attempts:
                        raise RateLimitedError(
                            retry_after_seconds=retry_after,
                            status=status,
                            url=url,
                        )
                    wait_seconds = retry_after
                    if wait_seconds is None:
                        wait_seconds = _compute_backoff_seconds(
                            attempt=attempt,
                            base_delay_seconds=retry_base_delay_seconds,
                            max_delay_seconds=retry_max_delay_seconds,
                            jitter_min_seconds=jitter_min_seconds,
                            jitter_max_seconds=jitter_max_seconds,
                        )
                    else:
                        wait_seconds = max(
                            0.0,
                            float(wait_seconds) + random.uniform(jitter_min_seconds, jitter_max_seconds),
                        )
                    await sleep_func(wait_seconds)
                    continue
                if status == 403:
                    diagnostics = extract_html_diagnostics(text)
                    raise BlockedError(status=status, url=url, diagnostics=diagnostics)
                if status >= 500:
                    if attempt >= max_attempts:
                        raise TransientHttpError(status=status, url=url)
                    wait_seconds = _compute_backoff_seconds(
                        attempt=attempt,
                        base_delay_seconds=retry_base_delay_seconds,
                        max_delay_seconds=retry_max_delay_seconds,
                        jitter_min_seconds=jitter_min_seconds,
                        jitter_max_seconds=jitter_max_seconds,
                    )
                    await sleep_func(wait_seconds)
                    continue
                if status in (408, 425):
                    if attempt >= max_attempts:
                        raise TransientHttpError(status=status, url=url)
                    wait_seconds = _compute_backoff_seconds(
                        attempt=attempt,
                        base_delay_seconds=retry_base_delay_seconds,
                        max_delay_seconds=retry_max_delay_seconds,
                        jitter_min_seconds=jitter_min_seconds,
                        jitter_max_seconds=jitter_max_seconds,
                    )
                    await sleep_func(wait_seconds)
                    continue
                raise HttpStatusError(status=status, url=url)
        except (BlockedError, RateLimitedError, HttpStatusError):
            raise
        except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise TransientHttpError(status=0, url=url) from exc
            wait_seconds = _compute_backoff_seconds(
                attempt=attempt,
                base_delay_seconds=retry_base_delay_seconds,
                max_delay_seconds=retry_max_delay_seconds,
                jitter_min_seconds=jitter_min_seconds,
                jitter_max_seconds=jitter_max_seconds,
            )
            await sleep_func(wait_seconds)
            continue
        except TransientHttpError as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise
            wait_seconds = _compute_backoff_seconds(
                attempt=attempt,
                base_delay_seconds=retry_base_delay_seconds,
                max_delay_seconds=retry_max_delay_seconds,
                jitter_min_seconds=jitter_min_seconds,
                jitter_max_seconds=jitter_max_seconds,
            )
            await sleep_func(wait_seconds)
            continue

    if isinstance(last_error, Exception):
        raise last_error
    raise RuntimeError(f"Request failed without explicit exception: {url}")
