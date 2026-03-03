import asyncio

from utils.polite_http import AsyncRateLimiter, fetch_text_polite


def test_async_rate_limiter_enforces_min_interval_spacing():
    now = {"value": 0.0}
    sleep_calls = []

    def fake_now():
        return now["value"]

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        now["value"] += seconds

    limiter = AsyncRateLimiter(
        min_interval_seconds=1.5,
        now_func=fake_now,
        sleep_func=fake_sleep,
    )

    async def run_case():
        await limiter.wait()
        now["value"] = 0.5
        await limiter.wait()

    asyncio.run(run_case())

    assert len(sleep_calls) == 1
    assert sleep_calls[0] == 1.0


class _FakeResponse:
    def __init__(self, status, text, headers=None):
        self.status = status
        self._text = text
        self.headers = headers or {}

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, _url, headers=None):
        return self._responses.pop(0)


def test_fetch_text_polite_respects_retry_after_on_429():
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    session = _FakeSession(
        [
            _FakeResponse(429, "Too many requests", headers={"Retry-After": "3"}),
            _FakeResponse(200, "<html>ok</html>"),
        ]
    )

    async def run_case():
        return await fetch_text_polite(
            session,
            "https://example.com",
            headers={},
            retries=2,
            retry_base_delay_seconds=0.5,
            retry_max_delay_seconds=10.0,
            jitter_min_seconds=0.0,
            jitter_max_seconds=0.0,
            sleep_func=fake_sleep,
        )

    body = asyncio.run(run_case())

    assert body == "<html>ok</html>"
    assert sleep_calls == [3.0]
