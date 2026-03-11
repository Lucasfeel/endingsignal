import asyncio

from services import crawler_verification_service as service


def test_verify_naver_webtoon_skips_browser_when_no_candidates():
    verdict = asyncio.run(service.verify_naver_webtoon({"source_name": "naver_webtoon"}))

    assert verdict["gate"] == "not_applicable"
    assert verdict["reason"] == "no_candidate_changes"
    assert verdict["apply_allowed"] is True
    assert verdict["items"] == []


def test_build_verification_gate_preserves_registered_verifier_payload(monkeypatch):
    async def _fake_verifier(write_plan):
        return {
            "gate": "passed",
            "mode": "playwright_browser",
            "reason": "verified_all_changed_items",
            "message": f"verified:{write_plan['source_name']}",
            "apply_allowed": True,
            "changed_count": 1,
            "verified_count": 1,
            "items": [{"content_id": "CID-1", "ok": True}],
        }

    monkeypatch.setitem(service.VERIFIER_REGISTRY, "test_source", _fake_verifier)

    verdict = asyncio.run(
        service.build_verification_gate()(
            {
                "source_name": "test_source",
                "verification_candidates": [{"content_id": "CID-1"}],
            }
        )
    )

    assert verdict["reason"] == "verified_all_changed_items"
    assert verdict["verified_count"] == 1
    assert verdict["items"][0]["content_id"] == "CID-1"


def test_extract_naver_webtoon_status_prefers_weekday_block():
    body_text = "\uc644\uacb0\uc791"
    html = (
        '<div class="week_day">'
        '<dt>\uc5f0\uc7ac</dt>'
        '<dd><ul class="list_detail"><li>\uc6d4</li></ul></dd>'
        '</div>'
    )

    observed = service._extract_naver_webtoon_status(body_text, html)

    assert observed == service.STATUS_ONGOING


def test_verify_naver_series_search_fallback_matches_public_search(monkeypatch):
    class FakePage:
        def __init__(self, html: str):
            self.url = ""
            self._html = html

        async def content(self):
            return self._html

    async def _fake_navigate(page, url):
        page.url = url

    html = (
        "<html><body><ul>"
        "<li>"
        '<h3><a href="/novel/detail.series?productNo=13796521" '
        'title="\uc7a1\uc544\uba39\uace0 \uc2f6\uc5b4\uc11c [\ub2e8\ud589\ubcf8] (\ucd1d 3\uad8c/\uc644\uacb0)">'
        "\uc7a1\uc544\uba39\uace0 \uc2f6\uc5b4\uc11c [\ub2e8\ud589\ubcf8] (\ucd1d 3\uad8c/\uc644\uacb0)"
        "</a></h3>"
        '<p class="info">\ud3c9\uc810 10.0 | \ud558\ub2e4\ubbfc | 2026.03.06. | \ucd1d3\uad8c/\uc644\uacb0</p>'
        "</li>"
        "</ul></body></html>"
    )
    page = FakePage(html)
    monkeypatch.setattr(service, "_navigate", _fake_navigate)

    result = asyncio.run(
        service._verify_naver_series_search_fallback(
            page,
            {
                "content_id": "13796521",
                "title": "\uc7a1\uc544\uba39\uace0 \uc2f6\uc5b4\uc11c [\ub2e8\ud589\ubcf8]",
                "expected_status": service.STATUS_COMPLETED,
            },
        )
    )

    assert result is not None
    assert result["ok"] is True
    assert result["verification_method"] == "search"
    assert result["observed_status"] == service.STATUS_COMPLETED


def test_extract_ridi_status_treats_special_set_as_completed():
    body_text = (
        "[\ud2b9\ubcc4 \uc138\ud2b8] \ubcc0\uacbd\uc758 \ud314\ub77c\ub518 (\ucd1d 5\uad8c) "
        "\uc0c1\uc138\ud398\uc774\uc9c0 5\uad8c \uc138\ud2b8\ubbf8\ub9ac\ubcf4\uae30"
    )
    page_title = "[\ud2b9\ubcc4 \uc138\ud2b8] \ubcc0\uacbd\uc758 \ud314\ub77c\ub518 (\ucd1d 5\uad8c) - \ub9ac\ub514"

    observed = service._extract_ridi_status(body_text, "", page_title=page_title)

    assert observed == service.STATUS_COMPLETED


def test_kakaowebtoon_playwright_cookies_bridge_cookie_header(monkeypatch):
    monkeypatch.setenv("KAKAOWEBTOON_COOKIE", "foo=1; bar=2")

    cookies = service._kakaowebtoon_playwright_cookies()

    assert cookies == [
        {
            "name": "foo",
            "value": "1",
            "domain": ".kakao.com",
            "path": "/",
            "httpOnly": False,
            "secure": True,
        },
        {
            "name": "bar",
            "value": "2",
            "domain": ".kakao.com",
            "path": "/",
            "httpOnly": False,
            "secure": True,
        },
    ]


def test_browser_context_ignores_https_errors_by_default(monkeypatch):
    monkeypatch.delenv("VERIFIED_SYNC_PLAYWRIGHT_IGNORE_HTTPS_ERRORS", raising=False)

    kwargs = service._browser_context_kwargs()

    assert kwargs["locale"] == "ko-KR"
    assert kwargs["ignore_https_errors"] is True


def test_browser_context_can_disable_https_ignore(monkeypatch):
    monkeypatch.setenv("VERIFIED_SYNC_PLAYWRIGHT_IGNORE_HTTPS_ERRORS", "false")

    kwargs = service._browser_context_kwargs()

    assert kwargs["ignore_https_errors"] is False


def test_parse_kakaowebtoon_listing_items_extracts_adult_title():
    html = (
        '<a href="/content/example-adult-title/4465">'
        '<img alt="" src="bg.jpg" />'
        '<img alt="3다무" src="badge.png" />'
        '<img alt="Example Adult Title [19]" src="title.png" />'
        '<img alt="\uc131\uc778" src="adult.png" />'
        "</a>"
    )

    items = service._parse_kakaowebtoon_listing_items(html, seed_completed=True)

    assert items == [
        {
            "content_id": "4465",
            "content_url": "https://webtoon.kakao.com/content/example-adult-title/4465",
            "title": "Example Adult Title [19]",
            "status": service.STATUS_COMPLETED,
            "adult": True,
        }
    ]


def test_verify_kakao_webtoon_candidate_uses_listing_fallback_when_detail_title_missing(monkeypatch):
    class FakePage:
        def __init__(self):
            self.url = ""

        async def title(self):
            return ""

    async def _fake_navigate(page, url):
        page.url = "https://webtoon.kakao.com/"

    async def _fake_page_text(page):
        return ""

    async def _fake_listing_fallback(page, candidate):
        return {
            "content_id": candidate["content_id"],
            "title": candidate["title"],
            "expected_status": candidate["expected_status"],
            "observed_status": service.STATUS_COMPLETED,
            "ok": True,
            "verification_method": "listing",
            "listing_url": service.KAKAOWEBTOON_COMPLETED_URL,
            "evidence": {"matched_by": "content_id"},
        }

    monkeypatch.setattr(service, "_navigate", _fake_navigate)
    monkeypatch.setattr(service, "_page_text", _fake_page_text)
    monkeypatch.setattr(service, "_verify_kakao_webtoon_listing_fallback", _fake_listing_fallback)

    result = asyncio.run(
        service._verify_kakao_webtoon_candidate(
            FakePage(),
            {
                "content_id": "4465",
                "title": "Example Adult Title [19]",
                "content_url": "https://webtoon.kakao.com/content/example-adult-title/4465",
                "expected_status": service.STATUS_COMPLETED,
            },
        )
    )

    assert result["verification_method"] == "listing"
    assert result["ok"] is True


class _FakeLocator:
    def __init__(self, *, count: int, should_click: bool):
        self._count = count
        self._should_click = should_click

    @property
    def first(self):
        return self

    async def count(self):
        return self._count

    async def click(self):
        if not self._should_click:
            raise RuntimeError("click failed")
        return None


class _DelayedKakaoListingPage:
    def __init__(self, html_versions, *, role_click=True, text_click=False):
        self.url = "about:blank"
        self._html_versions = list(html_versions)
        self._content_index = 0
        self._role_click = role_click
        self._text_click = text_click
        self.waited_timeouts = []
        self.load_state_calls = []
        self.evaluated_scripts = []
        self.waited_selectors = []

    def get_by_role(self, *args, **kwargs):
        return _FakeLocator(count=1 if self._role_click else 0, should_click=self._role_click)

    def get_by_text(self, *args, **kwargs):
        return _FakeLocator(count=1 if self._text_click else 0, should_click=self._text_click)

    async def wait_for_timeout(self, ms):
        self.waited_timeouts.append(ms)
        return None

    async def wait_for_selector(self, selector, **kwargs):
        self.waited_selectors.append((selector, kwargs))
        return None

    async def wait_for_load_state(self, state, timeout=None):
        self.load_state_calls.append((state, timeout))
        return None

    async def content(self):
        index = min(self._content_index, len(self._html_versions) - 1)
        html = self._html_versions[index]
        if self._content_index < len(self._html_versions) - 1:
            self._content_index += 1
        return html

    async def evaluate(self, script):
        self.evaluated_scripts.append(script)
        return None


def test_verify_kakao_webtoon_listing_fallback_retries_until_target_hydrates(monkeypatch):
    async def _fake_navigate(page, url):
        page.url = url

    initial_html = (
        '<a href="/content/placeholder/4784">'
        '<img alt="너에게 하고 싶은 말" />'
        "</a>"
    )
    hydrated_html = (
        '<a href="/content/placeholder/4784">'
        '<img alt="너에게 하고 싶은 말" />'
        "</a>"
        '<a href="/content/example-adult-title/4465">'
        '<img alt="성인" />'
        '<img alt="남주의 남자친구가 내게 집착한다 [19세 완전판]" />'
        "</a>"
    )
    page = _DelayedKakaoListingPage([initial_html, initial_html, hydrated_html])
    candidate = {
        "content_id": "4465",
        "title": "남주의 남자친구가 내게 집착한다 [19세 완전판]",
        "expected_status": service.STATUS_COMPLETED,
    }
    monkeypatch.setattr(service, "_navigate", _fake_navigate)

    verdict = asyncio.run(service._verify_kakao_webtoon_listing_fallback(page, candidate))

    assert verdict is not None
    assert verdict["verification_method"] == "listing"
    assert verdict["ok"] is True
    assert verdict["evidence"]["scrolls"] == 1
    assert verdict["evidence"]["settle_attempt"] >= 2
    assert page.load_state_calls
