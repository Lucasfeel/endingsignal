import asyncio
import json
import requests

from crawlers.coupang_play_ott_crawler import CoupangPlayOttCrawler
from crawlers.disney_plus_ott_crawler import DisneyPlusOttCrawler
from crawlers.netflix_ott_crawler import NetflixOttCrawler
from crawlers.ott_parser_utils import parse_flexible_datetime
from crawlers.tving_ott_crawler import TvingOttCrawler
from crawlers.wavve_ott_crawler import WavveOttCrawler


def test_tving_parser_filters_movie_codes():
    crawler = TvingOttCrawler()
    payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "state": {
                                "data": {
                                    "pages": [
                                        {
                                            "data": {
                                                "band": {
                                                    "items": [
                                                        {"code": "P001", "title": "시리즈 A", "imageUrl": "https://img/a.jpg"},
                                                        {"code": "M001", "title": "영화 B", "imageUrl": "https://img/b.jpg"},
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload, ensure_ascii=False)}</script>'

    parsed = crawler._parse_page(html)

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["title"] == "시리즈 A"
    assert item["platform_source"] == "tving"


def test_tving_data_route_parser_filters_movie_codes():
    crawler = TvingOttCrawler()
    payload = {
        "pageProps": {
            "dehydratedState": {
                "queries": [
                    {
                        "state": {
                            "data": {
                                "pages": [
                                    {
                                        "data": {
                                            "band": {
                                                "items": [
                                                    {"code": "P001", "title": "Series A", "imageUrl": "https://img/a.jpg"},
                                                    {"code": "M001", "title": "Movie B", "imageUrl": "https://img/b.jpg"},
                                                ]
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }

    parsed = crawler._parse_data_route_json(json.dumps(payload))

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["title"] == "Series A"
    assert item["platform_source"] == "tving"


def test_coupang_parser_only_uses_weekly_tv_row():
    crawler = CoupangPlayOttCrawler()
    payload = {
        "props": {
            "pageProps": {
                "feeds": [
                    {
                        "row_name": "새로 올라온 콘텐츠",
                        "data": [
                            {
                                "id": "show-2",
                                "type": "TITLE",
                                "sub_type": "TVSHOW",
                                "title": "다른 섹션 작품",
                            }
                        ],
                    },
                    {
                        "row_name": "TV프로그램, 매주 새 에피소드",
                        "data": [
                            {
                                "id": "show-1",
                                "type": "TITLE",
                                "sub_type": "TVSHOW",
                                "title": "쿠플 시리즈",
                                "airing_date_friendly": "02월 27일 2026년",
                                "description": "매주 금요일 공개\n설명",
                                "badgeKey": "NEW_EP_WEEKLY",
                            },
                            {
                                "id": "movie-1",
                                "type": "TITLE",
                                "sub_type": "MOVIE",
                                "title": "쿠플 영화",
                            },
                        ],
                    },
                ]
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload, ensure_ascii=False)}</script>'

    parsed = crawler._parse_page(html)

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["title"] == "쿠플 시리즈"
    assert item["release_start_at"].year == 2026
    assert item["raw_schedule_note"] == "매주 금요일 공개"
    assert item["episode_hint"] == "NEW_EP_WEEKLY"


def test_disney_parser_only_uses_series_sections():
    crawler = DisneyPlusOttCrawler()
    payload = {
        "props": {
            "pageProps": {
                "stitchDocument": {
                    "mainContent": [
                        {
                            "_type": "SetGroup",
                            "items": [
                                {
                                    "title": "이번 주 새 에피소드",
                                    "items": [
                                        {
                                            "_id": "entity-1",
                                            "title": "디즈니 시리즈",
                                            "url": "/browse/entity-1",
                                            "imageVariants": {
                                                "defaultImage": {"source": "https://img.example/disney.jpg"}
                                            },
                                        }
                                    ],
                                },
                                {
                                    "title": "공개 예정 영화",
                                    "items": [
                                        {
                                            "_id": "entity-2",
                                            "title": "디즈니 영화",
                                            "url": "/browse/entity-2",
                                        }
                                    ],
                                },
                            ],
                        }
                    ]
                }
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload, ensure_ascii=False)}</script>'

    parsed = crawler._parse_page(html)

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["title"] == "디즈니 시리즈"
    assert item["platform_url"].endswith("/browse/entity-1")


def test_netflix_parser_reads_group_release_date_and_preserves_season_label():
    crawler = NetflixOttCrawler()
    html = """
    <div class="grid">
      <h2 class="fs-20 mt-10 text-line">2026.03.12 (목)</h2>
      <div class="row gy-6">
        <div class="item">
          <div class="card">
            <figure class="overlay"><img src="https://img.example/netflix.jpg" /></figure>
            <div class="card-body">
              <blockquote>
                <div class="info">
                  <h5><span class="badge">시리즈</span><span class="inline-with-badge">버진리버- 시즌 7</span></h5>
                  <p>배우 A,배우 B</p>
                </div>
              </blockquote>
              <blockquote class="border-0"><p>전편 동시 공개</p></blockquote>
              <button data-netflix-id="12345678">관심 콘텐츠</button>
              <a href="https://netflix.com/title/12345678">넷플릭스에서 보기</a>
            </div>
          </div>
        </div>
      </div>
    </div>
    """

    parsed = crawler._parse_page(
        html,
        official_metadata_by_id={
            "12345678": {
                "official_title": "버진리버",
                "platform_url": "https://www.netflix.com/kr/title/12345678",
                "official_text": "버진리버 시즌 7 전편 공개",
            }
        },
    )

    assert len(parsed) == 1
    item = next(iter(parsed.values()))
    assert item["platform_content_id"] == "12345678"
    assert item["title"] == "버진리버 시즌 7"
    assert item["cast"] == ["배우 A", "배우 B"]
    assert item["release_start_at"].year == 2026
    assert item["release_end_at"] == item["release_start_at"]
    assert item["release_end_status"] == "scheduled"


def test_netflix_parser_keeps_end_unknown_without_batch_hint():
    crawler = NetflixOttCrawler()
    html = """
    <div class="grid">
      <h2 class="fs-20 mt-10 text-line">2026.03.13 (금)</h2>
      <div class="row gy-6">
        <div class="item">
          <div class="card">
            <div class="card-body">
              <blockquote>
                <div class="info">
                  <h5><span class="badge">시리즈</span><span class="inline-with-badge">치명적 유혹- 시즌 3</span></h5>
                  <p>배우 C</p>
                </div>
              </blockquote>
              <blockquote class="border-0"><p>새 시즌 공개</p></blockquote>
              <button data-netflix-id="87654321">관심 콘텐츠</button>
            </div>
          </div>
        </div>
      </div>
    </div>
    """

    parsed = crawler._parse_page(
        html,
        official_metadata_by_id={
            "87654321": {
                "official_title": "치명적 유혹",
                "official_text": "치명적 유혹 시즌 3",
            }
        },
    )

    item = next(iter(parsed.values()))
    assert item["title"] == "치명적 유혹 시즌 3"
    assert item["release_end_at"] is None
    assert item["release_end_status"] == "unknown"


def test_wavve_normalize_title_strips_schedule_suffix():
    crawler = WavveOttCrawler()

    assert crawler._normalize_title("exclusive 대한민국에서 건물주 되는 법 3월 14일 밤 9시 10분 첫 방송") == "대한민국에서 건물주 되는 법"


def test_parse_flexible_datetime_supports_month_day_without_year():
    parsed = parse_flexible_datetime("3월 14일 밤 9시 10분 첫 방송")

    assert parsed is not None
    assert parsed.month == 3
    assert parsed.day == 14


def test_tving_fetch_uses_playwright_fallback_when_request_fails(monkeypatch):
    crawler = TvingOttCrawler()
    payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "state": {
                                "data": {
                                    "pages": [
                                        {
                                            "data": {
                                                "band": {
                                                    "items": [
                                                        {"code": "P001", "title": "Series A", "imageUrl": "https://img/a.jpg"},
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'

    def _raise(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("crawlers.tving_ott_crawler.requests.get", _raise)
    monkeypatch.setattr(
        TvingOttCrawler,
        "_fetch_with_playwright",
        lambda self: asyncio.sleep(
            0,
            result=(
                html,
                crawler._parse_page(html),
                ["PLAYWRIGHT_SELECTOR_TIMEOUT"],
            ),
        ),
    )

    _, _, _, all_content, meta = asyncio.run(crawler.fetch_all_data())

    assert len(all_content) == 1
    assert meta["fetch_method"] == "playwright"
    assert any("REQUEST_FETCH_FAILED" in item for item in meta["errors"])
    assert "PLAYWRIGHT_SELECTOR_TIMEOUT" in meta["errors"]


def test_tving_fetch_retries_without_headers_when_crawler_headers_fail(monkeypatch):
    crawler = TvingOttCrawler()
    payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "state": {
                                "data": {
                                    "pages": [
                                        {
                                            "data": {
                                                "band": {
                                                    "items": [
                                                        {"code": "P001", "title": "Series A", "imageUrl": "https://img/a.jpg"},
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'

    class DummyResponse:
        def __init__(self, *, status_code, url, text):
            self.status_code = status_code
            self.url = url
            self.text = text

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"{self.status_code} error", response=self)

    def _fake_get(_url, headers=None, timeout=None):
        if headers:
            return DummyResponse(status_code=500, url="https://www.tving.com/500", text="")
        return DummyResponse(status_code=200, url="https://www.tving.com/more/band/HM257176", text=html)

    monkeypatch.setattr("crawlers.tving_ott_crawler.requests.get", _fake_get)

    _, _, _, all_content, meta = asyncio.run(crawler.fetch_all_data())

    assert len(all_content) == 1
    assert meta["fetch_method"] == "requests:default_headers"
    assert any("REQUEST_FETCH_FAILED:crawler_headers" in item for item in meta["errors"])


def test_tving_entries_from_dom_link_items_prefers_text_then_image_alt():
    crawler = TvingOttCrawler()

    parsed = crawler._entries_from_dom_link_items(
        [
            {"href": "/contents/P001", "titleText": "  Series A  ", "imgAlt": "Poster A"},
            {"href": "/contents/P002", "titleText": "", "imgAlt": "Poster B"},
            {"href": "/contents/M003", "titleText": "Movie", "imgAlt": "Movie"},
        ]
    )

    assert len(parsed) == 2
    titles = sorted(item["title"] for item in parsed.values())
    assert titles == ["Poster B", "Series A"]


def test_wavve_extract_raw_items_from_html_uses_dom_titles():
    crawler = WavveOttCrawler()
    html = """
    <div>
      <a class="click-area" href="javascript:void(0)">
        <img alt="웨이브 라인업 3월에도 JUST DIVE, Wavve!" src="hero.jpg" />
        <div class="title1">웨이브 라인업</div>
        <div class="title2">3월에도 JUST DIVE, Wavve!</div>
      </a>
      <a class="click-area" href="javascript:void(0)">
        <img alt="" src="show.jpg" />
        <div class="title1">대한민국에서 건물주 되는 법</div>
        <div class="title2">3월 14일, 밤 9시 10분 첫 방송</div>
      </a>
    </div>
    """

    rows = crawler._extract_raw_items_from_html(html)

    assert len(rows) == 2
    assert rows[1]["title"] == "대한민국에서 건물주 되는 법 3월 14일, 밤 9시 10분 첫 방송"
