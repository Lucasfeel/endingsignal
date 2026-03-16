from services import ott_verification_service as service
from datetime import datetime


class _DummySession:
    pass


def test_fetch_tmdb_documents_builds_structured_tv_doc(monkeypatch):
    monkeypatch.setattr(service.config, "TMDB_BEARER_TOKEN", "tmdb-token")

    def fake_tmdb_get_json(session, path, *, params=None):
        if path == "/search/tv":
            return {
                "results": [
                    {"id": 100, "name": "Honor", "original_name": "Honor"},
                ]
            }
        if path == "/tv/100":
            return {
                "id": 100,
                "name": "Honor",
                "original_name": "Honor",
                "overview": "Legal crime drama.",
                "genres": [{"name": "Crime"}, {"name": "Drama"}],
                "status": "Ended",
                "in_production": False,
                "first_air_date": "2026-02-02",
                "last_air_date": "2026-03-10",
            }
        if path == "/tv/100/aggregate_credits":
            return {
                "cast": [
                    {"name": "Actor A"},
                    {"name": "Actor B"},
                    {"name": "Actor C"},
                    {"name": "Actor D"},
                    {"name": "Actor E"},
                ]
            }
        return None

    monkeypatch.setattr(service, "_tmdb_get_json", fake_tmdb_get_json)

    docs = service._fetch_tmdb_documents(
        _DummySession(),
        {
            "source_name": "coupangplay",
            "title": "아너: 그녀들의 법정 시즌 1",
            "source_item": {
                "title": "아너: 그녀들의 법정 시즌 1",
                "title_alias": ["Honor"],
                "alt_title": ["Honor"],
            },
        },
    )

    assert len(docs) == 1
    assert docs[0]["source"] == "tmdb"
    assert docs[0]["genre_text"] == "Crime, Drama"
    assert docs[0]["cast"] == ["Actor A", "Actor B", "Actor C", "Actor D"]
    assert docs[0]["release_start_at"].isoformat() == "2026-02-02T00:00:00"
    assert docs[0]["release_end_at"].isoformat() == "2026-03-10T00:00:00"
    assert docs[0]["release_end_status"] == "confirmed"


def test_tmdb_crime_genre_still_normalizes_to_drama(monkeypatch):
    monkeypatch.setattr(service.config, "TMDB_BEARER_TOKEN", "tmdb-token")

    def fake_tmdb_get_json(session, path, *, params=None):
        if path == "/search/tv":
            return {"results": [{"id": 200, "name": "Industry", "original_name": "Industry"}]}
        if path == "/tv/200":
            return {
                "id": 200,
                "name": "Industry",
                "original_name": "Industry",
                "overview": "Crime thriller series.",
                "genres": [{"name": "Crime"}],
                "status": "Returning Series",
                "in_production": True,
                "first_air_date": "2026-01-15",
            }
        if path == "/tv/200/aggregate_credits":
            return {"cast": [{"name": "Actor A"}]}
        if path == "/tv/200/season/4":
            return {
                "name": "Season 4",
                "air_date": "2026-01-15",
                "episodes": [
                    {"episode_number": 1, "air_date": "2026-01-15"},
                    {"episode_number": 8, "air_date": "2026-03-05"},
                ],
            }
        if path == "/tv/200/season/4/aggregate_credits":
            return {"cast": [{"name": "Actor A"}]}
        return None

    monkeypatch.setattr(service, "_tmdb_get_json", fake_tmdb_get_json)

    docs = service._fetch_tmdb_documents(
        _DummySession(),
        {
            "source_name": "coupangplay",
            "title": "인더스트리 시즌 4",
            "source_item": {
                "title": "인더스트리 시즌 4",
                "title_alias": ["Industry"],
                "alt_title": ["Industry"],
            },
        },
    )

    genres = service.normalize_ott_genres(docs[0]["genre_text"], platform_source="coupangplay")
    assert genres == ["drama"]


def test_merge_verification_metadata_prefers_official_dates_over_tmdb_dates():
    candidate = {
        "source_name": "coupangplay",
        "title": "DTF 세인트루이스",
        "source_item": {
            "title": "DTF 세인트루이스",
            "title_alias": ["DTF St. Louis"],
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "ok": True,
            "source": "official_episode_schedule",
            "title": "DTF 세인트루이스 시즌 1",
            "payload_titles": ["DTF 세인트루이스 시즌 1"],
            "body_text": "episode schedule",
            "description": "",
            "genre_text": "코미디 수사물",
            "cast": ["제이슨 베이트먼"],
            "release_start_at": datetime(2026, 3, 6),
            "release_end_at": datetime(2026, 4, 17),
            "release_end_status": "scheduled",
        },
        {
            "ok": True,
            "source": "tmdb",
            "title": "Dope Thief",
            "payload_titles": ["DTF St. Louis"],
            "body_text": "Crime drama",
            "description": "Crime drama",
            "genre_text": "Crime, Drama",
            "cast": ["Actor A"],
            "release_start_at": datetime(2026, 3, 6),
            "release_end_at": datetime(2026, 3, 8),
            "release_end_status": "confirmed",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["release_start_at"] == datetime(2026, 3, 6)
    assert metadata["release_end_at"] == datetime(2026, 4, 17)
    assert metadata["release_end_status"] == "scheduled"


def test_merge_verification_metadata_keeps_netflix_source_start_before_tmdb_secondary_fallback():
    candidate = {
        "source_name": "netflix",
        "title": "기묘한 이야기: 1985년에는",
        "source_item": {
            "title": "기묘한 이야기: 1985년에는",
            "title_alias": ["Stranger Things: 1985", "Stranger Things"],
            "release_start_at": datetime(2026, 4, 23),
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "ok": True,
            "source": "official_public_page",
            "url": "https://www.netflix.com/kr/title/81398721",
            "title": "기묘한 이야기: 1985년에는, 지금 시청하세요 | 넷플릭스 공식 사이트",
            "payload_titles": ["기묘한 이야기: 1985년에는"],
            "body_text": "1985년 겨울, 다시 호킨스의 이야기가 펼쳐진다.",
            "description": "1985년 겨울, 다시 호킨스의 이야기가 펼쳐진다.",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
        },
        {
            "ok": True,
            "source": "tmdb",
            "url": "https://www.themoviedb.org/tv/999999",
            "title": "Unrelated Legacy Match",
            "payload_titles": ["Stranger Things"],
            "body_text": "Old series",
            "description": "Old series",
            "cast": [],
            "release_start_at": datetime(1959, 10, 2),
            "release_end_at": None,
            "release_end_status": "confirmed",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["release_start_at"] == datetime(2026, 4, 23)
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"
