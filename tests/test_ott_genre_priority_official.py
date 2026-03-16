from services import ott_verification_service as service


def test_resolve_verified_genres_prefers_official_genre_over_source_noise():
    source_item = {
        "genre": "예능",
        "genres": ["예능"],
        "category": "예능",
    }
    matched_docs = [
        {
            "ok": True,
            "source": "official_rendered_dom",
            "title": "대한민국에서 건물주 되는 법 | TVING",
            "genre_text": "드라마",
            "description": "서스펜스 드라마",
            "body_text": "장르 드라마",
        }
    ]

    genres = service._resolve_verified_genres(
        "tving",
        source_item,
        matched_docs,
        "대한민국에서 건물주 되는 법",
    )

    assert genres == ["drama"]


def test_resolve_verified_genres_ignores_noisy_official_host_copy_and_accepts_tmdb_scripted_genre():
    source_item = {"genre": "", "genres": [], "category": ""}
    matched_docs = [
        {
            "ok": True,
            "source": "official_coupang_metadata",
            "title": "DTF 세인트루이스",
            "genre_text": "코미디 수사물",
            "description": "코미디 수사물",
            "body_text": "코미디 수사물",
        },
        {
            "ok": True,
            "source": None,
            "title": "DTF 세인트루이스",
            "genre_text": "최신 예능 라이브 정주행",
            "description": "HBO 등 해외 명작부터, 국내 최신 예능 라이브 정주행",
            "body_text": "HBO 등 해외 명작부터, 국내 최신 예능 라이브 정주행",
        },
        {
            "ok": True,
            "source": "tmdb",
            "title": "Dope Thief",
            "genre_text": "Crime, Drama",
            "description": "Crime drama",
            "body_text": "Crime drama",
        },
    ]

    genres = service._resolve_verified_genres(
        "coupangplay",
        source_item,
        matched_docs,
        "코미디 수사물",
    )

    assert genres == ["drama"]
