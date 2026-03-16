from datetime import datetime

from services import ott_content_service as content_service
from services import ott_verification_service as verification_service


def test_rendered_official_sources_include_coupangplay():
    assert "coupangplay" in verification_service.RENDERED_OFFICIAL_SOURCES


def test_merge_verification_metadata_prefers_official_cast_and_caps_to_four():
    candidate = {
        "source_name": "tving",
        "title": "Example Drama",
        "source_item": {
            "title": "Example Drama",
            "description": "Drama series",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.tving.com/contents/P001",
            "ok": True,
            "title": "Example Drama",
            "payload_titles": ["Example Drama"],
            "body_text": "Drama series",
            "description": "Drama series",
            "genre_text": "드라마",
            "cast": ["Actor A", "Actor B", "Actor C", "Actor D", "Actor E"],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
        {
            "url": "https://namu.wiki/w/Example_Drama",
            "ok": True,
            "title": "Example Drama - NamuWiki",
            "payload_titles": ["Example Drama"],
            "body_text": "Variety cast Wiki A, Wiki B",
            "description": "예능",
            "genre_text": "예능",
            "cast": ["Wiki A", "Wiki B", "Wiki C", "Wiki D"],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        },
    ]

    metadata = verification_service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["cast"] == ["Actor A", "Actor B", "Actor C", "Actor D"]
    assert metadata["genre"] == "drama"


def test_merge_verification_metadata_falls_back_to_public_cast_when_official_missing():
    candidate = {
        "source_name": "disney_plus",
        "title": "Fallback Series",
        "source_item": {
            "title": "Fallback Series",
            "description": "Mystery drama",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.disneyplus.com/browse/entity-123",
            "ok": True,
            "title": "Fallback Series",
            "payload_titles": ["Fallback Series"],
            "body_text": "Mystery drama",
            "description": "Mystery drama",
            "genre_text": "드라마",
            "cast": [],
            "release_start_at": datetime(2026, 4, 2),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
        {
            "url": "https://en.wikipedia.org/wiki/Fallback_Series",
            "ok": True,
            "title": "Fallback Series",
            "payload_titles": ["Fallback Series"],
            "body_text": "Starring Emma Stone, Ryan Gosling, Carey Mulligan, John Boyega, Steven Yeun",
            "description": "Drama",
            "genre_text": "drama",
            "cast": ["Emma Stone", "Ryan Gosling", "Carey Mulligan", "John Boyega", "Steven Yeun"],
            "release_start_at": datetime(2026, 4, 2),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        },
    ]

    metadata = verification_service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["cast"] == ["Emma Stone", "Ryan Gosling", "Carey Mulligan", "John Boyega"]


def test_resolve_display_meta_uses_capped_cast_for_authors():
    meta = {
        "common": {},
        "ott": {
            "cast": ["Actor A", "Actor B", "Actor C", "Actor D", "Actor E"],
            "genres": ["drama"],
            "platforms": [],
        },
    }

    resolved_meta, _ = content_service.resolve_display_meta(meta, requested_sources=["netflix"])

    assert resolved_meta["common"]["authors"] == ["Actor A", "Actor B", "Actor C", "Actor D"]
    assert resolved_meta["ott"]["cast"] == ["Actor A", "Actor B", "Actor C", "Actor D"]


def test_build_canonical_meta_prefers_incoming_verified_cast_and_genre_over_existing_noise():
    meta, _ = content_service._build_canonical_meta(
        existing_meta={
            "common": {
                "authors": ["Noise A", "Noise B", "Noise C", "Noise D"],
                "genre": "drama",
                "genres": ["drama"],
            },
            "ott": {
                "cast": ["Noise A", "Noise B", "Noise C", "Noise D"],
                "genre": "drama",
                "genres": ["drama"],
            },
            "attributes": {
                "genre": "drama",
                "genres": ["drama"],
            },
        },
        entry={
            "cast": ["붐", "이용진", "정이랑", "서은광", "조째즈"],
            "genre": "예능",
            "genres": ["예능"],
            "platform_content_id": "tving-1",
            "platform_url": "https://www.tving.com/contents/P001783904",
            "availability_status": "scheduled",
        },
        platform_source="tving",
        now_value=datetime(2026, 3, 13),
    )

    assert meta["common"]["authors"] == ["붐", "이용진", "정이랑", "서은광"]
    assert meta["ott"]["cast"] == ["붐", "이용진", "정이랑", "서은광"]
    assert meta["common"]["genre"] == "variety"
    assert meta["ott"]["genre"] == "variety"
def test_resolve_cast_values_sanitizes_noisy_official_blob():
    resolved = verification_service._resolve_cast_values(
        "coupangplay",
        {"cast": []},
        [
            {
                "url": "https://www.coupangplay.com/content/example",
                "source": "official_coupang_metadata",
                "cast": [
                    "Jason Bateman, Linda Cardellini, David Harbour, Laura Linney director Steve Conrad watch now membership"
                ],
                "body_text": "Cast Jason Bateman, Linda Cardellini, David Harbour, Laura Linney Director Steve Conrad",
                "description": "Drama",
            }
        ],
    )

    assert resolved == [
        "Jason Bateman",
        "Linda Cardellini",
        "David Harbour",
        "Laura Linney",
    ]


def test_resolve_cast_values_prefers_structured_official_cast_over_noisy_body_text():
    resolved = verification_service._resolve_cast_values(
        "coupangplay",
        {"cast": []},
        [
            {
                "url": "https://www.coupangplay.com/content/example",
                "source": "official_coupang_metadata",
                "cast": ["제이슨 베이트먼", "데이빗 하버", "린다 카델리니", "플로이드 스머니치"],
                "body_text": "제이슨 베이트먼, 데이빗 하버, 린다 카델리니 감독 스티브 콘래드 쿠팡플레이 시작하기 회차 정보 와우 멤버십으로 더 많은 쿠팡플레이 혜택",
                "description": "드라마",
            }
        ],
    )

    assert resolved == [
        "제이슨 베이트먼",
        "데이빗 하버",
        "린다 카델리니",
        "플로이드 스머니치",
    ]


def test_extract_cast_from_text_requires_explicit_cast_label_not_verb_phrase():
    text = (
        "새로 돌아온 코미디 시리즈 스크럽스에서는 재크 브래프가 존 JD 도리안 역으로, "
        "도널드 파이슨이 크리스토퍼 터크 역으로, 사라 샤크가 엘리엇 리드 역으로 출연한다. "
        "원년 멤버 주디 레이예스와 존 C. 맥긴리는 각각 칼라와 페리 콕스 박사 역으로 특별 출연한다. "
        "출연: 재크 브래프 사라 샤크 도널드 파이슨 켄 젠킨스"
    )

    resolved = verification_service._extract_cast_from_text(text)

    assert resolved == ["재크 브래프", "사라 샤크", "도널드 파이슨", "켄 젠킨스"]
