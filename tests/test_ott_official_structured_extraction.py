from services import ott_verification_service as service


def test_extract_cast_from_text_parses_structured_disney_line() -> None:
    text = """
    상세 정보
    출연: 비 김 무열 빠니보틀 이 승훈
    전 세계 '크레이지'를 찾아 떠나는 지구상 가장 미친 여행
    """

    assert service._extract_cast_from_text(text) == [
        "비",
        "김무열",
        "빠니보틀",
        "이승훈",
    ]


def test_extract_structured_genre_text_reads_tving_genre_chip() -> None:
    text = """
    목 오후8:40
    예능 tvN 시즌 1개 3월 19일 공개
    출연 붐, 이용진, 정이랑, 서은광
    """

    assert service.normalize_ott_genres(
        service._extract_structured_genre_text(text),
        platform_source="tving",
    ) == ["variety"]


def test_extract_structured_genre_text_reads_disney_genre_line() -> None:
    text = """
    2026 • 시즌 1개 어드벤처, 액션, 리얼리티
    공개일: 2026
    """

    assert service.normalize_ott_genres(
        service._extract_structured_genre_text(text),
        platform_source="disney_plus",
    ) == ["variety"]


def test_resolve_verified_genres_prefers_official_structured_signal() -> None:
    genres = service._resolve_verified_genres(
        "tving",
        {"genre": "", "genres": [], "category": ""},
        [
            {
                "url": "https://www.tving.com/contents/P001783904",
                "source": "official_rendered_dom",
                "genre_text": "예능 tvN 시즌 1개",
                "description": "놀라운 목요일",
                "title": "놀라운 목요일 | TVING",
            }
        ],
        "예능 드라마 영화 스포츠 애니 뉴스 라이브",
    )

    assert genres == ["variety"]


def test_extract_cast_from_text_compacts_split_korean_names() -> None:
    text = """
    상세 정보
    출연:
    비 김 무열 빠니보틀 이 승훈
    """

    assert service._extract_cast_from_text(text) == ["비", "김무열", "빠니보틀", "이승훈"]
