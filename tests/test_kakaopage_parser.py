import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from services.kakaopage_parser import (
    STATUS_COMPLETED,
    STATUS_ONGOING,
    is_noise_author_token,
    parse_kakaopage_detail,
)


def _fixture_text(name: str) -> str:
    path = Path(__file__).resolve().parent / "fixtures" / name
    return path.read_text(encoding="utf-8")


def test_parse_kakaopage_ongoing_detail_fixture():
    html = _fixture_text("kakaopage_content_ongoing.html")

    parsed = parse_kakaopage_detail(html, fallback_genres=["fallback-genre"])

    assert parsed["title"]
    assert parsed["authors"]
    assert parsed["status"] == STATUS_ONGOING
    assert "fallback-genre" in parsed["genres"]


def test_parse_kakaopage_completed_detail_fixture():
    html = _fixture_text("kakaopage_content_completed.html")

    parsed = parse_kakaopage_detail(html)

    assert parsed["title"]
    assert parsed["authors"]
    assert parsed["status"] == STATUS_COMPLETED


def test_parse_kakaopage_authors_from_meta_ignores_noise():
    html = _fixture_text("kakaopage_content_meta_author_noise.html")

    parsed = parse_kakaopage_detail(html)

    assert parsed["title"] == "Meta Author Novel"
    assert parsed["authors"] == ["Jane Doe"]
    assert "\ub0b4\uc5ed" not in parsed["authors"]


def test_parse_kakaopage_authors_from_json_ld_ignores_noise():
    html = _fixture_text("kakaopage_content_jsonld_author_noise.html")

    parsed = parse_kakaopage_detail(html)

    assert parsed["title"] == "JSON LD Novel"
    assert "\ub0b4\uc5ed" not in parsed["authors"]
    assert "\ud64d\uae38\ub3d9" in parsed["authors"]
    assert "Alex Writer" in parsed["authors"]


def test_parse_kakaopage_authors_from_next_data_ignores_noise():
    html = _fixture_text("kakaopage_content_next_data_author.html")

    parsed = parse_kakaopage_detail(html)

    assert parsed["title"] == "Next Data Novel"
    assert parsed["authors"] == ["Kim Writer", "\ud64d\uae38\ub3d9"]
    assert parsed.get("_author_source") == "next_data"


def test_parse_kakaopage_author_noise_variants_are_rejected():
    html = _fixture_text("kakaopage_content_meta_author_noise_variants.html")

    parsed = parse_kakaopage_detail(html)

    assert parsed["authors"] == ["Jane Real"]


def test_noise_author_token_variants():
    variants = [
        "\ub0b4\uc5ed",
        "\ub0b4\uc5ed\ubcf4\uae30",
        "\ub0b4\u200b\uc5ed",
        "\ub0b4\uc5ed\u00a0",
    ]

    for token in variants:
        assert is_noise_author_token(token) is True
