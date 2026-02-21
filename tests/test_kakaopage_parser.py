import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from services.kakaopage_parser import STATUS_COMPLETED, STATUS_ONGOING, parse_kakaopage_detail


def _fixture_text(name: str) -> str:
    path = Path(__file__).resolve().parent / "fixtures" / name
    return path.read_text(encoding="utf-8")


def test_parse_kakaopage_ongoing_detail_fixture():
    html = _fixture_text("kakaopage_content_ongoing.html")

    parsed = parse_kakaopage_detail(html, fallback_genres=["현판"])

    assert parsed["title"] == "회귀자의 서재"
    assert parsed["authors"] == ["김연재"]
    assert parsed["status"] == STATUS_ONGOING
    assert "판타지" in parsed["genres"]
    assert "현판" in parsed["genres"]


def test_parse_kakaopage_completed_detail_fixture():
    html = _fixture_text("kakaopage_content_completed.html")

    parsed = parse_kakaopage_detail(html)

    assert parsed["title"] == "황혼의 계약자"
    assert parsed["authors"] == ["박완결"]
    assert parsed["status"] == STATUS_COMPLETED
    assert "로맨스 판타지" in parsed["genres"]
