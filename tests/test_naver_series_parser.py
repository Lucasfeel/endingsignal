import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from services.naver_series_parser import STATUS_COMPLETED, STATUS_ONGOING, parse_naver_series_list


def _fixture_text(name: str) -> str:
    path = Path(__file__).resolve().parent / "fixtures" / name
    return path.read_text(encoding="utf-8")


def test_parse_naver_series_list_fixture_extracts_required_fields():
    html = _fixture_text("naver_series_page1.html")

    parsed = parse_naver_series_list(html, is_finished_page=False)

    assert len(parsed) == 2
    first = parsed[0]
    second = parsed[1]

    assert first["content_id"] == "12345"
    assert first["title"] == "별빛 연대기"
    assert first["authors"] == ["김작가"]
    assert first["status"] == STATUS_ONGOING

    assert second["content_id"] == "67890"
    assert second["authors"] == ["이작가", "박작가"]
    assert second["status"] == STATUS_COMPLETED
