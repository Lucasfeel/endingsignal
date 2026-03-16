from datetime import datetime, timedelta

import pytest

from app import app as flask_app
import views.contents as contents_view


TYPE_PRIORITY = {
    "webtoon": 0,
    "novel": 1,
    "ott": 2,
}


class FakeCursor:
    def __init__(self, rows_by_type):
        self.rows_by_type = rows_by_type
        self.executed = []
        self.closed = False
        self._rows = []

    def execute(self, query, params=None):
        self.executed.append((query, params))
        per_type = params[2] if params and len(params) > 2 else None
        limit = params[3] if params and len(params) > 3 else None
        self._rows = self._materialize_rows(per_type=per_type, limit=limit)

    def _materialize_rows(self, *, per_type, limit):
        merged = []
        for content_type in ("webtoon", "novel", "ott"):
            rows = list(self.rows_by_type.get(content_type, []))
            rows.sort(
                key=lambda row: (
                    -row["updated_at"].timestamp(),
                    row["content_id"],
                )
            )
            if isinstance(per_type, int):
                rows = rows[:per_type]
            merged.extend(rows)

        merged.sort(
            key=lambda row: (
                -row["updated_at"].timestamp(),
                TYPE_PRIORITY.get(row["content_type"], 99),
                row["content_id"],
            )
        )

        deduped = []
        seen_keys = set()
        for row in merged:
            key = (row["content_id"], row["source"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)
            if isinstance(limit, int) and len(deduped) >= limit:
                break

        return deduped

    def fetchall(self):
        return list(self._rows)

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _make_row(content_id, source, content_type, updated_rank, *, status=None, meta=None):
    updated_at = datetime(2026, 1, 1, 0, 0, 0) + timedelta(minutes=updated_rank)
    return {
        "content_id": content_id,
        "title": f"title-{content_id}",
        "status": status or contents_view.STATUS_ONGOING,
        "meta": meta or {"common": {"authors": ["a"]}},
        "source": source,
        "content_type": content_type,
        "updated_at": updated_at,
    }


def _stub_contents_db(monkeypatch, rows_by_type):
    fake_cursor = FakeCursor(rows_by_type)
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    return fake_cursor


def test_recommendations_returns_list_with_required_fields(monkeypatch, client):
    fake_cursor = _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [_make_row("w-1", "naver_webtoon", "webtoon", 3)],
            "novel": [_make_row("n-1", "ridi", "novel", 2)],
            "ott": [_make_row("o-1", "netflix", "ott", 1)],
        },
    )

    response = client.get("/api/contents/recommendations")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 3
    required_keys = {"content_id", "title", "status", "meta", "source", "content_type"}
    assert required_keys.issubset(payload[0].keys())
    assert all(isinstance(item.get("meta"), dict) for item in payload)
    assert fake_cursor.closed is True
    assert len(fake_cursor.executed) == 1


def test_recommendations_limit_clamps_low(monkeypatch, client):
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [_make_row("w-1", "naver_webtoon", "webtoon", 30)],
            "novel": [_make_row("n-1", "ridi", "novel", 20)],
            "ott": [_make_row("o-1", "netflix", "ott", 10)],
        },
    )

    response = client.get("/api/contents/recommendations?limit=0")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 1


def test_recommendations_limit_clamps_high(monkeypatch, client):
    webtoon_rows = [_make_row(f"w-{i}", "naver_webtoon", "webtoon", 300 - i) for i in range(60)]
    novel_rows = [_make_row(f"n-{i}", "ridi", "novel", 200 - i) for i in range(60)]
    ott_rows = [_make_row(f"o-{i}", "netflix", "ott", 100 - i) for i in range(60)]
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": webtoon_rows,
            "novel": novel_rows,
            "ott": ott_rows,
        },
    )

    response = client.get("/api/contents/recommendations?limit=999")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 50


def test_recommendations_dedupes_by_content_and_source(monkeypatch, client):
    duplicate_old = _make_row("dup-1", "shared", "webtoon", 10)
    duplicate_new = _make_row("dup-1", "shared", "novel", 50)
    unique_row = _make_row("unique-1", "netflix", "ott", 40)

    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [duplicate_old],
            "novel": [duplicate_new],
            "ott": [unique_row],
        },
    )

    response = client.get("/api/contents/recommendations?limit=12")
    payload = response.get_json()

    assert response.status_code == 200
    assert isinstance(payload, list)
    assert len(payload) == 2
    assert len({(item["content_id"], item["source"]) for item in payload}) == 2
    duplicate_item = next(item for item in payload if item["content_id"] == "dup-1")
    assert duplicate_item["content_type"] == "novel"


def test_recommendations_preserve_type_priority_when_updated_at_ties(monkeypatch, client):
    shared_rank = 100
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [_make_row("w-1", "naver_webtoon", "webtoon", shared_rank)],
            "novel": [_make_row("n-1", "ridi", "novel", shared_rank)],
            "ott": [_make_row("o-1", "netflix", "ott", shared_rank)],
        },
    )

    response = client.get("/api/contents/recommendations?limit=3")
    payload = response.get_json()

    assert response.status_code == 200
    assert [item["content_type"] for item in payload] == ["webtoon", "novel", "ott"]


def test_recommendations_v2_returns_compact_card_payload(monkeypatch, client):
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [
                _make_row("w-1", "naver_webtoon", "webtoon", 3, status=contents_view.STATUS_HIATUS)
            ],
            "novel": [],
            "ott": [],
        },
    )

    response = client.get("/api/contents/recommendations_v2?limit=12")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["returned"] == 1
    assert payload["limit"] == 12
    card = payload["contents"][0]
    assert card["content_id"] == "w-1"
    assert card["source"] == "naver_webtoon"
    assert card["content_type"] == "webtoon"
    assert card["status"] == contents_view.STATUS_HIATUS
    assert card["final_state_badge"] == contents_view.STATUS_HIATUS
    assert card["display_meta"]["authors"] == ["a"]
    assert card["cursor"] is not None


def test_recommendations_v2_keeps_novel_genres_without_ott_normalization(monkeypatch, client):
    _stub_contents_db(
        monkeypatch,
        {
            "webtoon": [],
            "novel": [
                _make_row(
                    "n-1",
                    "ridi",
                    "novel",
                    5,
                    meta={
                        "common": {"authors": ["writer"]},
                        "attributes": {"genres": ["fantasy", "판타지"]},
                    },
                )
            ],
            "ott": [],
        },
    )

    response = client.get("/api/contents/recommendations_v2?limit=12")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["returned"] == 1
    card = payload["contents"][0]
    assert card["content_type"] == "novel"
    assert card["display_meta"]["genres"] == ["fantasy", "판타지"]
