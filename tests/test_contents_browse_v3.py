import pytest

from app import app as flask_app
import views.contents as contents_view


class RecordingCursor:
    def __init__(self, fetchall_batches):
        self.fetchall_batches = list(fetchall_batches)
        self.executed = []
        self.closed = False
        self._fetchall_idx = 0

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        if self._fetchall_idx >= len(self.fetchall_batches):
            return []
        rows = self.fetchall_batches[self._fetchall_idx]
        self._fetchall_idx += 1
        rows = list(rows)
        if not self.executed:
            return rows
        query, params = self.executed[-1]
        if "novel_genre_groups &&" not in str(query):
            return rows
        requested_groups = next((param for param in (params or ()) if isinstance(param, list)), [])
        if not requested_groups:
            return rows
        requested_set = set(requested_groups)
        return [
            row
            for row in rows
            if requested_set & set(contents_view.extract_novel_genre_groups_from_meta(row.get("meta") or {}))
        ]

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _stub_db(monkeypatch, fetchall_batches):
    fake_cursor = RecordingCursor(fetchall_batches)
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    return fake_cursor


def _row(
    content_id,
    *,
    title=None,
    source="naver_webtoon",
    status=contents_view.STATUS_ONGOING,
    content_type="webtoon",
    meta=None,
):
    return {
        "content_id": content_id,
        "title": title or f"title-{content_id}",
        "status": status,
        "meta": meta or {"common": {"authors": ["writer"]}},
        "source": source,
        "content_type": content_type,
    }


def test_browse_v3_webtoon_applies_compact_filters(monkeypatch, client):
    cursor_token = contents_view.encode_cursor("A", "1", source="naver_webtoon")
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "100",
                title="B",
                source="naver_webtoon",
                content_type="webtoon",
                meta={
                    "common": {
                        "authors": ["author"],
                        "thumbnail_url": "https://img.example/100.jpg",
                        "content_url": "https://example.com/100",
                    },
                    "attributes": {"weekdays": ["mon"]},
                },
            )
        ]],
    )

    response = client.get(
        f"/api/contents/browse_v3?type=webtoon&status=ongoing&day=mon&sources=naver_webtoon,kakaowebtoon&cursor={cursor_token}&per_page=5"
    )
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[0]
    assert "source IN (%s, %s)" in query
    assert 'title COLLATE "ko-KR-x-icu"' in query
    assert "char_length(title)" in query
    assert "(meta->'attributes'->'weekdays') ? %s" in query
    assert query.count("%s") == len(params)
    assert params[-1] == 5
    assert payload["filters"]["type"] == "webtoon"
    assert payload["filters"]["status"] == "ongoing"
    assert payload["filters"]["days"] == ["mon"]
    card = payload["contents"][0]
    assert card["display_meta"]["authors"] == ["author"]
    assert card["thumbnail_url"] == "https://img.example/100.jpg"
    assert card["content_url"] == "https://example.com/100"
    assert card["cursor"] is not None


def test_browse_v3_novel_completed_respects_genre_groups(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-1",
                source="ridi",
                status=contents_view.STATUS_COMPLETED,
                content_type="novel",
                meta={"attributes": {"genres": ["fantasy"]}, "common": {"authors": ["a"]}},
            ),
            _row(
                "romance-1",
                source="ridi",
                status=contents_view.STATUS_COMPLETED,
                content_type="novel",
                meta={"attributes": {"genres": ["romance"]}, "common": {"authors": ["b"]}},
            ),
        ]],
    )

    response = client.get(
        "/api/contents/browse_v3?type=novel&status=completed&genre_group=fantasy&per_page=10"
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["filters"]["status"] == "completed"
    assert payload["filters"]["genre_groups"] == ["FANTASY"]
    assert payload["filters"]["is_completed"] is True
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-1"}
    query, _ = fake_cursor.executed[0]
    assert "novel_genre_groups && %s::text[]" in query
    card = payload["contents"][0]
    assert card["display_meta"]["genres"] == ["fantasy"]
    assert fake_cursor.closed is True


def test_browse_v3_resolves_rows_once_before_serializing(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "ott-1",
                source=contents_view.OTT_CANONICAL_SOURCE,
                content_type="ott",
                meta={
                    "common": {"authors": ["author"], "content_url": "https://example.com/ott-1"},
                    "ott": {"platforms": ["netflix"]},
                },
            )
        ]],
    )

    original_resolver = contents_view._resolve_row_for_display
    resolve_calls = []

    def _recording_resolver(row, *, requested_sources=None):
        resolve_calls.append((row.get("content_id"), tuple(requested_sources or [])))
        return original_resolver(row, requested_sources=requested_sources)

    monkeypatch.setattr(contents_view, "_resolve_row_for_display", _recording_resolver)

    response = client.get("/api/contents/browse_v3?type=ott&sources=netflix&per_page=5")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["returned"] == 1
    assert resolve_calls == [("ott-1", ("netflix",))]
    assert fake_cursor.closed is True
