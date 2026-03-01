import base64
import json

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
        return list(rows)

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
        "meta": meta or {},
        "source": source,
        "content_type": content_type,
    }


def _legacy_cursor(title, content_id):
    payload = {"t": title, "id": content_id}
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return encoded.rstrip("=")


def test_decode_cursor_supports_legacy_and_new_payload():
    legacy = _legacy_cursor("legacy-title", "legacy-id")
    assert contents_view.decode_cursor(legacy) == ("legacy-title", None, "legacy-id")

    new_cursor = contents_view.encode_cursor("new-title", "new-id", source="ridi")
    assert contents_view.decode_cursor(new_cursor) == ("new-title", "ridi", "new-id")


def test_ongoing_v2_applies_day_and_cursor_filters(monkeypatch, client):
    cursor_token = contents_view.encode_cursor("A", "1", source="naver_webtoon")
    fake_cursor = _stub_db(
        monkeypatch,
        [[_row("100", source="naver_webtoon", content_type="webtoon")]],
    )

    response = client.get(
        f"/api/contents/ongoing_v2?type=webtoon&day=mon&source=naver_webtoon&cursor={cursor_token}&per_page=5"
    )
    payload = response.get_json()

    assert response.status_code == 200
    executed_query, _ = fake_cursor.executed[0]
    assert "(meta->'attributes'->'weekdays') ? %s" in executed_query
    assert "(title, source, content_id) > (%s, %s, %s)" in executed_query
    assert "ORDER BY title ASC, source ASC, content_id ASC" in executed_query

    assert isinstance(payload.get("contents"), list)
    assert "next_cursor" in payload
    assert payload["page_size"] == 5
    assert payload["returned"] == 1
    assert payload["filters"]["type"] == "webtoon"
    assert payload["filters"]["day"] == "mon"


def test_novels_v2_applies_completed_filter(monkeypatch, client):
    fake_cursor = _stub_db(monkeypatch, [[]])

    response = client.get("/api/contents/novels_v2?is_completed=true")
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[0]
    assert "status = %s" in query
    assert contents_view.STATUS_COMPLETED in params
    assert payload["filters"]["is_completed"] is True


def test_novels_v2_genre_group_filtering_kept(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uD604\uD310"]}},
            ),
            _row(
                "romance-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
            ),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genre_group=fantasy&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-1"}
    assert len(fake_cursor.executed) == 1


def test_novels_v2_supports_comma_separated_multi_genres(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uD604\uD310"]}},
            ),
            _row(
                "romance-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
            ),
            _row(
                "wuxia-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uBB34\uD611"]}},
            ),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genre_group=fantasy,romance&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-1", "romance-1"}
    assert set(payload["filters"]["genre_groups"]) == {"FANTASY", "ROMANCE"}
    assert payload["filters"]["genre_group"] == "ALL"
    assert len(fake_cursor.executed) == 1


def test_novels_v2_supports_repeated_multi_genre_params(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uD604\uD310"]}},
            ),
            _row(
                "romance-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
            ),
            _row(
                "wuxia-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uBB34\uD611"]}},
            ),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genre_group=fantasy&genre_group=romance&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-1", "romance-1"}
    assert set(payload["filters"]["genre_groups"]) == {"FANTASY", "ROMANCE"}
    assert len(fake_cursor.executed) == 1


def test_novels_v2_legacy_alias_supports_multi_genres(monkeypatch, client):
    _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uD604\uD310"]}},
            ),
            _row(
                "romance-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
            ),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genreGroup=fantasy,romance&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-1", "romance-1"}
    assert set(payload["filters"]["genre_groups"]) == {"FANTASY", "ROMANCE"}


def test_novels_v2_ignores_unknown_tokens_when_valid_exists(monkeypatch, client):
    _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uD604\uD310"]}},
            ),
            _row(
                "romance-1",
                source="ridi",
                content_type="novel",
                meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
            ),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genre_group=fantasy,unknown&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-1"}
    assert payload["filters"]["genre_groups"] == ["FANTASY"]
    assert payload["filters"]["genre_group"] == "FANTASY"


def test_novels_v2_completed_filter_combines_with_multi_genres(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row(
                "fantasy-completed",
                source="ridi",
                status=contents_view.STATUS_COMPLETED,
                content_type="novel",
                meta={"attributes": {"genres": ["\uD604\uD310"]}},
            ),
            _row(
                "romance-completed",
                source="ridi",
                status=contents_view.STATUS_COMPLETED,
                content_type="novel",
                meta={"attributes": {"genres": ["\uB85C\uB9E8\uC2A4"]}},
            ),
            _row(
                "wuxia-completed",
                source="ridi",
                status=contents_view.STATUS_COMPLETED,
                content_type="novel",
                meta={"attributes": {"genres": ["\uBB34\uD611"]}},
            ),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genre_group=fantasy,romance&is_completed=true&per_page=10")
    payload = response.get_json()

    assert response.status_code == 200
    ids = {item["content_id"] for item in payload["contents"]}
    assert ids == {"fantasy-completed", "romance-completed"}

    query, params = fake_cursor.executed[0]
    assert "status = %s" in query
    assert contents_view.STATUS_COMPLETED in params


def test_novels_v2_returns_next_cursor_when_page_is_full(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [[
            _row("novel-1", title="A", source="ridi", content_type="novel"),
        ]],
    )

    response = client.get("/api/contents/novels_v2?genre_group=all&per_page=1")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["returned"] == 1
    assert payload["page_size"] == 1
    assert payload["next_cursor"] is not None

    decoded = contents_view.decode_cursor(payload["next_cursor"])
    assert decoded == ("A", "ridi", "novel-1")
    assert fake_cursor.closed is True


def test_completed_supports_sources_and_3field_cursor(monkeypatch, client):
    cursor_token = contents_view.encode_cursor("A", "1", source="naver_webtoon")
    fake_cursor = _stub_db(
        monkeypatch,
        [[_row("finished-1", status=contents_view.STATUS_COMPLETED)]],
    )

    response = client.get(
        f"/api/contents/completed?type=webtoon&sources=naver_webtoon,kakaowebtoon&cursor={cursor_token}&per_page=2"
    )
    payload = response.get_json()

    assert response.status_code == 200
    query, _ = fake_cursor.executed[0]
    assert "source IN (%s, %s)" in query
    assert "(title, source, content_id) > (%s, %s, %s)" in query
    assert "ORDER BY title ASC, source ASC, content_id ASC" in query
    assert "contents" in payload
    assert "next_cursor" in payload


def test_hiatus_supports_sources_and_3field_cursor(monkeypatch, client):
    cursor_token = contents_view.encode_cursor("A", "1", source="naver_webtoon")
    fake_cursor = _stub_db(
        monkeypatch,
        [[_row("pause-1", status=contents_view.STATUS_HIATUS)]],
    )

    response = client.get(
        f"/api/contents/hiatus?type=webtoon&sources=naver_webtoon,kakaowebtoon&cursor={cursor_token}&per_page=2"
    )

    assert response.status_code == 200
    query, _ = fake_cursor.executed[0]
    assert "source IN (%s, %s)" in query
    assert "(title, source, content_id) > (%s, %s, %s)" in query
    assert "ORDER BY title ASC, source ASC, content_id ASC" in query


def test_completed_accepts_legacy_cursor_without_source(monkeypatch, client):
    legacy = _legacy_cursor("legacy-title", "legacy-id")
    fake_cursor = _stub_db(monkeypatch, [[]])

    response = client.get(f"/api/contents/completed?cursor={legacy}&per_page=2")
    assert response.status_code == 200

    query, _ = fake_cursor.executed[0]
    assert "(title, content_id) > (%s, %s)" in query
    assert "(title, source, content_id) > (%s, %s, %s)" not in query
