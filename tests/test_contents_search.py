import pytest

from app import app as flask_app
import views.contents as contents_view


class RecordingCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def _stub_db(monkeypatch, rows):
    fake_cursor = RecordingCursor(rows)
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)
    return fake_cursor


def test_search_uses_candidate_rollup_and_search_document(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [
            {
                "content_id": "cid-1",
                "title": "테스트 제목",
                "status": contents_view.STATUS_ONGOING,
                "meta": {"common": {"authors": ["작가A"]}},
                "source": "ridi",
                "content_type": "novel",
            }
        ],
    )

    response = client.get("/api/contents/search?q=테스트")
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[-1]
    assert "WITH candidate_hits AS" in query
    assert "candidate_rollup" in query
    assert "search_document %% %s" in query
    assert "content_type = %s" not in query
    assert "source = %s" not in query
    assert "rollup.title_exact DESC" in query
    assert "rollup.author_exact DESC" in query
    assert payload[0]["content_type"] == "novel"
    assert fake_cursor.closed is True
    assert "webtoon" not in tuple(str(value) for value in (params or ()))


def test_search_applies_type_and_source_filters_in_candidate_stage(monkeypatch, client):
    fake_cursor = _stub_db(monkeypatch, [])

    response = client.get("/api/contents/search?q=abcd&type=novel&source=ridi")

    assert response.status_code == 200
    query, params = fake_cursor.executed[-1]
    assert "content_type = %s" in query
    assert "source = %s" in query
    assert params.count("novel") >= 1
    assert params.count("ridi") >= 1


def test_search_ranks_title_matches_before_author_matches(monkeypatch, client):
    fake_cursor = _stub_db(monkeypatch, [])

    response = client.get("/api/contents/search?q=abcd")

    assert response.status_code == 200
    query, _ = fake_cursor.executed[-1]
    title_order_index = query.index("rollup.title_exact DESC")
    author_order_index = query.index("rollup.author_exact DESC")
    assert title_order_index < author_order_index
    assert "similarity(COALESCE(c.search_document, ''), %s)" in query


def test_search_single_character_query_skips_search_document_candidate_branch(monkeypatch, client):
    fake_cursor = _stub_db(monkeypatch, [])

    response = client.get("/api/contents/search?q=비")

    assert response.status_code == 200
    query, params = fake_cursor.executed[-1]
    assert "search_document LIKE %s" not in query
    assert "similarity(COALESCE(c.search_document, ''), %s)" in query
    assert "%비%" in tuple(str(value) for value in (params or ()))
