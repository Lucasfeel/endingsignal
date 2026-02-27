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


def test_search_defaults_to_all_types_and_uses_normalized_fallback(monkeypatch, client):
    fake_cursor = _stub_db(
        monkeypatch,
        [
            {
                "content_id": "cid-1",
                "title": "테스트 제목",
                "status": contents_view.STATUS_ONGOING,
                "meta": {"common": {"authors": "작가A"}},
                "source": "ridi",
                "content_type": "novel",
            }
        ],
    )

    response = client.get("/api/contents/search?q=테스트")
    payload = response.get_json()

    assert response.status_code == 200
    query, params = fake_cursor.executed[0]
    assert "content_type = %s" not in query
    assert "source = %s" not in query
    assert "NULLIF(normalized_title, '')" in query
    assert "NULLIF(normalized_authors, '')" in query
    assert payload[0]["content_type"] == "novel"
    assert fake_cursor.closed is True
    assert "webtoon" not in tuple(str(value) for value in (params or ()))


def test_search_applies_type_and_source_filters_when_requested(monkeypatch, client):
    fake_cursor = _stub_db(monkeypatch, [])

    response = client.get("/api/contents/search?q=abcd&type=novel&source=ridi")

    assert response.status_code == 200
    query, params = fake_cursor.executed[0]
    assert "content_type = %s" in query
    assert "source = %s" in query
    assert params[0] == "novel"
    assert params[1] == "ridi"


def test_search_uses_or_similarity_clause_for_long_queries(monkeypatch, client):
    fake_cursor = _stub_db(monkeypatch, [])

    response = client.get("/api/contents/search?q=abcd")

    assert response.status_code == 200
    query, _ = fake_cursor.executed[0]
    assert "OR (similarity(" in query
    assert "AND (similarity(" not in query


def test_search_short_query_returns_empty_without_db_call(monkeypatch, client):
    def _fail_get_db():
        raise AssertionError("DB should not be called for 1-char query")

    monkeypatch.setattr(contents_view, "get_db", _fail_get_db)

    response = client.get("/api/contents/search?q=a")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload == []
