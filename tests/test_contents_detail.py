import pytest

from app import app as flask_app
import views.contents as contents_view


class RecordingCursor:
    def __init__(self, fetchone_result):
        self.fetchone_result = fetchone_result
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def test_detail_missing_required_params_returns_400(monkeypatch, client):
    def _fail_get_db():
        raise AssertionError("DB should not be called when params are missing")

    monkeypatch.setattr(contents_view, "get_db", _fail_get_db)

    response = client.get("/api/contents/detail")
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["success"] is False
    assert payload["error"]["code"] == "BAD_REQUEST"


def test_detail_not_found_returns_404(monkeypatch, client):
    fake_cursor = RecordingCursor(fetchone_result=None)
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = client.get("/api/contents/detail?content_id=cid-404&source=ridi")
    payload = response.get_json()

    assert response.status_code == 404
    assert payload["success"] is False
    assert payload["error"]["code"] == "NOT_FOUND"
    query, params = fake_cursor.executed[0]
    assert "COALESCE(is_deleted, FALSE) = FALSE" in query
    assert params == ("cid-404", "ridi")
    assert fake_cursor.closed is True


def test_detail_found_returns_content_with_meta_dict(monkeypatch, client):
    fake_cursor = RecordingCursor(
        fetchone_result={
            "content_id": "cid-1",
            "title": "Sample",
            "status": contents_view.STATUS_ONGOING,
            "meta": '{"common": {"authors": ["a"]}}',
            "source": "ridi",
            "content_type": "novel",
        }
    )
    monkeypatch.setattr(contents_view, "get_db", lambda: object())
    monkeypatch.setattr(contents_view, "get_cursor", lambda _conn: fake_cursor)

    response = client.get("/api/contents/detail?content_id=cid-1&source=ridi")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["content_id"] == "cid-1"
    assert payload["source"] == "ridi"
    assert isinstance(payload["meta"], dict)
    assert payload["meta"]["common"]["authors"] == ["a"]
    assert fake_cursor.closed is True
