import views.status as status_view
from app import app as flask_app


class FakeCursor:
    def __init__(self):
        self.closed = False

    def execute(self, _query):
        raise Exception("secret-details")

    def close(self):
        self.closed = True


def test_status_does_not_leak_exception_details(monkeypatch):
    cursor = FakeCursor()
    monkeypatch.setattr(status_view, "get_db", lambda: object())
    monkeypatch.setattr(status_view, "get_cursor", lambda _conn: cursor)

    flask_app.config["TESTING"] = True
    client = flask_app.test_client()

    response = client.get("/api/status")

    payload = response.get_json()
    assert response.status_code == 500
    assert payload == {"status": "error", "message": "internal error"}
    assert cursor.closed is True
    assert "secret-details" not in response.get_data(as_text=True)
