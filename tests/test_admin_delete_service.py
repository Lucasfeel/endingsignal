from datetime import datetime

import services.admin_delete_service as delete_service


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.last_result = None
        self.rowcount = 0

    def execute(self, query, params):
        if "FROM contents" in query and "WHERE content_id = %s AND source = %s" in query:
            content_id, source = params
            row = self.db.contents.get((content_id, source))
            self.last_result = [] if row is None else [row]
        elif "UPDATE contents" in query and "SET is_deleted = TRUE" in query:
            reason, admin_id, content_id, source = params
            row = self.db.contents[(content_id, source)]
            row.update(
                {
                    "is_deleted": True,
                    "deleted_at": self.db.now,
                    "deleted_reason": reason,
                    "deleted_by": admin_id,
                }
            )
            self.last_result = []
        elif "UPDATE contents" in query and "SET is_deleted = FALSE" in query:
            content_id, source = params
            row = self.db.contents[(content_id, source)]
            row.update(
                {
                    "is_deleted": False,
                    "deleted_at": None,
                    "deleted_reason": None,
                    "deleted_by": None,
                }
            )
            self.last_result = []
        elif "DELETE FROM subscriptions" in query:
            content_id, source = params
            before = len(self.db.subscriptions)
            self.db.subscriptions = {
                sub
                for sub in self.db.subscriptions
                if not (sub[0] == content_id and sub[1] == source)
            }
            self.rowcount = before - len(self.db.subscriptions)
            self.last_result = []
        elif "COALESCE(is_deleted, FALSE) = TRUE" in query:
            if len(params) == 4:
                like_title, like_normalized, limit, offset = params
                needle = like_title.strip("%").lower()
            else:
                limit, offset = params
                needle = None
            rows = [
                row
                for row in self.db.contents.values()
                if row["is_deleted"]
                and (
                    needle is None
                    or needle in row["title"].lower()
                    or needle in (row.get("normalized_title") or "").lower()
                )
            ]
            rows.sort(
                key=lambda row: (
                    row["deleted_at"] is None,
                    row["deleted_at"],
                    row["title"],
                ),
                reverse=True,
            )
            self.last_result = rows[offset : offset + limit]
        else:
            raise NotImplementedError(query)

    def fetchone(self):
        if not self.last_result:
            return None
        return self.last_result[0]

    def fetchall(self):
        return self.last_result or []

    def close(self):
        pass


class FakeDB:
    def __init__(self, contents, subscriptions=None, now=None):
        self.contents = contents
        self.subscriptions = subscriptions or set()
        self.now = now or datetime(2024, 1, 1, 0, 0, 0)
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class RowNoGet:
    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        return self.data[key]

    def __contains__(self, key):
        return key in self.data


def test_soft_delete_missing_content_returns_not_found_and_no_commit(monkeypatch):
    db = FakeDB(contents={})

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = delete_service.soft_delete_content(
        db,
        admin_id=1,
        content_id="CID",
        source="SRC",
        reason="spam",
    )

    assert result == {"error": "CONTENT_NOT_FOUND"}
    assert db.committed is False


def test_soft_delete_marks_deleted_and_commits_and_retains_subscriptions(monkeypatch):
    contents = {
        ("CID", "SRC"): {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "comic",
            "title": "Title",
            "normalized_title": "title",
            "status": "active",
            "is_deleted": False,
            "meta": {},
            "deleted_at": None,
            "deleted_reason": None,
            "deleted_by": None,
        }
    }
    subscriptions = {
        ("CID", "SRC", 1),
        ("CID", "SRC", 2),
        ("OTHER", "SRC", 3),
    }
    db = FakeDB(contents=contents, subscriptions=subscriptions, now=datetime(2024, 2, 1, 0, 0, 0))

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = delete_service.soft_delete_content(
        db,
        admin_id=7,
        content_id="CID",
        source="SRC",
        reason="duplicate",
    )

    assert result["content"]["is_deleted"] is True
    assert result["content"]["deleted_reason"] == "duplicate"
    assert result["subscriptions_retained"] is True
    assert "subscriptions_deleted" not in result
    assert ("CID", "SRC", 1) in db.subscriptions
    assert ("CID", "SRC", 2) in db.subscriptions


def test_soft_delete_is_idempotent_when_already_deleted(monkeypatch):
    existing = datetime(2024, 3, 1, 0, 0, 0)
    contents = {
        ("CID", "SRC"): {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "comic",
            "title": "Title",
            "normalized_title": "title",
            "status": "active",
            "is_deleted": True,
            "meta": {},
            "deleted_at": existing,
            "deleted_reason": "original",
            "deleted_by": 2,
        }
    }
    db = FakeDB(contents=contents, subscriptions={("CID", "SRC", 1)})

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = delete_service.soft_delete_content(
        db,
        admin_id=9,
        content_id="CID",
        source="SRC",
        reason="new",
    )

    assert result["content"]["deleted_reason"] == "original"
    assert result["content"]["deleted_by"] == 2
    assert result["subscriptions_retained"] is True
    assert ("CID", "SRC", 1) in db.subscriptions


def test_serialize_deleted_content_row_handles_row_without_get():
    row = RowNoGet(
        {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "comic",
            "title": "Title",
            "status": "active",
            "is_deleted": True,
            "meta": {},
            "deleted_at": None,
            "deleted_reason": "spam",
            "deleted_by": 1,
            "override_status": "완결",
            "override_completed_at": None,
        }
    )

    result = delete_service._serialize_deleted_content_row(row)

    assert result["override_status"] == "완결"
    assert result["override_completed_at"] is None
    assert result["subscription_count"] == 0


def test_restore_missing_content_returns_not_found(monkeypatch):
    db = FakeDB(contents={})

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = delete_service.restore_content(db, content_id="CID", source="SRC")

    assert result == {"error": "CONTENT_NOT_FOUND"}
    assert db.committed is False


def test_restore_clears_deleted_fields_and_commits(monkeypatch):
    contents = {
        ("CID", "SRC"): {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "comic",
            "title": "Title",
            "normalized_title": "title",
            "status": "active",
            "is_deleted": True,
            "meta": {},
            "deleted_at": datetime(2024, 4, 1, 0, 0, 0),
            "deleted_reason": "spam",
            "deleted_by": 4,
        }
    }
    db = FakeDB(contents=contents)

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = delete_service.restore_content(db, content_id="CID", source="SRC")

    assert result["content"]["is_deleted"] is False
    assert result["content"]["deleted_at"] is None


def test_restore_is_idempotent_when_not_deleted(monkeypatch):
    contents = {
        ("CID", "SRC"): {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "comic",
            "title": "Title",
            "normalized_title": "title",
            "status": "active",
            "is_deleted": False,
            "meta": {},
            "deleted_at": None,
            "deleted_reason": None,
            "deleted_by": None,
        }
    }
    db = FakeDB(contents=contents)

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = delete_service.restore_content(db, content_id="CID", source="SRC")

    assert result["content"]["is_deleted"] is False


def test_list_deleted_contents_filters_by_q(monkeypatch):
    contents = {
        ("CID", "SRC"): {
            "content_id": "CID",
            "source": "SRC",
            "content_type": "comic",
            "title": "Alpha Title",
            "normalized_title": "alphatitle",
            "status": "active",
            "is_deleted": True,
            "meta": {},
            "deleted_at": datetime(2024, 5, 1, 0, 0, 0),
            "deleted_reason": "spam",
            "deleted_by": 1,
        },
        ("CID2", "SRC"): {
            "content_id": "CID2",
            "source": "SRC",
            "content_type": "comic",
            "title": "Beta",
            "normalized_title": "beta",
            "status": "active",
            "is_deleted": True,
            "meta": {},
            "deleted_at": datetime(2024, 5, 2, 0, 0, 0),
            "deleted_reason": "spam",
            "deleted_by": 1,
        },
    }
    db = FakeDB(contents=contents)

    monkeypatch.setattr(delete_service, "get_cursor", lambda conn: FakeCursor(conn))

    results = delete_service.list_deleted_contents(db, limit=50, offset=0, q="alpha")

    assert len(results) == 1
    assert results[0]["content_id"] == "CID"
