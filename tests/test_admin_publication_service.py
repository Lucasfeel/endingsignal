from datetime import datetime

import services.admin_publication_service as publication_service


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.last_result = None

    def execute(self, query, params):
        if "SELECT 1 FROM contents" in query:
            content_id, source = params
            exists = (content_id, source) in self.db.contents
            self.last_result = [] if not exists else [{"exists": 1}]
        elif "INSERT INTO admin_content_metadata" in query:
            content_id, source, public_at, reason, admin_id = params
            key = (content_id, source)
            row = self.db.publications.get(
                key,
                {
                    "id": len(self.db.publications) + 1,
                    "content_id": content_id,
                    "source": source,
                    "created_at": self.db.now,
                },
            )
            row.update(
                {
                    "public_at": public_at,
                    "reason": reason,
                    "admin_id": admin_id,
                    "updated_at": self.db.now,
                }
            )
            self.db.publications[key] = row
            self.last_result = [row]
        elif "DELETE FROM admin_content_metadata" in query:
            content_id, source = params
            self.db.publications.pop((content_id, source), None)
            self.last_result = []
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
    def __init__(self, contents, publications=None, now=None):
        self.contents = contents
        self.publications = publications or {}
        self.now = now or datetime(2024, 1, 1, 0, 0, 0)
        self.committed = False

    def commit(self):
        self.committed = True


def test_upsert_publication_missing_content(monkeypatch):
    db = FakeDB(set())

    monkeypatch.setattr(publication_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = publication_service.upsert_publication(
        db,
        admin_id=1,
        content_id="CID",
        source="SRC",
        public_at=None,
        reason=None,
    )

    assert result == {"error": "CONTENT_NOT_FOUND"}


def test_upsert_publication_creates_and_commits(monkeypatch):
    db = FakeDB({("CID", "SRC")})

    monkeypatch.setattr(publication_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = publication_service.upsert_publication(
        db,
        admin_id=3,
        content_id="CID",
        source="SRC",
        public_at=datetime(2024, 2, 1, 9, 0, 0),
        reason="manual publish",
    )

    assert result["publication"]["content_id"] == "CID"
    assert result["publication"]["reason"] == "manual publish"


def test_upsert_publication_updates_existing(monkeypatch):
    now = datetime(2024, 3, 1, 12, 0, 0)
    existing = {
        ("CID", "SRC"): {
            "id": 7,
            "content_id": "CID",
            "source": "SRC",
            "public_at": None,
            "reason": None,
            "admin_id": 1,
            "created_at": datetime(2024, 1, 1, 0, 0, 0),
            "updated_at": datetime(2024, 1, 2, 0, 0, 0),
        }
    }
    db = FakeDB({("CID", "SRC")}, publications=existing, now=now)

    monkeypatch.setattr(publication_service, "get_cursor", lambda conn: FakeCursor(conn))

    result = publication_service.upsert_publication(
        db,
        admin_id=9,
        content_id="CID",
        source="SRC",
        public_at=datetime(2024, 4, 1, 10, 30, 0),
        reason="update",
    )

    assert result["publication"]["id"] == 7
    assert result["publication"]["admin_id"] == 9
    assert result["publication"]["updated_at"] == now


def test_delete_publication_commits_and_is_idempotent(monkeypatch):
    db = FakeDB({("CID", "SRC")}, publications={})

    monkeypatch.setattr(publication_service, "get_cursor", lambda conn: FakeCursor(conn))

    publication_service.delete_publication(db, content_id="CID", source="SRC")
