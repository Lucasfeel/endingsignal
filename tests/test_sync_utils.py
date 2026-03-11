from crawlers import sync_utils


class FakeCursor:
    def __init__(self, *, rows=None):
        self.rows = list(rows or [])
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def executemany(self, query, params_seq):
        self.executed.append((query, list(params_seq)))

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True


class FakeConnection:
    pass


def test_sync_prepared_content_rows_skips_unchanged_and_updates_only_changed(monkeypatch):
    cursor = FakeCursor()
    monkeypatch.setattr(sync_utils, "get_cursor", lambda _conn: cursor)

    existing_snapshot = {
        "same-1": {
            "content_type": "webtoon",
            "title": "Same",
            "normalized_title": "same",
            "normalized_authors": "author",
            "status": "연재중",
            "meta_json": '{"attributes":{"weekdays":["mon"]},"common":{"authors":["Author"]}}',
            "search_document": "Same author",
        },
        "changed-1": {
            "content_type": "webtoon",
            "title": "Changed",
            "normalized_title": "changed",
            "normalized_authors": "author",
            "status": "연재중",
            "meta_json": '{"common":{"authors":["Author"]}}',
            "search_document": "Changed changed author",
        },
    }
    prepared_rows = [
        sync_utils.build_sync_row(
            content_id="same-1",
            source="naver_webtoon",
            content_type="webtoon",
            title="Same",
            normalized_title="same",
            normalized_authors="author",
            status="연재중",
            meta={"common": {"authors": ["Author"]}, "attributes": {"weekdays": ["mon"]}},
        ),
        sync_utils.build_sync_row(
            content_id="changed-1",
            source="naver_webtoon",
            content_type="webtoon",
            title="Changed",
            normalized_title="changed",
            normalized_authors="author",
            status="완결",
            meta={"common": {"authors": ["Author"]}},
        ),
        sync_utils.build_sync_row(
            content_id="new-1",
            source="naver_webtoon",
            content_type="webtoon",
            title="New",
            normalized_title="new",
            normalized_authors="author",
            status="연재중",
            meta={"common": {"authors": ["Author"]}},
        ),
    ]

    stats = sync_utils.sync_prepared_content_rows(
        FakeConnection(),
        source_name="naver_webtoon",
        prepared_rows=prepared_rows,
        existing_snapshot=existing_snapshot,
        write_skipped_count=2,
        cursor_getter=sync_utils.get_cursor,
    )

    assert stats == {
        "inserted_count": 1,
        "updated_count": 1,
        "unchanged_count": 1,
        "write_skipped_count": 2,
    }
    assert len(cursor.executed) == 2
    update_query, update_rows = cursor.executed[0]
    insert_query, insert_rows = cursor.executed[1]
    assert "UPDATE contents" in update_query
    assert len(update_rows) == 1
    assert update_rows[0][-2] == "changed-1"
    assert "INSERT INTO contents" in insert_query
    assert len(insert_rows) == 1
    assert insert_rows[0][0] == "new-1"


def test_build_search_document_includes_aliases_and_normalized_forms():
    document = sync_utils.build_search_document(
        title="메인 제목",
        normalized_title="메인제목",
        normalized_authors="작가a",
        meta={
            "common": {
                "alt_title": "서브 제목",
                "title_alias": ["별칭 하나", "별칭 둘"],
            }
        },
    )

    assert "메인 제목" in document
    assert "메인제목" in document
    assert "서브 제목" in document
    assert "서브제목" in document
    assert "별칭 하나" in document
    assert "별칭하나" in document
