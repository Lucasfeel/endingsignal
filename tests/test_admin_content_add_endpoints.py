import json
from datetime import datetime

import pytest

from app import app as flask_app
import services.admin_audit_service as audit_service
import utils.auth as auth
import views.admin as admin_view


class ScriptedCursor:
    def __init__(self, steps):
        self.steps = list(steps)
        self.step_index = 0
        self.current_step = None
        self.executed = []

    def execute(self, query, params=None):
        if self.step_index >= len(self.steps):
            raise AssertionError(f"Unexpected query executed: {query}")

        step = self.steps[self.step_index]
        self.step_index += 1
        self.current_step = step
        self.executed.append({"query": query, "params": params})

        contains = step.get("contains")
        if contains and contains.lower() not in query.lower():
            raise AssertionError(
                f"Expected query containing '{contains}', got:\n{query}"
            )

        expected_params = step.get("params")
        if expected_params is not None and params != expected_params:
            raise AssertionError(
                f"Unexpected params for query containing '{contains}'. "
                f"Expected={expected_params} Actual={params}"
            )

    def fetchone(self):
        if not self.current_step:
            return None
        return self.current_step.get("fetchone")

    def fetchall(self):
        if not self.current_step:
            return []
        return self.current_step.get("fetchall", [])

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


@pytest.fixture(autouse=True)
def stub_decode_token(monkeypatch):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {"uid": 1, "email": "admin@example.com", "role": "admin"},
    )


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer testtoken"}


def _patch_db(monkeypatch, steps):
    conn = FakeConnection()
    cursor = ScriptedCursor(steps)
    monkeypatch.setattr(admin_view, "get_db", lambda: conn)
    monkeypatch.setattr(admin_view, "get_cursor", lambda _conn: cursor)
    return conn, cursor


def test_list_content_types(monkeypatch, client, auth_headers):
    now = datetime(2026, 2, 19, 10, 0, 0)
    _, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_types",
                "fetchall": [
                    {"id": 1, "name": "웹툰", "created_at": now, "updated_at": now},
                    {"id": 2, "name": "웹소설", "created_at": now, "updated_at": now},
                ],
            }
        ],
    )

    response = client.get("/api/admin/content-types", headers=auth_headers)
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert [item["name"] for item in payload["types"]] == ["웹툰", "웹소설"]
    assert cursor.step_index == 1


def test_create_content_type_success(monkeypatch, client, auth_headers):
    now = datetime(2026, 2, 19, 10, 0, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "INSERT INTO content_types",
                "params": ("게임",),
                "fetchone": {"id": 10, "name": "게임", "created_at": now, "updated_at": now},
            }
        ],
    )

    response = client.post(
        "/api/admin/content-types",
        json={"name": "게임"},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert payload["type"]["name"] == "게임"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 1


def test_create_content_type_duplicate_returns_409(monkeypatch, client, auth_headers):
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "INSERT INTO content_types",
                "params": ("웹툰",),
                "fetchone": None,
            }
        ],
    )

    response = client.post(
        "/api/admin/content-types",
        json={"name": "웹툰"},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["error"]["code"] == "DUPLICATE_CONTENT_TYPE"
    assert conn.committed is False
    assert conn.rolled_back is True
    assert cursor.step_index == 1


def test_list_content_sources_by_type(monkeypatch, client, auth_headers):
    now = datetime(2026, 2, 19, 10, 0, 0)
    _, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "SELECT id FROM content_types",
                "params": (1,),
                "fetchone": {"id": 1},
            },
            {
                "contains": "FROM content_sources",
                "params": (1,),
                "fetchall": [
                    {"id": 11, "type_id": 1, "name": "네이버웹툰", "created_at": now, "updated_at": now},
                    {"id": 12, "type_id": 1, "name": "카카오웹툰", "created_at": now, "updated_at": now},
                ],
            },
        ],
    )

    response = client.get("/api/admin/content-sources?typeId=1", headers=auth_headers)
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert [item["name"] for item in payload["sources"]] == ["네이버웹툰", "카카오웹툰"]
    assert cursor.step_index == 2


def test_create_content_source_duplicate_returns_409(monkeypatch, client, auth_headers):
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "SELECT id FROM content_types",
                "params": (1,),
                "fetchone": {"id": 1},
            },
            {
                "contains": "INSERT INTO content_sources",
                "params": (1, "네이버웹툰"),
                "fetchone": None,
            },
        ],
    )

    response = client.post(
        "/api/admin/content-sources",
        json={"typeId": 1, "name": "네이버웹툰"},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["error"]["code"] == "DUPLICATE_CONTENT_SOURCE"
    assert conn.committed is False
    assert conn.rolled_back is True
    assert cursor.step_index == 2


def test_create_content_source_invalid_type_returns_400(monkeypatch, client, auth_headers):
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "SELECT id FROM content_types",
                "params": (999,),
                "fetchone": None,
            }
        ],
    )

    response = client.post(
        "/api/admin/content-sources",
        json={"typeId": 999, "name": "테스트소스"},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_TYPE_ID"
    assert conn.committed is False
    assert conn.rolled_back is False
    assert cursor.step_index == 1


def test_create_admin_content_success(monkeypatch, client, auth_headers):
    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (11,),
                "fetchall": [
                    {
                        "source_id": 11,
                        "source_name": "네이버웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("naver_webtoon", "webtoon", "신규 웹툰"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:abc123",
                    "source": "naver_webtoon",
                    "content_type": "webtoon",
                    "title": "신규 웹툰",
                    "status": "연재중",
                    "meta": {"manual_registration": {"source_id": 11}},
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={"title": "신규 웹툰", "typeId": 1, "sourceId": 11},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert payload["content"]["content_id"] == "manual:abc123"
    assert payload["content"]["source"] == "naver_webtoon"
    assert payload["content"]["content_type"] == "webtoon"
    assert payload["content_type"]["id"] == 1
    assert payload["content_source"]["id"] == 11
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 3


def test_create_admin_content_supports_multiple_sources(monkeypatch, client, auth_headers):
    class FakeUuid:
        hex = "multisourceid"

    monkeypatch.setattr(admin_view, "uuid4", lambda: FakeUuid())

    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (11, 12),
                "fetchall": [
                    {
                        "source_id": 11,
                        "source_name": "네이버웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    },
                    {
                        "source_id": 12,
                        "source_name": "카카오웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    },
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("naver_webtoon", "kakao_webtoon", "webtoon", "멀티 소스 웹툰"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:multisourceid",
                    "source": "naver_webtoon",
                    "content_type": "webtoon",
                    "title": "멀티 소스 웹툰",
                    "status": "연재중",
                    "meta": {"manual_registration": {"source_id": 11}},
                    "created_at": now,
                    "updated_at": now,
                },
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:multisourceid",
                    "source": "kakao_webtoon",
                    "content_type": "webtoon",
                    "title": "멀티 소스 웹툰",
                    "status": "연재중",
                    "meta": {"manual_registration": {"source_id": 12}},
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "멀티 소스 웹툰",
            "typeId": 1,
            "sources": [{"sourceId": 11}, {"sourceId": 12}],
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert payload["content"]["content_id"] == "manual:multisourceid"
    assert payload["content"]["source"] == "naver_webtoon"
    assert [item["source"] for item in payload["contents"]] == ["naver_webtoon", "kakao_webtoon"]
    assert payload["sourceIds"] == [11, 12]
    assert [item["id"] for item in payload["sources"]] == [11, 12]
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 4

    first_insert = cursor.executed[2]["params"]
    second_insert = cursor.executed[3]["params"]
    assert first_insert[0] == "manual:multisourceid"
    assert first_insert[1] == "naver_webtoon"
    assert second_insert[0] == "manual:multisourceid"
    assert second_insert[1] == "kakao_webtoon"


def test_create_admin_content_saves_content_url_in_meta(monkeypatch, client, auth_headers):
    class FakeUuid:
        hex = "manualfixedid"

    monkeypatch.setattr(admin_view, "uuid4", lambda: FakeUuid())

    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (11,),
                "fetchall": [
                    {
                        "source_id": 11,
                        "source_name": "네이버웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("naver_webtoon", "webtoon", "URL 포함 웹툰"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:manualfixedid",
                    "source": "naver_webtoon",
                    "content_type": "webtoon",
                    "title": "URL 포함 웹툰",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {"source_id": 11},
                        "common": {"content_url": "https://example.com/webtoon/123"},
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "URL 포함 웹툰",
            "typeId": 1,
            "sourceId": 11,
            "contentUrl": "https://example.com/webtoon/123",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 3

    insert_call = cursor.executed[2]
    insert_params = insert_call["params"]
    assert insert_params[0] == "manual:manualfixedid"
    assert insert_params[1] == "naver_webtoon"
    assert insert_params[2] == "webtoon"
    assert insert_params[3] == "URL 포함 웹툰"
    meta = json.loads(insert_params[5])
    assert meta["common"]["content_url"] == "https://example.com/webtoon/123"


def test_create_admin_content_saves_author_name_in_meta(monkeypatch, client, auth_headers):
    class FakeUuid:
        hex = "manualauthorid"

    monkeypatch.setattr(admin_view, "uuid4", lambda: FakeUuid())

    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (11,),
                "fetchall": [
                    {
                        "source_id": 11,
                        "source_name": "네이버웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("naver_webtoon", "webtoon", "작가명 포함 웹툰"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:manualauthorid",
                    "source": "naver_webtoon",
                    "content_type": "webtoon",
                    "title": "작가명 포함 웹툰",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {"source_id": 11},
                        "common": {"authors": "홍길동"},
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "작가명 포함 웹툰",
            "typeId": 1,
            "sourceId": 11,
            "authorName": "홍길동",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 3

    insert_call = cursor.executed[2]
    insert_params = insert_call["params"]
    assert insert_params[0] == "manual:manualauthorid"
    assert insert_params[1] == "naver_webtoon"
    assert insert_params[2] == "webtoon"
    assert insert_params[3] == "작가명 포함 웹툰"
    meta = json.loads(insert_params[5])
    assert meta["common"]["authors"] == "홍길동"


def test_create_admin_content_saves_novel_l2_hyeonpan(monkeypatch, client, auth_headers):
    class FakeUuid:
        hex = "manuall2novelid"

    monkeypatch.setattr(admin_view, "uuid4", lambda: FakeUuid())

    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (31,),
                "fetchall": [
                    {
                        "source_id": 31,
                        "source_name": "리디",
                        "type_id": 2,
                        "type_name": "웹소설",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("ridi", "novel", "현판 수동 등록"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:manuall2novelid",
                    "source": "ridi",
                    "content_type": "novel",
                    "title": "현판 수동 등록",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {"source_id": 31, "l2_id": "hyeonpan"},
                        "attributes": {"genres": ["현판"]},
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "현판 수동 등록",
            "typeId": 2,
            "sourceId": 31,
            "l2Id": "hyeonpan",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 3

    insert_params = cursor.executed[2]["params"]
    assert insert_params[0] == "manual:manuall2novelid"
    assert insert_params[1] == "ridi"
    assert insert_params[2] == "novel"
    assert insert_params[4] == "연재중"
    meta = json.loads(insert_params[5])
    assert meta["manual_registration"]["l2_id"] == "hyeonpan"
    assert meta["manual_registration"]["l2_label"] == "현판"
    assert meta["attributes"]["genres"] == ["현판"]


def test_create_admin_content_saves_novel_l2_mystery(monkeypatch, client, auth_headers):
    class FakeUuid:
        hex = "manualmysterynovelid"

    monkeypatch.setattr(admin_view, "uuid4", lambda: FakeUuid())

    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (31,),
                "fetchall": [
                    {
                        "source_id": 31,
                        "source_name": "리디",
                        "type_id": 2,
                        "type_name": "웹소설",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("ridi", "novel", "미스터리 수동 등록"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:manualmysterynovelid",
                    "source": "ridi",
                    "content_type": "novel",
                    "title": "미스터리 수동 등록",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {"source_id": 31, "l2_id": "mystery"},
                        "attributes": {"genres": ["mystery"]},
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "미스터리 수동 등록",
            "typeId": 2,
            "sourceId": 31,
            "l2Id": "mystery",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 3

    insert_params = cursor.executed[2]["params"]
    assert insert_params[0] == "manual:manualmysterynovelid"
    assert insert_params[1] == "ridi"
    assert insert_params[2] == "novel"
    assert insert_params[4] == "연재중"
    meta = json.loads(insert_params[5])
    assert meta["manual_registration"]["l2_id"] == "mystery"
    assert meta["manual_registration"]["l2_label"] == "미스터리"
    assert meta["attributes"]["genres"] == ["mystery"]


def test_create_admin_content_sets_ott_completed_status_from_l2(monkeypatch, client, auth_headers):
    class FakeUuid:
        hex = "manuall2ottid"

    monkeypatch.setattr(admin_view, "uuid4", lambda: FakeUuid())

    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (41,),
                "fetchall": [
                    {
                        "source_id": 41,
                        "source_name": "넷플릭스",
                        "type_id": 3,
                        "type_name": "OTT",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("netflix", "ott", "완결 OTT 수동 등록"),
                "fetchone": None,
            },
            {
                "contains": "INSERT INTO contents",
                "fetchone": {
                    "content_id": "manual:manuall2ottid",
                    "source": "netflix",
                    "content_type": "ott",
                    "title": "완결 OTT 수동 등록",
                    "status": "완결",
                    "meta": {
                        "manual_registration": {"source_id": 41, "l2_id": "completed"},
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "완결 OTT 수동 등록",
            "typeId": 3,
            "sourceId": 41,
            "l2Id": "completed",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 201
    assert payload["success"] is True
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 3

    insert_params = cursor.executed[2]["params"]
    assert insert_params[0] == "manual:manuall2ottid"
    assert insert_params[1] == "netflix"
    assert insert_params[2] == "ott"
    assert insert_params[4] == "완결"
    meta = json.loads(insert_params[5])
    assert meta["manual_registration"]["l2_id"] == "completed"
    assert meta["manual_registration"]["l2_label"] == "완결"
    assert "attributes" not in meta


def test_create_admin_content_rejects_l2_type_mismatch(monkeypatch, client, auth_headers):
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (11,),
                "fetchall": [
                    {
                        "source_id": 11,
                        "source_name": "네이버웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    }
                ],
            }
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={
            "title": "웹툰에 잘못된 L2",
            "typeId": 1,
            "sourceId": 11,
            "l2Id": "hyeonpan",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_L2_ID"
    assert conn.committed is False
    assert conn.rolled_back is False
    assert cursor.step_index == 1


def test_create_admin_content_rejects_source_type_mismatch(monkeypatch, client, auth_headers):
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (55,),
                "fetchall": [
                    {
                        "source_id": 55,
                        "source_name": "네이버 시리즈",
                        "type_id": 2,
                        "type_name": "웹소설",
                    }
                ],
            }
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={"title": "잘못된 매칭", "typeId": 1, "sourceId": 55},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["error"]["code"] == "SOURCE_TYPE_MISMATCH"
    assert conn.committed is False
    assert conn.rolled_back is False
    assert cursor.step_index == 1


def test_create_admin_content_duplicate_returns_409(monkeypatch, client, auth_headers):
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (11,),
                "fetchall": [
                    {
                        "source_id": 11,
                        "source_name": "네이버웹툰",
                        "type_id": 1,
                        "type_name": "웹툰",
                    }
                ],
            },
            {
                "contains": "FROM contents",
                "params": ("naver_webtoon", "webtoon", "기존 웹툰"),
                "fetchone": {"exists": 1},
            },
        ],
    )

    response = client.post(
        "/api/admin/contents",
        json={"title": "기존 웹툰", "typeId": 1, "sourceId": 11},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 409
    assert payload["error"]["code"] == "DUPLICATE_CONTENT"
    assert conn.committed is False
    assert conn.rolled_back is False
    assert cursor.step_index == 2


def test_create_admin_content_missing_fields_returns_400(client, auth_headers):
    response = client.post(
        "/api/admin/contents",
        json={"title": "제목만"},
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_REQUEST"


def test_create_admin_content_rejects_invalid_content_url(client, auth_headers):
    response = client.post(
        "/api/admin/contents",
        json={
            "title": "잘못된 URL 테스트",
            "typeId": 1,
            "sourceId": 11,
            "contentUrl": "not-a-url",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_REQUEST"
    assert payload["error"]["message"] == "contentUrl must be a valid http(s) URL"


def test_update_admin_content_success(monkeypatch, client, auth_headers):
    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (12,),
                "fetchone": {
                    "source_id": 12,
                    "source_name": "카카오웹툰",
                    "type_id": 1,
                    "type_name": "웹툰",
                },
            },
            {
                "contains": "FROM contents",
                "params": ("CID-1", "naver_webtoon"),
                "fetchone": {
                    "content_id": "CID-1",
                    "source": "naver_webtoon",
                    "title": "수정 대상",
                    "content_type": "webtoon",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {
                            "type_id": 1,
                            "type_name": "웹툰",
                            "source_id": 11,
                            "source_name": "네이버웹툰",
                        },
                        "common": {
                            "authors": "기존 작가",
                            "content_url": "https://example.com/old",
                        },
                    },
                },
            },
            {
                "contains": "SELECT 1",
                "params": ("kakao_webtoon", "webtoon", "수정 대상", "CID-1", "naver_webtoon"),
                "fetchone": None,
            },
            {
                "contains": "FROM contents",
                "params": ("CID-1", "kakao_webtoon"),
                "fetchone": None,
            },
            {
                "contains": "UPDATE contents",
                "fetchone": {
                    "content_id": "CID-1",
                    "source": "kakao_webtoon",
                    "content_type": "webtoon",
                    "title": "수정 대상",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {
                            "type_id": 1,
                            "type_name": "웹툰",
                            "source_id": 12,
                            "source_name": "카카오웹툰",
                        },
                        "common": {
                            "authors": "새 작가",
                            "content_url": "https://example.com/new",
                        },
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
            {"contains": "UPDATE admin_content_overrides"},
            {"contains": "UPDATE admin_content_metadata"},
            {"contains": "UPDATE subscriptions"},
            {"contains": "UPDATE cdc_events"},
            {"contains": "INSERT INTO admin_action_logs"},
        ],
    )
    monkeypatch.setattr(audit_service, "get_cursor", lambda _conn: cursor)

    response = client.post(
        "/api/admin/contents/update",
        json={
            "content_id": "CID-1",
            "source": "naver_webtoon",
            "typeId": 1,
            "sourceId": 12,
            "authorName": "새 작가",
            "contentUrl": "https://example.com/new",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["content"]["source"] == "kakao_webtoon"
    assert payload["content_source"]["id"] == 12
    assert payload["previous"]["source"] == "naver_webtoon"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 10

    update_params = cursor.executed[4]["params"]
    assert update_params[0] == "kakao_webtoon"
    assert update_params[1] == "webtoon"
    meta = json.loads(update_params[2])
    assert meta["manual_registration"]["source_id"] == 12
    assert meta["common"]["authors"] == "새 작가"
    assert meta["common"]["content_url"] == "https://example.com/new"


def test_update_admin_content_supports_title_and_l2(monkeypatch, client, auth_headers):
    now = datetime(2026, 2, 19, 11, 30, 0)
    conn, cursor = _patch_db(
        monkeypatch,
        [
            {
                "contains": "FROM content_sources s",
                "params": (41,),
                "fetchone": {
                    "source_id": 41,
                    "source_name": "티빙",
                    "type_id": 3,
                    "type_name": "OTT",
                },
            },
            {
                "contains": "FROM contents",
                "params": ("CID-9", "tving"),
                "fetchone": {
                    "content_id": "CID-9",
                    "source": "tving",
                    "title": "기존 제목",
                    "content_type": "ott",
                    "status": "연재중",
                    "meta": {
                        "manual_registration": {
                            "type_id": 3,
                            "type_name": "OTT",
                            "source_id": 41,
                            "source_name": "티빙",
                            "l2_id": "etc",
                            "l2_label": "기타",
                        },
                        "attributes": {"genres": ["etc"]},
                    },
                },
            },
            {
                "contains": "SELECT 1",
                "params": ("tving", "ott", "새 제목", "CID-9", "tving"),
                "fetchone": None,
            },
            {
                "contains": "UPDATE contents",
                "fetchone": {
                    "content_id": "CID-9",
                    "source": "tving",
                    "content_type": "ott",
                    "title": "새 제목",
                    "status": "완결",
                    "meta": {
                        "manual_registration": {
                            "type_id": 3,
                            "type_name": "OTT",
                            "source_id": 41,
                            "source_name": "티빙",
                            "l2_id": "completed",
                            "l2_label": "완결",
                        },
                    },
                    "created_at": now,
                    "updated_at": now,
                },
            },
            {"contains": "INSERT INTO admin_action_logs"},
        ],
    )
    monkeypatch.setattr(audit_service, "get_cursor", lambda _conn: cursor)

    response = client.post(
        "/api/admin/contents/update",
        json={
            "content_id": "CID-9",
            "source": "tving",
            "title": "새 제목",
            "typeId": 3,
            "sourceId": 41,
            "l2Id": "completed",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["content"]["title"] == "새 제목"
    assert payload["content"]["status"] == "완결"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert cursor.step_index == 5

    update_params = cursor.executed[3]["params"]
    assert update_params[0] == "tving"
    assert update_params[1] == "ott"
    assert update_params[3] == "새 제목"
    assert update_params[5] == "완결"
    meta = json.loads(update_params[2])
    assert meta["manual_registration"]["l2_id"] == "completed"
    assert meta["manual_registration"]["l2_label"] == "완결"
    assert "attributes" not in meta


def test_update_admin_content_rejects_invalid_content_url(client, auth_headers):
    response = client.post(
        "/api/admin/contents/update",
        json={
            "content_id": "CID-1",
            "source": "naver_webtoon",
            "typeId": 1,
            "sourceId": 11,
            "contentUrl": "not-a-url",
        },
        headers=auth_headers,
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["error"]["code"] == "INVALID_REQUEST"
    assert payload["error"]["message"] == "contentUrl must be a valid http(s) URL"


@pytest.mark.parametrize(
    "method,url,json_body",
    [
        ("get", "/api/admin/content-types", None),
        ("post", "/api/admin/content-types", {"name": "테스트"}),
        ("get", "/api/admin/content-sources?typeId=1", None),
        ("post", "/api/admin/content-sources", {"typeId": 1, "name": "테스트"}),
        ("post", "/api/admin/contents", {"title": "x", "typeId": 1, "sourceId": 1}),
        (
            "post",
            "/api/admin/contents/update",
            {"content_id": "x", "source": "naver_webtoon", "typeId": 1, "sourceId": 1},
        ),
    ],
)
def test_new_admin_endpoints_require_admin_role(
    monkeypatch,
    client,
    auth_headers,
    method,
    url,
    json_body,
):
    monkeypatch.setattr(
        auth,
        "_decode_token",
        lambda token: {"uid": 2, "email": "user@example.com", "role": "user"},
    )

    if method == "get":
        response = client.get(url, headers=auth_headers)
    else:
        response = client.post(url, json=json_body, headers=auth_headers)

    payload = response.get_json()
    assert response.status_code == 403
    assert payload["error"]["code"] == "FORBIDDEN"
