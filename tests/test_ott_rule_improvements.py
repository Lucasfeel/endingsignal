from datetime import datetime

from services import ott_verification_service as service


def test_infer_implied_next_season_label_for_global_streaming_followup():
    label = service._infer_implied_next_season_label(
        source_name="netflix",
        source_item={
            "release_start_at": datetime(2026, 4, 16),
            "cast": ["오스카 아이작", "캐리 멀리건", "찰스 멜튼", "케일리 스페이니"],
        },
        matched_docs=[
            {
                "title": "성난 사람들",
                "payload_titles": ["BEEF"],
                "release_start_at": datetime(2023, 4, 6),
                "release_end_at": datetime(2023, 4, 6),
                "cast": ["스티븐 연", "앨리 웡"],
            }
        ],
        season_label="",
    )

    assert label == "시즌 2"


def test_infer_implied_next_season_label_allows_future_followup_without_source_cast_when_season_signal_is_strong():
    label = service._infer_implied_next_season_label(
        source_name="netflix",
        source_item={
            "release_start_at": datetime(2026, 4, 2),
            "cast": [],
        },
        matched_docs=[
            {
                "title": "닥터 스톤",
                "payload_titles": ["Dr. Stone"],
                "body_text": "애니메이션 (1기 · 2기 · SP · 3기 · 4기)",
                "release_start_at": datetime(2019, 7, 5),
                "release_end_at": datetime(2025, 9, 25),
                "cast": ["코바야시 유스케", "후루카와 마코토"],
                "source": "public_web",
            }
        ],
        season_label="",
    )

    assert label == "시즌 5"


def test_infer_implied_next_season_label_keeps_empty_when_future_release_lacks_strong_season_signal():
    label = service._infer_implied_next_season_label(
        source_name="netflix",
        source_item={
            "release_start_at": datetime(2026, 4, 2),
            "cast": [],
        },
        matched_docs=[
            {
                "title": "Old Imported Series",
                "payload_titles": ["Old Imported Series"],
                "body_text": "completed series",
                "release_start_at": datetime(2019, 7, 5),
                "release_end_at": datetime(2025, 9, 25),
                "cast": ["Actor A", "Actor B"],
                "source": "public_web",
            }
        ],
        season_label="",
    )

    assert label == ""


def test_infer_implied_next_season_label_ignores_current_source_start_in_history_window():
    label = service._infer_implied_next_season_label(
        source_name="netflix",
        source_item={
            "release_start_at": datetime(2026, 4, 2),
            "cast": [],
        },
        matched_docs=[
            {
                "title": "닥터 스톤",
                "payload_titles": ["닥터 스톤", "Dr. Stone"],
                "body_text": "애니메이션 (1기 · 2기 · SP · 3기 · 4기)",
                "release_start_at": datetime(2026, 4, 2),
                "release_end_at": None,
                "cast": [],
                "source": "official_crawl_metadata",
            },
            {
                "title": "닥터 스톤",
                "payload_titles": ["Dr. Stone"],
                "body_text": "애니메이션 (1기 · 2기 · SP · 3기 · 4기)",
                "release_start_at": datetime(2019, 7, 5),
                "release_end_at": datetime(2025, 9, 25),
                "cast": ["코바야시 유스케", "후루카와 마코토"],
                "source": "public_web",
            },
        ],
        season_label="",
    )

    assert label == "시즌 5"


def test_infer_implied_next_season_label_promotes_latest_doc_row_without_source_start():
    label = service._infer_implied_next_season_label(
        source_name="disney_plus",
        source_item={
            "release_start_at": None,
            "cast": [],
        },
        matched_docs=[
            {
                "title": "하이 포텐셜",
                "payload_titles": ["High Potential"],
                "body_text": "방송 기간 시즌 1 : 2024년 9월 17일 시즌 2 : 2025년 9월 16일 ~ ON AIR",
                "release_start_at": datetime(2024, 9, 17),
                "release_end_at": datetime(2025, 9, 16),
                "release_end_status": "confirmed",
                "cast": ["케이틀린 올슨", "다니엘 순자타"],
                "source": "public_web",
            }
        ],
        season_label="",
    )

    assert label == "시즌 2"


def test_resolve_cast_values_does_not_parse_prose_when_no_explicit_cast_exists():
    resolved = service._resolve_cast_values(
        "coupangplay",
        {"cast": []},
        [
            {
                "source": "official_coupang_metadata",
                "title": "네이버스",
                "description": "해리슨 피시먼과 딜런 레드포드가 선보이는 강렬한 다큐멘터리.",
                "body_text": "해리슨 피시먼과 딜런 레드포드가 선보이는 강렬한 다큐멘터리.",
                "cast": [],
            },
            {
                "source": "tmdb",
                "title": "Neighbors",
                "cast": [],
            },
        ],
    )

    assert resolved == []


def test_merge_verification_metadata_infers_next_season_for_netflix_followup():
    metadata = service._merge_verification_metadata(
        candidate={
            "source_name": "netflix",
            "title": "성난 사람들",
            "content_url": "https://www.netflix.com/kr/title/81447461",
            "source_item": {
                "title": "성난 사람들",
                "release_start_at": datetime(2026, 4, 16),
                "cast": ["오스카 아이작", "캐리 멀리건", "찰스 멜튼", "케일리 스페이니"],
            },
        },
        documents=[
            {
                "ok": True,
                "source": None,
                "title": "성난 사람들, 지금 시청하세요 | 넷플릭스 공식 사이트",
                "payload_titles": ["성난 사람들"],
                "cast": ["오스카 아이작", "캐리 멀리건", "찰스 멜튼", "케일리 스페이니"],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            },
            {
                "ok": True,
                "source": "tmdb",
                "title": "BEEF",
                "payload_titles": ["BEEF", "성난 사람들"],
                "genre_text": "Crime, Drama",
                "cast": ["스티븐 연", "앨리 웡"],
                "release_start_at": datetime(2023, 4, 6),
                "release_end_at": datetime(2023, 4, 6),
                "release_end_status": "confirmed",
            }
        ],
    )

    assert metadata["resolved_title"] == "성난 사람들 시즌 2"
    assert metadata["release_start_at"] == datetime(2026, 4, 16)
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"


def test_merge_verification_metadata_infers_followup_season_for_anime_with_future_release_and_strong_signal():
    metadata = service._merge_verification_metadata(
        candidate={
            "source_name": "netflix",
            "title": "닥터 스톤",
            "content_url": "https://www.netflix.com/kr/title/81046193",
            "source_item": {
                "title": "닥터 스톤",
                "release_start_at": datetime(2026, 4, 2),
                "release_end_status": "unknown",
                "cast": [],
            },
        },
        documents=[
            {
                "ok": True,
                "source": "official_public_page",
                "title": "닥터 스톤, 지금 시청하세요 | 넷플릭스 공식 사이트",
                "payload_titles": ["닥터 스톤"],
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            },
            {
                "ok": True,
                "source": "public_web",
                "title": "닥터 스톤 - 나무위키",
                "payload_titles": ["닥터 스톤", "Dr. Stone"],
                "body_text": "애니메이션 (1기 · 2기 · SP · 3기 · 4기)",
                "cast": ["코바야시 유스케", "후루카와 마코토"],
                "release_start_at": datetime(2019, 7, 5),
                "release_end_at": datetime(2025, 9, 25),
                "release_end_status": "confirmed",
            },
        ],
    )

    assert metadata["resolved_title"] == "닥터 스톤 시즌 5"
    assert metadata["season_label"] == "시즌 5"
    assert metadata["release_start_at"] == datetime(2026, 4, 2)
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"


def test_merge_verification_metadata_promotes_latest_doc_row_without_source_start():
    metadata = service._merge_verification_metadata(
        candidate={
            "source_name": "disney_plus",
            "title": "하이 포텐셜",
            "content_url": "https://www.disneyplus.com/browse/entity-d58ab636-473f-4276-b421-d27825b42fce",
            "source_item": {
                "title": "하이 포텐셜",
                "release_end_status": "unknown",
                "cast": [],
            },
        },
        documents=[
            {
                "ok": True,
                "source": "official_public_page",
                "title": "하이 포텐셜",
                "payload_titles": ["High Potential"],
                "cast": [],
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
            },
            {
                "ok": True,
                "source": "public_web",
                "title": "하이 포텐셜",
                "payload_titles": ["High Potential"],
                "body_text": "방송 기간 시즌 1 : 2024년 9월 17일 시즌 2 : 2025년 9월 16일 ~ ON AIR",
                "cast": ["케이틀린 올슨", "다니엘 순자타"],
                "release_start_at": datetime(2024, 9, 17),
                "release_end_at": datetime(2025, 9, 16),
                "release_end_status": "confirmed",
                "source": "public_web",
            },
        ],
    )

    assert metadata["resolved_title"] == "하이 포텐셜 시즌 2"
    assert metadata["season_label"] == "시즌 2"
    assert metadata["release_start_at"] == datetime(2025, 9, 16)
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"


def test_merge_verification_metadata_confirms_binge_after_release_plus_one_day(monkeypatch):
    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 15, 0, 0, 0))

    metadata = service._merge_verification_metadata(
        candidate={
            "source_name": "netflix",
            "title": "그날 밤",
            "content_url": "https://www.netflix.com/kr/title/81613228",
            "source_item": {
                "title": "그날 밤",
                "release_start_at": datetime(2026, 3, 14),
                "release_end_status": "unknown",
                "cast": [],
            },
        },
        documents=[
            {
                "ok": True,
                "source": "official_public_page",
                "url": "https://www.netflix.com/kr/title/81613228",
                "title": "그날 밤",
                "payload_titles": ["One Night"],
                "visible_episode_count": 6,
                "episode_total": None,
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
                "cast": [],
            },
            {
                "ok": True,
                "source": "tmdb",
                "url": "https://www.themoviedb.org/tv/999998",
                "title": "One Night",
                "payload_titles": ["One Night"],
                "episode_total": 6,
                "release_start_at": datetime(2026, 3, 14),
                "release_end_at": None,
                "release_end_status": "unknown",
                "cast": [],
            },
        ],
    )

    assert metadata["release_start_at"] == datetime(2026, 3, 14)
    assert metadata["release_end_at"] == datetime(2026, 3, 14)
    assert metadata["release_end_status"] == "confirmed"


def test_merge_verification_metadata_keeps_unknown_when_visible_episodes_do_not_cover_tmdb_total(monkeypatch):
    monkeypatch.setattr(service, "now_kst_naive", lambda: datetime(2026, 3, 15, 0, 0, 0))

    metadata = service._merge_verification_metadata(
        candidate={
            "source_name": "netflix",
            "title": "작품 B",
            "content_url": "https://www.netflix.com/kr/title/00000001",
            "source_item": {
                "title": "작품 B",
                "release_start_at": datetime(2026, 3, 14),
                "release_end_status": "unknown",
                "cast": [],
            },
        },
        documents=[
            {
                "ok": True,
                "source": "official_public_page",
                "url": "https://www.netflix.com/kr/title/00000001",
                "title": "작품 B",
                "payload_titles": ["Work B"],
                "visible_episode_count": 2,
                "episode_total": None,
                "release_start_at": None,
                "release_end_at": None,
                "release_end_status": "unknown",
                "cast": [],
            },
            {
                "ok": True,
                "source": "tmdb",
                "url": "https://www.themoviedb.org/tv/999997",
                "title": "Work B",
                "payload_titles": ["Work B"],
                "episode_total": 10,
                "release_start_at": datetime(2026, 3, 14),
                "release_end_at": None,
                "release_end_status": "unknown",
                "cast": [],
            },
        ],
    )

    assert metadata["release_start_at"] == datetime(2026, 3, 14)
    assert metadata["release_end_at"] is None
    assert metadata["release_end_status"] == "unknown"
