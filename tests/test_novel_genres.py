from crawlers import sync_utils
from utils import novel_genres


def test_extract_novel_genre_groups_uses_seed_url_and_root_tokens():
    meta = {
        "common": {"content_url": "https://series.naver.com/novel/detail.series?productNo=100"},
        "attributes": {
            "crawl_roots": [
                "https://series.naver.com/novel/categoryProductList.series?categoryTypeCode=genre&genreCode=201",
                "fantasy_completed",
            ]
        },
    }

    assert novel_genres.extract_novel_genre_groups_from_meta(meta) == ["ROMANCE", "FANTASY"]


def test_build_sync_row_derives_novel_genre_columns_from_meta():
    row = sync_utils.build_sync_row(
        content_id="novel-1",
        source="ridi",
        content_type="novel",
        title="Novel",
        normalized_title="novel",
        normalized_authors="author",
        status="연재중",
        meta={
            "common": {"authors": ["Author"]},
            "attributes": {"genres": ["현판"], "crawl_roots": ["fantasy_completed"]},
        },
    )

    assert row["novel_genre_group"] == "HYEONPAN"
    assert row["novel_genre_groups"] == ["HYEONPAN", "FANTASY"]


def test_expand_query_genre_groups_keeps_fantasy_and_hyeonpan_separate():
    assert novel_genres.expand_query_genre_groups(["FANTASY"]) == ["FANTASY"]
    assert novel_genres.expand_query_genre_groups(["HYEONPAN"]) == ["HYEONPAN"]
