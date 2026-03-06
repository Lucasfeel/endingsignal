import scripts.backfill_novels_once as backfill


def test_naver_series_seeds_cover_genre_pages_and_mystery():
    seed_by_key = {seed["key"]: seed for seed in backfill.NAVER_SERIES_SEEDS}

    assert seed_by_key["romance_ongoing"]["base_url"] == (
        "https://series.naver.com/novel/categoryProductList.series"
        "?categoryTypeCode=genre&genreCode=201"
    )
    assert seed_by_key["mystery_ongoing"]["genre"] == backfill.GENRE_MYSTERY
    assert seed_by_key["mystery_completed"]["base_url"] == (
        "https://series.naver.com/novel/categoryProductList.series"
        "?categoryTypeCode=genre&genreCode=203&orderTypeCode=new&is&isFinished=true"
    )
    assert seed_by_key["light_novel_completed"]["base_url"] == (
        "https://series.naver.com/novel/categoryProductList.series"
        "?categoryTypeCode=genre&genreCode=205&orderTypeCode=new&is&isFinished=true"
    )


def test_ensure_naver_seed_state_initializes_genre_seed_progress():
    state = {
        "modes": {
            "ongoing": {"next_page": 12, "done": True},
            "completed": {"next_page": 8, "done": True},
        }
    }

    changed = backfill._ensure_naver_seed_state(state)

    assert changed is True
    assert state["seeds"]["romance_ongoing"] == {"next_page": 1, "done": False}
    assert state["seeds"]["mystery_completed"] == {"next_page": 1, "done": False}
    assert state["modes"]["ongoing"] == {"next_page": 12, "done": True}
