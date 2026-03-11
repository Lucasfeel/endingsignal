import views.contents as contents_view


def test_append_browse_cursor_filter_with_source_supplies_all_title_params():
    where_parts = []
    params = []

    contents_view._append_browse_cursor_filter(
        where_parts,
        params,
        "길들여 줘",
        "kakao_page",
        "56548301",
    )

    query_fragment = where_parts[0]

    assert query_fragment.count("%s") == len(params)
    assert params[:-2] == ["길들여 줘"] * 10
    assert params[-2:] == ["kakao_page", "56548301"]


def test_append_browse_cursor_filter_without_source_supplies_all_title_params():
    where_parts = []
    params = []

    contents_view._append_browse_cursor_filter(
        where_parts,
        params,
        "A",
        None,
        "100",
    )

    query_fragment = where_parts[0]

    assert query_fragment.count("%s") == len(params)
    assert params[:-1] == ["A"] * 10
    assert params[-1] == "100"
