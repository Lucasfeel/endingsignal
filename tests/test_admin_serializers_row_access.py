from datetime import datetime

import views.admin as admin_view


class RowNoGet:
    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        return self.data[key]

    def __contains__(self, key):
        return key in self.data


def test_serialize_cdc_event_row_no_get_does_not_throw():
    now = datetime(2024, 8, 1, 10, 0, 0)
    row = RowNoGet(
        {
            "id": 1,
            "content_id": "CID",
            "source": "SRC",
            "event_type": "CONTENT_COMPLETED",
            "final_status": "완결",
            "final_completed_at": now,
            "resolved_by": "crawler",
            "created_at": now,
            "title": "Title",
            "content_type": "webtoon",
            "status": "연재중",
            "meta": {},
            "is_deleted": False,
        }
    )

    result = admin_view._serialize_cdc_event(row)

    assert result["created_at"] == now.isoformat()
    assert result["final_completed_at"] == now.isoformat()
    assert result["content_id"] == "CID"
    assert result["event_type"] == "CONTENT_COMPLETED"


def test_serialize_daily_crawler_report_row_no_get_does_not_throw():
    now = datetime(2024, 8, 2, 9, 30, 0)
    row = RowNoGet(
        {
            "id": 2,
            "crawler_name": "naver",
            "status": "ok",
            "report_data": {},
            "created_at": now,
        }
    )

    result = admin_view._serialize_daily_crawler_report(row)

    assert result["created_at"] == now.isoformat()
    assert result["crawler_name"] == "naver"
