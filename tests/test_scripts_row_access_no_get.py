from datetime import datetime

import scripts.archive.backfill_content_urls as backfill
import scripts.diagnose_kakaoreport_errors as diagnose


class RowNoGet:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        return self._data[key]

    def __contains__(self, key):
        return key in self._data


def test_backfill_get_field_handles_row_without_get():
    row = RowNoGet({"content_id": "CID"})

    assert backfill.get_field(row, "content_id") == "CID"


def test_diagnose_extract_report_row_handles_row_without_get():
    created_at = datetime(2024, 1, 2, 3, 4, 5)
    row = RowNoGet(
        {
            "id": 10,
            "created_at": created_at,
            "report_data": {
                "cdc_info": {
                    "fetch_meta": {
                        "errors": ["err"],
                        "request_samples": [{"http_status": 500}],
                        "status": "failed",
                    }
                },
                "summary": "summary",
            },
        }
    )

    extracted = diagnose._extract_report_row(row)

    assert extracted["id"] == 10
    assert extracted["created_at"] == created_at
    assert extracted["errors"] == ["err"]
    assert extracted["request_samples"] == [{"http_status": 500}]
    assert extracted["status"] == "failed"
    assert extracted["summary"] == "summary"
