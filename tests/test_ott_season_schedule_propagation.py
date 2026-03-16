from datetime import datetime

from services import ott_content_service as content_service
from services import ott_verification_service as verification_service


def test_apply_entry_enrichment_updates_representative_year_for_season_title():
    entry = {
        "title": "더 피트",
        "platform_content_id": "cp-1",
        "platform_url": "https://www.coupangplay.com/content/cp-1",
        "release_start_at": datetime(2025, 5, 14),
        "representative_year": 2025,
    }
    verification_service._apply_entry_enrichment(
        entry,
        {
            "resolved_title": "더 피트 시즌 2",
            "season_label": "시즌 2",
            "release_start_at": datetime(2026, 1, 12),
            "release_end_at": datetime(2026, 4, 20),
            "release_end_status": "scheduled",
        },
    )

    assert entry["title"] == "더 피트 시즌 2"
    assert entry["representative_year"] == 2026
    assert entry["canonical_content_id"].startswith("ott_series:2026:")


def test_resolve_entry_canonical_identity_prefers_release_year_for_season_titles():
    canonical_id, normalized = content_service._resolve_entry_canonical_identity(
        {
            "title": "더 피트 시즌 2",
            "release_start_at": datetime(2026, 1, 12),
            "representative_year": 2025,
        }
    )

    assert canonical_id.startswith("ott_series:2026:")
    assert normalized["title"] == "더 피트 시즌 2"
