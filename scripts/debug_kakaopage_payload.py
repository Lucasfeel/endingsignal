"""Local smoke test for KakaoPage GraphQL payload formatting.

Usage:
    python scripts/debug_kakaopage_payload.py

This script does not make any network calls. It simply prints and
asserts the computed sectionId and normalized param match DevTools.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from services.kakaopage_graphql import build_section_id, normalize_kakaopage_param

DEFAULT_VERIFY_PARAM = {
    "categoryUid": 10,
    "subcategoryUid": "0",
    "bnType": "A",
    "dayTabUid": "2",
    "screenUid": 52,
    "page": 1,
}


def main() -> None:
    normalized = normalize_kakaopage_param(DEFAULT_VERIFY_PARAM)
    section_id = build_section_id(
        normalized["categoryUid"],
        normalized["subcategoryUid"],
        normalized["bnType"],
        normalized["dayTabUid"],
        normalized["screenUid"],
    )
    print("Normalized param:", normalized)
    print("Section ID:", section_id)
    expected_section_id = "static-landing-DayOfWeek-section-layout-10-0-A-2-52"
    assert section_id == expected_section_id, (
        "Section ID mismatch. Expected"
        f" {expected_section_id}, got {section_id}"
    )


if __name__ == "__main__":
    main()
