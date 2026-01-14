import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawlers.kakao_webtoon_crawler import KakaoWebtoonCrawler


def test_extract_profile_status_completed():
    crawler = KakaoWebtoonCrawler()
    payload = {"data": {"badges": [{"type": "STATUS", "code": "COMPLETED"}]}}

    assert crawler._extract_profile_status_from_payload(payload) == "COMPLETED"


def test_extract_profile_status_pause():
    crawler = KakaoWebtoonCrawler()
    payload = {"badges": [{"badgeType": "STATUS", "code": "pause"}]}

    assert crawler._extract_profile_status_from_payload(payload) == "PAUSE"


def test_extract_profile_status_missing():
    crawler = KakaoWebtoonCrawler()
    payload = {"badges": [{"type": "GENRE", "code": "ROMANCE"}]}

    assert crawler._extract_profile_status_from_payload(payload) is None


def test_needs_profile_lookup_ttl_logic():
    crawler = KakaoWebtoonCrawler()
    now = datetime(2024, 1, 10, 10, 0, 0)

    db_info_recent = {
        "status": "완결",
        "kakao_profile_status": "COMPLETED",
        "kakao_profile_status_checked_at": now - timedelta(days=1),
    }
    assert not crawler._needs_profile_lookup("1", db_info_recent, now, ttl_days=7)

    db_info_expired = {
        "status": "완결",
        "kakao_profile_status": "COMPLETED",
        "kakao_profile_status_checked_at": now - timedelta(days=8),
    }
    assert crawler._needs_profile_lookup("1", db_info_expired, now, ttl_days=7)

    db_info_transition = {
        "status": "연재중",
        "kakao_profile_status": "COMPLETED",
        "kakao_profile_status_checked_at": now - timedelta(days=1),
    }
    assert crawler._needs_profile_lookup("1", db_info_transition, now, ttl_days=7)

    db_info_missing = {
        "status": "완결",
        "kakao_profile_status": None,
        "kakao_profile_status_checked_at": None,
    }
    assert crawler._needs_profile_lookup("1", db_info_missing, now, ttl_days=7)

    assert crawler._needs_profile_lookup("1", None, now, ttl_days=7)
