from datetime import datetime

from services import ott_verification_service as service


def test_part_label_is_not_promoted_into_title():
    candidate = {
        "source_name": "tving",
        "title": "대한민국에서 건물주 되는 법",
        "source_item": {
            "title": "대한민국에서 건물주 되는 법",
            "description": "토·일 / 오후 09:10",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.tving.com/contents/P001783289",
            "ok": True,
            "title": "대한민국에서 건물주 되는 법",
            "payload_titles": ["Mad Concrete Dreams"],
            "body_text": "대한민국에서 건물주 되는 법 파트 1 2026년 3월 14일 공개",
            "description": "토·일 / 오후 09:10",
            "cast": [],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
        {
            "url": "https://namu.wiki/w/%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%97%90%EC%84%9C_%EA%B1%B4%EB%AC%BC%EC%A3%BC_%EB%90%98%EB%8A%94_%EB%B2%95",
            "ok": True,
            "title": "대한민국에서 건물주 되는 법 - 나무위키",
            "payload_titles": ["대한민국에서 건물주 되는 법"],
            "body_text": "방송 기간 2026년 3월 14일 ~ 2026년 4월 19일 (예정)",
            "description": "",
            "cast": [],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": datetime(2026, 4, 19),
            "release_end_status": "scheduled",
            "source": "public_web",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["resolved_title"] == "대한민국에서 건물주 되는 법"
    assert metadata["release_start_at"] == datetime(2026, 3, 14)
    assert metadata["release_end_at"] == datetime(2026, 4, 19)
    assert metadata["release_end_status"] == "scheduled"


def test_nonscripted_requires_verified_finite_six_month_season():
    candidate = {
        "source_name": "coupangplay",
        "title": "Long Variety",
        "source_item": {
            "title": "Long Variety",
            "description": "variety season 1",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.coupangplay.com/content/long-variety",
            "ok": True,
            "title": "Long Variety season 1",
            "payload_titles": ["Long Variety Season 1"],
            "body_text": "variety season 1 2026-01-01 ~ 2026-09-01",
            "description": "variety season 1",
            "cast": [],
            "release_start_at": datetime(2026, 1, 1),
            "release_end_at": datetime(2026, 9, 1),
            "release_end_status": "scheduled",
            "source": "official_episode_schedule",
            "season_specific_match": True,
        }
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["exclude_from_sync"] is True
    assert metadata["exclude_reason"] == "nonscripted_requires_finite_verified_season"


def test_scripted_broadcast_period_is_not_filtered_as_variety():
    candidate = {
        "source_name": "tving",
        "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
        "source_item": {
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
            "description": "\uc11c\uc2a4\ud39c\uc2a4 \ub4dc\ub77c\ub9c8",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.tving.com/contents/P001783289",
            "ok": True,
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95 | TVING",
            "payload_titles": ["Mad Concrete Dreams"],
            "body_text": "\ub4dc\ub77c\ub9c8 tvN 3\uc6d4 14\uc77c \uacf5\uac1c",
            "description": "\uc11c\uc2a4\ud39c\uc2a4 \ub4dc\ub77c\ub9c8",
            "cast": [],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
        {
            "url": "https://namu.wiki/w/%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%97%90%EC%84%9C_%EA%B1%B4%EB%AC%BC%EC%A3%BC_%EB%90%98%EB%8A%94_%EB%B2%95",
            "ok": True,
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95 - \ub098\ubb34\uc704\ud0a4",
            "payload_titles": ["\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95"],
            "body_text": "\ubc29\uc1a1 \uae30\uac04 \ubc29\uc1a1 \uc608\uc815 2026\ub144 3\uc6d4 14\uc77c ~ 2026\ub144 4\uc6d4 19\uc77c (\uc608\uc815)",
            "description": "",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["exclude_from_sync"] is False
    assert metadata["resolved_title"] == "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95"
    assert metadata["release_start_at"] == datetime(2026, 3, 14)
    assert metadata["release_end_at"] == datetime(2026, 4, 19)
    assert metadata["release_end_status"] == "scheduled"


def test_official_body_noise_does_not_promote_unrelated_season_label():
    candidate = {
        "source_name": "tving",
        "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
        "source_item": {
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
            "description": "\uc11c\uc2a4\ud39c\uc2a4 \ub4dc\ub77c\ub9c8",
        },
    }
    documents = [
        {
            "url": "https://www.tving.com/contents/P001783289",
            "ok": True,
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95 | TVING",
            "payload_titles": ["Mad Concrete Dreams"],
            "body_text": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95 \uc2dc\uc98c 1\uac1c \uad00\ub828\uc791 \ud558\uc774\uc7ac\ud0b9 \uc2dc\uc98c2 \ub9ac\ubd80\ud2b8",
            "description": "\uc11c\uc2a4\ud39c\uc2a4 \ub4dc\ub77c\ub9c8",
            "cast": [],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["resolved_title"] == "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95"
    assert metadata["season_label"] == ""


def test_explicit_variety_signal_beats_incidental_scripted_noise():
    text = (
        "1\ubc15 2\uc77c \uc2dc\uc98c4 "
        "\uc608\ub2a5 \ub9ac\uc5bc\ub9ac\ud2f0 \ub85c\ub4dc \ubc84\ub77c\uc774\uc5b4\ud2f0 "
        "\uad00\ub828 \ubb38\uc11c\uc5d0 \ub4dc\ub77c\ub9c8 \ub2e8\uc5b4\uac00 \uc11e\uc5ec \uc788\uc5b4\ub3c4"
    )

    assert service._looks_variety_nonscripted(text) is True


def test_scripted_title_ignores_navigation_noise_in_body_text():
    candidate = {
        "source_name": "wavve",
        "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
        "source_item": {
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
            "description": "\ube5a\uc5d0 \ud5c8\ub355\uc774\ub294 \uc0dd\uacc4\ud615 \uac74\ubb3c\uc8fc\uac00 \uac00\uc871\uc744 \uc9c0\ud0a4\uae30 \uc704\ud55c \uc11c\uc2a4\ud39c\uc2a4 \ub4dc\ub77c\ub9c8",
            "release_end_status": "unknown",
        },
    }
    documents = [
        {
            "url": "https://www.wavve.com/view-more?code=EN100000----GN51#contentid=C3519_C35000000076",
            "ok": True,
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95",
            "payload_titles": [],
            "body_text": "\uc608\ub2a5 \ub4dc\ub77c\ub9c8 \uc601\ud654 \uc2dc\uc0ac\uad50\uc591 \uc560\ub2c8 \ud574\uc678\uc2dc\ub9ac\uc988 LIVE \uac80\uc0c9\ud504\ub85c\ud544 \ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95 3\uc6d4 14\uc77c \uccab \ubc29\uc1a1",
            "description": "\ube5a\uc5d0 \ud5c8\ub355\uc774\ub294 \uc0dd\uacc4\ud615 \uac74\ubb3c\uc8fc\uac00 \uac00\uc871\uc744 \uc9c0\ud0a4\uae30 \uc704\ud55c \uc11c\uc2a4\ud39c\uc2a4 \ub4dc\ub77c\ub9c8",
            "cast": [],
            "release_start_at": datetime(2026, 3, 14),
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "official_rendered_dom",
        },
        {
            "url": "https://namu.wiki/w/%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%97%90%EC%84%9C_%EA%B1%B4%EB%AC%BC%EC%A3%BC_%EB%90%98%EB%8A%94_%EB%B2%95",
            "ok": True,
            "title": "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95 - \ub098\ubb34\uc704\ud0a4",
            "payload_titles": ["\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95"],
            "body_text": "\ubc29\uc1a1 \uae30\uac04 \ubc29\uc1a1 \uc608\uc815 2026\ub144 3\uc6d4 14\uc77c ~ 2026\ub144 4\uc6d4 19\uc77c (\uc608\uc815)",
            "description": "\uc11c\uc2a4\ud39c\uc2a4, \ubc94\uc8c4, \uc2a4\ub9b4\ub7ec, \ube14\ub799 \ucf54\ubbf8\ub514, \uac00\uc871 \ub4dc\ub77c\ub9c8",
            "cast": [],
            "release_start_at": None,
            "release_end_at": None,
            "release_end_status": "unknown",
            "source": "public_web",
        },
    ]

    metadata = service._merge_verification_metadata(candidate=candidate, documents=documents)

    assert metadata["exclude_from_sync"] is False
    assert metadata["resolved_title"] == "\ub300\ud55c\ubbfc\uad6d\uc5d0\uc11c \uac74\ubb3c\uc8fc \ub418\ub294 \ubc95"
    assert metadata["release_start_at"] == datetime(2026, 3, 14)
    assert metadata["release_end_at"] == datetime(2026, 4, 19)
    assert metadata["release_end_status"] == "scheduled"
