import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import aiohttp

import config


STATIC_LANDING_QUERY = """
query staticLandingDayOfWeekSection($sectionId: ID!, $param: StaticLandingDayOfWeekParamInput!) {
  staticLandingDayOfWeekSection(sectionId: $sectionId, param: $param) {
    isEnd
    totalCount
    param {
      page
      size
    }
    businessModelList {
      param
    }
    subcategoryList {
      param
    }
    dayTabList {
      param
    }
    groups {
      items {
        title
        thumbnail
        scheme
        isLegacy
        eventLog {
          eventMeta {
            series_id
            name
          }
        }
      }
    }
  }
}
"""


def normalize_kakaopage_param(raw: Dict[str, Any]) -> Dict[str, Any]:
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return int(default)

    def _to_str(value: Any, default: str) -> str:
        if value is None:
            return str(default)
        return str(value)

    return {
        "categoryUid": _to_int(raw.get("categoryUid", 0), 0),
        "subcategoryUid": _to_str(raw.get("subcategoryUid", "0"), "0"),
        "bnType": _to_str(raw.get("bnType", ""), ""),
        "dayTabUid": _to_str(raw.get("dayTabUid", ""), ""),
        "screenUid": _to_int(raw.get("screenUid", 0), 0),
        "page": _to_int(raw.get("page", 1), 1),
    }


def build_section_id(
    category_uid: Any,
    subcategory_uid: Any,
    bn_type: Any,
    day_tab_uid: Any,
    screen_uid: Any,
) -> str:
    return "-".join(
        [
            "static",
            "landing",
            "DayOfWeek",
            "section",
            "layout",
            str(category_uid),
            str(subcategory_uid),
            str(bn_type),
            str(day_tab_uid),
            str(screen_uid),
        ]
    )


def _parse_series_id(item: Dict[str, Any]) -> Optional[str]:
    event_meta = ((item.get("eventLog") or {}).get("eventMeta", {}) or {}).get("series_id")
    if event_meta:
        return str(event_meta)

    scheme = item.get("scheme") or ""
    try:
        parsed = urlparse(scheme)
        query_series_id = parse_qs(parsed.query).get("series_id")
        if query_series_id and query_series_id[0]:
            return str(query_series_id[0])
    except Exception:
        return None

    if "series_id=" in scheme:
        try:
            return scheme.split("series_id=")[-1].split("&")[0]
        except Exception:
            return None
    return None


def _parse_thumbnail(thumbnail_field: Any) -> Optional[str]:
    if isinstance(thumbnail_field, str):
        return thumbnail_field
    if isinstance(thumbnail_field, dict):
        return (
            thumbnail_field.get("url")
            or thumbnail_field.get("imageUrl")
            or thumbnail_field.get("link")
            or thumbnail_field.get("src")
        )
    return None


def parse_section_payload(section_payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {
        "isEnd": section_payload.get("isEnd"),
        "totalCount": section_payload.get("totalCount"),
    }

    meta["businessModelList"] = [
        str(entry.get("param"))
        for entry in (section_payload.get("businessModelList") or [])
        if entry.get("param") is not None
    ]
    meta["subcategoryList"] = [
        str(entry.get("param"))
        for entry in (section_payload.get("subcategoryList") or [])
        if entry.get("param") is not None
    ]
    meta["dayTabList"] = [
        str(entry.get("param"))
        for entry in (section_payload.get("dayTabList") or [])
        if entry.get("param") is not None
    ]

    for group in section_payload.get("groups", []) or []:
        for item in group.get("items", []) or []:
            series_id = _parse_series_id(item)
            if not series_id:
                continue
            items.append(
                {
                    "series_id": series_id,
                    "title": (item.get("title") or "").strip(),
                    "thumbnail": _parse_thumbnail(item.get("thumbnail")),
                    "scheme": item.get("scheme"),
                    "isLegacy": bool(item.get("isLegacy")),
                    "eventMeta": (item.get("eventLog") or {}).get("eventMeta"),
                }
            )

    return items, meta


async def fetch_static_landing_section(
    session: aiohttp.ClientSession,
    section_id: str,
    param: Dict[str, Any],
) -> Dict[str, Any]:
    normalized_param = normalize_kakaopage_param(param)
    payload = {
        "operationName": "staticLandingDayOfWeekSection",
        "query": STATIC_LANDING_QUERY,
        "variables": {"sectionId": section_id, "param": normalized_param},
    }

    headers = {
        **config.CRAWLER_HEADERS,
        "Content-Type": "application/json",
        "origin": "https://page.kakao.com",
        "referer": "https://page.kakao.com/",
    }

    async with session.post(config.KAKAOPAGE_GRAPHQL_URL, json=payload, headers=headers) as resp:
        resp.raise_for_status()
        text = await resp.text()
        data = json.loads(text)
        if "errors" in data and data["errors"]:
            raise ValueError(str(data["errors"]))
        return data.get("data", {})
