import json
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import config


STATIC_LANDING_QUERY = """
query StaticLandingDayOfWeekSection($sectionId: String!, $param: StaticLandingDayOfWeekSectionParam!) {
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


def build_section_id(category_uid: str, subcategory_uid: str, bm_type: str, day_tab_uid: str, screen_uid: str) -> str:
    return f"static-landing-DayOfWeek-section-Layout-{category_uid}-{subcategory_uid}-{bm_type}-{day_tab_uid}-{screen_uid}"


def _parse_series_id(item: Dict[str, Any]) -> Optional[str]:
    event_meta = (
        (item.get("eventLog") or {})
        .get("eventMeta", {})
        .get("series_id")
    )
    if event_meta:
        return str(event_meta)

    scheme = item.get("scheme") or ""
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
    payload = {
        "operationName": "StaticLandingDayOfWeekSection",
        "query": STATIC_LANDING_QUERY,
        "variables": {"sectionId": section_id, "param": param},
    }

    async with session.post(
        config.KAKAOPAGE_GRAPHQL_URL,
        json=payload,
        headers={**config.CRAWLER_HEADERS, "Content-Type": "application/json"},
    ) as resp:
        resp.raise_for_status()
        text = await resp.text()
        data = json.loads(text)
        if "errors" in data and data["errors"]:
            raise ValueError(str(data["errors"]))
        return data.get("data", {})
