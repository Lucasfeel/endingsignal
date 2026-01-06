import json
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import config


STATIC_LANDING_QUERY = """
query staticLandingDayOfWeekSection($sectionId: ID!, $param: StaticLandingDayOfWeekParamInput!) {
  staticLandingDayOfWeekSection(sectionId: $sectionId, param: $param) {
    id
    uid
    type
    title
    isEnd
    totalCount
    param {
      categoryUid
      businessModel {
        name
        param {
          uid
        }
      }
      subcategory {
        name
        param {
          uid
        }
      }
      dayTab {
        name
        param {
          uid
        }
      }
    }
    items {
      id
      content {
        id
        title
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
    content = item.get("content") or {}
    content_id = content.get("id")
    if content_id is not None:
        return str(content_id)

    if "id" in item:
        return str(item.get("id"))
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
        "title": section_payload.get("title"),
        "uid": section_payload.get("uid"),
        "type": section_payload.get("type"),
    }

    param = section_payload.get("param") or {}
    meta["businessModelList"] = []
    if param.get("businessModel"):
        bm_param = (param.get("businessModel") or {}).get("param") or {}
        if bm_param.get("uid") is not None:
            meta["businessModelList"].append(str(bm_param.get("uid")))

    meta["subcategoryList"] = []
    if param.get("subcategory"):
        sub_param = (param.get("subcategory") or {}).get("param") or {}
        if sub_param.get("uid") is not None:
            meta["subcategoryList"].append(str(sub_param.get("uid")))

    meta["dayTabList"] = []
    if param.get("dayTab"):
        day_param = (param.get("dayTab") or {}).get("param") or {}
        if day_param.get("uid") is not None:
            meta["dayTabList"].append(str(day_param.get("uid")))

    for item in section_payload.get("items", []) or []:
        series_id = _parse_series_id(item)
        if not series_id:
            continue
        content = item.get("content") or {}
        title = (content.get("title") or item.get("title") or "").strip()
        if not title:
            continue
        items.append(
            {
                "series_id": series_id,
                "title": title,
                "thumbnail": _parse_thumbnail(item.get("thumbnail")),
                "isLegacy": bool(item.get("isLegacy")),
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
        "Accept": "*/*",
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
