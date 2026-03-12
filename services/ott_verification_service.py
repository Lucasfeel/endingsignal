from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from crawlers.ott_parser_utils import clean_text, parse_flexible_datetime
from utils.text import normalize_search_text
from utils.time import now_kst_naive, parse_iso_naive_kst

SEARCH_ENDPOINT = "https://html.duckduckgo.com/html/"
SEARCH_TIMEOUT_SECONDS = 20
DOCUMENT_TIMEOUT_SECONDS = 20
MAX_SEARCH_RESULTS = 4
MAX_PUBLIC_DOCUMENTS = 3

ALLOWED_PUBLIC_HOST_SUFFIXES = (
    "namu.wiki",
    "wikipedia.org",
    "mydramalist.com",
    "themoviedb.org",
    "asianwiki.com",
    "imdb.com",
    "netflix.com",
    "disneyplus.com",
    "wavve.com",
    "tving.com",
    "coupangplay.com",
)

PLATFORM_QUERY_LABELS = {
    "coupangplay": "쿠팡플레이",
    "disney_plus": "디즈니 플러스",
    "netflix": "넷플릭스",
    "tving": "티빙",
    "wavve": "웨이브",
}

_KOREAN_RANGE_RE = re.compile(
    r"(?P<start>\d{4}\D+\d{1,2}\D+\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)\s*"
    r"(?:\((?:예정|확정)\))?\s*[~〜\-]\s*"
    r"(?P<end>\d{4}\D+\d{1,2}\D+\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)"
)
_LABELED_SINGLE_DATE_RE = re.compile(
    r"(?:공개일|첫\s*공개|첫\s*방송|방영일|방송일|startDate|dateCreated)\s*[:：]?\s*"
    r"(?P<date>\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{4}\D+\d{1,2}\D+\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)",
    re.I,
)
_BINGE_HINT_RE = re.compile(
    r"(전편|전체\s*에피소드|모든\s*에피소드|전\s*회차|일괄\s*공개|한\s*번에|all\s+episodes)",
    re.I,
)
_WEEKLY_HINT_RE = re.compile(
    r"(매주|주간|weekly|new\s+episodes?)",
    re.I,
)
_CONFIRMED_END_HINT_RE = re.compile(
    r"(종영|완결|피날레|season finale|series finale|방송 종료)",
    re.I,
)
_CAST_LABEL_RE = re.compile(
    r"(?:출연|주연|Starring|Actors?)\s*[:：]?\s*(?P<cast>[^\n\r|]{3,180})",
    re.I,
)


def _normalize_title_tokens(*values: Any) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for value in values:
        if isinstance(value, (list, tuple, set)):
            nested = _normalize_title_tokens(*value)
            for item in nested:
                lowered = item.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                deduped.append(item)
            continue
        text = clean_text(value)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(text)
    return deduped


def _title_matches_any(targets: Sequence[str], *observed_values: Any) -> bool:
    observed = normalize_search_text(" ".join(_normalize_title_tokens(*observed_values)))
    if not observed:
        return False
    for target in _normalize_title_tokens(targets):
        normalized_target = normalize_search_text(target)
        if not normalized_target:
            continue
        if (
            normalized_target == observed
            or normalized_target in observed
            or observed in normalized_target
        ):
            return True
    return False


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return parse_iso_naive_kst(value) or parse_flexible_datetime(value)
    return None


def _allowed_public_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().strip()
    if not host:
        return False
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in ALLOWED_PUBLIC_HOST_SUFFIXES)


def _extract_duckduckgo_result_url(raw_href: str) -> str:
    href = clean_text(raw_href)
    if not href:
        return ""
    parsed = urlparse(href)
    if parsed.netloc.endswith("duckduckgo.com"):
        target = parse_qs(parsed.query).get("uddg", [""])[0]
        return unquote(target) if target else ""
    if href.startswith("//duckduckgo.com/"):
        parsed = urlparse(f"https:{href}")
        target = parse_qs(parsed.query).get("uddg", [""])[0]
        return unquote(target) if target else ""
    return href


def _build_search_queries(candidate: Mapping[str, Any]) -> List[str]:
    source_name = clean_text(candidate.get("source_name"))
    platform_label = PLATFORM_QUERY_LABELS.get(source_name, source_name)
    titles = _normalize_title_tokens(
        candidate.get("title"),
        (candidate.get("source_item") or {}).get("title_alias"),
        (candidate.get("source_item") or {}).get("alt_title"),
    )
    queries: List[str] = []
    for title in titles[:2]:
        queries.append(f'"{title}" {platform_label} 방영 기간')
        queries.append(f'"{title}" {platform_label} 출연')
        queries.append(f'"{title}" 나무위키')
    return _normalize_title_tokens(queries)


def _search_public_result_urls(session: requests.Session, candidate: Mapping[str, Any]) -> List[str]:
    urls: List[str] = []
    seen = set()
    for query in _build_search_queries(candidate):
        try:
            response = session.get(
                SEARCH_ENDPOINT,
                params={"q": query},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=SEARCH_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except Exception:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.select("a.result__a[href]"):
            target = _extract_duckduckgo_result_url(anchor.get("href") or "")
            if not target or target in seen or not _allowed_public_host(target):
                continue
            seen.add(target)
            urls.append(target)
            if len(urls) >= MAX_SEARCH_RESULTS:
                return urls
    return urls


def _extract_json_ld_payloads(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    payloads: List[Dict[str, Any]] = []
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, dict):
            payloads.append(parsed)
        elif isinstance(parsed, list):
            payloads.extend(item for item in parsed if isinstance(item, dict))
    return payloads


def _extract_cast_from_payloads(payloads: Sequence[Mapping[str, Any]]) -> List[str]:
    cast: List[str] = []
    seen = set()
    for payload in payloads:
        for key in ("actors", "actor", "creators", "creator"):
            raw = payload.get(key)
            values = raw if isinstance(raw, list) else [raw]
            for item in values:
                if isinstance(item, dict):
                    name = clean_text(item.get("name"))
                else:
                    name = clean_text(item)
                if not name:
                    continue
                lowered = name.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                cast.append(name)
    return cast


def _extract_cast_from_text(text: str) -> List[str]:
    compact = clean_text(text)
    match = _CAST_LABEL_RE.search(compact)
    if not match:
        return []
    values = re.split(r"[,/|·]\s*|\s{2,}", match.group("cast"))
    deduped: List[str] = []
    seen = set()
    for value in values:
        name = clean_text(value)
        if len(name) < 2:
            continue
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(name)
    return deduped[:10]


def _parse_range_dates(text: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    compact = clean_text(text)
    match = _KOREAN_RANGE_RE.search(compact)
    if not match:
        return None, None
    start = parse_flexible_datetime(match.group("start"))
    end = parse_flexible_datetime(match.group("end"))
    if start and end and start.year != end.year and "월" in match.group("end") and not re.search(r"\d{4}", match.group("end")):
        end = end.replace(year=start.year)
    return start, end


def _extract_date_signals(text: str, *, fallback_start: Any = None) -> Dict[str, Any]:
    compact = clean_text(text)
    start_dt, end_dt = _parse_range_dates(compact)
    if start_dt is None:
        labeled = _LABELED_SINGLE_DATE_RE.search(compact)
        if labeled:
            start_dt = parse_flexible_datetime(labeled.group("date"))
    if start_dt is None:
        start_dt = _coerce_datetime(fallback_start)

    binge = bool(_BINGE_HINT_RE.search(compact))
    weekly = bool(_WEEKLY_HINT_RE.search(compact))
    confirmed_hint = bool(_CONFIRMED_END_HINT_RE.search(compact))
    now_value = now_kst_naive()

    release_end_status = "unknown"
    if end_dt is not None:
        if confirmed_hint or end_dt <= now_value:
            release_end_status = "confirmed"
        else:
            release_end_status = "scheduled"
    elif start_dt is not None and binge and not weekly:
        end_dt = start_dt
        release_end_status = "confirmed" if start_dt <= now_value else "scheduled"

    return {
        "release_start_at": start_dt,
        "release_end_at": end_dt,
        "release_end_status": release_end_status,
        "binge_hint": binge,
        "weekly_hint": weekly,
        "confirmed_end_hint": confirmed_hint,
    }


def _fetch_document(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    target = clean_text(url)
    if not target:
        return None
    try:
        response = session.get(
            target,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
            },
            timeout=DOCUMENT_TIMEOUT_SECONDS,
            allow_redirects=True,
        )
        response.raise_for_status()
    except Exception as exc:
        return {
            "url": target,
            "ok": False,
            "error": str(exc),
        }

    soup = BeautifulSoup(response.text, "html.parser")
    payloads = _extract_json_ld_payloads(soup)
    document_title = clean_text(soup.title.string if soup.title and soup.title.string else "")
    body_text = clean_text(soup.get_text(" ", strip=True))
    cast = _extract_cast_from_payloads(payloads) or _extract_cast_from_text(body_text)

    payload_titles = _normalize_title_tokens(
        [payload.get("name") for payload in payloads if isinstance(payload, Mapping)],
    )
    description = ""
    for payload in payloads:
        description = clean_text(payload.get("description"))
        if description:
            break
    if not description:
        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description is not None:
            description = clean_text(meta_description.get("content"))

    date_texts = [body_text]
    for payload in payloads:
        for key in ("startDate", "endDate", "dateCreated"):
            value = clean_text(payload.get(key))
            if value:
                date_texts.append(value)
    date_signal = _extract_date_signals(" ".join(date_texts))

    return {
        "url": clean_text(response.url) or target,
        "ok": True,
        "title": document_title,
        "payload_titles": payload_titles,
        "body_text": body_text,
        "description": description,
        "cast": cast,
        **date_signal,
    }


def _merge_verification_metadata(
    *,
    candidate: Mapping[str, Any],
    documents: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    source_item = dict(candidate.get("source_item") or {})
    titles = _normalize_title_tokens(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    matched_docs = [
        doc
        for doc in documents
        if doc.get("ok")
        and _title_matches_any(
            titles,
            doc.get("title"),
            doc.get("payload_titles"),
            doc.get("body_text"),
        )
    ]

    start_candidates = [_coerce_datetime(source_item.get("release_start_at"))]
    end_candidates: List[datetime] = []
    cast_values = _normalize_title_tokens(source_item.get("cast"))
    description = clean_text(source_item.get("description"))
    title_alias = _normalize_title_tokens(source_item.get("title_alias"), source_item.get("alt_title"))
    evidence_urls = []

    for doc in matched_docs:
        evidence_urls.append(clean_text(doc.get("url")))
        start_dt = _coerce_datetime(doc.get("release_start_at"))
        end_dt = _coerce_datetime(doc.get("release_end_at"))
        if start_dt is not None:
            start_candidates.append(start_dt)
        if end_dt is not None:
            end_candidates.append(end_dt)
        cast_values = _normalize_title_tokens(cast_values, doc.get("cast"))
        description = clean_text(doc.get("description")) or description
        title_alias = _normalize_title_tokens(title_alias, doc.get("title"), doc.get("payload_titles"))

    release_start_at = min([item for item in start_candidates if item is not None], default=None)
    distinct_end_dates = sorted({item.isoformat(): item for item in end_candidates}.values(), key=lambda item: item.isoformat())

    release_end_at = None
    release_end_status = clean_text(source_item.get("release_end_status")).lower() or "unknown"
    resolution_state = "tracking"
    if len(distinct_end_dates) > 1:
        resolution_state = "conflict"
        release_end_status = "unknown"
    elif len(distinct_end_dates) == 1:
        release_end_at = distinct_end_dates[0]
        release_end_status = "confirmed" if release_end_at <= now_kst_naive() else "scheduled"

    if release_end_at is None:
        for doc in matched_docs:
            if doc.get("release_end_status") in {"scheduled", "confirmed"}:
                release_end_status = str(doc["release_end_status"])
                if release_end_status == "confirmed" and release_start_at and doc.get("binge_hint"):
                    release_end_at = release_start_at
                break

    return {
        "matched_docs": matched_docs,
        "matched_count": len(matched_docs),
        "release_start_at": release_start_at,
        "release_end_at": release_end_at,
        "release_end_status": release_end_status,
        "resolution_state": resolution_state,
        "cast": cast_values,
        "description": description,
        "title_alias": title_alias,
        "evidence_urls": [url for url in evidence_urls if url],
    }


def _apply_entry_enrichment(entry: Dict[str, Any], metadata: Mapping[str, Any]) -> None:
    if metadata.get("cast"):
        entry["cast"] = list(metadata["cast"])
    if metadata.get("description"):
        entry["description"] = clean_text(metadata["description"])
    if metadata.get("title_alias"):
        entry["title_alias"] = list(metadata["title_alias"])
    if metadata.get("release_start_at") is not None:
        entry["release_start_at"] = metadata["release_start_at"]
    if metadata.get("release_end_at") is not None:
        entry["release_end_at"] = metadata["release_end_at"]
    if metadata.get("release_end_status"):
        entry["release_end_status"] = metadata["release_end_status"]
    if metadata.get("resolution_state"):
        entry["resolution_state"] = metadata["resolution_state"]

    start_dt = _coerce_datetime(entry.get("release_start_at"))
    if start_dt is not None:
        entry["upcoming"] = start_dt > now_kst_naive()
        entry["availability_status"] = "scheduled" if entry["upcoming"] else "available"


def _find_snapshot_existing_row(
    write_plan: Mapping[str, Any],
    canonical_content_id: str,
) -> Optional[Dict[str, Any]]:
    for row in write_plan.get("snapshot_existing_rows") or []:
        if not isinstance(row, dict):
            continue
        if clean_text(row.get("content_id")) == canonical_content_id:
            return dict(row)
    return None


def _find_platform_link(
    write_plan: Mapping[str, Any],
    *,
    canonical_content_id: str,
    source_name: str,
) -> Optional[Dict[str, Any]]:
    for row in write_plan.get("platform_links") or []:
        if not isinstance(row, dict):
            continue
        if (
            clean_text(row.get("canonical_content_id")) == canonical_content_id
            and clean_text(row.get("platform_source")) == source_name
        ):
            return dict(row)
    return None


def _build_snapshot_entry(
    write_plan: Mapping[str, Any],
    row: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    canonical_content_id = clean_text(row.get("canonical_content_id"))
    source_name = clean_text(write_plan.get("source_name"))
    existing_row = _find_snapshot_existing_row(write_plan, canonical_content_id)
    if not existing_row:
        return None

    meta = dict(existing_row.get("meta") or {})
    common = dict(meta.get("common") or {})
    ott = dict(meta.get("ott") or {})
    platform_link = _find_platform_link(
        write_plan,
        canonical_content_id=canonical_content_id,
        source_name=source_name,
    )
    platforms = [
        dict(item)
        for item in (ott.get("platforms") or [])
        if isinstance(item, Mapping)
    ]
    platform_meta = next(
        (item for item in platforms if clean_text(item.get("source")) == source_name),
        {},
    )

    content_url = (
        clean_text(platform_meta.get("content_url"))
        or clean_text((platform_link or {}).get("platform_url"))
        or clean_text(common.get("content_url"))
        or clean_text(common.get("url"))
    )
    title_alias = _normalize_title_tokens(
        common.get("title_alias"),
        common.get("alt_title"),
    )

    return {
        "title": clean_text(existing_row.get("title")) or canonical_content_id,
        "status": clean_text(existing_row.get("status")) or "",
        "platform_content_id": (
            clean_text(platform_meta.get("platform_content_id"))
            or clean_text((platform_link or {}).get("platform_content_id"))
            or canonical_content_id
        ),
        "platform_url": content_url,
        "content_url": content_url,
        "availability_status": clean_text(platform_meta.get("availability_status"))
        or ("scheduled" if bool(ott.get("upcoming")) else "available"),
        "thumbnail_url": clean_text(platform_meta.get("thumbnail_url")) or clean_text(common.get("thumbnail_url")),
        "alt_title": clean_text(common.get("alt_title")),
        "title_alias": title_alias,
        "cast": _normalize_title_tokens(ott.get("cast"), common.get("authors")),
        "description": clean_text(ott.get("description")),
        "release_start_at": _coerce_datetime(ott.get("release_start_at")) or _coerce_datetime(row.get("release_start_at")),
        "release_end_at": _coerce_datetime(ott.get("release_end_at")) or _coerce_datetime(row.get("release_end_at")),
        "release_end_status": clean_text(ott.get("release_end_status")).lower()
        or clean_text(row.get("release_end_status")).lower()
        or "unknown",
        "resolution_state": clean_text(ott.get("resolution_state")) or clean_text(row.get("resolution_state")) or "tracking",
        "upcoming": bool(ott.get("upcoming")),
    }


def _build_watchlist_candidate(write_plan: Mapping[str, Any], row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    canonical_content_id = clean_text(row.get("canonical_content_id"))
    if not canonical_content_id:
        return None
    raw_all_content_today = write_plan.get("all_content_today")
    all_content_today = raw_all_content_today if isinstance(raw_all_content_today, dict) else {}
    entry = all_content_today.get(canonical_content_id)
    if not isinstance(entry, dict):
        entry = _build_snapshot_entry(write_plan, row)
        if isinstance(all_content_today, dict) and isinstance(entry, dict):
            all_content_today[canonical_content_id] = entry
    if not isinstance(entry, dict):
        return None
    next_check_at = _coerce_datetime(row.get("next_check_at"))
    if next_check_at is not None and next_check_at > now_kst_naive():
        return None

    release_end_status = clean_text(entry.get("release_end_status")).lower() or clean_text(row.get("release_end_status")).lower() or "unknown"

    return {
        "content_id": canonical_content_id,
        "source_name": clean_text(write_plan.get("source_name")),
        "title": clean_text(entry.get("title")) or canonical_content_id,
        "expected_status": clean_text(entry.get("status")) or clean_text(row.get("status")),
        "previous_status": clean_text(entry.get("status")) or clean_text(row.get("status")) or None,
        "content_url": clean_text(entry.get("platform_url") or entry.get("content_url")),
        "change_kinds": ["watchlist_recheck"],
        "source_item": {
            **dict(entry),
            "release_end_status": release_end_status,
            "release_end_at": _coerce_datetime(entry.get("release_end_at")) or _coerce_datetime(row.get("release_end_at")),
            "release_start_at": _coerce_datetime(entry.get("release_start_at")) or _coerce_datetime(row.get("release_start_at")),
        },
        "watchlist_recheck": True,
    }


def collect_ott_verification_targets(write_plan: Mapping[str, Any]) -> List[Dict[str, Any]]:
    candidates = [
        dict(item)
        for item in (write_plan.get("verification_candidates") or [])
        if isinstance(item, dict)
    ]
    seen = {clean_text(item.get("content_id")) for item in candidates}
    for row in write_plan.get("watchlist_rows") or []:
        if not isinstance(row, dict):
            continue
        candidate = _build_watchlist_candidate(write_plan, row)
        if candidate is None:
            continue
        content_id = clean_text(candidate.get("content_id"))
        if not content_id or content_id in seen:
            continue
        seen.add(content_id)
        candidates.append(candidate)
    return candidates


def verify_ott_write_plan(write_plan: Mapping[str, Any], *, source_name: str) -> Dict[str, Any]:
    targets = collect_ott_verification_targets(write_plan)
    if not targets:
        return {
            "gate": "not_applicable",
            "mode": "official_public_web",
            "reason": "no_candidate_changes",
            "message": f"no candidate or watchlist items to verify for {source_name}",
            "apply_allowed": True,
            "changed_count": 0,
            "verified_count": 0,
            "watchlist_rechecked_count": 0,
            "items": [],
        }

    raw_all_content_today = write_plan.get("all_content_today")
    all_content_today = raw_all_content_today if isinstance(raw_all_content_today, dict) else {}
    results: List[Dict[str, Any]] = []
    blocking_failures = []
    verified_changed_count = 0
    watchlist_rechecked_count = 0

    with requests.Session() as session:
        for candidate in targets:
            candidate_id = clean_text(candidate.get("content_id"))
            documents = []
            official_url = clean_text(candidate.get("content_url"))
            if official_url:
                official_doc = _fetch_document(session, official_url)
                if isinstance(official_doc, dict):
                    documents.append(official_doc)

            source_item = candidate.get("source_item") or {}
            need_public_search = (
                not any(doc.get("ok") for doc in documents)
                or not _coerce_datetime(source_item.get("release_start_at"))
                or clean_text(source_item.get("release_end_status")).lower() in {"", "unknown"}
                or not source_item.get("cast")
                or bool(candidate.get("watchlist_recheck"))
            )
            if need_public_search:
                public_urls = _search_public_result_urls(session, candidate)
                for url in public_urls[:MAX_PUBLIC_DOCUMENTS]:
                    doc = _fetch_document(session, url)
                    if isinstance(doc, dict):
                        documents.append(doc)

            metadata = _merge_verification_metadata(candidate=candidate, documents=documents)
            matched_docs = metadata.get("matched_docs") or []
            ok = bool(matched_docs)

            entry = all_content_today.get(candidate_id)
            if ok and isinstance(entry, dict):
                _apply_entry_enrichment(entry, metadata)

            is_watchlist_recheck = bool(candidate.get("watchlist_recheck"))
            if is_watchlist_recheck:
                watchlist_rechecked_count += 1
            elif ok:
                verified_changed_count += 1
            else:
                blocking_failures.append(candidate_id)

            results.append(
                {
                    "content_id": candidate_id,
                    "title": clean_text(candidate.get("title")) or candidate_id,
                    "ok": ok or is_watchlist_recheck,
                    "reason": "evidence_matched" if ok else ("watchlist_unresolved" if is_watchlist_recheck else "no_web_evidence"),
                    "verification_method": "official_public_web",
                    "watchlist_recheck": is_watchlist_recheck,
                    "matched_count": metadata.get("matched_count", 0),
                    "evidence_urls": metadata.get("evidence_urls") or [],
                    "observed_release_start_at": metadata.get("release_start_at").isoformat() if metadata.get("release_start_at") else None,
                    "observed_release_end_at": metadata.get("release_end_at").isoformat() if metadata.get("release_end_at") else None,
                    "observed_release_end_status": metadata.get("release_end_status"),
                    "observed_cast_count": len(metadata.get("cast") or []),
                    "change_kinds": list(candidate.get("change_kinds") or []),
                }
            )

    changed_candidates = [item for item in targets if not item.get("watchlist_recheck")]
    if blocking_failures:
        return {
            "gate": "blocked",
            "mode": "official_public_web",
            "reason": "verification_mismatch",
            "message": (
                f"{source_name} verified {verified_changed_count}/{len(changed_candidates)} changed items; "
                f"{len(blocking_failures)} changed items still lack external evidence"
            ),
            "apply_allowed": False,
            "changed_count": len(changed_candidates),
            "verified_count": verified_changed_count,
            "watchlist_rechecked_count": watchlist_rechecked_count,
            "items": results,
        }

    return {
        "gate": "passed" if changed_candidates else "not_applicable",
        "mode": "official_public_web",
        "reason": "verified_all_changed_items" if changed_candidates else "watchlist_rechecked",
        "message": (
            f"{source_name} verified {verified_changed_count}/{len(changed_candidates)} changed items"
            if changed_candidates
            else f"{source_name} rechecked {watchlist_rechecked_count} watchlist items"
        ),
        "apply_allowed": True,
        "changed_count": len(changed_candidates),
        "verified_count": verified_changed_count,
        "watchlist_rechecked_count": watchlist_rechecked_count,
        "items": results,
    }
