from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup

import config
from crawlers.ott_parser_utils import clean_text, parse_flexible_datetime
from services.ott_content_service import build_canonical_content_id, normalize_ott_genres
from utils.text import normalize_search_text
from utils.time import now_kst_naive, parse_iso_naive_kst

SEARCH_ENDPOINT = "https://html.duckduckgo.com/html/"
SEARCH_TIMEOUT_SECONDS = 20
DOCUMENT_TIMEOUT_SECONDS = 20
TMDB_TIMEOUT_SECONDS = 15
MAX_SEARCH_RESULTS = 4
MAX_PUBLIC_DOCUMENTS = 6
MAX_CAST_MEMBERS = 4
TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_WEB_BASE = "https://www.themoviedb.org"
TMDB_MAX_RESULTS = 3
WIKIPEDIA_SEARCH_ENDPOINTS = (
    "https://en.wikipedia.org/w/api.php",
    "https://ko.wikipedia.org/w/api.php",
)
WIKIPEDIA_SEARCH_TIMEOUT_SECONDS = 12
MAX_WIKIPEDIA_SEARCH_RESULTS = 2
RENDERED_OFFICIAL_SOURCES = {"tving", "disney_plus", "coupangplay"}
RENDERED_DOCUMENT_TIMEOUT_MS = 45_000
MIN_REASONABLE_RELEASE_YEAR = 1900
MAX_REASONABLE_RELEASE_YEAR_OFFSET = 10
_OFFICIAL_NOISE_LINE_MARKERS = (
    "좋아하실 만한 콘텐츠",
    "비슷한 콘텐츠",
    "관련 영상",
    "고객센터",
    "이용약관",
    "개인정보",
    "대표자",
    "대표이사",
    "사업자",
    "호스팅서비스",
    "호스팅사업자",
    "월트디즈니컴퍼니코리아",
    "통신판매업",
    "디즈니+ 가입",
    "추가 유의사항 확인",
    "할인 혜택 종료",
    "브랜드 바로가기",
    "그룹 계열사 바로가기",
    "챗봇/채팅 상담",
    "1:1 게시판 문의",
    "all rights reserved",
    "copyright",
    "©",
)
_FIELD_STOP_MARKERS = (
    "좋아하실 만한 콘텐츠",
    "비슷한 콘텐츠",
    "관련 영상",
    "관련 콘텐츠",
    "관람 등급",
    "시청 등급",
    "제작:",
    "제작 ",
    "감독:",
    "감독 ",
    "크리에이터",
    "creator",
    "장르:",
    "장르 ",
    "공개일:",
    "공개일 ",
    "스트리밍",
    "링크",
    "디즈니+ 이용약관",
    "이용약관",
    "개인정보",
    "고객센터",
    "대표자",
    "사업자",
    "월트디즈니컴퍼니코리아",
    "티빙 시작하기",
    "디즈니+ 가입",
)
_GENRE_SIGNAL_RE = re.compile(
    "|".join(
        [
            r"\b(?:drama|animation|anime|documentary|docuseries|variety|reality)\b",
            r"\uB4DC\uB77C\uB9C8",
            r"\uC560\uB2C8(?:\uBA54\uC774\uC158)?",
            r"\uC608\uB2A5",
            r"\uB2E4\uD050(?:\uBA58\uD130\uB9AC)?",
            r"\uB9AC\uC5BC\uB9AC\uD2F0",
            r"\uBC84\uB77C\uC774\uC5B4\uD2F0",
        ]
    ),
    re.I,
)
_NAME_NOISE_RE = re.compile(
    "|".join(
        [
            r"retrieved",
            r"privacy",
            r"terms",
            r"\uC774\uC6A9\uC57D\uAD00",
            r"\uAC1C\uC778\uC815\uBCF4",
            r"\uACE0\uAC1D\uC13C\uD130",
            r"\uB300\uD45C",
            r"\uC0AC\uC5C5\uC790",
            r"\uC2A4\uD2B8\uB9AC\uBC0D",
            r"\uAD00\uB78C",
            r"\uC2DC\uCCAD",
            r"\uACF5\uAC1C",
            r"\uBC29\uC601",
            r"\uC2DC\uC98C",
            r"\uC5D0\uD53C\uC18C\uB4DC",
            r"\uC88B\uC544\uD558\uC2E4",
            r"\uCF58\uD150\uCE20",
            r"\uB514\uC988\uB2C8",
            r"\uD2F0\uBE59",
            r"\uCFE0\uD321",
            r"\uAC10\uB3C5",
            r"\uC5F0\uCD9C",
            r"\uAC01\uBCF8",
            r"\uAE30\uD68D",
            r"\uC81C\uC791",
            r"wikipedia",
            r"namuwiki",
            r"director",
            r"producer",
            r"writer",
            r"creator",
            r"watch",
            r"membership",
            r"episode\s*info",
        ]
    ),
    re.I,
)
_ASCII_NAME_TOKEN_RE = re.compile(r"^[A-Za-z][A-Za-z.'-]*$")
_CJK_NAME_TOKEN_RE = re.compile(r"^[\u3131-\u318E\uAC00-\uD7A3\u3040-\u30FF\u3400-\u9FFF·]+$")
_COMMON_KOREAN_SURNAMES = {
    "김", "이", "박", "최", "정", "강", "조", "윤", "장", "임", "오", "한",
    "송", "신", "권", "황", "안", "류", "전", "홍", "고", "문", "양", "손",
    "배", "백", "허", "유", "나", "노", "심", "곽", "주", "우", "차", "민",
    "진", "엄", "변", "천", "염",
}
_PERSON_NAME_STOPWORDS = {
    "drama",
    "series",
    "season",
    "episode",
    "episodes",
    "mystery",
    "thriller",
    "comedy",
    "family",
    "crime",
    "legal",
    "office",
    "variety",
    "reality",
    "documentary",
    "anime",
    "animation",
    "content",
    "membership",
    "button",
    "watch",
    "streaming",
    "cast",
    "starring",
    "director",
    "producer",
    "writer",
    "creator",
    "작가",
    "감독",
    "제작",
    "프로듀서",
    "총괄",
    "쇼러너",
    "멤버",
    "원년",
    "특별",
    "특별출연",
    "박사",
}

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

_DATE_TOKEN_RE = r"(?:\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일|\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)"
_LABELED_RANGE_RE = re.compile(
    rf"(?:방송\s*기간|방영\s*기간|공개\s*기간)\s*[:|]?\s*(?P<start>{_DATE_TOKEN_RE})\s*[~〜∼-]\s*(?P<end>{_DATE_TOKEN_RE})(?:\s*\((?P<hint>예정|확정)\))?",
    re.I,
)
_KOREAN_RANGE_RE = re.compile(
    r"(?P<start>\d{4}\D+\d{1,2}\D+\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)\s*"
    r"(?:\((?:예정|확정)\))?\s*[~〜\-]\s*"
    r"(?P<end>\d{4}\D+\d{1,2}\D+\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)(?:\s*\((?P<hint>예정|확정)\))?"
)
_LABELED_SINGLE_DATE_RE = re.compile(
    r"(?:공개일|첫\s*공개|첫\s*방송|방영일|방송일|startDate|dateCreated)\s*[:：]?\s*"
    r"(?P<date>\d{4}[./-]\d{1,2}[./-]\d{1,2}|\d{4}\D+\d{1,2}\D+\d{1,2}|\d{1,2}\s*월\s*\d{1,2}\s*일)",
    re.I,
)
_LABELED_OPEN_RANGE_RE = re.compile(
    rf"(?:방송\s*예정|방영\s*예정|공개\s*예정)\s*(?P<start>{_DATE_TOKEN_RE})\s*[~〜∼-](?:\s*\[\d+\])?",
    re.I,
)
_ON_AIR_OPEN_RANGE_RE = re.compile(
    rf"(?P<start>{_DATE_TOKEN_RE})\s*[~〜∼-]\s*(?:ON\s*AIR|방영\s*중|공개\s*중|방송\s*중)",
    re.I,
)
_TRAILING_SINGLE_DATE_HINT_RE = re.compile(
    rf"(?P<date>{_DATE_TOKEN_RE})\s*(?:공개|방영|첫\s*방송|첫\s*공개|오픈|premiere|streaming)",
    re.I,
)
_OPEN_ENDED_START_RE = re.compile(
    rf"(?P<date>{_DATE_TOKEN_RE})\s*(?:부터(?:\s*(?:방영|공개)\s*중)?|(?:방영|공개)\s*중)",
    re.I,
)
_SINGLE_DATE_CONTEXT_HINT_RE = re.compile(
    r"(공개|방영|첫\s*방송|첫\s*공개|오픈|premiere|streaming|방송\s*중|방영\s*중|공개\s*중)",
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
    r"(?:^|[\s(])(?:출연|주연|Starring|Actors?)\s*[:：]\s*(?P<cast>[^\n\r|]{3,240})",
    re.I,
)
_VISIBLE_EPISODE_LINE_RE = re.compile(r"^(?P<number>\d{1,3})\.\s*\S")
_GENRE_LABEL_RE = re.compile(
    r"(?:장르|genre|genres?)\s*[:|]?\s*(?P<genre>[^\n\r|]{2,180})",
    re.I,
)
_EPISODE_TOTAL_PATTERNS = [
    re.compile(r"(?:총\s*)?(?P<count>\d{1,3})\s*부작", re.I),
    re.compile(r"총\s*(?P<count>\d{1,3})\s*화", re.I),
    re.compile(r"no\.\s*of\s*episodes\s*(?P<count>\d{1,3})", re.I),
    re.compile(r"(?P<count>\d{1,3})\s+episodes", re.I),
]
_MULTI_WEEKDAY_RE = re.compile(
    r"(?P<labels>(?:월요일|화요일|수요일|목요일|금요일|토요일|일요일|월|화|수|목|금|토|일)"
    r"(?:\s*[·ㆍ,/&]\s*(?:월요일|화요일|수요일|목요일|금요일|토요일|일요일|월|화|수|목|금|토|일))+)"
)
_SINGLE_WEEKDAY_RE = re.compile(
    r"(?:매주|방송\s*시간|편성|공개|방영)[^\n\r]{0,24}?"
    r"(?P<label>월요일|화요일|수요일|목요일|금요일|토요일|일요일|월|화|수|목|금|토|일)",
    re.I,
)
_WEEKDAY_TOKEN_MAP = {
    "월요일": 0,
    "월": 0,
    "화요일": 1,
    "화": 1,
    "수요일": 2,
    "수": 2,
    "목요일": 3,
    "목": 3,
    "금요일": 4,
    "금": 4,
    "토요일": 5,
    "토": 5,
    "일요일": 6,
    "일": 6,
}


_SEASON_LABEL_RE = re.compile(
    r"(?P<label>(?:\uc2dc\uc98c|season)\s*\d+)(?!\s*(?:\uac1c|\ud3b8))",
    re.I,
)
_ANIME_SEASON_NUMBER_RE = re.compile(
    r"(?<!\d)(?P<number>\d{1,2})\s*기(?!\s*(?:개|명|분|년|월|화))"
)
_NONSCRIPTED_KEYWORD_RE = re.compile(
    "|".join(
        [
            r"\uc608\ub2a5",
            r"\ubc84\ub77c\uc774\uc5b4\ud2f0",
            r"\ub9ac\uc5bc\ub9ac\ud2f0",
            r"\ud1a0\ud06c\uc1fc",
            r"\ud1a0\ud06c",
            r"\ub2e4\ud050",
            r"\ub2e4\ud050\uba58\ud130\ub9ac",
            r"\uad50\uc591",
            r"\uc5ec\ud589\s*\uc608\ub2a5",
            r"\uc5f0\uc560\s*\ub9ac\uc5bc\ub9ac\ud2f0",
            r"\uc11c\ubc14\uc774\ubc8c",
            r"\uad00\ucc30",
            r"\ud734\uba3c\ub2e4\ud050",
            r"\uac8c\uc784\uc1fc",
            r"reality",
            r"variety",
            r"talk\s*show",
            r"documentary",
            r"docuseries",
            r"competition",
            r"survival",
        ]
    ),
    re.I,
)
_VARIETY_NONSCRIPTED_KEYWORD_RE = re.compile(
    "|".join(
        [
            r"\uc608\ub2a5",
            r"\ubc84\ub77c\uc774\uc5b4\ud2f0",
            r"\ub9ac\uc5bc\ub9ac\ud2f0",
            r"\ud1a0\ud06c\uc1fc",
            r"\ud1a0\ud06c",
            r"\uac8c\uc784\uc1fc",
            r"\uc5f0\uc560\s*\ub9ac\uc5bc\ub9ac\ud2f0",
            r"\uad00\ucc30",
            r"\uc5ec\ud589\s*\uc608\ub2a5",
            r"reality",
            r"variety",
            r"talk\s*show",
            r"game\s*show",
            r"dating",
            r"observational",
        ]
    ),
    re.I,
)
_STRONG_VARIETY_KEYWORD_RE = re.compile(
    "|".join(
        [
            r"\uc608\ub2a5",
            r"\ubc84\ub77c\uc774\uc5b4\ud2f0",
            r"\ub9ac\uc5bc\ub9ac\ud2f0",
            r"\ud1a0\ud06c\uc1fc",
            r"\uac8c\uc784\uc1fc",
            r"\uc5f0\uc560\s*\ub9ac\uc5bc\ub9ac\ud2f0",
            r"\uc5ec\ud589\s*\uc608\ub2a5",
            r"reality",
            r"variety",
            r"talk\s*show",
            r"game\s*show",
            r"dating",
        ]
    ),
    re.I,
)
_SCRIPTED_KEYWORD_RE = re.compile(
    "|".join(
        [
            r"\ub4dc\ub77c\ub9c8",
            r"\uc2dc\ub9ac\uc988",
            r"\uc2a4\ub9b4\ub7ec",
            r"\ubc94\uc8c4",
            r"\ubbf8\uc2a4\ud130\ub9ac",
            r"\ub85c\ub9e8\uc2a4",
            r"\ubc95\uc815",
            r"\uc758\ud559",
            r"\uc561\uc158",
            r"\ucf54\ubbf8\ub514",
            r"\ud310\ud0c0\uc9c0",
            r"\uc560\ub2c8",
            r"drama",
            r"thriller",
            r"crime",
            r"mystery",
            r"romance",
            r"legal",
            r"medical",
            r"fantasy",
            r"animation",
            r"scripted",
        ]
    ),
    re.I,
)
_WEEKLY_CURRENT_HINT_RE = re.compile(
    r"(\ub9e4\uc8fc|weekly|new\s+episodes?)",
    re.I,
)
_HISTORY_DOC_HINT_RE = re.compile(
    r"(/\uc5ed\uc0ac/|/\ubc29\uc601\s*\ubaa9\ub85d|/\ud68c\ucc28\s*\uc815\ubcf4|\uc5d0\ud53c\uc18c\ub4dc\s*\ubaa9\ub85d)",
    re.I,
)

OFFICIAL_HOST_SUFFIXES = {
    "coupangplay": "coupangplay.com",
    "disney_plus": "disneyplus.com",
    "netflix": "netflix.com",
    "tving": "tving.com",
    "wavve": "wavve.com",
}


def _source_name_from_url(url: Any) -> str:
    host = _doc_host(url)
    for source_name, suffix in OFFICIAL_HOST_SUFFIXES.items():
        if host == suffix or host.endswith(f".{suffix}"):
            return source_name
    return ""

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


def _limit_cast_values(*values: Any, max_items: int = MAX_CAST_MEMBERS) -> List[str]:
    if max_items <= 0:
        return []
    return _normalize_title_tokens(*values)[:max_items]


def _normalize_multiline_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = value.replace("\r\n", "\n").replace("\r", "\n").replace("\u00A0", " ")
    lines = []
    for raw_line in text.split("\n"):
        cleaned = re.sub(r"[ \t]+", " ", raw_line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def _split_text_lines(*values: Any) -> List[str]:
    lines: List[str] = []
    seen = set()
    for value in values:
        normalized = _normalize_multiline_text(value)
        if not normalized:
            compact = clean_text(value)
            if compact:
                lowered = compact.lower()
                if lowered not in seen:
                    seen.add(lowered)
                    lines.append(compact)
            continue
        for line in normalized.split("\n"):
            cleaned = clean_text(line)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            lines.append(cleaned)
    return lines


def _contains_noise_marker(text: str, markers: Sequence[str]) -> bool:
    lowered = clean_text(text).lower()
    if not lowered:
        return False
    return any(marker.lower() in lowered for marker in markers)


def _extract_focus_lines(*values: Any) -> List[str]:
    return [
        line
        for line in _split_text_lines(*values)
        if not _contains_noise_marker(line, _OFFICIAL_NOISE_LINE_MARKERS)
    ]


def _truncate_at_markers(text: str, markers: Sequence[str]) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    cutoff = len(cleaned)
    lowered = cleaned.lower()
    for marker in markers:
        index = lowered.find(marker.lower())
        if index > 0:
            cutoff = min(cutoff, index)
    return clean_text(cleaned[:cutoff])


def _looks_like_person_name(value: Any) -> bool:
    text = clean_text(value)
    if not text or len(text) > 40:
        return False
    if _NAME_NOISE_RE.search(text):
        return False
    tokens = [token.lower() for token in re.split(r"\s+", text) if token]
    if any(token in _PERSON_NAME_STOPWORDS for token in tokens):
        return False
    if any(char.isdigit() for char in text):
        return False
    if re.search(r"https?://|@|[<>{}\\[\\]=*_~]", text):
        return False
    if len(text.split()) > 4:
        return False
    return re.fullmatch(r"[A-Za-z\u3131-\u318E\uAC00-\uD7A3\u3040-\u30FF\u3400-\u9FFF .'\-·]+", text) is not None


def _parse_name_sequence(text: str) -> List[str]:
    tokens = [clean_text(token) for token in text.split() if clean_text(token)]
    names: List[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if _NAME_NOISE_RE.search(token):
            break
        if _ASCII_NAME_TOKEN_RE.fullmatch(token):
            chunk = [token]
            index += 1
            while index < len(tokens) and _ASCII_NAME_TOKEN_RE.fullmatch(tokens[index]) and len(chunk) < 4:
                chunk.append(tokens[index])
                index += 1
            candidate = clean_text(" ".join(chunk))
            if _looks_like_person_name(candidate):
                names.append(candidate)
            continue
        if _CJK_NAME_TOKEN_RE.fullmatch(token):
            if len(token) == 1 and token not in _COMMON_KOREAN_SURNAMES:
                if (
                    index + 2 < len(tokens)
                    and tokens[index + 1] in _COMMON_KOREAN_SURNAMES
                    and _CJK_NAME_TOKEN_RE.fullmatch(tokens[index + 2])
                ):
                    if _looks_like_person_name(token):
                        names.append(token)
                    index += 1
                    continue
                if index + 1 < len(tokens) and _CJK_NAME_TOKEN_RE.fullmatch(tokens[index + 1]):
                    candidate = clean_text(f"{token} {tokens[index + 1]}")
                    if _looks_like_person_name(candidate):
                        names.append(candidate)
                        index += 2
                        continue
                if _looks_like_person_name(token):
                    names.append(token)
                index += 1
                continue
            if (
                len(token) >= 4
                and index + 2 < len(tokens)
                and tokens[index + 1] in _COMMON_KOREAN_SURNAMES
                and _CJK_NAME_TOKEN_RE.fullmatch(tokens[index + 2])
            ):
                if _looks_like_person_name(token):
                    names.append(token)
                index += 1
                continue
            if index + 1 < len(tokens) and _CJK_NAME_TOKEN_RE.fullmatch(tokens[index + 1]):
                if token in _COMMON_KOREAN_SURNAMES and len(tokens[index + 1]) <= 2:
                    candidate = clean_text(f"{token}{tokens[index + 1]}")
                else:
                    candidate = clean_text(f"{token} {tokens[index + 1]}")
                if _looks_like_person_name(candidate):
                    names.append(candidate)
                    index += 2
                    continue
            if _looks_like_person_name(token):
                names.append(token)
            index += 1
            continue
        index += 1
    return names


def _extract_people_from_candidate(text: str) -> List[str]:
    candidate = _truncate_at_markers(text, _FIELD_STOP_MARKERS)
    candidate = re.sub(r"(?i)^(?:\uCD9C\uC5F0|\uC8FC\uC5F0|starring|actors?)\s*[:|]?\s*", "", candidate).strip()
    candidate = re.sub(r"\s+(?:\uC678|and more|etc\.?)\b.*$", "", candidate, flags=re.I).strip()
    if not candidate:
        return []
    if any(separator in candidate for separator in [",", "/", "|", "·"]):
        raw_parts = re.split(r"\s*[,/|·]\s*", candidate)
    else:
        raw_parts = _parse_name_sequence(candidate)
    people: List[str] = []
    seen = set()
    for raw_part in raw_parts:
        person = clean_text(raw_part)
        if not _looks_like_person_name(person):
            continue
        lowered = person.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        people.append(person)
    return people[:MAX_CAST_MEMBERS]


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


def _normalize_season_label(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    match = _SEASON_LABEL_RE.search(text)
    if not match:
        return ""
    label = clean_text(match.group("label"))
    label = re.sub(r"(?i)^season", "\uc2dc\uc98c", label)
    return clean_text(label)


def _extract_season_label(*values: Any) -> str:
    for value in values:
        if isinstance(value, (list, tuple, set)):
            label = _extract_season_label(*value)
            if label:
                return label
            continue
        label = _normalize_season_label(value)
        if label:
            return label
    return ""


def _extract_season_number(value: Any) -> Optional[int]:
    label = _normalize_season_label(value)
    if not label:
        return None
    match = re.search(r"(\d+)", label)
    if not match:
        return None
    try:
        season_number = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return season_number if season_number > 0 else None


def _extract_followup_season_numbers(*values: Any) -> List[int]:
    numbers = set()
    normalized_values = _normalize_title_tokens(*values)
    anime_context = any(
        re.search(r"(?i)\banime\b|\banimation\b|\uC560\uB2C8\uBA54\uC774\uC158", value)
        for value in normalized_values
    )
    for value in normalized_values:
        for match in _SEASON_LABEL_RE.finditer(value):
            explicit = _extract_season_number(match.group("label"))
            if explicit is not None:
                numbers.add(explicit)
        if not anime_context:
            continue
        for match in _ANIME_SEASON_NUMBER_RE.finditer(value):
            try:
                number = int(match.group("number"))
            except (TypeError, ValueError):
                continue
            if 1 <= number <= 20:
                numbers.add(number)
    return sorted(numbers)


def _mentions_other_season(text: Any, season_label: str) -> bool:
    compact = clean_text(text)
    expected = _extract_season_number(season_label)
    if not compact or expected is None:
        return False
    seen = set()
    for match in _SEASON_LABEL_RE.finditer(compact):
        number = _extract_season_number(match.group("label"))
        if number is not None:
            seen.add(number)
    return bool(seen and any(number != expected for number in seen))


def _strip_season_label(text: Any) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    return clean_text(_SEASON_LABEL_RE.sub("", cleaned))


def _promote_title_with_season(title: Any, season_label: str) -> str:
    safe_title = clean_text(title)
    label = _normalize_season_label(season_label)
    if not safe_title or not label:
        return safe_title
    if _normalize_season_label(safe_title) == label:
        return safe_title
    seasonless_title = _strip_season_label(safe_title)
    if not seasonless_title:
        seasonless_title = safe_title
    return clean_text(f"{seasonless_title} {label}")


def _extract_season_specific_range(text: Any, season_label: str) -> Tuple[Optional[datetime], Optional[datetime], Optional[str]]:
    compact = clean_text(text)
    label = _normalize_season_label(season_label)
    if not compact or not label:
        return None, None, None
    number_match = re.search(r"(\d+)", label)
    if not number_match:
        return None, None, None
    season_number = number_match.group(1)
    pattern = re.compile(
        rf"(?:\uc2dc\uc98c|season|part|\ud30c\ud2b8)\s*{season_number}(?:[^0-9]{{0,48}}?)"
        rf"(?P<start>{_DATE_TOKEN_RE})\s*[~\-]\s*(?P<end>{_DATE_TOKEN_RE})(?:\s*\((?P<hint>\uc608\uc815|\ud655\uc815)\))?",
        re.I,
    )
    match = pattern.search(compact)
    if not match:
        return None, None, None
    start = parse_flexible_datetime(match.group("start"))
    end = parse_flexible_datetime(match.group("end"))
    if start and end and start.year != end.year and not re.search(r"\d{4}", match.group("end")):
        end = end.replace(year=start.year)
    return start, end, clean_text(match.groupdict().get("hint")).lower() or None


def _infer_implied_next_season_label(
    *,
    source_name: str,
    source_item: Mapping[str, Any],
    matched_docs: Sequence[Mapping[str, Any]],
    season_label: str,
) -> str:
    if season_label or source_name not in {"netflix", "disney_plus"}:
        return season_label

    latest_doc_season_numbers = set()
    for doc in matched_docs:
        if not isinstance(doc, Mapping):
            continue
        doc_numbers = set()
        for value in (
            doc.get("season_label"),
            doc.get("title"),
            doc.get("payload_titles"),
            doc.get("body_text"),
            doc.get("description"),
        ):
            doc_numbers.update(_extract_followup_season_numbers(value))
        if len(doc_numbers) < 2:
            continue
        latest_doc_season = max(doc_numbers)
        latest_doc_signal = _extract_date_signals(
            " ".join(
                part
                for part in [
                    clean_text(doc.get("title")),
                    clean_text(doc.get("description")),
                    clean_text(doc.get("body_text")),
                    " ".join(_normalize_title_tokens(doc.get("payload_titles"))),
                ]
                if part
            ),
            season_label=f"시즌 {latest_doc_season}",
        )
        if (
            latest_doc_signal.get("release_start_at") is not None
            or latest_doc_signal.get("release_end_at") is not None
            or latest_doc_signal.get("release_end_status") in {"scheduled", "confirmed"}
        ):
            latest_doc_season_numbers.add(latest_doc_season)

    if latest_doc_season_numbers and source_item.get("release_start_at") in {None, ""}:
        return f"시즌 {max(latest_doc_season_numbers)}"

    source_start_at = _coerce_datetime(source_item.get("release_start_at"))
    source_cast = _sanitize_cast_values(source_item.get("cast"))
    if source_start_at is None:
        return season_label

    historical_dates: List[datetime] = []
    historical_cast_inputs = []
    max_season = 0
    dated_seasons = set()
    for doc in matched_docs:
        if not isinstance(doc, Mapping):
            continue
        doc_seasons = set()
        for value in (
            doc.get("season_label"),
            doc.get("title"),
            doc.get("payload_titles"),
            doc.get("body_text"),
        ):
            for season_number in _extract_followup_season_numbers(value):
                if season_number > max_season:
                    max_season = season_number
                doc_seasons.add(season_number)
        dated_for_history = False
        for value in (doc.get("release_end_at"), doc.get("release_start_at")):
            resolved = _coerce_datetime(value)
            if resolved is not None and resolved < source_start_at:
                historical_dates.append(resolved)
                dated_for_history = True
        if dated_for_history and not _is_official_signal_doc(source_name, doc):
            historical_cast_inputs.append(doc.get("cast"))
            dated_seasons.update(doc_seasons)
    latest_known = max(historical_dates) if historical_dates else None
    if latest_known is None or source_start_at <= latest_known + timedelta(days=90):
        return season_label

    future_followup_with_strong_season_signal = bool(
        source_start_at > now_kst_naive()
        and latest_known <= now_kst_naive()
        and source_start_at <= latest_known + timedelta(days=365)
        and max_season > 0
        and dated_seasons
    )

    historical_cast = _sanitize_cast_values(historical_cast_inputs)
    overlap = {name.lower() for name in source_cast} & {name.lower() for name in historical_cast}
    if not source_cast and not future_followup_with_strong_season_signal:
        return season_label
    if historical_cast and overlap and not future_followup_with_strong_season_signal:
        return season_label

    if max_season > 0:
        if future_followup_with_strong_season_signal and dated_seasons:
            inferred_number = max(dated_seasons) + 1
        elif historical_cast and not overlap:
            inferred_number = max_season
        else:
            inferred_number = max(dated_seasons) + 1 if dated_seasons else max_season
    else:
        inferred_number = 2
    return f"시즌 {inferred_number}"


def _looks_non_scripted(*values: Any) -> bool:
    merged = " ".join(_normalize_title_tokens(*values))
    return bool(merged and _NONSCRIPTED_KEYWORD_RE.search(merged))


def _looks_variety_nonscripted(*values: Any) -> bool:
    merged = " ".join(_normalize_title_tokens(*values))
    if not merged or not _VARIETY_NONSCRIPTED_KEYWORD_RE.search(merged):
        return False
    if _STRONG_VARIETY_KEYWORD_RE.search(merged):
        return True
    return not _SCRIPTED_KEYWORD_RE.search(merged)


def _looks_scripted(*values: Any) -> bool:
    merged = " ".join(_normalize_title_tokens(*values))
    return bool(merged and _SCRIPTED_KEYWORD_RE.search(merged))


def _has_weekly_current_hint(*values: Any) -> bool:
    merged = " ".join(_normalize_title_tokens(*values))
    return bool(merged and _WEEKLY_CURRENT_HINT_RE.search(merged))


def _is_history_like_doc(doc: Mapping[str, Any]) -> bool:
    merged = " ".join(
        _normalize_title_tokens(
            doc.get("url"),
            doc.get("title"),
            doc.get("body_text"),
        )
    )
    return bool(merged and _HISTORY_DOC_HINT_RE.search(merged))


def _doc_host(url: Any) -> str:
    return (urlparse(clean_text(url)).hostname or "").lower().strip()


def _is_official_doc(source_name: str, doc: Mapping[str, Any]) -> bool:
    host = _doc_host(doc.get("url"))
    suffix = OFFICIAL_HOST_SUFFIXES.get(clean_text(source_name))
    return bool(host and suffix and (host == suffix or host.endswith(f".{suffix}")))


def _is_official_signal_doc(source_name: str, doc: Mapping[str, Any]) -> bool:
    source = clean_text(doc.get("source"))
    return source in {"official_episode_schedule", "official_rendered_dom"} or _is_official_doc(source_name, doc)


def _is_official_priority_doc(source_name: str, doc: Mapping[str, Any]) -> bool:
    source = clean_text(doc.get("source"))
    if source in {
        "official_crawl_metadata",
        "official_coupang_metadata",
        "official_episode_schedule",
        "official_rendered_dom",
    }:
        return True
    return _is_official_doc(source_name, doc)


def _official_doc_priority(doc: Mapping[str, Any]) -> int:
    source = clean_text(doc.get("source"))
    if source == "official_episode_schedule":
        return 0
    if source == "official_rendered_dom":
        return 1
    if source == "official_coupang_metadata":
        return 2
    if source == "official_crawl_metadata":
        return 3
    if clean_text(doc.get("url")):
        return 4
    return 5


def _is_namuwiki_doc(doc: Mapping[str, Any]) -> bool:
    host = _doc_host(doc.get("url"))
    return bool(host == "namu.wiki" or host.endswith(".namu.wiki"))


def _is_trusted_date_doc(source_name: str, doc: Mapping[str, Any]) -> bool:
    source = clean_text(doc.get("source"))
    if source == "official_crawl_metadata":
        return False
    if source in {"official_episode_schedule", "official_rendered_dom"}:
        return True
    return _is_official_doc(source_name, doc) or _is_namuwiki_doc(doc)


def _doc_mentions_season(doc: Mapping[str, Any], season_label: str) -> bool:
    label = _normalize_season_label(season_label)
    if not label:
        return False
    merged = " ".join(
        _normalize_title_tokens(
            doc.get("title"),
            doc.get("payload_titles"),
            doc.get("body_text"),
            doc.get("description"),
            doc.get("url"),
        )
    )
    return bool(merged and normalize_search_text(label) in normalize_search_text(merged))


def _extract_episode_total(*values: Any) -> Optional[int]:
    merged = " ".join(clean_text(value) for value in values if clean_text(value))
    if not merged:
        return None
    candidates: List[int] = []
    for pattern in _EPISODE_TOTAL_PATTERNS:
        for match in pattern.finditer(merged):
            try:
                count = int(match.group("count"))
            except Exception:
                continue
            if 1 <= count <= 300:
                candidates.append(count)
    return max(candidates) if candidates else None


def _extract_visible_episode_count_from_text(*values: Any) -> Optional[int]:
    numbers = set()
    for line in _split_text_lines(*values):
        match = _VISIBLE_EPISODE_LINE_RE.match(clean_text(line))
        if not match:
            continue
        try:
            number = int(match.group("number"))
        except (TypeError, ValueError):
            continue
        if 1 <= number <= 300:
            numbers.add(number)
    return max(numbers) if numbers else None


def _extract_visible_episode_count_from_soup(soup: BeautifulSoup) -> Optional[int]:
    root = soup.select_one("#episodes")
    if root is None:
        return None
    numbers = set()
    for item in root.select("li"):
        match = _VISIBLE_EPISODE_LINE_RE.search(clean_text(item.get_text(" ", strip=True)))
        if not match:
            continue
        try:
            number = int(match.group("number"))
        except (TypeError, ValueError):
            continue
        if 1 <= number <= 300:
            numbers.add(number)
    if numbers:
        return max(numbers)
    return _extract_visible_episode_count_from_text(root.get_text("\n", strip=True))


def _extract_schedule_weekdays(*values: Any) -> List[int]:
    merged = " ".join(clean_text(value) for value in values if clean_text(value))
    if not merged:
        return []
    resolved: List[int] = []
    seen = set()
    for match in _MULTI_WEEKDAY_RE.finditer(merged):
        labels = re.split(r"\s*[·ㆍ,/&]\s*", clean_text(match.group("labels")))
        for label in labels:
            weekday = _WEEKDAY_TOKEN_MAP.get(label)
            if weekday is None or weekday in seen:
                continue
            seen.add(weekday)
            resolved.append(weekday)
    if resolved:
        return sorted(resolved)
    single = _SINGLE_WEEKDAY_RE.search(merged)
    if not single:
        return []
    weekday = _WEEKDAY_TOKEN_MAP.get(clean_text(single.group("label")))
    return [weekday] if weekday is not None else []


def _infer_release_end_at(
    release_start_at: Optional[datetime],
    episode_total: Optional[int],
    schedule_weekdays: Sequence[int],
) -> Optional[datetime]:
    if release_start_at is None or not episode_total or episode_total <= 1:
        return None
    weekdays = sorted({int(day) for day in schedule_weekdays if 0 <= int(day) <= 6})
    if not weekdays:
        return None
    current = release_start_at
    emitted = 1
    guard = 0
    while emitted < episode_total and guard < 800:
        current = current + timedelta(days=1)
        if current.weekday() in weekdays:
            emitted += 1
        guard += 1
    if emitted == episode_total and current >= release_start_at:
        return current
    return None


def _coerce_datetime(value: Any) -> Optional[datetime]:
    now_value = now_kst_naive()

    def _sanitize(resolved: Optional[datetime]) -> Optional[datetime]:
        if resolved is None:
            return None
        if resolved.year < MIN_REASONABLE_RELEASE_YEAR:
            return None
        if resolved.year > now_value.year + MAX_REASONABLE_RELEASE_YEAR_OFFSET:
            return None
        return resolved

    if isinstance(value, datetime):
        return _sanitize(value)
    if isinstance(value, str):
        return _sanitize(parse_iso_naive_kst(value) or parse_flexible_datetime(value))
    return None


def _allowed_public_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower().strip()
    if not host:
        return False
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in ALLOWED_PUBLIC_HOST_SUFFIXES)


def _extract_latin_tokens(*values: Any) -> List[str]:
    tokens: List[str] = []
    seen = set()
    for value in values:
        for token in re.findall(r"[A-Za-z0-9]{3,}", clean_text(value)):
            lowered = token.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            tokens.append(token)
    return tokens


def _shares_latin_token(*values: Any) -> bool:
    token_sets = []
    for value in values:
        tokens = {token.lower() for token in _extract_latin_tokens(value)}
        if tokens:
            token_sets.append(tokens)
    if len(token_sets) < 2:
        return False
    base = token_sets[0]
    return any(base & other for other in token_sets[1:])


def _extract_leading_latin_query(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    match = re.match(r"([A-Za-z0-9][A-Za-z0-9 .:&'_-]{1,32})", text)
    if not match:
        return ""
    fragment = clean_text(match.group(1))
    if not fragment or not _extract_latin_tokens(fragment):
        return ""
    return fragment


def _date_floor(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime(value.year, value.month, value.day)


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
    source_item = dict(candidate.get("source_item") or {})
    season_label = _extract_season_label(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        source_item.get("raw_schedule_note"),
        source_item.get("description"),
    )
    titles = _normalize_title_tokens(
        _promote_title_with_season(candidate.get("title"), season_label),
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        _extract_latin_tokens(
            source_item.get("title_alias"),
            source_item.get("alt_title"),
            source_item.get("description"),
            source_item.get("raw_schedule_note"),
        ),
    )
    queries: List[str] = []
    cast_values = _sanitize_cast_values(source_item.get("cast"))[:2]
    for title in titles[:2]:
        queries.append(f'"{title}" {platform_label} 방영 기간')
        queries.append(f'"{title}" {platform_label} 공개일')
        queries.append(f'"{title}" {platform_label} 회차')
        queries.append(f'"{title}" {platform_label} 출연')
        queries.append(f'"{title}" 나무위키')
        queries.append(f'"{title}" 위키백과')
        queries.append(f'"{title}" IMDb')
        if cast_values:
            queries.append(" ".join([f'"{title}"', *[f'"{name}"' for name in cast_values], "IMDb"]))
            queries.append(" ".join([f'"{title}"', *[f'"{name}"' for name in cast_values], "season"]))
    return _normalize_title_tokens(queries)


def _augment_candidate_with_doc_aliases(
    candidate: Mapping[str, Any],
    docs: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    source_item = dict(candidate.get("source_item") or {})
    latin_aliases: List[str] = []
    for doc in docs:
        if not isinstance(doc, Mapping):
            continue
        for value in (
            doc.get("title"),
            doc.get("payload_titles"),
            _extract_leading_latin_query(doc.get("title")),
            _extract_leading_latin_query(doc.get("payload_titles")),
            _extract_leading_latin_query(doc.get("body_text")),
        ):
            for token in _normalize_title_tokens(value):
                if _extract_latin_tokens(token):
                    latin_aliases.append(token)
    aliases = _normalize_title_tokens(
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        [doc.get("title") for doc in docs if isinstance(doc, Mapping)],
        [doc.get("payload_titles") for doc in docs if isinstance(doc, Mapping)],
        latin_aliases,
    )
    source_item["title_alias"] = aliases
    enriched = dict(candidate)
    enriched["source_item"] = source_item
    return enriched


def _dedupe_urls(urls: Iterable[str]) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for url in urls:
        cleaned = clean_text(url)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def _build_direct_public_result_urls(candidate: Mapping[str, Any]) -> List[str]:
    source_item = dict(candidate.get("source_item") or {})
    titles = _normalize_title_tokens(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    season_label = _extract_season_label(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        source_item.get("raw_schedule_note"),
        source_item.get("description"),
    )

    urls: List[str] = []
    for title in titles[:4]:
        base_title = _strip_season_label(title) or title
        short_title = re.split(r"\s*[:：\-–]\s*", base_title, 1)[0].strip()
        urls.append(f"https://namu.wiki/w/{quote(title, safe='')}")
        urls.append(f"https://namu.wiki/w/{quote(f'{base_title}(드라마)', safe='')}")
        urls.append(f"https://namu.wiki/w/{quote(f'{base_title} (드라마)', safe='')}")
        if short_title and short_title != base_title:
            urls.append(f"https://namu.wiki/w/{quote(f'{short_title}(드라마)', safe='')}")
            urls.append(f"https://namu.wiki/w/{quote(f'{short_title} (드라마)', safe='')}")
        if season_label:
            urls.append(
                f"https://namu.wiki/w/{quote(_promote_title_with_season(base_title, season_label), safe='')}"
            )

    return _dedupe_urls(url for url in urls if _allowed_public_host(url))


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


def _search_wikipedia_result_candidates(
    session: requests.Session,
    candidate: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    source_item = dict(candidate.get("source_item") or {})
    titles = _normalize_title_tokens(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    queries = _normalize_title_tokens(
        titles,
        [_extract_leading_latin_query(title) for title in titles],
    )
    results: List[Dict[str, Any]] = []
    seen = set()
    for endpoint in WIKIPEDIA_SEARCH_ENDPOINTS:
        for query in queries[:3]:
            try:
                response = session.get(
                    endpoint,
                    params={
                        "action": "query",
                        "list": "search",
                        "srsearch": query,
                        "srlimit": MAX_WIKIPEDIA_SEARCH_RESULTS,
                        "format": "json",
                    },
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=WIKIPEDIA_SEARCH_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                continue
            host = urlparse(endpoint).netloc
            for item in payload.get("query", {}).get("search", []) or []:
                title = clean_text(item.get("title"))
                if not title:
                    continue
                if _extract_latin_tokens(query):
                    if not _shares_latin_token(query, title):
                        continue
                elif not _title_matches_any(titles, title):
                    continue
                page_url = f"https://{host}/wiki/{quote(title.replace(' ', '_'), safe=':_()')}"
                if page_url in seen or not _allowed_public_host(page_url):
                    continue
                seen.add(page_url)
                results.append(
                    {
                        "url": page_url,
                        "search_source": "wikipedia_api",
                        "search_query": query,
                        "search_title": title,
                        "allow_query_alias": _shares_latin_token(query, title),
                    }
                )
    return results


def _collect_public_result_urls(session: requests.Session, candidate: Mapping[str, Any]) -> List[str]:
    return _dedupe_urls(
        [
            *_build_direct_public_result_urls(candidate),
            *_search_public_result_urls(session, candidate),
        ]
    )


def _public_docs_have_useful_signals(documents: Sequence[Mapping[str, Any]]) -> bool:
    for doc in documents:
        if not isinstance(doc, Mapping) or not doc.get("ok"):
            continue
        if (
            _coerce_datetime(doc.get("release_start_at")) is not None
            or _coerce_datetime(doc.get("release_end_at")) is not None
            or clean_text(doc.get("release_end_status")).lower() in {"scheduled", "confirmed"}
            or bool(_normalize_title_tokens(doc.get("cast")))
            or isinstance(doc.get("episode_total"), int)
        ):
            return True
    return False


def _fetch_public_documents(
    session: requests.Session,
    candidate: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    documents: List[Dict[str, Any]] = []
    seen = set()

    for doc in _fetch_tmdb_documents(session, candidate):
        if not isinstance(doc, dict) or not doc.get("ok"):
            continue
        cleaned_url = clean_text(doc.get("url"))
        if not cleaned_url or cleaned_url in seen or len(documents) >= MAX_PUBLIC_DOCUMENTS:
            continue
        seen.add(cleaned_url)
        documents.append(doc)

    def _append_document(url: str, *, extra_titles: Optional[Sequence[str]] = None) -> None:
        cleaned_url = clean_text(url)
        if not cleaned_url or cleaned_url in seen or len(documents) >= MAX_PUBLIC_DOCUMENTS:
            return
        seen.add(cleaned_url)
        doc = _fetch_document(session, cleaned_url)
        if not isinstance(doc, dict) or not doc.get("ok"):
            return
        if extra_titles:
            doc["payload_titles"] = _normalize_title_tokens(
                doc.get("payload_titles"),
                extra_titles,
            )
        documents.append(doc)

    for url in _build_direct_public_result_urls(candidate):
        _append_document(url)
        if len(documents) >= MAX_PUBLIC_DOCUMENTS or _public_docs_have_useful_signals(documents):
            return documents

    for result in _search_wikipedia_result_candidates(session, candidate):
        extra_titles = [result.get("search_title")]
        if result.get("allow_query_alias"):
            extra_titles.append(result.get("search_query"))
        _append_document(result.get("url") or "", extra_titles=extra_titles)
        if len(documents) >= MAX_PUBLIC_DOCUMENTS or _public_docs_have_useful_signals(documents):
            return documents

    for url in _collect_public_result_urls(session, candidate):
        _append_document(url)
        if len(documents) >= MAX_PUBLIC_DOCUMENTS:
            return documents

    return documents


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
    return cast[:10]


def _extract_cast_from_text(text: str) -> List[str]:
    compact = clean_text(text)
    match = _CAST_LABEL_RE.search(compact)
    if not match:
        return []
    values = re.split(r"[,/|·]\s*|\s{2,}", match.group("cast"))
    deduped: List[str] = []
    seen = set()
    for value in values:
        for name in _extract_people_from_candidate(value):
            lowered = name.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(name)
    return deduped[:10]


def _extract_labeled_cast_values(docs: Sequence[Mapping[str, Any]]) -> List[str]:
    values: List[str] = []
    for doc in docs:
        if not isinstance(doc, Mapping):
            continue
        values.extend(_extract_cast_from_text(doc.get("body_text") or ""))
        values.extend(_extract_cast_from_text(doc.get("description") or ""))
    return _sanitize_cast_values(values)


def _merge_cast_priority(primary: Sequence[str], fallback: Sequence[str]) -> List[str]:
    merged: List[str] = []
    seen = set()
    for value in list(primary) + list(fallback):
        name = clean_text(value)
        if not name:
            continue
        lowered = name.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        merged.append(name)
        if len(merged) >= MAX_CAST_MEMBERS:
            break
    return merged


def _resolve_cast_values(
    source_name: str,
    source_item: Mapping[str, Any],
    matched_docs: Sequence[Mapping[str, Any]],
) -> List[str]:
    official_docs = sorted(
        (
            dict(doc)
            for doc in matched_docs
            if isinstance(doc, Mapping) and _is_official_priority_doc(source_name, doc)
        ),
        key=_official_doc_priority,
    )
    official_structured_cast_values = _sanitize_cast_values(
        [doc.get("cast") for doc in official_docs],
    )
    tmdb_structured_cast_values = _sanitize_cast_values(
        [
            doc.get("cast")
            for doc in matched_docs
            if isinstance(doc, Mapping) and clean_text(doc.get("source")) == "tmdb"
        ]
    )
    if official_structured_cast_values:
        if len(official_structured_cast_values) >= 3:
            return official_structured_cast_values[:MAX_CAST_MEMBERS]
        return _merge_cast_priority(official_structured_cast_values, tmdb_structured_cast_values)

    official_text_cast_values = _extract_labeled_cast_values(official_docs)
    if official_text_cast_values:
        if len(official_text_cast_values) >= 3:
            return official_text_cast_values[:MAX_CAST_MEMBERS]
        return _merge_cast_priority(official_text_cast_values, tmdb_structured_cast_values)

    source_item_cast = _sanitize_cast_values(source_item.get("cast"))
    if source_item_cast:
        return _merge_cast_priority(source_item_cast, tmdb_structured_cast_values)

    public_structured_cast_values = _sanitize_cast_values(
        [
            doc.get("cast")
            for doc in matched_docs
            if isinstance(doc, Mapping) and not _is_official_priority_doc(source_name, doc)
        ]
    )
    if public_structured_cast_values:
        return public_structured_cast_values

    public_cast_values = _extract_labeled_cast_values(
        [
            doc
            for doc in matched_docs
            if isinstance(doc, Mapping) and not _is_official_priority_doc(source_name, doc)
        ]
    )
    return public_cast_values


def _resolve_verified_genres(
    source_name: str,
    source_item: Mapping[str, Any],
    matched_docs: Sequence[Mapping[str, Any]],
    classification_context_text: str,
) -> List[str]:
    explicit_official_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping)
        and clean_text(doc.get("source")) in {
            "official_crawl_metadata",
            "official_coupang_metadata",
            "official_episode_schedule",
            "official_rendered_dom",
        }
    ]
    explicit_official_genres = normalize_ott_genres(
        _collect_strict_genre_inputs(
            [doc.get("genre_text") for doc in explicit_official_docs],
            [doc.get("description") for doc in explicit_official_docs],
            [doc.get("title") for doc in explicit_official_docs],
            [doc.get("payload_titles") for doc in explicit_official_docs],
        ),
        platform_source=source_name,
    )
    if explicit_official_genres and explicit_official_genres[0] != "etc":
        return explicit_official_genres

    official_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping) and _is_official_priority_doc(source_name, doc)
    ]
    official_genres = normalize_ott_genres(
        source_item.get("genre"),
        source_item.get("genres"),
        source_item.get("category"),
        [doc.get("genre_text") for doc in official_docs],
        [doc.get("description") for doc in official_docs],
        [doc.get("payload_titles") for doc in official_docs],
        [doc.get("title") for doc in official_docs],
        platform_source=source_name,
    )
    if official_genres and official_genres[0] != "etc":
        return official_genres

    public_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping) and not _is_official_priority_doc(source_name, doc)
    ]
    public_genres = normalize_ott_genres(
        [doc.get("genre_text") for doc in public_docs],
        [doc.get("description") for doc in public_docs],
        [doc.get("payload_titles") for doc in public_docs],
        [doc.get("title") for doc in public_docs],
        classification_context_text,
        platform_source=source_name,
    )
    if public_genres and public_genres[0] != "etc":
        return public_genres
    return official_genres or public_genres or normalize_ott_genres(classification_context_text, platform_source=source_name)


def _extract_labeled_genre_text(*values: Any) -> str:
    for value in values:
        compact = clean_text(value)
        if not compact:
            continue
        match = _GENRE_LABEL_RE.search(compact)
        if not match:
            continue
        genre_text = clean_text(match.group("genre"))
        if genre_text:
            return genre_text
    return ""


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
    return _limit_cast_values(cast)


def _extract_cast_from_text(text: str) -> List[str]:
    candidate_values: List[str] = []
    lines = _split_text_lines(text)
    for index, line in enumerate(lines):
        match = _CAST_LABEL_RE.search(line)
        if match:
            candidate_values.append(match.group("cast"))
            continue
        lowered = line.lower().strip(":| ")
        if lowered in {"출연", "주연", "starring", "actors", "actor"} and index + 1 < len(lines):
            for next_line in lines[index + 1:index + 1 + MAX_CAST_MEMBERS]:
                if _contains_noise_marker(next_line, _FIELD_STOP_MARKERS):
                    break
                if _GENRE_LABEL_RE.search(next_line) or _CAST_LABEL_RE.search(next_line):
                    break
                candidate_values.append(next_line)
    return _sanitize_cast_values(
        [_extract_people_from_candidate(candidate) for candidate in candidate_values]
    )


def _extract_labeled_genre_text(*values: Any) -> str:
    for line in _split_text_lines(*values):
        match = _GENRE_LABEL_RE.search(line)
        if not match:
            continue
        genre_text = _truncate_at_markers(match.group("genre"), _FIELD_STOP_MARKERS)
        if genre_text:
            return genre_text
    return ""


def _extract_structured_genre_text(*values: Any) -> str:
    best_line = ""
    best_score = -1
    for line in _extract_focus_lines(*values):
        compact = clean_text(line)
        if not compact or len(compact) > 120:
            continue
        if not _GENRE_SIGNAL_RE.search(compact):
            continue
        score = 0
        if _GENRE_LABEL_RE.search(compact):
            score += 5
        if re.fullmatch(r"(?:\uB4DC\uB77C\uB9C8|\uC560\uB2C8(?:\uBA54\uC774\uC158)?|\uC608\uB2A5|\uB2E4\uD050(?:\uBA58\uD130\uB9AC)?)", compact, re.I):
            score += 5
        if len(compact) <= 40:
            score += 2
        if "시즌" in compact or "tvn" in compact.lower() or "disney" in compact.lower():
            score += 1
        if score > best_score:
            best_score = score
            best_line = compact
    if best_line:
        return _truncate_at_markers(best_line, _FIELD_STOP_MARKERS)
    return ""


def _sanitize_cast_values(*values: Any) -> List[str]:
    cleaned: List[str] = []
    seen = set()
    for value in values:
        if isinstance(value, Mapping):
            extracted = _sanitize_cast_values(
                value.get("cast"),
                value.get("actors"),
                value.get("actor"),
                value.get("starring"),
                value.get("body_text"),
            )
            for item in extracted:
                lowered = item.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                cleaned.append(item)
            continue
        if isinstance(value, (list, tuple, set)):
            extracted = _sanitize_cast_values(*value)
            for item in extracted:
                lowered = item.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                cleaned.append(item)
            continue
        text = clean_text(value)
        if not text:
            continue
        text = clean_text(re.sub(r"(?i)^cast\s*[:|]?\s*", "", text))
        text = clean_text(
            re.sub(
                r"(?i)\b(?:director|producer|writer|creator|watch|membership|episode\s*info)\b.*$",
                "",
                text,
            )
        )
        if _looks_like_person_name(text):
            candidates = [text]
        else:
            candidates = _extract_people_from_candidate(text)
        for item in candidates:
            person = clean_text(item)
            if not person or not _looks_like_person_name(person):
                continue
            lowered = person.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            cleaned.append(person)
    return _limit_cast_values(cleaned)


def _looks_explicit_genre_signal(text: Any) -> bool:
    compact = clean_text(text)
    if not compact or len(compact) > 96:
        return False
    if _contains_noise_marker(compact, _OFFICIAL_NOISE_LINE_MARKERS) or _contains_noise_marker(compact, _FIELD_STOP_MARKERS):
        return False
    if normalize_ott_genres(compact)[0] == "etc" and not _GENRE_SIGNAL_RE.search(compact):
        return False
    if _GENRE_LABEL_RE.search(compact):
        return True
    if re.fullmatch(r"[A-Za-z가-힣0-9 ,/&+\-]{2,96}", compact, re.I):
        return True
    return False


def _collect_strict_genre_inputs(*values: Any) -> List[str]:
    collected: List[str] = []
    seen = set()
    for value in values:
        if isinstance(value, Mapping):
            extracted = _collect_strict_genre_inputs(
                value.get("genre_text"),
                value.get("genre"),
                value.get("genres"),
                value.get("category"),
                value.get("description"),
                value.get("body_text"),
            )
            for item in extracted:
                lowered = item.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                collected.append(item)
            continue
        if isinstance(value, (list, tuple, set)):
            extracted = _collect_strict_genre_inputs(*value)
            for item in extracted:
                lowered = item.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)
                collected.append(item)
            continue
        compact = clean_text(value)
        if not compact:
            continue
        for candidate in (
            _extract_labeled_genre_text(compact),
            _extract_structured_genre_text(compact),
            compact if _looks_explicit_genre_signal(compact) else "",
        ):
            genre_text = clean_text(candidate)
            if not genre_text:
                continue
            lowered = genre_text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            collected.append(genre_text)
    return collected


def _resolve_verified_genres(
    source_name: str,
    source_item: Mapping[str, Any],
    matched_docs: Sequence[Mapping[str, Any]],
    classification_context_text: str,
) -> List[str]:
    explicit_official_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping)
        and clean_text(doc.get("source")) in {
            "official_crawl_metadata",
            "official_coupang_metadata",
            "official_episode_schedule",
            "official_rendered_dom",
        }
    ]
    explicit_official_genres = normalize_ott_genres(
        _collect_strict_genre_inputs(
            [doc.get("genre_text") for doc in explicit_official_docs],
            [doc.get("description") for doc in explicit_official_docs],
            [doc.get("title") for doc in explicit_official_docs],
            [doc.get("payload_titles") for doc in explicit_official_docs],
        ),
        platform_source=source_name,
    )
    if explicit_official_genres and explicit_official_genres[0] != "etc":
        return explicit_official_genres

    official_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping) and _is_official_priority_doc(source_name, doc)
    ]
    official_genre_inputs = _collect_strict_genre_inputs(
        [doc.get("genre_text") for doc in official_docs],
        [doc.get("description") for doc in official_docs],
        [doc.get("body_text") for doc in official_docs],
        [doc.get("payload_titles") for doc in official_docs],
        [doc.get("title") for doc in official_docs],
    )
    official_genres = normalize_ott_genres(
        official_genre_inputs,
        platform_source=source_name,
    )
    if official_genres and official_genres[0] != "etc":
        return official_genres

    tmdb_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping) and clean_text(doc.get("source")) == "tmdb"
    ]
    tmdb_genres = normalize_ott_genres(
        _collect_strict_genre_inputs(
            [doc.get("genre_text") for doc in tmdb_docs],
            [doc.get("description") for doc in tmdb_docs],
            [doc.get("title") for doc in tmdb_docs],
            [doc.get("payload_titles") for doc in tmdb_docs],
        ),
        platform_source=source_name,
    )
    if tmdb_genres and tmdb_genres[0] != "etc":
        return tmdb_genres

    public_docs = [
        dict(doc)
        for doc in matched_docs
        if isinstance(doc, Mapping) and not _is_official_priority_doc(source_name, doc)
    ]
    public_genre_inputs = _collect_strict_genre_inputs(
        [doc.get("genre_text") for doc in public_docs],
        [doc.get("description") for doc in public_docs],
        [doc.get("body_text") for doc in public_docs],
        [doc.get("payload_titles") for doc in public_docs],
        [doc.get("title") for doc in public_docs],
    )
    public_genres = normalize_ott_genres(
        public_genre_inputs,
        platform_source=source_name,
    )
    if public_genres and public_genres[0] != "etc":
        return public_genres
    source_genre_inputs = _collect_strict_genre_inputs(
        source_item.get("genre"),
        source_item.get("genres"),
        source_item.get("category"),
    )
    fallback_inputs = _collect_strict_genre_inputs(
        source_genre_inputs,
        classification_context_text,
    )
    return official_genres or public_genres or normalize_ott_genres(fallback_inputs, platform_source=source_name) or ["etc"]


def _parse_range_dates(text: str) -> Tuple[Optional[datetime], Optional[datetime], Optional[str]]:
    compact = clean_text(text)
    match = _LABELED_RANGE_RE.search(compact) or _KOREAN_RANGE_RE.search(compact)
    if not match:
        return None, None, None
    start = parse_flexible_datetime(match.group("start"))
    end = parse_flexible_datetime(match.group("end"))
    if start and end and start.year != end.year and "월" in match.group("end") and not re.search(r"\d{4}", match.group("end")):
        end = end.replace(year=start.year)
    return start, end, clean_text(match.groupdict().get("hint")).lower() or None


def _parse_labeled_range_dates(text: str) -> Tuple[Optional[datetime], Optional[datetime], Optional[str]]:
    compact = clean_text(text)
    if not compact:
        return None, None, None
    labels = (
        "\ubc29\uc1a1 \uae30\uac04",
        "\ubc29\uc601 \uae30\uac04",
        "\uacf5\uac1c \uae30\uac04",
    )
    hints = {
        "\uc608\uc815": "\uc608\uc815",
        "\ud655\uc815": "\ud655\uc815",
    }
    for label in labels:
        start_at = compact.find(label)
        while start_at >= 0:
            window = compact[start_at:start_at + 160]
            tokens = _extract_single_date_tokens(window)
            if len(tokens) >= 2:
                range_hint = None
                for hint_text, hint_value in hints.items():
                    if hint_text in window:
                        range_hint = hint_value
                        break
                return tokens[0], tokens[1], range_hint
            start_at = compact.find(label, start_at + len(label))
    return None, None, None


def _parse_open_ended_range_start(text: str) -> Optional[datetime]:
    compact = clean_text(text)
    if not compact:
        return None
    for pattern in (_ON_AIR_OPEN_RANGE_RE, _LABELED_OPEN_RANGE_RE):
        match = pattern.search(compact)
        if not match:
            continue
        resolved = parse_flexible_datetime(match.group("start"))
        if resolved is not None:
            return resolved
    return None


def _extract_single_date_tokens(text: str) -> List[datetime]:
    compact = clean_text(text)
    tokens: List[datetime] = []
    seen = set()
    for match in re.finditer(_DATE_TOKEN_RE, compact):
        parsed = parse_flexible_datetime(match.group(0))
        if parsed is None:
            continue
        iso = parsed.isoformat()
        if iso in seen:
            continue
        seen.add(iso)
        tokens.append(parsed)
    return tokens


def _extract_date_signals(
    text: str,
    *,
    fallback_start: Any = None,
    season_label: str = "",
) -> Dict[str, Any]:
    compact = clean_text(text)
    season_start, season_end, season_hint = _extract_season_specific_range(compact, season_label)
    start_dt = season_start
    end_dt = season_end
    range_hint = season_hint
    allow_generic_range_fallback = not (
        season_label and start_dt is None and end_dt is None and _mentions_other_season(compact, season_label)
    )
    if start_dt is None and end_dt is None:
        if allow_generic_range_fallback:
            start_dt, end_dt, range_hint = _parse_range_dates(compact)
    if start_dt is None and end_dt is None:
        if allow_generic_range_fallback:
            start_dt, end_dt, range_hint = _parse_labeled_range_dates(compact)
    if start_dt is None and end_dt is None:
        start_dt = _parse_open_ended_range_start(compact)
    if start_dt is None:
        labeled = _LABELED_SINGLE_DATE_RE.search(compact)
        if labeled:
            start_dt = parse_flexible_datetime(labeled.group("date"))
    if start_dt is None:
        trailing = _TRAILING_SINGLE_DATE_HINT_RE.search(compact) or _OPEN_ENDED_START_RE.search(compact)
        if trailing:
            start_dt = parse_flexible_datetime(trailing.group("date"))
    if start_dt is None and _SINGLE_DATE_CONTEXT_HINT_RE.search(compact):
        token_dates = _extract_single_date_tokens(compact)
        if len(token_dates) == 1:
            start_dt = token_dates[0]
    if start_dt is None:
        start_dt = _coerce_datetime(fallback_start)

    binge = bool(_BINGE_HINT_RE.search(compact))
    weekly = bool(_WEEKLY_HINT_RE.search(compact))
    confirmed_hint = bool(_CONFIRMED_END_HINT_RE.search(compact))
    now_value = now_kst_naive()

    release_end_status = "unknown"
    if end_dt is not None:
        if range_hint == "\uc608\uc815":
            release_end_status = "scheduled"
        elif range_hint == "\ud655\uc815":
            release_end_status = "confirmed" if end_dt <= now_value else "scheduled"
        elif end_dt <= now_value:
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

    resolved_url = clean_text(response.url) or target
    host = _doc_host(resolved_url)
    is_official_ott_host = any(
        host == suffix or host.endswith(f".{suffix}")
        for suffix in OFFICIAL_HOST_SUFFIXES.values()
    )

    soup = BeautifulSoup(response.text, "html.parser")
    payloads = _extract_json_ld_payloads(soup)
    document_title = clean_text(soup.title.string if soup.title and soup.title.string else "")
    if not document_title:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title is not None:
            document_title = clean_text(og_title.get("content"))
    full_body_text = clean_text(soup.get_text(" ", strip=True))

    payload_titles = _normalize_title_tokens(
        [payload.get("name") for payload in payloads if isinstance(payload, Mapping)],
    )
    if not payload_titles:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title is not None:
            payload_titles = _normalize_title_tokens(og_title.get("content"))
    description = ""
    for payload in payloads:
        description = clean_text(payload.get("description"))
        if description:
            break
    if not description:
        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description is not None:
            description = clean_text(meta_description.get("content"))
    if not description:
        og_description = soup.find("meta", attrs={"property": "og:description"})
        if og_description is not None:
            description = clean_text(og_description.get("content"))

    body_text = full_body_text
    if is_official_ott_host:
        body_text = clean_text(
            " ".join(
                _normalize_title_tokens(
                    document_title,
                    payload_titles,
                    description,
                )
            )
        )
    cast = _extract_cast_from_payloads(payloads)
    if not cast:
        cast_source_text = full_body_text if is_official_ott_host else body_text
        cast = _extract_cast_from_text(cast_source_text)
    if is_official_ott_host:
        genre_text = _extract_labeled_genre_text(body_text, description, document_title)
        if not genre_text:
            genre_text = _extract_structured_genre_text(description, document_title)
    else:
        genre_text = _extract_labeled_genre_text(full_body_text, body_text, description, document_title)
        if not genre_text:
            genre_text = _extract_structured_genre_text(full_body_text, body_text, description, document_title)

    date_texts = [body_text, description, full_body_text]
    for payload in payloads:
        for key in ("startDate", "endDate", "dateCreated"):
            value = clean_text(payload.get(key))
            if value:
                date_texts.append(value)
    date_signal = _extract_date_signals(" ".join(date_texts))
    episode_total = _extract_episode_total(full_body_text, body_text, description)
    visible_episode_count = _extract_visible_episode_count_from_soup(soup)
    release_weekdays = _extract_schedule_weekdays(full_body_text, body_text, description)

    return {
        "url": resolved_url,
        "ok": True,
        "title": document_title,
        "payload_titles": payload_titles,
        "body_text": body_text,
        "description": description,
        "genre_text": genre_text,
        "cast": _sanitize_cast_values(cast),
        "episode_total": episode_total,
        "visible_episode_count": visible_episode_count,
        "release_weekdays": release_weekdays,
        **date_signal,
    }


def _tmdb_enabled() -> bool:
    return bool(clean_text(config.TMDB_BEARER_TOKEN))


def _tmdb_headers() -> Dict[str, str]:
    token = clean_text(config.TMDB_BEARER_TOKEN)
    if not token:
        return {}
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def _tmdb_get_json(
    session: requests.Session,
    path: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    headers = _tmdb_headers()
    if not headers:
        return None
    url = f"{TMDB_API_BASE}{path}"
    try:
        response = session.get(
            url,
            params={**(dict(params or {})), "language": "ko-KR"},
            headers=headers,
            timeout=TMDB_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _strip_tmdb_season_suffix(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    stripped = re.sub(r"(?i)\bseason\s*\d+\b", "", text)
    stripped = re.sub(r"\s*시즌\s*\d+\b", "", stripped)
    return clean_text(stripped)


def _extract_tmdb_episode_end_at(season_payload: Mapping[str, Any]) -> Optional[datetime]:
    episodes = season_payload.get("episodes") or []
    dates = []
    for item in episodes:
        if not isinstance(item, Mapping):
            continue
        resolved = _coerce_datetime(item.get("air_date"))
        if resolved is not None:
            dates.append(resolved)
    if dates:
        return max(dates)
    return _coerce_datetime(season_payload.get("air_date"))


def _build_tmdb_document(
    *,
    details: Mapping[str, Any],
    aggregate_credits: Optional[Mapping[str, Any]],
    season_payload: Optional[Mapping[str, Any]],
    source_name: str,
    season_number: Optional[int],
    extra_titles: Optional[Sequence[str]] = None,
) -> Optional[Dict[str, Any]]:
    tv_id = details.get("id")
    if not tv_id:
        return None
    title = clean_text(details.get("name")) or clean_text(details.get("original_name"))
    if not title:
        return None

    payload_titles = _normalize_title_tokens(
        title,
        details.get("original_name"),
        details.get("original_title"),
        extra_titles,
        [item.get("name") for item in (details.get("alternative_titles", {}).get("results") or []) if isinstance(item, Mapping)],
    )
    overview = clean_text(details.get("overview"))
    genres = [item.get("name") for item in (details.get("genres") or []) if isinstance(item, Mapping)]
    cast_payload = aggregate_credits or {}
    cast = [
        clean_text(item.get("name"))
        for item in (cast_payload.get("cast") or [])
        if isinstance(item, Mapping) and clean_text(item.get("name"))
    ]
    cast = _sanitize_cast_values(cast)

    release_start_at = None
    release_end_at = None
    release_end_status = "unknown"
    season_label = ""
    if season_payload:
        season_number = season_number or _extract_season_number(season_payload.get("name"))
        if season_number:
            season_label = f"시즌 {season_number}"
        release_start_at = _coerce_datetime(season_payload.get("air_date"))
        release_end_at = _extract_tmdb_episode_end_at(season_payload)
    else:
        release_start_at = _coerce_datetime(details.get("first_air_date"))
        release_end_at = _coerce_datetime(details.get("last_air_date"))

    status_text = clean_text(details.get("status")).lower()
    in_production = bool(details.get("in_production"))
    now_value = now_kst_naive()
    if release_end_at is not None:
        if release_end_at <= now_value and (status_text in {"ended", "canceled"} or not in_production or season_payload):
            release_end_status = "confirmed"
        elif release_end_at > now_value:
            release_end_status = "scheduled"
    elif status_text in {"ended", "canceled"} and release_start_at is not None:
        release_end_status = "confirmed"

    tmdb_url = f"{TMDB_WEB_BASE}/tv/{tv_id}"
    if season_number:
        tmdb_url = f"{tmdb_url}/season/{season_number}"

    return {
        "url": tmdb_url,
        "ok": True,
        "title": _promote_title_with_season(title, season_label),
        "payload_titles": payload_titles,
        "body_text": clean_text(" ".join([title, overview, " ".join(_normalize_title_tokens(genres)), clean_text(details.get("status"))])),
        "description": overview,
        "genre_text": clean_text(", ".join(_normalize_title_tokens(genres))),
        "cast": cast,
        "episode_total": _extract_episode_total(
            season_payload.get("episode_count") if isinstance(season_payload, Mapping) else None,
            details.get("number_of_episodes"),
        ),
        "release_weekdays": [],
        "release_start_at": release_start_at,
        "release_end_at": release_end_at,
        "release_end_status": release_end_status,
        "season_label": season_label,
        "source": "tmdb",
        "tmdb_id": tv_id,
    }


def _fetch_tmdb_documents(
    session: requests.Session,
    candidate: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    if not _tmdb_enabled():
        return []

    source_item = dict(candidate.get("source_item") or {})
    season_label = _extract_season_label(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        source_item.get("raw_schedule_note"),
        source_item.get("description"),
    )
    season_number = _extract_season_number(season_label)
    queries = []
    for raw in _normalize_title_tokens(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        _extract_latin_tokens(
            source_item.get("title_alias"),
            source_item.get("alt_title"),
            source_item.get("description"),
            source_item.get("raw_schedule_note"),
        ),
    ):
        base = _strip_tmdb_season_suffix(raw)
        if base:
            queries.append(base)
    deduped_queries = []
    seen_queries = set()
    for query in queries:
        lowered = query.lower()
        if lowered in seen_queries:
            continue
        seen_queries.add(lowered)
        deduped_queries.append(query)
    deduped_queries.sort(key=lambda value: (0 if _extract_latin_tokens(value) else 1, len(value)))

    documents: List[Dict[str, Any]] = []
    seen_urls = set()
    for query in deduped_queries[:4]:
        search_payload = _tmdb_get_json(
            session,
            "/search/tv",
            params={"query": query, "include_adult": "false"},
        )
        results = (search_payload or {}).get("results") or []
        for result in results[:TMDB_MAX_RESULTS]:
            if not isinstance(result, Mapping):
                continue
            tv_id = result.get("id")
            if not tv_id:
                continue
            details = _tmdb_get_json(
                session,
                f"/tv/{tv_id}",
                params={},
            )
            if not isinstance(details, Mapping):
                continue
            aggregate_credits = _tmdb_get_json(
                session,
                f"/tv/{tv_id}/aggregate_credits",
                params={},
            )
            season_payload = None
            season_aggregate_credits = None
            if season_number:
                season_payload = _tmdb_get_json(
                    session,
                    f"/tv/{tv_id}/season/{season_number}",
                    params={},
                )
                season_aggregate_credits = _tmdb_get_json(
                    session,
                    f"/tv/{tv_id}/season/{season_number}/aggregate_credits",
                    params={},
                )
            doc = _build_tmdb_document(
                details=details,
                aggregate_credits=(
                    season_aggregate_credits
                    if isinstance(season_aggregate_credits, Mapping)
                    else aggregate_credits
                    if isinstance(aggregate_credits, Mapping)
                    else None
                ),
                season_payload=season_payload if isinstance(season_payload, Mapping) else None,
                source_name=clean_text(candidate.get("source_name")),
                season_number=season_number,
                extra_titles=_normalize_title_tokens(
                    query,
                    candidate.get("title"),
                    source_item.get("title_alias"),
                    source_item.get("alt_title"),
                    result.get("name"),
                    result.get("original_name"),
                ),
            )
            if not isinstance(doc, dict):
                continue
            url = clean_text(doc.get("url"))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            documents.append(doc)
    return documents


@lru_cache(maxsize=64)
def _fetch_rendered_official_document(url: str, source_name: str = "") -> Optional[Dict[str, Any]]:
    target = clean_text(url)
    resolved_source = clean_text(source_name) or _source_name_from_url(target)
    if not target or resolved_source not in RENDERED_OFFICIAL_SOURCES:
        return None

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return {
            "url": target,
            "ok": False,
            "error": f"playwright_import_failed:{exc}",
        }

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(
                locale="ko-KR",
                user_agent="Mozilla/5.0",
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8"},
            )
            page.goto(target, wait_until="domcontentloaded", timeout=RENDERED_DOCUMENT_TIMEOUT_MS)
            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
            page.wait_for_timeout(2_000)

            document_title = clean_text(page.title())
            description = clean_text(
                page.evaluate(
                    """() => {
                        const meta = document.querySelector('meta[name="description"], meta[property="og:description"]');
                        return meta ? (meta.getAttribute('content') || '') : '';
                    }"""
                )
            )
            body_text = clean_text(
                page.evaluate(
                    """() => {
                        const body = document.body;
                        return body ? body.innerText || '' : '';
                    }"""
                )
            )
            browser.close()
    except Exception as exc:
        return {
            "url": target,
            "ok": False,
            "error": f"rendered_fetch_failed:{type(exc).__name__}:{exc}",
        }

    payload_titles = _normalize_title_tokens(document_title)
    date_signal = _extract_date_signals(" ".join(part for part in [body_text, description, document_title] if part))
    episode_total = _extract_episode_total(body_text, description)
    release_weekdays = _extract_schedule_weekdays(body_text, description)
    genre_text = _extract_labeled_genre_text(body_text, description, document_title)

    return {
        "url": target,
        "ok": True,
        "title": document_title,
        "payload_titles": payload_titles,
        "body_text": body_text,
        "description": description,
        "genre_text": genre_text,
        "cast": _sanitize_cast_values(_extract_cast_from_text(body_text)),
        "episode_total": episode_total,
        "release_weekdays": release_weekdays,
        "source": "official_rendered_dom",
        **date_signal,
    }


def _build_official_focus_text(*values: Any) -> str:
    focus_lines = _extract_focus_lines(*values)
    if not focus_lines:
        return clean_text(" ".join(_normalize_title_tokens(*values)))
    return clean_text(" ".join(focus_lines[:40]))


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

    resolved_url = clean_text(response.url) or target
    host = _doc_host(resolved_url)
    is_official_ott_host = any(
        host == suffix or host.endswith(f".{suffix}")
        for suffix in OFFICIAL_HOST_SUFFIXES.values()
    )

    soup = BeautifulSoup(response.text, "html.parser")
    payloads = _extract_json_ld_payloads(soup)
    document_title = clean_text(soup.title.string if soup.title and soup.title.string else "")
    if not document_title:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title is not None:
            document_title = clean_text(og_title.get("content"))

    full_body_text = _normalize_multiline_text(soup.get_text("\n", strip=True))

    payload_titles = _normalize_title_tokens(
        [payload.get("name") for payload in payloads if isinstance(payload, Mapping)],
    )
    if not payload_titles:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title is not None:
            payload_titles = _normalize_title_tokens(og_title.get("content"))

    description = ""
    for payload in payloads:
        description = clean_text(payload.get("description"))
        if description:
            break
    if not description:
        meta_description = soup.find("meta", attrs={"name": "description"})
        if meta_description is not None:
            description = clean_text(meta_description.get("content"))
    if not description:
        og_description = soup.find("meta", attrs={"property": "og:description"})
        if og_description is not None:
            description = clean_text(og_description.get("content"))

    body_text = full_body_text
    if is_official_ott_host:
        body_text = _build_official_focus_text(full_body_text, description, document_title, payload_titles)

    cast = _extract_cast_from_payloads(payloads)
    if not cast:
        cast_source_text = full_body_text if is_official_ott_host else body_text
        cast = _extract_cast_from_text(cast_source_text)

    if is_official_ott_host:
        genre_text = _extract_labeled_genre_text(body_text, description, document_title)
        if not genre_text:
            genre_text = _extract_structured_genre_text(description, document_title)
    else:
        genre_text = _extract_labeled_genre_text(full_body_text, description, document_title)
        if not genre_text:
            genre_text = _extract_structured_genre_text(full_body_text, description, document_title)

    date_texts = [body_text, description, full_body_text]
    for payload in payloads:
        for key in ("startDate", "endDate", "dateCreated"):
            value = clean_text(payload.get(key))
            if value:
                date_texts.append(value)
    date_signal = _extract_date_signals(" ".join(date_texts))
    episode_total = _extract_episode_total(full_body_text, body_text, description)
    visible_episode_count = _extract_visible_episode_count_from_soup(soup)
    release_weekdays = _extract_schedule_weekdays(full_body_text, body_text, description)

    return {
        "url": resolved_url,
        "ok": True,
        "title": document_title,
        "payload_titles": payload_titles,
        "body_text": body_text,
        "description": description,
        "genre_text": genre_text,
        "cast": _sanitize_cast_values(cast),
        "episode_total": episode_total,
        "visible_episode_count": visible_episode_count,
        "release_weekdays": release_weekdays,
        **date_signal,
    }


@lru_cache(maxsize=64)
def _fetch_rendered_official_document(url: str, source_name: str = "") -> Optional[Dict[str, Any]]:
    target = clean_text(url)
    resolved_source = clean_text(source_name) or _source_name_from_url(target)
    if not target or resolved_source not in RENDERED_OFFICIAL_SOURCES:
        return None

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return {
            "url": target,
            "ok": False,
            "error": f"playwright_import_failed:{exc}",
        }

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(
                locale="ko-KR",
                user_agent="Mozilla/5.0",
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8"},
            )
            page.goto(target, wait_until="domcontentloaded", timeout=RENDERED_DOCUMENT_TIMEOUT_MS)
            try:
                page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass
            page.wait_for_timeout(2_000)

            document_title = clean_text(page.title())
            description = clean_text(
                page.evaluate(
                    """() => {
                        const meta = document.querySelector('meta[name="description"], meta[property="og:description"]');
                        return meta ? (meta.getAttribute('content') || '') : '';
                    }"""
                )
            )
            raw_main_text = str(
                page.evaluate(
                    """() => {
                        const root = document.querySelector('main') || document.body;
                        return root ? (root.innerText || '') : '';
                    }"""
                )
                or ""
            )
            browser.close()
    except Exception as exc:
        return {
            "url": target,
            "ok": False,
            "error": f"rendered_fetch_failed:{type(exc).__name__}:{exc}",
        }

    normalized_main_text = _normalize_multiline_text(raw_main_text)
    payload_titles = _normalize_title_tokens(document_title)
    body_text = _build_official_focus_text(normalized_main_text, description, document_title)
    date_signal = _extract_date_signals(" ".join(part for part in [normalized_main_text, description, document_title] if part))
    episode_total = _extract_episode_total(normalized_main_text, description)
    visible_episode_count = _extract_visible_episode_count_from_text(normalized_main_text)
    release_weekdays = _extract_schedule_weekdays(normalized_main_text, description)
    genre_text = _extract_labeled_genre_text(normalized_main_text, description, document_title)
    if not genre_text:
        genre_text = _extract_structured_genre_text(normalized_main_text, description, document_title)

    return {
        "url": target,
        "ok": True,
        "title": document_title,
        "payload_titles": payload_titles,
        "body_text": body_text,
        "description": description,
        "genre_text": genre_text,
        "cast": _sanitize_cast_values(_extract_cast_from_text(normalized_main_text)),
        "episode_total": episode_total,
        "visible_episode_count": visible_episode_count,
        "release_weekdays": release_weekdays,
        "source": "official_rendered_dom",
        **date_signal,
    }


def _build_official_source_item_document(candidate: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    source_item = dict(candidate.get("source_item") or {})
    season_label = _extract_season_label(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        source_item.get("raw_schedule_note"),
        source_item.get("description"),
    )
    url = clean_text(
        candidate.get("content_url")
        or source_item.get("platform_url")
        or source_item.get("content_url")
    )
    title = _promote_title_with_season(
        clean_text(source_item.get("title")) or clean_text(candidate.get("title")),
        season_label,
    )
    if not url or not title or not _allowed_public_host(url):
        return None

    payload_titles = _normalize_title_tokens(
        title,
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    release_start_at = _coerce_datetime(source_item.get("release_start_at"))
    if season_label:
        release_start_at = None
    body_parts = payload_titles + _normalize_title_tokens(source_item.get("cast"))
    for key in ("description", "raw_schedule_note", "episode_hint"):
        value = clean_text(source_item.get(key))
        if value:
            body_parts.append(value)

    return {
        "url": url,
        "ok": True,
        "title": title,
        "payload_titles": payload_titles,
        "body_text": clean_text(" ".join(body_parts)),
        "description": clean_text(source_item.get("description")),
        "genre_text": _extract_labeled_genre_text(
            source_item.get("genre"),
            source_item.get("genres"),
            source_item.get("category"),
            source_item.get("description"),
        ),
        "cast": _sanitize_cast_values(source_item.get("cast")),
        "episode_total": _extract_episode_total(
            source_item.get("description"),
            source_item.get("raw_schedule_note"),
            source_item.get("episode_hint"),
        ),
        "release_weekdays": _extract_schedule_weekdays(
            source_item.get("description"),
            source_item.get("raw_schedule_note"),
            source_item.get("episode_hint"),
        ),
        "release_start_at": release_start_at,
        "release_end_at": _coerce_datetime(source_item.get("release_end_at")),
        "release_end_status": clean_text(source_item.get("release_end_status")).lower() or "unknown",
        "source": "official_crawl_metadata",
    }


def _extract_coupang_season_number(season_label: str) -> Optional[int]:
    label = _normalize_season_label(season_label)
    if not label:
        return None
    match = re.search(r"(\d+)", label)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _parse_coupang_page_metadata(html: str) -> Dict[str, Any]:
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', str(html or ""), re.S)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
    except Exception:
        return {}
    metadata = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("metadata", {})
    )
    if not isinstance(metadata, Mapping):
        return {}
    return dict(metadata)


def _fetch_coupang_metadata_document(
    session: requests.Session,
    candidate: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    content_url = clean_text(candidate.get("content_url"))
    if not content_url:
        return None
    try:
        response = session.get(
            content_url,
            headers=config.CRAWLER_HEADERS,
            timeout=DOCUMENT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception:
        return None

    metadata = _parse_coupang_page_metadata(response.text)
    if not metadata:
        return None

    tag_labels = _normalize_title_tokens(
        [
            tag.get("label") or tag.get("tag")
            for tag in (metadata.get("tags") or [])
            if isinstance(tag, Mapping)
        ]
    )
    badge_parts = _normalize_title_tokens(
        [
            text_row.get("text")
            for badge in (metadata.get("badges") or [])
            if isinstance(badge, Mapping)
            for text_row in (badge.get("text") or [])
            if isinstance(text_row, Mapping)
        ]
    )
    cast = _sanitize_cast_values(
        [
            person.get("name")
            for person in (metadata.get("people") or [])
            if isinstance(person, Mapping)
            and clean_text(person.get("role")).upper() == "CAST"
        ]
    )
    description = clean_text(metadata.get("description")) or clean_text(metadata.get("short_description"))
    title = clean_text(metadata.get("title")) or clean_text(candidate.get("title"))
    payload_titles = _normalize_title_tokens(
        title,
        metadata.get("title_canonical"),
        (candidate.get("source_item") or {}).get("title_alias"),
        (candidate.get("source_item") or {}).get("alt_title"),
    )
    body_parts = _normalize_title_tokens(
        title,
        description,
        metadata.get("short_description"),
        tag_labels,
        badge_parts,
        metadata.get("upcoming_text"),
        f"releaseYear {metadata.get('meta', {}).get('releaseYear')}" if isinstance(metadata.get("meta"), Mapping) and metadata.get("meta", {}).get("releaseYear") else "",
    )
    return {
        "url": content_url,
        "ok": True,
        "title": title,
        "payload_titles": payload_titles,
        "body_text": clean_text(" ".join(body_parts)),
        "description": description,
        "genre_text": clean_text(" ".join(tag_labels)),
        "cast": cast,
        "episode_total": None,
        "release_weekdays": _extract_schedule_weekdays(description, metadata.get("short_description")),
        "release_start_at": None,
        "release_end_at": None,
        "release_end_status": "unknown",
        "source": "official_coupang_metadata",
    }


def _fetch_coupang_episode_schedule_document(
    session: requests.Session,
    candidate: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    content_url = clean_text(candidate.get("content_url"))
    if not content_url:
        return None
    parsed = urlparse(content_url)
    content_id = clean_text(parsed.path.rstrip("/").split("/")[-1])
    if not content_id:
        return None

    season_label = _extract_season_label(
        candidate.get("title"),
        (candidate.get("source_item") or {}).get("title_alias"),
        (candidate.get("source_item") or {}).get("alt_title"),
        (candidate.get("source_item") or {}).get("raw_schedule_note"),
        (candidate.get("source_item") or {}).get("description"),
    )
    season_number = _extract_coupang_season_number(season_label)

    try:
        response = session.get(
            content_url,
            headers=config.CRAWLER_HEADERS,
            timeout=DOCUMENT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception:
        return None

    metadata = _parse_coupang_page_metadata(response.text)
    raw_season_list = metadata.get("seasonList") or []
    season_list = []
    for value in raw_season_list:
        try:
            season_list.append(int(value))
        except Exception:
            continue
    default_season = metadata.get("defaultSeason")
    try:
        default_season = int(default_season) if default_season is not None else None
    except Exception:
        default_season = None

    if season_number is not None:
        if season_list and season_number not in season_list:
            return None
        selected_season = season_number
    elif default_season is not None and (not season_list or default_season in season_list):
        selected_season = default_season
    elif season_list:
        selected_season = max(season_list)
    elif len(season_list) == 1:
        selected_season = season_list[0]
    else:
        return None

    episodes: List[Dict[str, Any]] = []
    page = 1
    per_page = 50
    while page <= 5:
        episode_url = (
            f"https://www.coupangplay.com/api-discover/v1/discover/titles/{content_id}/episodes"
            f"?platform=WEBCLIENT&sort=false&page={page}&titleId={content_id}"
            f"&season={selected_season}&locale=ko&perPage={per_page}"
        )
        try:
            api_response = session.get(
                episode_url,
                headers=config.CRAWLER_HEADERS,
                timeout=DOCUMENT_TIMEOUT_SECONDS,
            )
            api_response.raise_for_status()
            payload = api_response.json()
        except Exception:
            break
        batch = payload.get("data") or []
        if not isinstance(batch, list) or not batch:
            break
        episodes.extend(item for item in batch if isinstance(item, Mapping))
        if len(batch) < per_page:
            break
        page += 1

    if not episodes:
        return None

    episode_rows = []
    release_weekdays = set()
    for item in episodes:
        try:
            episode_no = int(item.get("episode") or 0)
        except Exception:
            episode_no = 0
        release_at = (
            parse_flexible_datetime(item.get("short_description"))
            or parse_flexible_datetime(item.get("description"))
            or _coerce_datetime(item.get("vod_start_at"))
            or _coerce_datetime(item.get("published_at"))
        )
        release_at = _date_floor(release_at)
        if release_at is not None:
            release_weekdays.add(release_at.weekday())
        episode_rows.append(
            {
                "episode": episode_no,
                "title": clean_text(item.get("title")),
                "short_description": clean_text(item.get("short_description")),
                "description": clean_text(item.get("description")),
                "release_at": release_at,
                "streamable": bool(item.get("streamable")),
            }
        )

    episode_rows = [row for row in episode_rows if row.get("episode") > 0]
    if not episode_rows:
        return None
    episode_rows.sort(key=lambda row: row["episode"])
    release_points = [row["release_at"] for row in episode_rows if row.get("release_at") is not None]
    release_start_at = min(release_points) if release_points else None
    last_episode = episode_rows[-1]
    release_end_at = last_episode.get("release_at")
    if release_end_at is not None:
        release_end_status = "confirmed" if release_end_at <= now_kst_naive() else "scheduled"
    elif last_episode.get("streamable") and len(episode_rows) == max(row["episode"] for row in episode_rows):
        release_end_status = "confirmed"
    else:
        release_end_status = "unknown"

    body_text = " ".join(
        clean_text(part)
        for row in episode_rows
        for part in (
            row.get("title"),
            row.get("short_description"),
            row.get("description"),
        )
        if clean_text(part)
    )

    source_item = dict(candidate.get("source_item") or {})
    selected_season_label = season_label
    if not selected_season_label and selected_season:
        selected_season_label = f"\uc2dc\uc98c {selected_season}"
    title = _promote_title_with_season(
        clean_text(source_item.get("title")) or clean_text(candidate.get("title")),
        selected_season_label,
    )
    payload_titles = _normalize_title_tokens(
        title,
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    return {
        "url": content_url,
        "ok": True,
        "title": title,
        "payload_titles": payload_titles,
        "body_text": body_text,
        "description": clean_text(source_item.get("description")),
        "genre_text": _extract_labeled_genre_text(
            source_item.get("genre"),
            source_item.get("genres"),
            source_item.get("category"),
            source_item.get("description"),
            body_text,
        ),
        "cast": [],
        "episode_total": max(row["episode"] for row in episode_rows),
        "release_weekdays": sorted(release_weekdays),
        "release_start_at": release_start_at,
        "release_end_at": release_end_at,
        "release_end_status": release_end_status,
        "season_label": selected_season_label,
        "source": "official_episode_schedule",
    }


def _collect_signal_docs(
    docs: Sequence[Mapping[str, Any]],
    *,
    season_label: str,
) -> List[Dict[str, Any]]:
    filtered = [dict(doc) for doc in docs if isinstance(doc, Mapping)]
    if season_label:
        return [
            doc for doc in filtered
            if doc.get("season_specific_match") or _doc_mentions_season(doc, season_label)
        ]
    return filtered


def _pick_release_start_at(
    primary_docs: Sequence[Mapping[str, Any]],
    secondary_docs: Sequence[Mapping[str, Any]],
    source_start_at: Optional[datetime],
    *,
    prefer_source_over_secondary: bool = False,
) -> Optional[datetime]:
    for index, docs in enumerate((primary_docs, secondary_docs)):
        starts = [
            _coerce_datetime(doc.get("release_start_at"))
            for doc in docs
            if _coerce_datetime(doc.get("release_start_at")) is not None
        ]
        if starts:
            return min(starts)
        if index == 0 and prefer_source_over_secondary and source_start_at is not None:
            return source_start_at
    return source_start_at


def _docs_have_date_signal(docs: Sequence[Mapping[str, Any]]) -> bool:
    for doc in docs:
        if not isinstance(doc, Mapping):
            continue
        if (
            _coerce_datetime(doc.get("release_start_at")) is not None
            or _coerce_datetime(doc.get("release_end_at")) is not None
            or clean_text(doc.get("release_end_status")).lower() in {"scheduled", "confirmed"}
        ):
            return True
    return False


def _resolve_post_release_binge_completion(
    *,
    source_name: str,
    source_item: Mapping[str, Any],
    matched_docs: Sequence[Mapping[str, Any]],
    release_start_at: Optional[datetime],
    release_end_at: Optional[datetime],
    release_end_status: str,
) -> Tuple[Optional[datetime], Optional[str]]:
    if source_name not in {"netflix", "disney_plus"}:
        return None, None
    if release_start_at is None or release_end_at is not None:
        return None, None
    if clean_text(release_end_status).lower() == "confirmed":
        return None, None

    now_value = now_kst_naive()
    if now_value < release_start_at + timedelta(days=1):
        return None, None

    merged_schedule_text = " ".join(
        _normalize_title_tokens(
            source_item.get("description"),
            source_item.get("raw_schedule_note"),
            source_item.get("episode_hint"),
            [doc.get("body_text") for doc in matched_docs if isinstance(doc, Mapping)],
            [doc.get("description") for doc in matched_docs if isinstance(doc, Mapping)],
        )
    )
    if _WEEKLY_HINT_RE.search(merged_schedule_text):
        return None, None

    official_visible_episode_count = max(
        [
            int(doc.get("visible_episode_count"))
            for doc in matched_docs
            if isinstance(doc, Mapping)
            and _is_official_signal_doc(source_name, doc)
            and isinstance(doc.get("visible_episode_count"), int)
            and int(doc.get("visible_episode_count")) > 0
        ],
        default=0,
    )
    tmdb_episode_total = max(
        [
            int(doc.get("episode_total"))
            for doc in matched_docs
            if isinstance(doc, Mapping)
            and clean_text(doc.get("source")) == "tmdb"
            and isinstance(doc.get("episode_total"), int)
            and int(doc.get("episode_total")) > 0
        ],
        default=0,
    )

    if official_visible_episode_count <= 0 or tmdb_episode_total <= 0:
        return None, None
    if official_visible_episode_count < tmdb_episode_total:
        return None, None
    return release_start_at, "confirmed"


def _pick_release_end_candidates(
    primary_docs: Sequence[Mapping[str, Any]],
    secondary_docs: Sequence[Mapping[str, Any]],
    release_start_at: Optional[datetime],
) -> Tuple[List[datetime], List[Mapping[str, Any]]]:
    for docs in (primary_docs, secondary_docs):
        distinct = sorted(
            {
                value.isoformat(): value
                for value in (
                    _coerce_datetime(doc.get("release_end_at"))
                    for doc in docs
                )
                if value is not None
            }.values(),
            key=lambda value: value.isoformat(),
        )
        if release_start_at is not None:
            distinct = [value for value in distinct if value >= release_start_at]
        if distinct:
            return distinct, list(docs)
    return [], list(primary_docs or secondary_docs)


def _merge_verification_metadata(
    *,
    candidate: Mapping[str, Any],
    documents: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    source_item = dict(candidate.get("source_item") or {})
    source_name = clean_text(candidate.get("source_name"))
    official_aliases = _normalize_title_tokens(
        [
            raw_doc.get("payload_titles")
            for raw_doc in documents
            if isinstance(raw_doc, Mapping)
            and raw_doc.get("ok")
            and (
                clean_text(raw_doc.get("source")) == "official_crawl_metadata"
                or _is_official_doc(source_name, raw_doc)
            )
        ]
    )
    season_label = _extract_season_label(
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        source_item.get("raw_schedule_note"),
        source_item.get("description"),
    )
    seed_season_label = season_label
    resolved_title = _promote_title_with_season(
        clean_text(source_item.get("title")) or clean_text(candidate.get("title")),
        season_label,
    )
    titles = _normalize_title_tokens(
        resolved_title,
        candidate.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        official_aliases,
    )
    matched_docs: List[Dict[str, Any]] = []
    for raw_doc in documents:
        if not raw_doc.get("ok"):
            continue
        title_match = _title_matches_any(
            titles,
            raw_doc.get("title"),
            raw_doc.get("payload_titles"),
        )
        body_match = _title_matches_any(titles, raw_doc.get("body_text"))
        source = clean_text(raw_doc.get("source"))
        official_schedule_same_url = (
            source == "official_episode_schedule"
            and clean_text(raw_doc.get("url"))
            and clean_text(raw_doc.get("url")) == clean_text(candidate.get("content_url"))
            and (
                _coerce_datetime(raw_doc.get("release_start_at")) is not None
                or _coerce_datetime(raw_doc.get("release_end_at")) is not None
                or clean_text(raw_doc.get("release_end_status")).lower() in {"scheduled", "confirmed"}
            )
        )
        if not title_match and not (_is_namuwiki_doc(raw_doc) and body_match) and not official_schedule_same_url:
            continue
        doc = dict(raw_doc)
        date_signal = _extract_date_signals(
            " ".join(
                part
                for part in [
                    clean_text(doc.get("title")),
                    clean_text(doc.get("description")),
                    clean_text(doc.get("body_text")),
                    " ".join(_normalize_title_tokens(doc.get("payload_titles"))),
                ]
                if part
            ),
            fallback_start=doc.get("release_start_at"),
            season_label=season_label,
        )
        existing_doc_start_at = _coerce_datetime(doc.get("release_start_at"))
        if date_signal.get("release_start_at") is not None and (
            existing_doc_start_at is None or bool(season_label)
        ):
            doc["release_start_at"] = date_signal["release_start_at"]
        if date_signal.get("release_end_at") is not None and (
            _coerce_datetime(doc.get("release_end_at")) is None or bool(season_label)
        ):
            doc["release_end_at"] = date_signal["release_end_at"]
        if date_signal.get("release_end_status") and clean_text(doc.get("release_end_status")).lower() in {"", "unknown"}:
            doc["release_end_status"] = date_signal["release_end_status"]
        doc["season_specific_match"] = _doc_mentions_season(doc, season_label)
        doc["trusted_date_doc"] = _is_trusted_date_doc(source_name, doc)
        doc["history_like_doc"] = _is_history_like_doc(doc)
        matched_docs.append(doc)

    official_season_docs = [
        doc for doc in matched_docs
        if _is_official_signal_doc(source_name, doc)
    ]
    inferred_season_label = ""
    if not season_label:
        season_label = _extract_season_label(
            [doc.get("title") for doc in official_season_docs],
            [doc.get("payload_titles") for doc in official_season_docs],
            [doc.get("description") for doc in official_season_docs],
            [doc.get("season_label") for doc in official_season_docs],
        )
    if not season_label:
        inferred_season_label = _infer_implied_next_season_label(
            source_name=source_name,
            source_item=source_item,
            matched_docs=matched_docs,
            season_label=season_label,
        )
        season_label = inferred_season_label or season_label
    if season_label:
        resolved_title = _promote_title_with_season(resolved_title, season_label)
        titles = _normalize_title_tokens(
            resolved_title,
            candidate.get("title"),
            source_item.get("title_alias"),
            source_item.get("alt_title"),
            official_aliases,
        )
        for doc in matched_docs:
            doc["season_specific_match"] = _doc_mentions_season(doc, season_label)
            season_date_signal = _extract_date_signals(
                " ".join(
                    part
                    for part in [
                        clean_text(doc.get("title")),
                        clean_text(doc.get("description")),
                        clean_text(doc.get("body_text")),
                        " ".join(_normalize_title_tokens(doc.get("payload_titles"))),
                    ]
                    if part
                ),
                season_label=season_label,
            )
            if season_date_signal.get("release_start_at") is not None:
                doc["release_start_at"] = season_date_signal["release_start_at"]
                if season_date_signal.get("release_end_at") is not None:
                    doc["release_end_at"] = season_date_signal["release_end_at"]
                else:
                    doc["release_end_at"] = None
                    doc["release_end_status"] = "unknown"
            elif season_date_signal.get("release_end_at") is not None:
                doc["release_end_at"] = season_date_signal["release_end_at"]

    source_start_at = _coerce_datetime(source_item.get("release_start_at"))
    start_candidates: List[datetime] = []
    end_candidates: List[datetime] = []
    cast_values: List[str] = []
    description = clean_text(source_item.get("description"))
    episode_total_candidates: List[int] = []
    schedule_weekdays = _extract_schedule_weekdays(
        source_item.get("description"),
        source_item.get("raw_schedule_note"),
        source_item.get("episode_hint"),
    )
    source_episode_total = _extract_episode_total(
        source_item.get("description"),
        source_item.get("raw_schedule_note"),
        source_item.get("episode_hint"),
    )
    if source_episode_total:
        episode_total_candidates.append(source_episode_total)
    title_alias = _normalize_title_tokens(
        source_item.get("title_alias"),
        source_item.get("alt_title"),
        resolved_title,
    )
    evidence_urls = []
    classification_context_text = " ".join(
        _normalize_title_tokens(
            source_item.get("genre"),
            source_item.get("genres"),
            source_item.get("category"),
            source_item.get("description"),
            source_item.get("raw_schedule_note"),
            source_item.get("episode_hint"),
            [doc.get("genre_text") for doc in matched_docs],
            [doc.get("description") for doc in matched_docs],
            [doc.get("title") for doc in matched_docs if _is_official_priority_doc(source_name, doc)],
            [doc.get("payload_titles") for doc in matched_docs if _is_official_priority_doc(source_name, doc)],
        )
    )
    is_scripted = _looks_scripted(classification_context_text)
    is_non_scripted = _looks_non_scripted(classification_context_text)
    is_variety_nonscripted = _looks_variety_nonscripted(classification_context_text)
    if is_scripted and not is_variety_nonscripted:
        is_non_scripted = False
    is_weekly_current = _has_weekly_current_hint(classification_context_text)
    trusted_docs = [
        doc for doc in matched_docs
        if doc.get("trusted_date_doc")
        and not (is_non_scripted and is_weekly_current and doc.get("history_like_doc"))
    ]
    non_tmdb_docs = [
        doc for doc in matched_docs
        if clean_text(doc.get("source")) != "tmdb"
    ]
    trusted_non_tmdb_docs = [
        doc for doc in trusted_docs
        if clean_text(doc.get("source")) != "tmdb"
    ]
    tmdb_date_docs = [
        doc for doc in matched_docs
        if clean_text(doc.get("source")) == "tmdb"
    ]
    date_docs = trusted_non_tmdb_docs or trusted_docs or non_tmdb_docs or matched_docs
    official_docs = [doc for doc in date_docs if _is_official_signal_doc(source_name, doc)]
    official_signal_docs = _collect_signal_docs(official_docs, season_label=season_label)
    non_tmdb_signal_docs = _collect_signal_docs(date_docs, season_label=season_label)
    tmdb_signal_docs = _collect_signal_docs(tmdb_date_docs, season_label=season_label)
    source_has_anchor_start = source_start_at is not None

    if source_name in {"disney_plus", "netflix"}:
        if _docs_have_date_signal(non_tmdb_signal_docs) or source_has_anchor_start:
            primary_date_docs = non_tmdb_signal_docs
            secondary_date_docs = tmdb_signal_docs or non_tmdb_signal_docs
        elif tmdb_signal_docs:
            primary_date_docs = tmdb_signal_docs
            secondary_date_docs = non_tmdb_signal_docs or tmdb_signal_docs
        else:
            primary_date_docs = official_signal_docs
            secondary_date_docs = non_tmdb_signal_docs
    else:
        primary_date_docs = official_signal_docs
        secondary_date_docs = non_tmdb_signal_docs
    if inferred_season_label and not any(doc.get("season_specific_match") for doc in matched_docs):
        primary_date_docs = []
        secondary_date_docs = []
    if inferred_season_label and source_start_at is not None:
        recent_dates = []
        for docs in (primary_date_docs, secondary_date_docs):
            for doc in docs:
                for value in (doc.get("release_start_at"), doc.get("release_end_at")):
                    resolved = _coerce_datetime(value)
                    if resolved is not None:
                        recent_dates.append(resolved)
        if recent_dates and all(value < source_start_at - timedelta(days=180) for value in recent_dates):
            primary_date_docs = []
            secondary_date_docs = []

    evidence_docs = non_tmdb_docs or matched_docs
    for doc in matched_docs:
        description = clean_text(doc.get("description")) or description
        title_alias = _normalize_title_tokens(title_alias, doc.get("title"), doc.get("payload_titles"))
        doc_episode_total = doc.get("episode_total")
        if isinstance(doc_episode_total, int) and doc_episode_total > 0:
            episode_total_candidates.append(doc_episode_total)
        schedule_weekdays = sorted(
            set(schedule_weekdays)
            | {
                int(day)
                for day in (doc.get("release_weekdays") or [])
                if isinstance(day, int) and 0 <= day <= 6
            }
        )

    cast_values = _resolve_cast_values(source_name, source_item, matched_docs)
    evidence_urls = [clean_text(doc.get("url")) for doc in evidence_docs]

    release_start_at = _pick_release_start_at(
        primary_date_docs,
        secondary_date_docs,
        source_start_at,
        prefer_source_over_secondary=bool(
            source_start_at is not None and source_name in {"disney_plus", "netflix"}
        ),
    )
    anchored_to_source_start = bool(
        source_name in {"disney_plus", "netflix"}
        and source_start_at is not None
        and release_start_at == source_start_at
        and not any(_coerce_datetime(doc.get("release_start_at")) is not None for doc in primary_date_docs)
    )
    distinct_end_dates, selected_date_docs = _pick_release_end_candidates(
        primary_date_docs,
        [] if anchored_to_source_start else secondary_date_docs,
        release_start_at,
    )

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
        fallback_status_docs = selected_date_docs or ([] if anchored_to_source_start else secondary_date_docs)
        for doc in fallback_status_docs:
            doc_end_at = _coerce_datetime(doc.get("release_end_at"))
            if (
                doc_end_at is not None
                and release_start_at is not None
                and doc_end_at < release_start_at
            ):
                continue
            if doc.get("release_end_status") in {"scheduled", "confirmed"}:
                release_end_status = str(doc["release_end_status"])
                if release_end_status == "confirmed" and release_start_at and doc.get("binge_hint"):
                    release_end_at = release_start_at
                break

    if release_end_at is None and release_start_at is not None:
        binge_release_end_at, binge_release_end_status = _resolve_post_release_binge_completion(
            source_name=source_name,
            source_item=source_item,
            matched_docs=matched_docs,
            release_start_at=release_start_at,
            release_end_at=release_end_at,
            release_end_status=release_end_status,
        )
        if binge_release_end_at is not None and binge_release_end_status:
            release_end_at = binge_release_end_at
            release_end_status = binge_release_end_status

    allow_schedule_inference = True
    if inferred_season_label:
        recent_season_specific_dates = []
        for doc in matched_docs:
            if not doc.get("season_specific_match"):
                continue
            for value in (doc.get("release_start_at"), doc.get("release_end_at")):
                resolved = _coerce_datetime(value)
                if resolved is not None and (
                    release_start_at is None or resolved >= release_start_at - timedelta(days=90)
                ):
                    recent_season_specific_dates.append(resolved)
        if not recent_season_specific_dates:
            allow_schedule_inference = False

    if release_end_at is None and release_start_at is not None and allow_schedule_inference:
        inferred_episode_total = max(episode_total_candidates) if episode_total_candidates else None
        inferred_end_at = _infer_release_end_at(
            release_start_at,
            inferred_episode_total,
            schedule_weekdays,
        )
        if inferred_end_at is not None:
            release_end_at = inferred_end_at
            release_end_status = "confirmed" if inferred_end_at <= now_kst_naive() else "scheduled"

    season_verified = bool(
        season_label
        and any(
            doc.get("season_specific_match")
            and not doc.get("history_like_doc")
            and (
                _coerce_datetime(doc.get("release_start_at")) is not None
                or _coerce_datetime(doc.get("release_end_at")) is not None
                or clean_text(doc.get("release_end_status")).lower() in {"scheduled", "confirmed"}
            )
            and _is_official_signal_doc(source_name, doc)
            for doc in (primary_date_docs or secondary_date_docs)
        )
    )
    season_number = _extract_season_number(season_label)
    observed_starts = [
        candidate_start
        for candidate_start in (
            source_start_at,
            *(_coerce_datetime(doc.get("release_start_at")) for doc in matched_docs),
        )
        if candidate_start is not None
    ]
    oldest_observed_start = min(observed_starts) if observed_starts else None
    long_running_first_airing = bool(
        oldest_observed_start is not None
        and oldest_observed_start <= now_kst_naive() - timedelta(days=3650)
    )
    excessive_season_number = bool(season_number is not None and season_number >= 10)
    season_window_days = None
    if release_start_at is not None and release_end_at is not None and release_end_at >= release_start_at:
        season_window_days = (release_end_at - release_start_at).days
    legacy_public_history = bool(
        any(
            (
                doc.get("history_like_doc")
                or (
                    not _is_official_signal_doc(source_name, doc)
                    and _coerce_datetime(doc.get("release_start_at")) is not None
                    and _coerce_datetime(doc.get("release_start_at")) <= now_kst_naive() - timedelta(days=365)
                )
            )
            for doc in matched_docs
        )
    )
    finite_episode_series_verified = bool(
        release_end_status in {"scheduled", "confirmed"}
        and any(
            isinstance(doc.get("episode_total"), int)
            and 1 < int(doc.get("episode_total")) <= 20
            and (
                clean_text(doc.get("source")) == "official_episode_schedule"
                or _is_official_doc(source_name, doc)
            )
            for doc in (primary_date_docs or secondary_date_docs)
        )
    )
    legacy_non_scripted = bool(
        is_non_scripted
        and release_start_at is not None
        and release_start_at < now_kst_naive() - timedelta(days=180)
        and not seed_season_label
    )
    valid_nonscripted_season = bool(
        is_variety_nonscripted
        and season_verified
        and season_number is not None
        and release_start_at is not None
        and release_end_at is not None
        and season_window_days is not None
        and 0 <= season_window_days <= 183
    )
    exclude_from_sync = False
    exclude_reason = ""
    if is_variety_nonscripted and (excessive_season_number or long_running_first_airing):
        release_end_at = None
        release_end_status = "unknown"
        resolution_state = "tracking"
        exclude_from_sync = True
        exclude_reason = "long_running_nonscripted_policy"
    elif is_variety_nonscripted and not valid_nonscripted_season:
        release_end_at = None
        release_end_status = "unknown"
        resolution_state = "tracking"
        exclude_from_sync = True
        exclude_reason = "nonscripted_requires_finite_verified_season"
    elif is_non_scripted and legacy_public_history and not season_verified:
        release_end_at = None
        release_end_status = "unknown"
        resolution_state = "tracking"
        exclude_from_sync = True
        exclude_reason = "ambiguous_long_running_nonscripted"
    elif is_non_scripted and is_weekly_current and not (
        season_verified or finite_episode_series_verified
    ):
        release_end_at = None
        release_end_status = "unknown"
        resolution_state = "tracking"
        exclude_from_sync = True
        exclude_reason = "ambiguous_long_running_nonscripted"
    elif legacy_non_scripted:
        release_end_at = None
        release_end_status = "unknown"
        resolution_state = "tracking"
        exclude_from_sync = True
        exclude_reason = "legacy_long_running_nonscripted"

    deduped_evidence_urls = []
    seen_urls = set()
    for url in evidence_urls:
        cleaned = clean_text(url)
        if not cleaned or cleaned in seen_urls:
            continue
        seen_urls.add(cleaned)
        deduped_evidence_urls.append(cleaned)

    genres = _resolve_verified_genres(
        source_name,
        source_item,
        matched_docs,
        classification_context_text,
    )

    return {
        "matched_docs": matched_docs,
        "matched_count": len(matched_docs),
        "release_start_at": release_start_at,
        "release_end_at": release_end_at,
        "release_end_status": release_end_status,
        "resolution_state": resolution_state,
        "cast": _limit_cast_values(cast_values),
        "genres": genres,
        "genre": genres[0] if genres else "etc",
        "description": description,
        "title_alias": title_alias,
        "resolved_title": resolved_title,
        "season_label": season_label,
        "exclude_from_sync": exclude_from_sync,
        "exclude_reason": exclude_reason,
        "evidence_urls": deduped_evidence_urls,
    }


def _apply_entry_enrichment(entry: Dict[str, Any], metadata: Mapping[str, Any]) -> None:
    if metadata.get("resolved_title"):
        entry["title"] = clean_text(metadata["resolved_title"])
    if "cast" in metadata:
        entry["cast"] = list(metadata["cast"])
        entry["_clear_cast"] = not bool(metadata.get("cast"))
    if metadata.get("description"):
        entry["description"] = clean_text(metadata["description"])
    if metadata.get("title_alias"):
        entry["title_alias"] = list(metadata["title_alias"])
    if metadata.get("genres"):
        entry["genres"] = list(metadata["genres"])
    if metadata.get("genre"):
        entry["genre"] = clean_text(metadata["genre"])
    if metadata.get("release_start_at") is not None:
        entry["release_start_at"] = metadata["release_start_at"]
        season_label = clean_text(metadata.get("season_label")) or _normalize_season_label(entry.get("title"))
        if season_label:
            entry["representative_year"] = metadata["release_start_at"].year
    if metadata.get("release_end_at") is not None:
        entry["release_end_at"] = metadata["release_end_at"]
    if metadata.get("release_end_status"):
        entry["release_end_status"] = metadata["release_end_status"]
    if metadata.get("resolution_state"):
        entry["resolution_state"] = metadata["resolution_state"]
    if metadata.get("exclude_from_sync"):
        entry["exclude_from_sync"] = True
        if metadata.get("exclude_reason"):
            entry["exclude_reason"] = clean_text(metadata["exclude_reason"])

    start_dt = _coerce_datetime(entry.get("release_start_at"))
    if start_dt is not None:
        entry["upcoming"] = start_dt > now_kst_naive()
        entry["availability_status"] = "scheduled" if entry["upcoming"] else "available"
        season_label = clean_text(metadata.get("season_label")) or _normalize_season_label(entry.get("title"))
        if season_label:
            entry["canonical_content_id"] = build_canonical_content_id(
                title=clean_text(entry.get("title")) or clean_text(metadata.get("resolved_title")) or "",
                release_start_at=start_dt,
                representative_year=entry.get("representative_year") or start_dt,
            )


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


def _find_watchlist_row(
    write_plan: Mapping[str, Any],
    *,
    canonical_content_id: str,
    source_name: str,
) -> Optional[Dict[str, Any]]:
    for row in write_plan.get("watchlist_rows") or []:
        if not isinstance(row, dict):
            continue
        if (
            clean_text(row.get("canonical_content_id")) == canonical_content_id
            and clean_text(row.get("platform_source")) == source_name
        ):
            return dict(row)
    return None


def _find_current_source_entry(
    write_plan: Mapping[str, Any],
    *,
    canonical_content_id: str,
    source_name: str,
) -> Optional[Dict[str, Any]]:
    raw_all_content_today = write_plan.get("all_content_today")
    all_content_today = raw_all_content_today if isinstance(raw_all_content_today, dict) else {}

    direct_entry = all_content_today.get(canonical_content_id)
    if isinstance(direct_entry, dict):
        return direct_entry

    platform_link = _find_platform_link(
        write_plan,
        canonical_content_id=canonical_content_id,
        source_name=source_name,
    )
    platform_content_id = clean_text((platform_link or {}).get("platform_content_id"))
    if platform_content_id:
        linked_entry = all_content_today.get(platform_content_id)
        if isinstance(linked_entry, dict):
            return linked_entry

    platform_url = clean_text((platform_link or {}).get("platform_url"))
    for value in all_content_today.values():
        if not isinstance(value, dict):
            continue
        if platform_content_id and clean_text(value.get("platform_content_id")) == platform_content_id:
            return value
        candidate_url = clean_text(value.get("platform_url") or value.get("content_url"))
        if platform_url and candidate_url == platform_url:
            return value
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
    current_entry = _find_current_source_entry(
        write_plan,
        canonical_content_id=canonical_content_id,
        source_name=source_name,
    )
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

    snapshot_entry = {
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
    if isinstance(current_entry, dict):
        merged_entry = dict(snapshot_entry)
        for key, value in current_entry.items():
            if value in (None, "", [], {}):
                continue
            merged_entry[key] = value
        return merged_entry
    return snapshot_entry


def _row_needs_metadata_reverify(
    existing_row: Mapping[str, Any],
    *,
    watchlist_row: Optional[Mapping[str, Any]] = None,
) -> bool:
    meta = dict(existing_row.get("meta") or {})
    common = dict(meta.get("common") or {})
    ott = dict(meta.get("ott") or {})
    status = clean_text(existing_row.get("status"))
    release_start_at = _coerce_datetime(ott.get("release_start_at")) or _coerce_datetime((watchlist_row or {}).get("release_start_at"))
    release_end_at = _coerce_datetime(ott.get("release_end_at")) or _coerce_datetime((watchlist_row or {}).get("release_end_at"))
    release_end_status = clean_text(ott.get("release_end_status")).lower() or clean_text((watchlist_row or {}).get("release_end_status")).lower() or "unknown"
    resolution_state = clean_text(ott.get("resolution_state")) or clean_text((watchlist_row or {}).get("resolution_state")) or "tracking"
    cast_values = _normalize_title_tokens(ott.get("cast"), common.get("authors"))
    sanitized_cast = _sanitize_cast_values(cast_values)
    platform_source = clean_text((watchlist_row or {}).get("platform_source")) or clean_text(common.get("primary_source"))
    resolved_genres = normalize_ott_genres(
        ott.get("genres"),
        ott.get("genre"),
        common.get("genres"),
        common.get("genre"),
        platform_source=platform_source,
    )

    missing_core = release_start_at is None or not sanitized_cast
    unresolved_schedule = bool(ott.get("needs_end_date_verification")) or release_end_status in {"", "unknown"} or resolution_state == "conflict"
    completed_without_end = status == "완결" and release_end_status == "confirmed" and release_end_at is None
    suspicious_cast = bool(cast_values) and sanitized_cast != _limit_cast_values(cast_values)
    missing_genre = not resolved_genres or resolved_genres[0] == "etc"
    return missing_core or unresolved_schedule or completed_without_end or suspicious_cast or missing_genre


def _build_incomplete_metadata_candidate(
    write_plan: Mapping[str, Any],
    existing_row: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    canonical_content_id = clean_text(existing_row.get("content_id"))
    source_name = clean_text(write_plan.get("source_name"))
    if not canonical_content_id or not source_name:
        return None

    watchlist_row = _find_watchlist_row(
        write_plan,
        canonical_content_id=canonical_content_id,
        source_name=source_name,
    )
    if not _row_needs_metadata_reverify(existing_row, watchlist_row=watchlist_row):
        return None

    seed_row = watchlist_row or {"canonical_content_id": canonical_content_id}
    entry = _build_snapshot_entry(write_plan, seed_row)
    if not isinstance(entry, dict):
        return None

    release_end_status = (
        clean_text(entry.get("release_end_status")).lower()
        or clean_text((watchlist_row or {}).get("release_end_status")).lower()
        or "unknown"
    )
    return {
        "content_id": canonical_content_id,
        "source_name": source_name,
        "title": clean_text(entry.get("title")) or canonical_content_id,
        "expected_status": clean_text(entry.get("status")) or clean_text(existing_row.get("status")),
        "previous_status": clean_text(entry.get("status")) or clean_text(existing_row.get("status")) or None,
        "content_url": clean_text(entry.get("platform_url") or entry.get("content_url")),
        "change_kinds": ["metadata_reverify"],
        "source_item": {
            **dict(entry),
            "release_end_status": release_end_status,
            "release_end_at": _coerce_datetime(entry.get("release_end_at")) or _coerce_datetime((watchlist_row or {}).get("release_end_at")),
            "release_start_at": _coerce_datetime(entry.get("release_start_at")) or _coerce_datetime((watchlist_row or {}).get("release_start_at")),
        },
        "watchlist_recheck": True,
        "metadata_reverify": True,
    }


def _build_current_source_candidate(
    write_plan: Mapping[str, Any],
    *,
    raw_content_id: str,
    entry: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    source_name = clean_text(write_plan.get("source_name"))
    platform_content_id = clean_text(
        entry.get("platform_content_id")
        or entry.get("content_id")
        or raw_content_id
    )
    content_url = clean_text(entry.get("platform_url") or entry.get("content_url"))
    if not source_name or not platform_content_id:
        return None

    existing_row = None
    platform_link = None
    for row in write_plan.get("platform_links") or []:
        if not isinstance(row, dict):
            continue
        if (
            clean_text(row.get("platform_source")) == source_name
            and clean_text(row.get("platform_content_id")) == platform_content_id
        ):
            platform_link = dict(row)
            existing_row = _find_snapshot_existing_row(
                write_plan,
                clean_text(row.get("canonical_content_id")),
            )
            break

    previous_status = clean_text((existing_row or {}).get("status")) or None
    expected_status = clean_text(entry.get("status")) or previous_status or "연재중"
    title = (
        clean_text(entry.get("title"))
        or clean_text((existing_row or {}).get("title"))
        or platform_content_id
    )
    canonical_content_id = (
        clean_text(entry.get("canonical_content_id"))
        or clean_text((existing_row or {}).get("content_id"))
        or raw_content_id
        or platform_content_id
    )
    source_item = {
        **dict(entry),
        "platform_content_id": platform_content_id,
        "platform_url": content_url,
        "content_url": content_url,
    }
    if isinstance(platform_link, dict):
        source_item.setdefault("canonical_content_id", clean_text(platform_link.get("canonical_content_id")))

    return {
        "content_id": canonical_content_id,
        "source_name": source_name,
        "title": title,
        "expected_status": expected_status,
        "previous_status": previous_status,
        "content_url": content_url,
        "change_kinds": ["current_reverify"],
        "source_item": source_item,
        "metadata_reverify": True,
    }


def _build_watchlist_candidate(write_plan: Mapping[str, Any], row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    canonical_content_id = clean_text(row.get("canonical_content_id"))
    if not canonical_content_id:
        return None
    next_check_at = _coerce_datetime(row.get("next_check_at"))
    if next_check_at is not None and next_check_at > now_kst_naive():
        return None

    raw_all_content_today = write_plan.get("all_content_today")
    all_content_today = raw_all_content_today if isinstance(raw_all_content_today, dict) else {}
    entry = all_content_today.get(canonical_content_id)
    if not isinstance(entry, dict):
        entry = _find_current_source_entry(
            write_plan,
            canonical_content_id=canonical_content_id,
            source_name=clean_text(write_plan.get("source_name")),
        )
    if not isinstance(entry, dict):
        entry = _build_snapshot_entry(write_plan, row)
        if isinstance(all_content_today, dict) and isinstance(entry, dict):
            all_content_today[canonical_content_id] = entry
    if not isinstance(entry, dict):
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
    current_platform_ids = {
        clean_text((dict(item.get("source_item") or {})).get("platform_content_id"))
        for item in candidates
        if clean_text((dict(item.get("source_item") or {})).get("platform_content_id"))
    }
    raw_all_content_today = write_plan.get("all_content_today")
    all_content_today = raw_all_content_today if isinstance(raw_all_content_today, dict) else {}
    for raw_content_id, raw_entry in all_content_today.items():
        if not isinstance(raw_entry, dict):
            continue
        candidate = _build_current_source_candidate(
            write_plan,
            raw_content_id=clean_text(raw_content_id),
            entry=raw_entry,
        )
        if candidate is None:
            continue
        content_id = clean_text(candidate.get("content_id"))
        platform_content_id = clean_text((dict(candidate.get("source_item") or {})).get("platform_content_id"))
        if not content_id or content_id in seen:
            continue
        seen.add(content_id)
        if platform_content_id:
            current_platform_ids.add(platform_content_id)
        candidates.append(candidate)
    for row in write_plan.get("snapshot_existing_rows") or []:
        if not isinstance(row, dict):
            continue
        candidate = _build_incomplete_metadata_candidate(write_plan, row)
        if candidate is None:
            continue
        content_id = clean_text(candidate.get("content_id"))
        platform_content_id = clean_text((dict(candidate.get("source_item") or {})).get("platform_content_id"))
        if platform_content_id and platform_content_id in current_platform_ids:
            continue
        if not content_id or content_id in seen:
            continue
        seen.add(content_id)
        candidates.append(candidate)
    for row in write_plan.get("watchlist_rows") or []:
        if not isinstance(row, dict):
            continue
        candidate = _build_watchlist_candidate(write_plan, row)
        if candidate is None:
            continue
        content_id = clean_text(candidate.get("content_id"))
        platform_content_id = clean_text((dict(candidate.get("source_item") or {})).get("platform_content_id"))
        if platform_content_id and platform_content_id in current_platform_ids:
            continue
        if not content_id or content_id in seen:
            continue
        seen.add(content_id)
        candidates.append(candidate)
    return candidates


def _collect_official_only_ott_targets(write_plan: Mapping[str, Any], *, source_name: str) -> List[Dict[str, Any]]:
    seen_platform_ids = {
        clean_text(row.get("platform_content_id"))
        for row in (write_plan.get("platform_links") or [])
        if isinstance(row, Mapping)
        and clean_text(row.get("platform_source")) == clean_text(source_name)
        and clean_text(row.get("platform_content_id"))
    }
    targets: List[Dict[str, Any]] = []
    for candidate in write_plan.get("verification_candidates") or []:
        if not isinstance(candidate, Mapping):
            continue
        source_item = dict(candidate.get("source_item") or {})
        platform_content_id = clean_text(source_item.get("platform_content_id")) or clean_text(candidate.get("content_id"))
        official_url = clean_text(
            candidate.get("content_url")
            or source_item.get("platform_url")
            or source_item.get("content_url")
        )
        if not official_url or not _is_official_doc(source_name, {"url": official_url}):
            continue
        if platform_content_id and platform_content_id in seen_platform_ids:
            continue
        targets.append(dict(candidate))
    return targets


def _merge_official_only_metadata(
    *,
    candidate: Mapping[str, Any],
    source_name: str,
    documents: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    source_item = dict(candidate.get("source_item") or {})
    official_docs = [
        dict(doc)
        for doc in documents
        if isinstance(doc, Mapping)
        and doc.get("ok")
        and (
            clean_text(doc.get("source")).startswith("official_")
            or _is_official_doc(source_name, doc)
        )
    ]

    title_candidates = _normalize_title_tokens(
        [doc.get("payload_titles") for doc in official_docs],
        [doc.get("title") for doc in official_docs if "|" not in clean_text(doc.get("title"))],
        source_item.get("title"),
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    resolved_title = title_candidates[0] if title_candidates else clean_text(source_item.get("title")) or clean_text(candidate.get("title"))
    title_alias = _normalize_title_tokens(
        [doc.get("payload_titles") for doc in official_docs],
        source_item.get("title_alias"),
        source_item.get("alt_title"),
    )
    cast = _resolve_cast_values(source_name, source_item, official_docs)
    release_start_at = _pick_release_start_at(
        official_docs,
        [],
        _coerce_datetime(source_item.get("release_start_at")),
        prefer_source_over_secondary=True,
    )
    description = next(
        (
            clean_text(doc.get("description"))
            for doc in official_docs
            if clean_text(doc.get("description"))
        ),
        clean_text(source_item.get("description")),
    )
    evidence_urls = _dedupe_urls(clean_text(doc.get("url")) for doc in official_docs if clean_text(doc.get("url")))
    return {
        "matched_docs": official_docs,
        "matched_count": len(official_docs),
        "resolved_title": resolved_title,
        "season_label": "",
        "title_alias": title_alias,
        "cast": cast,
        "description": description,
        "genres": _normalize_title_tokens(source_item.get("genre"), source_item.get("genres"), source_item.get("category")),
        "genre": clean_text(source_item.get("genre")) or "etc",
        "release_start_at": release_start_at,
        "release_end_at": None,
        "release_end_status": "unknown",
        "resolution_state": "tracking",
        "exclude_from_sync": False,
        "exclude_reason": "",
        "evidence_urls": evidence_urls,
    }


def verify_ott_write_plan(write_plan: Mapping[str, Any], *, source_name: str) -> Dict[str, Any]:
    # Legacy public-web/TMDb/season-upgrade rules are intentionally kept in this
    # module for reference, but the active OTT path is official-only.
    targets = _collect_official_only_ott_targets(write_plan, source_name=source_name)
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
    filtered_out_ids = set()

    with requests.Session() as session:
        for candidate in targets:
            candidate_id = clean_text(candidate.get("content_id"))
            documents: List[Dict[str, Any]] = []
            source_item_doc = _build_official_source_item_document(candidate)
            if isinstance(source_item_doc, dict):
                documents.append(source_item_doc)
            official_url = clean_text(candidate.get("content_url"))
            if source_name == "coupangplay":
                official_metadata_doc = _fetch_coupang_metadata_document(session, candidate)
                if isinstance(official_metadata_doc, dict):
                    documents.append(official_metadata_doc)
            if official_url:
                official_doc = _fetch_document(session, official_url)
                if isinstance(official_doc, dict):
                    official_doc["source"] = clean_text(official_doc.get("source")) or "official_public_page"
                    documents.append(official_doc)
                rendered_doc = _fetch_rendered_official_document(official_url, source_name)
                if isinstance(rendered_doc, dict):
                    documents.append(rendered_doc)

            source_item = dict(candidate.get("source_item") or {})
            if not official_url:
                metadata = {
                    "matched_docs": [],
                    "matched_count": 0,
                    "resolved_title": clean_text(source_item.get("title")) or clean_text(candidate.get("title")) or candidate_id,
                    "season_label": "",
                    "title_alias": _normalize_title_tokens(source_item.get("title_alias"), source_item.get("alt_title")),
                    "cast": [],
                    "description": clean_text(source_item.get("description")),
                    "genres": [],
                    "genre": "etc",
                    "release_start_at": _coerce_datetime(source_item.get("release_start_at")),
                    "release_end_at": None,
                    "release_end_status": "unknown",
                    "resolution_state": "tracking",
                    "exclude_from_sync": True,
                    "exclude_reason": "missing_official_url",
                    "evidence_urls": [],
                }
            else:
                metadata = _merge_official_only_metadata(
                    candidate=candidate,
                    source_name=source_name,
                    documents=documents,
                )
                if not metadata.get("cast"):
                    tmdb_docs = [
                        dict(doc)
                        for doc in _fetch_tmdb_documents(session, candidate)
                        if isinstance(doc, Mapping) and doc.get("ok")
                    ]
                    tmdb_cast = _sanitize_cast_values([doc.get("cast") for doc in tmdb_docs])
                    if tmdb_cast:
                        metadata["cast"] = _limit_cast_values(tmdb_cast)
                        metadata["evidence_urls"] = _dedupe_urls(
                            list(metadata.get("evidence_urls") or [])
                            + [clean_text(doc.get("url")) for doc in tmdb_docs if clean_text(doc.get("url"))]
                        )

            matched_docs = metadata.get("matched_docs") or []
            ok = bool(clean_text(candidate.get("content_url"))) and not metadata.get("exclude_from_sync")

            entry = all_content_today.get(candidate_id)
            if not isinstance(entry, dict):
                entry = _find_current_source_entry(
                    write_plan,
                    canonical_content_id=candidate_id,
                    source_name=source_name,
                )
            if ok and isinstance(entry, dict):
                _apply_entry_enrichment(entry, metadata)

            is_watchlist_recheck = bool(candidate.get("watchlist_recheck"))
            is_filtered_out = bool(metadata.get("exclude_from_sync"))
            if is_watchlist_recheck:
                watchlist_rechecked_count += 1
            elif is_filtered_out:
                filtered_out_ids.add(candidate_id)
            elif ok:
                verified_changed_count += 1
            else:
                blocking_failures.append(candidate_id)

            results.append(
                {
                    "content_id": candidate_id,
                    "title": clean_text(metadata.get("resolved_title")) or clean_text(candidate.get("title")) or candidate_id,
                    "ok": ok or is_watchlist_recheck or is_filtered_out,
                    "reason": (
                        "filtered_out"
                        if is_filtered_out
                        else (
                        "evidence_matched"
                        if ok
                        else ("watchlist_unresolved" if is_watchlist_recheck else "no_official_url")
                        )
                    ),
                    "verification_method": "official_public_web",
                    "watchlist_recheck": is_watchlist_recheck,
                    "matched_count": metadata.get("matched_count", 0),
                    "evidence_urls": metadata.get("evidence_urls") or [],
                    "observed_release_start_at": metadata.get("release_start_at").isoformat() if metadata.get("release_start_at") else None,
                    "observed_release_end_at": metadata.get("release_end_at").isoformat() if metadata.get("release_end_at") else None,
                    "observed_release_end_status": metadata.get("release_end_status"),
                    "observed_cast_count": len(metadata.get("cast") or []),
                    "exclude_reason": metadata.get("exclude_reason"),
                    "change_kinds": list(candidate.get("change_kinds") or []),
                }
            )

    changed_candidates = [
        item
        for item in targets
        if not item.get("watchlist_recheck")
        and clean_text(item.get("content_id")) not in filtered_out_ids
    ]
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
