import json
import os


def _parse_cors_allow_origins(raw_value):
    if raw_value is None:
        return None
    stripped = raw_value.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
    return [item.strip() for item in stripped.split(",") if item.strip()]

# --- Crawler ---
CRAWLER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/108.0.0.0 Safari/537.36"
    )
}

# --- CORS ---
CORS_ALLOW_ORIGINS = _parse_cors_allow_origins(os.getenv("CORS_ALLOW_ORIGINS"))
CORS_SUPPORTS_CREDENTIALS = os.getenv("CORS_SUPPORTS_CREDENTIALS", "0") == "1"

# --- HTTP Client Defaults ---
CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS", 60))
CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS", 15))
CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS", 45))
CRAWLER_HTTP_CONCURRENCY_LIMIT = int(os.getenv("CRAWLER_HTTP_CONCURRENCY_LIMIT", 50))
CRAWLER_FETCH_HEALTH_MIN_RATIO = float(os.getenv("CRAWLER_FETCH_HEALTH_MIN_RATIO", 0.70))
CRAWLER_RUN_WALL_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_RUN_WALL_TIMEOUT_SECONDS", 1800))

# --- KakaoPage Crawler Controls ---
KAKAOPAGE_GRAPHQL_URL = os.getenv("KAKAOPAGE_GRAPHQL_URL", "https://bff-page.kakao.com/graphql")
KAKAOPAGE_GRAPHQL_BOOTSTRAP_URL = os.getenv(
    "KAKAOPAGE_GRAPHQL_BOOTSTRAP_URL", "https://page.kakao.com/"
)
DEBUG_KAKAOPAGE_GRAPHQL = os.getenv("DEBUG_KAKAOPAGE_GRAPHQL", "0") == "1"
KAKAOPAGE_DAYOFWEEK_SCREEN_UID = os.getenv("KAKAOPAGE_DAYOFWEEK_SCREEN_UID", "52")
KAKAOPAGE_CATEGORY_UID = os.getenv("KAKAOPAGE_CATEGORY_UID", "10")
KAKAOPAGE_MODE_DEFAULT = os.getenv("KAKAOPAGE_MODE", "verify")
KAKAOPAGE_VERIFY_ONLY_SUBSCRIBED = os.getenv("KAKAOPAGE_VERIFY_ONLY_SUBSCRIBED", "true").lower() == "true"
KAKAOPAGE_VERIFY_CONCURRENCY = int(os.getenv("KAKAOPAGE_VERIFY_CONCURRENCY", 10))
KAKAOPAGE_VERIFY_TIMEOUT_SECONDS = int(os.getenv("KAKAOPAGE_VERIFY_TIMEOUT_SECONDS", 12))
KAKAOPAGE_VERIFY_JITTER_MIN_SECONDS = float(os.getenv("KAKAOPAGE_VERIFY_JITTER_MIN_SECONDS", 0.05))
KAKAOPAGE_VERIFY_JITTER_MAX_SECONDS = float(os.getenv("KAKAOPAGE_VERIFY_JITTER_MAX_SECONDS", 0.25))

# --- Kakao Webtoon Timetable ---
KAKAOWEBTOON_TIMETABLE_BASE_URL = os.getenv(
    "KAKAOWEBTOON_TIMETABLE_BASE_URL", "https://gateway-kw.kakao.com/section/v2/timetables/days"
)
KAKAOWEBTOON_PLACEMENTS_WEEKDAYS = [
    placement.strip()
    for placement in os.getenv(
        "KAKAOWEBTOON_PLACEMENTS_WEEKDAYS",
        "timetable_mon,timetable_tue,timetable_wed,timetable_thu,timetable_fri,timetable_sat,timetable_sun",
    ).split(",")
    if placement.strip()
]
KAKAOWEBTOON_PLACEMENT_COMPLETED = os.getenv("KAKAOWEBTOON_PLACEMENT_COMPLETED", "timetable_completed")
KAKAOWEBTOON_COMPLETED_GENRE = os.getenv("KAKAOWEBTOON_COMPLETED_GENRE", "all")
KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET = int(os.getenv("KAKAOWEBTOON_PROFILE_LOOKUP_BUDGET", 200))
KAKAOWEBTOON_PROFILE_STATUS_TTL_DAYS = int(os.getenv("KAKAOWEBTOON_PROFILE_STATUS_TTL_DAYS", 7))
KAKAOWEBTOON_PROFILE_CONCURRENCY = int(os.getenv("KAKAOWEBTOON_PROFILE_CONCURRENCY", 15))

# --- Webtoon API ---
NAVER_API_URL = "https://comic.naver.com/api/webtoon/titlelist"
WEEKDAYS = {
    "mon": "mon",
    "tue": "tue",
    "wed": "wed",
    "thu": "thu",
    "fri": "fri",
    "sat": "sat",
    "sun": "sun",
    "daily": "daily",
    "dailyPlus": "daily",
}

# --- Naver Webtoon Crawl Controls ---
NAVER_FINISHED_MAX_PAGES = int(os.getenv("NAVER_FINISHED_MAX_PAGES", 400))
NAVER_FINISHED_ORDERS = [
    order.strip()
    for order in os.getenv("NAVER_FINISHED_ORDERS", "UPDATE,VIEW,STAR").split(",")
    if order.strip()
]

# --- Kakao Webtoon Discovery Controls ---
# 신기능(발견/디버깅) 우선: HTTP 오류 로그 on, 번들 스캔 범위 확장
KAKAO_DEBUG_HTTP_ERRORS = int(os.getenv("KAKAO_DEBUG_HTTP_ERRORS", 1))
KAKAO_DISCOVERY_MAX_BUNDLES = int(os.getenv("KAKAO_DISCOVERY_MAX_BUNDLES", 20))
KAKAO_DISCOVERY_MAX_PAGES_PER_SLUG = int(os.getenv("KAKAO_DISCOVERY_MAX_PAGES_PER_SLUG", 200))
KAKAO_DISCOVERY_SOFT_CAP = int(os.getenv("KAKAO_DISCOVERY_SOFT_CAP", 20000))
KAKAO_DISCOVERY_EXCLUDE_SLUG_REGEX = os.getenv(
    "KAKAO_DISCOVERY_EXCLUDE_SLUG_REGEX", "best-challenge|bestchallenge"
)
KAKAO_DISCOVERY_FALLBACK_SLUGS = [
    slug.strip()
    for slug in os.getenv(
        "KAKAO_DISCOVERY_FALLBACK_SLUGS",
        "ranking,complete,top,new,genre-romance,genre-fantasy",
    ).split(",")
    if slug.strip()
]

# --- KakaoPage Bootstrap Controls ---
KAKAOPAGE_AUTO_BOOTSTRAP = os.getenv("KAKAOPAGE_AUTO_BOOTSTRAP", "true").lower() == "true"
KAKAOPAGE_BOOTSTRAP_COOLDOWN_HOURS = float(
    os.getenv("KAKAOPAGE_BOOTSTRAP_COOLDOWN_HOURS", 6)
)
KAKAOPAGE_BOOTSTRAP_MAX_CONSECUTIVE_FAILURES = int(
    os.getenv("KAKAOPAGE_BOOTSTRAP_MAX_CONSECUTIVE_FAILURES", 3)
)
KAKAOPAGE_FORCE_BOOTSTRAP = os.getenv("KAKAOPAGE_FORCE_BOOTSTRAP", "false").lower() == "true"
