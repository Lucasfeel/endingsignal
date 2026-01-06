import os

# --- Crawler ---
CRAWLER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/108.0.0.0 Safari/537.36"
    )
}

# --- HTTP Client Defaults ---
CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS", 60))
CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS", 15))
CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS", 45))
CRAWLER_HTTP_CONCURRENCY_LIMIT = int(os.getenv("CRAWLER_HTTP_CONCURRENCY_LIMIT", 50))
CRAWLER_FETCH_HEALTH_MIN_RATIO = float(os.getenv("CRAWLER_FETCH_HEALTH_MIN_RATIO", 0.70))
CRAWLER_RUN_WALL_TIMEOUT_SECONDS = int(os.getenv("CRAWLER_RUN_WALL_TIMEOUT_SECONDS", 1800))

# --- KakaoPage Crawler Controls ---
KAKAOPAGE_GRAPHQL_URL = os.getenv("KAKAOPAGE_GRAPHQL_URL", "https://page.kakao.com/graphql")
KAKAOPAGE_MODE_DEFAULT = os.getenv("KAKAOPAGE_MODE", "verify")
KAKAOPAGE_VERIFY_ONLY_SUBSCRIBED = os.getenv("KAKAOPAGE_VERIFY_ONLY_SUBSCRIBED", "true").lower() == "true"
KAKAOPAGE_VERIFY_CONCURRENCY = int(os.getenv("KAKAOPAGE_VERIFY_CONCURRENCY", 10))
KAKAOPAGE_VERIFY_TIMEOUT_SECONDS = int(os.getenv("KAKAOPAGE_VERIFY_TIMEOUT_SECONDS", 12))
KAKAOPAGE_VERIFY_JITTER_MIN_SECONDS = float(os.getenv("KAKAOPAGE_VERIFY_JITTER_MIN_SECONDS", 0.05))
KAKAOPAGE_VERIFY_JITTER_MAX_SECONDS = float(os.getenv("KAKAOPAGE_VERIFY_JITTER_MAX_SECONDS", 0.25))

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

# --- Email ---
# [신규] 어떤 이메일 서비스를 사용할지 결정 (smtp 또는 sendgrid)
EMAIL_PROVIDER = os.getenv("EMAIL_PROVIDER", "smtp").lower()

# [기존] SMTP 설정 (SmtpService가 사용)
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
