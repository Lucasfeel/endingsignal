# config.py
import os

# --- Crawler ---
CRAWLER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}

# --- HTTP Client Defaults ---
CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS = int(os.getenv('CRAWLER_HTTP_TOTAL_TIMEOUT_SECONDS', 30))
CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS = int(os.getenv('CRAWLER_HTTP_CONNECT_TIMEOUT_SECONDS', 10))
CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS = int(os.getenv('CRAWLER_HTTP_SOCK_READ_TIMEOUT_SECONDS', 20))
CRAWLER_HTTP_CONCURRENCY_LIMIT = int(os.getenv('CRAWLER_HTTP_CONCURRENCY_LIMIT', 20))
CRAWLER_FETCH_HEALTH_MIN_RATIO = float(os.getenv('CRAWLER_FETCH_HEALTH_MIN_RATIO', 0.70))

# --- Webtoon API ---
NAVER_API_URL = "https://comic.naver.com/api/webtoon/titlelist"
WEEKDAYS = {
    'mon': 'mon',
    'tue': 'tue',
    'wed': 'wed',
    'thu': 'thu',
    'fri': 'fri',
    'sat': 'sat',
    'sun': 'sun',
    'daily': 'daily',
    'dailyPlus': 'daily'
}

# --- Email ---
# 🚨 [신규] 어떤 이메일 서비스를 사용할지 결정 (smtp 또는 sendgrid)
EMAIL_PROVIDER = os.getenv('EMAIL_PROVIDER', 'smtp').lower()

# [기존] SMTP 설정 (SmtpService가 사용)
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
