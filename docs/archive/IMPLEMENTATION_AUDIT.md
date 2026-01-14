# Implementation Audit vs. Project Jules Spec (v2.01)

This audit re-checks the current code against the supplied v2.01 specification to confirm what is implemented, what is partially covered, and what is missing.

## Coverage by Spec Section

### 1) Runtime, Framework, and Deployment
- **Python + Flask + Gunicorn Procfile**: The service uses Flask with `gunicorn app:app` defined in the Procfile, matching the specified runtime and deployment shape.【F:app.py†L1-L39】【F:Procfile†L1-L2】
- **Dependencies & async stack**: Requirements include Flask, Gunicorn, aiohttp, tenacity, psycopg2, aligning with the listed stack for web serving, async crawling, retries, and PostgreSQL access.【F:requirements.txt†L1-L11】

### 2) Database Schema & Search
- **Tables & constraints**: `contents` (PK on `content_id, source`), `subscriptions` (UNIQUE on email/content/source), and `daily_crawler_reports` (audit table) are provisioned exactly as described, including JSONB `meta` storage.【F:database.py†L36-L112】
- **Text search tuning**: The `pg_trgm` extension and GIN index on `contents.title` are created to support similarity search with mid-string matches as in the spec.【F:database.py†L91-L110】

### 3) Crawlers & CDC (Change Detection)
- **Naver crawler**: Performs paginated async fetches across weekday and finished endpoints, normalizes weekday lists, snapshots DB state, detects newly completed titles, issues notifications, and syncs DB with structured `meta.common/attributes`.【F:crawlers/naver_webtoon_crawler.py†L23-L189】
- **Kakao crawler (archived)**: Enforces cookie requirements via env vars, fetches weekday and completed feeds concurrently with retries, maps status (ongoing/hiatus/finished), normalizes authors/thumbnails/weekdays into the same `meta` shape, runs CDC comparison, sends notifications, and syncs the database.【F:crawlers/_archive/kakaowebtoon_crawler.py†L3-L187】
- **Retry & pagination**: Both crawlers apply `tenacity` retries to API calls and iterate paginated data where required (Naver finished list, Kakao completed list), matching the resiliency and completeness notes.【F:crawlers/naver_webtoon_crawler.py†L25-L58】【F:crawlers/_archive/kakaowebtoon_crawler.py†L9-L77】

### 4) Notification Strategy
- **Email reporting removed**: Email-based reporting and provider abstractions were removed in favor of Admin Console summaries and scheduled cleanup for crawler reports.
  This area now focuses on in-app admin observability instead of outbound email delivery.

### 5) API Surface
- **Search**: Uses trigram similarity (`title %% %s` with `similarity` ordering) over `contents`, scoped by `content_type`/`source` as in the spec.【F:views/contents.py†L10-L39】
- **Ongoing regrouping**: Reads `meta.attributes.weekdays` and returns day-grouped JSON for ongoing/hiatus items, reflecting the transformation step in the document.【F:views/contents.py†L41-L87】
- **Cursor-like pagination**: Completed and hiatus endpoints accept `last_title` as a cursor and return `next_cursor`, matching the cursor-based paging requirement.【F:views/contents.py†L89-L173】
- **Subscriptions**: Validates email format, verifies content existence, and inserts with `ON CONFLICT DO NOTHING` to guarantee idempotency and FK-style protection.【F:views/subscriptions.py†L1-L56】

### 6) Batch Orchestration & Reporting
- **Parallel crawler runs**: `run_all_crawlers.py` registers both crawlers and executes them concurrently with `asyncio.gather(..., return_exceptions=True)` so one failure does not block the other.【F:run_all_crawlers.py†L1-L79】
- **Audit logging**: Each crawler write a JSONB report into `daily_crawler_reports`; the standalone Naver script mirrors this for isolated runs.【F:run_all_crawlers.py†L41-L75】【F:crawlers/naver_webtoon_crawler.py†L191-L256】
- **Admin reporting cleanup**: Admin Console summaries now serve as the primary reporting surface, and a scheduled cleanup script removes old `daily_crawler_reports` rows to prevent unbounded growth.

### 7) Not Yet Implemented (by design or future work)
- **MSA / Celery / Redis job queue**: Present code remains a modular monolith with synchronous batch orchestration; no task queue or service split is in place.
- **Elasticsearch search**: Only PostgreSQL trigram search is implemented; no separate search engine integration exists.【F:database.py†L91-L110】【F:views/contents.py†L10-L39】
- **JWT/OAuth user accounts**: Authentication and personalized profiles are absent; subscriptions remain email-only without user tables or JWT issuance.【F:views/subscriptions.py†L10-L56】
- **Rate limiting & Pydantic validation**: Flask-Limiter and runtime validation layers are not present in the API routes.【F:views/contents.py†L10-L173】【F:views/subscriptions.py†L10-L56】

## Gaps & Risks to Address
- **Template Method 구현**: 공통 일일 점검 흐름을 `ContentCrawler.run_daily_check`에 템플릿 메서드로 올려 하위 크롤러는 수집/동기화만 오버라이드하도록 맞췄습니다.【F:crawlers/base_crawler.py†L1-L41】
- **제목 정규화 후 알림 발송**: 모든 크롤러가 `title` 필드를 채우고 알림 시 `title`→`titleName`→`content.title` 순으로 폴백하여 플랫폼 간 제목 누락 없이 이메일을 전송합니다.【F:crawlers/naver_webtoon_crawler.py†L53-L103】【F:crawlers/_archive/kakaowebtoon_crawler.py†L102-L132】【F:services/notification_service.py†L13-L35】
- **크롤러별 DB 커넥션 분리**: `run_all_crawlers.py`가 각 크롤러 실행마다 별도 연결을 생성/종료해 한 연결 장애가 다른 크롤러로 전파되지 않도록 격리했습니다.【F:run_all_crawlers.py†L20-L73】

## Overall Assessment
The implemented MVP matches the spec across stack choice, schema, async crawlers with CDC, notification strategy, search endpoints, subscriptions, and reporting. Remaining work is mostly structural hardening (template method, per-crawler DB isolation) and data normalization for multi-source notifications, alongside the unimplemented future roadmap items (Celery/Redis, Elasticsearch, JWT/OAuth, rate limiting, Pydantic).
