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
- **Kakao crawler**: Enforces cookie requirements via env vars, fetches weekday and completed feeds concurrently with retries, maps status (ongoing/hiatus/finished), normalizes authors/thumbnails/weekdays into the same `meta` shape, runs CDC comparison, sends notifications, and syncs the database.【F:crawlers/kakaowebtoon_crawler.py†L23-L186】
- **Retry & pagination**: Both crawlers apply `tenacity` retries to API calls and iterate paginated data where required (Naver finished list, Kakao completed list), matching the resiliency and completeness notes.【F:crawlers/naver_webtoon_crawler.py†L25-L58】【F:crawlers/kakaowebtoon_crawler.py†L29-L75】

### 4) Notification & Email Strategy
- **Pluggable providers**: `EMAIL_PROVIDER` switches between SMTP and SendGrid without code changes via `get_email_service`, consistent with the spec’s strategy/factory approach.【F:services/email.py†L1-L17】【F:config.py†L18-L24】
- **Completion-triggered delivery**: CDC results drive subscription lookups and per-user email sends through the chosen provider, matching the described workflow.【F:services/notification_service.py†L4-L37】

### 5) API Surface
- **Search**: Uses trigram similarity (`title %% %s` with `similarity` ordering) over `contents`, scoped by `content_type`/`source` as in the spec.【F:views/contents.py†L10-L39】
- **Ongoing regrouping**: Reads `meta.attributes.weekdays` and returns day-grouped JSON for ongoing/hiatus items, reflecting the transformation step in the document.【F:views/contents.py†L41-L87】
- **Cursor-like pagination**: Completed and hiatus endpoints accept `last_title` as a cursor and return `next_cursor`, matching the cursor-based paging requirement.【F:views/contents.py†L89-L173】
- **Subscriptions**: Validates email format, verifies content existence, and inserts with `ON CONFLICT DO NOTHING` to guarantee idempotency and FK-style protection.【F:views/subscriptions.py†L1-L56】

### 6) Batch Orchestration & Reporting
- **Parallel crawler runs**: `run_all_crawlers.py` registers both crawlers and executes them concurrently with `asyncio.gather(..., return_exceptions=True)` so one failure does not block the other.【F:run_all_crawlers.py†L1-L79】
- **Audit logging**: Each crawler write a JSONB report into `daily_crawler_reports`; the standalone Naver script mirrors this for isolated runs.【F:run_all_crawlers.py†L41-L75】【F:crawlers/naver_webtoon_crawler.py†L191-L256】
- **Admin reporting cleanup**: The report sender truncates audit rows only after a successful email, preserving logs on failure as specified.【F:report_sender.py†L10-L62】

### 7) Not Yet Implemented (by design or future work)
- **MSA / Celery / Redis job queue**: Present code remains a modular monolith with synchronous batch orchestration; no task queue or service split is in place.
- **Elasticsearch search**: Only PostgreSQL trigram search is implemented; no separate search engine integration exists.【F:database.py†L91-L110】【F:views/contents.py†L10-L39】
- **JWT/OAuth user accounts**: Authentication and personalized profiles are absent; subscriptions remain email-only without user tables or JWT issuance.【F:views/subscriptions.py†L10-L56】
- **Rate limiting & Pydantic validation**: Flask-Limiter and runtime validation layers are not present in the API routes.【F:views/contents.py†L10-L173】【F:views/subscriptions.py†L10-L56】

## Gaps & Risks to Address
- **Template Method embodiment**: `ContentCrawler` defines `run_daily_check` abstractly, but subclasses each own the orchestration. Extracting the shared flow into the base class (with hooks for fetch/transform/sync) would better reflect the spec’s Template Method narrative and reduce duplication.【F:crawlers/base_crawler.py†L1-L25】【F:crawlers/naver_webtoon_crawler.py†L150-L189】【F:crawlers/kakaowebtoon_crawler.py†L130-L186】
- **Title normalization for Kakao notifications**: Notifications reference `titleName`, which exists on Naver but not Kakao responses, leading to placeholder IDs in emails. Normalizing a `title` field across crawler outputs before notification would align with the cross-platform expectation in the document.【F:services/notification_service.py†L18-L37】【F:crawlers/kakaowebtoon_crawler.py†L82-L137】
- **Per-crawler DB isolation**: `run_all_crawlers.py` shares a single DB connection across all crawlers, so a connection failure could cascade. Issuing one connection per crawler instance would better honor the spec’s isolated execution intent.【F:run_all_crawlers.py†L41-L79】

## Overall Assessment
The implemented MVP matches the spec across stack choice, schema, async crawlers with CDC, notification strategy, search endpoints, subscriptions, and reporting. Remaining work is mostly structural hardening (template method, per-crawler DB isolation) and data normalization for multi-source notifications, alongside the unimplemented future roadmap items (Celery/Redis, Elasticsearch, JWT/OAuth, rate limiting, Pydantic).
