# ending-project-endingsignal

## Database schema notes

- `daily_crawler_reports` uses an `id SERIAL PRIMARY KEY` alongside `crawler_name`, `status`,
  `report_data JSONB`, and `created_at TIMESTAMP DEFAULT NOW()`. Either `SERIAL` or
  `BIGSERIAL` are acceptable for deployments; the current schema uses `SERIAL` to match the
  application setup in `database.py`.
- Webtoon publication defaults: when `admin_content_metadata.public_at` is not explicitly set,
  the app treats `contents.created_at` (first-seen crawl timestamp) as the effective
  publication date. Crawlers also try to seed this default into `admin_content_metadata`
  using the first available user id.

## Production auth config

- `JWT_SECRET` is required in production-like environments. The server will refuse to issue or
  validate tokens if it is missing.
- `JWT_ACCESS_TOKEN_EXP_MINUTES` controls access token lifetime. The default is 10080
  (7 days) if unset.
- Generate a strong secret with:

  ```bash
  python -c "import secrets; print(secrets.token_hex(32))"
  ```

## Docker deployment

- Build and run (no database required):

  ```bash
  docker compose up --build web
  ```

  - Default port is `5000`. To use a different port, set `PORT`:

  ```bash
  PORT=8080 docker compose up --build web
  ```

- Run with PostgreSQL (optional):

  ```bash
  DATABASE_URL=postgresql://endingsignal:endingsignal@db:5432/endingsignal \
  docker compose --profile with-db up --build
  ```

- Startup behavior:
  - `scripts/start_web.py` runs `init_db.py` only when DB config exists (`DATABASE_URL` or all `DB_*` vars).
  - Set `SKIP_DB_INIT=1` to force skip.
  - Set `RUN_DB_INIT=1` to force run.
  - Session safety env vars (applied to all DB connections created via `database._create_connection()`: web app, crawlers, scripts, init):
    - `DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT` (recommended: `60s` to `300s`): PostgreSQL kills sessions that stay `idle in transaction`, preventing stale crawler transactions from blocking DDL.
    - `DB_STATEMENT_TIMEOUT` (optional, recommended for production): global query timeout per session.
    - `DB_APPLICATION_NAME` (optional): sets PostgreSQL `application_name` for easier lock diagnostics.
  - DB init lock-safety env vars:
    - `DB_INIT_LOCK_TIMEOUT` (default: `5s`): session `lock_timeout` used by `init_db.py`/`database.setup_database_standalone()`.
    - `DB_INIT_STATEMENT_TIMEOUT` (default: unset): optional session `statement_timeout` for DB init only.
    - `DB_INIT_ADVISORY_LOCK_WAIT_SECONDS` (default: `60`): max wait to acquire the DB init advisory lock.
    - `DB_INIT_DDL_RETRY_ATTEMPTS` (default: `5`): retries for lock-timeout DDL on `contents`.
    - `DB_INIT_DDL_RETRY_BASE_DELAY_SECONDS` (default: `1.0`): base delay for exponential backoff + jitter between DDL retries.
    - `DB_INIT_STALE_DDL_MAX_AGE_SECONDS` (default: `300`): stale DDL waiter age threshold for cleanup.
    - `DB_INIT_STALE_DDL_CLEANUP_ACTION` (default: `cancel`): stale waiter cleanup mode (`cancel` or `terminate`).
    - `DB_INIT_BACKFILL_BATCH_SIZE` (default: `20000`): batch size for `contents` null-fill backfills.
    - `DB_INIT_STRICT_MAINTENANCE` (default: `false`): if `false`, lock/statement timeout during maintenance/backfill logs WARN and continues.
  - Web bind target is `0.0.0.0:${PORT}` (`PORT` default: `5000`).
  - Health check endpoint: `GET /healthz`.

## Deploy lock troubleshooting

- Symptom: deploy pre-step hangs or fails during DB init around DDL (`ALTER TABLE ...` / index creation).
- Crawler safety rule: never hold a DB transaction open across network I/O. `ContentCrawler.run_daily_check()` now ends the snapshot read transaction before `fetch_all_data()` and starts a fresh write-phase transaction afterward.
- `init_db.py` now applies session timeouts and an advisory lock (`endingsignal_init_db` app name).
- On lock timeout during schema DDL, setup retries with backoff, emits relation lock reports (`pg_locks` + lock mode/granted), and attempts stale waiter cleanup.
- On lock or statement timeout that still cannot be recovered, setup exits non-zero and prints blocker diagnostics from `pg_stat_activity` including `pg_blocking_pids`.
- Typical checks:
  - Look for long-running transactions holding locks on hot tables (especially `contents`).
  - If role permissions allow, manually cancel stale waiters:

    ```sql
    SELECT pg_cancel_backend(<pid>);
    ```

  - If cancel is insufficient and you have privileges:

    ```sql
    SELECT pg_terminate_backend(<pid>);
    ```

  - Re-run with tighter timeout for fast feedback:

    ```bash
    DB_INIT_LOCK_TIMEOUT=5s DB_INIT_ADVISORY_LOCK_WAIT_SECONDS=30 python init_db.py
    ```

  - If needed, also bound long-running statements:

    ```bash
    DB_INIT_LOCK_TIMEOUT=5s DB_INIT_STATEMENT_TIMEOUT=60s python init_db.py
    ```

  - Enable aggressive stale waiter cleanup:

    ```bash
    DB_INIT_STALE_DDL_CLEANUP_ACTION=terminate DB_INIT_STALE_DDL_MAX_AGE_SECONDS=120 python init_db.py
    ```

## Testing

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```

## Novel backfill (one-time)

Backfills novel works into `contents` for:
- Naver Series (`source='naver_series'`, 연재 웹소설 범위)
- KakaoPage (`source='kakao_page'`, 웹소설 genre/11)

Setup:

```bash
pip install -r requirements-backfill.txt
python -m playwright install --with-deps chromium
```

Build/run in a dedicated backfill container (recommended for KakaoPage):

```bash
docker build -f Dockerfile.backfill -t endingsignal-backfill:latest .
docker run --rm -e DATABASE_URL=postgresql://... endingsignal-backfill:latest \
  python scripts/backfill_novels_once.py --sources kakao_page --reset-state
```

Dry runs:

```bash
python scripts/backfill_novels_once.py --sources naver_series --max-pages 1 --dry-run
python scripts/backfill_novels_once.py --sources kakao_page --max-items 20 --dry-run
```

Real run:

```bash
python scripts/backfill_novels_once.py --sources naver_series,kakao_page
python scripts/backfill_novels_once.py --sources naver_series --reset-state
python scripts/backfill_novels_once.py --sources naver_series --rewind-pages 20
```

Notes:
- The backfill writes only to `contents` (batched upsert on `(content_id, source)`).
- DB batching dedupes duplicate `(content_id, source)` keys within a single batch (last record wins) to avoid PostgreSQL `ON CONFLICT` multi-hit errors.
- It does not run daily crawlers and does not emit CDC events.
- Resume state is stored under `.backfill_state/` by default (`--state-dir` configurable).
- `--reset-state` clears state files for the selected sources before the run starts, which is useful for deliberate full reruns.
- `--rewind-pages N` rewinds Naver Series mode state by `N` pages (minimum page `1`) before run start, useful to recover small missing ranges after a failed flush.
- Naver Series pagination includes guards to stop if pages are repeatedly identical or produce no new ids to avoid runaway loops on repeated responses.
  - `NAVER_SERIES_BACKFILL_NO_NEW_PAGES_THRESHOLD` (default: `3`)
  - `NAVER_SERIES_BACKFILL_REPEAT_PAGE_THRESHOLD` (default: `2`)
- Optional cookie env for age-gated KakaoPage pages:
  - `KAKAOPAGE_COOKIE_HEADER`
  - `KAKAOPAGE_COOKIES_JSON`
