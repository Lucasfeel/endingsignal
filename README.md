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

## Docker development (recommended)

- For day-to-day work, use Docker as the default development environment.
  This keeps the runtime closer to deployment and makes backfill / web behavior more consistent.

- Build and run the app with the bundled Postgres container:

  ```bash
  docker compose --profile with-db up --build
  ```

- Run only the web container when you want to point at an existing database:

  ```bash
  docker compose up --build web
  ```

- Default port is `5000`. To use a different port, set `PORT`:

  ```bash
  PORT=8080 docker compose up --build web
  ```

- Run with an explicit in-network PostgreSQL connection string:

  ```bash
  DATABASE_URL=postgresql://endingsignal:endingsignal@db:5432/endingsignal \
  docker compose --profile with-db up --build
  ```

  - Startup behavior:
    - `scripts/start_web.py` runs `init_db.py` only when DB config exists (`DATABASE_URL` or all `DB_*` vars).
    - Set `SKIP_DB_INIT=1` to force skip.
    - Set `RUN_DB_INIT=1` to force run.
    - DB init disables the heavy contents backfill/hardening phase by default during app startup and direct `init_db.py` runs. Set `RUN_DB_INIT_WITH_BACKFILL=1` or `DB_INIT_ENABLE_BACKFILL=1` only when you intentionally want that work included.
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
    - `DB_INIT_ENABLE_BACKFILL` (default: `false`): enables the contents backfill/hardening phase.
    - `DB_INIT_STRICT_MAINTENANCE` (default: `false`): if `false`, lock/statement timeout during maintenance/backfill logs WARN and continues.
  - Web bind target is `0.0.0.0:${PORT}` (`PORT` default: `5000`).
  - Health check endpoint: `GET /healthz`.

## Local development (optional)

- Direct local execution is still supported for learning, debugging, or quick experiments.

- Install dependencies:

  ```bash
  pip install -r requirements.txt
  npm install
  ```

- Run the frontend build watcher in one terminal:

  ```bash
  npm run dev
  ```

- Run the app in another terminal:

  ```bash
  python scripts/start_web.py
  ```

- Open `http://localhost:5000`.

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

Render memory note:
- Running KakaoPage discovery (Playwright/Chromium) inside a low-memory web shell (for example `512MB`) can OOM-kill the service.
- Prefer running KakaoPage discovery in a dedicated worker container (`Dockerfile.backfill`) or locally.
- You can split phases:
  - `--kakaopage-phase discovery` (Playwright discovery only)
  - `--kakaopage-phase detail` (detail fetch/upsert only, no Playwright import/launch)
  - `--kakaopage-phase all` (default: discovery + detail)

Render Worker (quote-safe Docker Command):
- Use image built from `Dockerfile.backfill`.
- Attach a persistent disk mounted at `/app/.backfill_state`.
- Set Docker Command to:

  ```bash
  python scripts/run_kakaopage_backfill_worker.py
  ```

- Worker behavior:
  - Always runs KakaoPage backfill once on each worker start, even if done marker already exists.
  - Writes marker files in state dir:
    - `/app/.backfill_state/kakaopage_backfill_done`
    - `/app/.backfill_state/kakaopage_backfill_failed`
  - On success, overwrites the done marker timestamp.
  - Then idles to avoid restart loops re-running the backfill.
- To rerun intentionally:
  - Redeploy/restart worker (done marker deletion is not required anymore).
  - Optional compatibility mode: set `BACKFILL_RESPECT_DONE_MARKER=1` to restore old behavior (skip run when done marker exists).

Docker Compose split backfill services:

```bash
docker compose --profile backfill build backfill-kakao-page backfill-naver-series
```

```bash
DATABASE_URL=postgresql://... docker compose --profile backfill run --rm backfill-naver-series --dry-run --max-pages 1
DATABASE_URL=postgresql://... docker compose --profile backfill run --rm backfill-kakao-page --dry-run --max-items 20
```

```bash
DATABASE_URL=postgresql://... docker compose --profile backfill run --rm backfill-naver-series
DATABASE_URL=postgresql://... docker compose --profile backfill run --rm backfill-kakao-page
```

- Both compose backfill services mount `./.backfill_state` to `/app/.backfill_state` for resumable runs.

Hybrid workflow example (cloud-friendly):

```bash
# 1) Discovery on a larger machine/worker
python scripts/backfill_novels_once.py --sources kakao_page --kakaopage-phase discovery --kakaopage-seed-set webnoveldb

# 2) Detail phase on Render or another constrained environment
python scripts/backfill_novels_once.py --sources kakao_page --kakaopage-phase detail --kakaopage-seed-set webnoveldb
```

Dry runs:

```bash
python scripts/backfill_novels_once.py --sources naver_series --max-pages 1 --dry-run
python scripts/backfill_novels_once.py --sources kakao_page --max-items 20 --dry-run
python scripts/backfill_novels_once.py --sources kakao_page --kakaopage-seed-set webnoveldb --max-items 20 --dry-run
python scripts/backfill_novels_once.py --sources kakao_page --kakaopage-phase detail --kakaopage-seed-set webnoveldb --max-items 20 --dry-run
```

Real run:

```bash
python scripts/backfill_novels_once.py --sources naver_series,kakao_page
python scripts/backfill_novels_once.py --sources naver_series --reset-state
python scripts/backfill_novels_once.py --sources naver_series --rewind-pages 20
python scripts/backfill_novels_once.py --sources kakao_page --kakaopage-seed-set webnoveldb --reset-state
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
- KakaoPage seed modes:
  - `--kakaopage-seed-set all` (default): existing discovery behavior.
  - `--kakaopage-seed-set webnoveldb`: fixed six WebNovelDB genre seeds (ongoing/completed variants), no dynamic tab expansion.
- KakaoPage phase controls:
  - `--kakaopage-phase {all,discovery,detail}` (default from `KAKAOPAGE_BACKFILL_PHASE`, fallback `all`).
  - `detail` phase never imports/launches Playwright and requires existing discovered IDs in state.
- KakaoPage discovery strategy controls:
  - `KAKAOPAGE_BACKFILL_DISCOVERY_STRATEGY` (default: `auto`)
    - `auto`: if discovered IDs already exist in state, behaves like `refresh`; otherwise `full`.
    - `full`: keep scrolling the current tab while each scroll reveals at least one new tab-local ID; stop the tab when a scroll yields no new tab-local IDs.
    - `refresh`: stop early when there is no global discovered-ID growth for N scrolls, even if tab-local IDs still change.
    - `skip`: skip Playwright discovery entirely and proceed to detail phase (if requested).
  - `KAKAOPAGE_BACKFILL_NO_NEW_IDS_SCROLLS` (default: `1`)
    - Stop the current tab after N consecutive scrolls that yield zero new tab-local IDs.
    - Backward-compatible fallback: `KAKAOPAGE_BACKFILL_STAGNANT_SCROLLS`
  - `KAKAOPAGE_BACKFILL_DISCOVERY_NO_GLOBAL_GROWTH_SCROLLS` (default: `8`)
  - `KAKAOPAGE_BACKFILL_DISCOVERY_MAX_MEMORY_USAGE_RATIO` (default: `0.85`)
    - During discovery, if container memory usage ratio exceeds this threshold, discovery stops gracefully after saving state.
  - Discovery state metadata (in `.backfill_state/kakao_page.json`):
    - `discovery_complete`, `discovery_seed_set`, `discovery_strategy_last`, `discovery_completed_at`
    - When already complete for the same seed set and strategy is not `full`, discovery is skipped to avoid repeated heavy Playwright runs.
- KakaoPage Playwright memory guard:
  - `KAKAOPAGE_BACKFILL_MIN_MEMORY_FOR_PLAYWRIGHT_MB` (default: `1024`)
  - `--kakaopage-allow-low-memory-playwright` (or env `KAKAOPAGE_BACKFILL_ALLOW_LOW_MEMORY_PLAYWRIGHT=1`) to override guard.
- KakaoPage Playwright launch args:
  - Default args: `--disable-dev-shm-usage --no-sandbox --disable-gpu`
  - Override list via `KAKAOPAGE_BACKFILL_PLAYWRIGHT_ARGS_JSON` (JSON list; replaces defaults when valid)
- KakaoPage polite crawling/backoff envs:
  - Detail worker concurrency:
    - `KAKAOPAGE_BACKFILL_DETAIL_CONCURRENCY` (default: `2`)
    - Backward-compatible fallback: `KAKAOPAGE_BACKFILL_HTTP_CONCURRENCY`
  - Global request pacing:
    - `KAKAOPAGE_BACKFILL_MIN_INTERVAL_SECONDS` (default: `1.0`)
    - Or set `KAKAOPAGE_BACKFILL_RPS` (converted to min-interval)
  - Per-request jitter before detail fetch:
    - `KAKAOPAGE_BACKFILL_DETAIL_JITTER_MIN_SECONDS` (default: `0.8`)
    - `KAKAOPAGE_BACKFILL_DETAIL_JITTER_MAX_SECONDS` (default: `1.8`)
  - HTTP retry behavior:
    - `KAKAOPAGE_BACKFILL_HTTP_RETRIES` (default: `4`)
    - `KAKAOPAGE_BACKFILL_HTTP_RETRY_BASE_DELAY_SECONDS` (default: `1.0`)
    - `KAKAOPAGE_BACKFILL_HTTP_RETRY_MAX_DELAY_SECONDS` (default: `60.0`)
  - Circuit breaker / cooldown guidance:
    - `KAKAOPAGE_BACKFILL_MAX_CONSECUTIVE_RATE_LIMITS` (default: `5`)
    - `KAKAOPAGE_BACKFILL_COOLDOWN_SECONDS` (default: `900`)
  - Discovery scroll pacing:
    - `KAKAOPAGE_BACKFILL_DISCOVERY_SCROLL_DELAY_MS` (default base: `1600`, jitter applied)
    - `KAKAOPAGE_BACKFILL_DISCOVERY_SCROLL_JITTER_RATIO` (default: `0.25`)
  - State checkpoint cadence during detail stage:
    - `KAKAOPAGE_BACKFILL_SAVE_STATE_EVERY` (default: `20`)

WebNovelDB Kakao coverage baselines:
- Final summary output always prints per-genre Kakao coverage lines for `--kakaopage-seed-set webnoveldb`, including `discovered`, `expected`, `coverage`, `target90`, `target97`, and `status`.
- Coverage targets:
  - `판타지`: expected `12242`, 90% `11018`, 97% `11875`
  - `현판`: expected `9378`, 90% `8441`, 97% `9097`
  - `로맨스`: expected `22188`, 90% `19970`, 97% `21523`
  - `로판`: expected `10652`, 90% `9587`, 97% `10333`
  - `무협`: expected `4624`, 90% `4162`, 97% `4486`
  - `BL`: expected `3786`, 90% `3408`, 97% `3673`
- The summary also prints a diagnostic `total` row based on the summed expected baseline (`62870`; 90% `56583`; 97% `60984`), but per-genre coverage is the primary validation signal because cross-genre overlap can reduce deduped unique totals.

Manual validation checklist (KakaoPage WebNovelDB seed mode):

```bash
python scripts/backfill_novels_once.py --sources kakao_page --kakaopage-seed-set webnoveldb --kakaopage-phase discovery --log-level INFO
```

- Confirm Kakao discovery does not terminate normally because of a fixed max-scroll cap.
- Confirm exhausted tabs log `stop_reason=no_new_ids`.
- Confirm progress logs expose both `global_new_ids` and `tab_local_new_ids`.
- Confirm the final summary includes one coverage line per canonical WebNovelDB genre plus the diagnostic `total` row.
- Compare the printed coverage lines against the targets above; `90%+` is the minimum goal and `97%+` is the stretch target per genre.

```sql
SELECT
  content_id,
  source,
  status,
  meta->'common'->>'content_url' AS content_url,
  meta->'attributes'->'genres' AS genres,
  meta->'attributes'->>'is_completed' AS is_completed
FROM contents
WHERE source='kakao_page' AND content_type='novel'
LIMIT 20;
```
