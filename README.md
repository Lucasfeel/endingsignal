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
  - Web bind target is `0.0.0.0:${PORT}` (`PORT` default: `5000`).
  - Health check endpoint: `GET /healthz`.

## Testing

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```
