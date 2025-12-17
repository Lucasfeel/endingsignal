# ending-project-endingsignal

## Database schema notes

- `daily_crawler_reports` uses an `id SERIAL PRIMARY KEY` alongside `crawler_name`, `status`,
  `report_data JSONB`, and `created_at TIMESTAMP DEFAULT NOW()`. Either `SERIAL` or
  `BIGSERIAL` are acceptable for deployments; the current schema uses `SERIAL` to match the
  application setup in `database.py`.
