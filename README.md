# ending-project-endingsignal

## Database schema notes

- `daily_crawler_reports` uses an `id SERIAL PRIMARY KEY` alongside `crawler_name`, `status`,
  `report_data JSONB`, and `created_at TIMESTAMP DEFAULT NOW()`. Either `SERIAL` or
  `BIGSERIAL` are acceptable for deployments; the current schema uses `SERIAL` to match the
  application setup in `database.py`.

## Production auth config

- `JWT_SECRET` is required in production-like environments. The server will refuse to issue or
  validate tokens if it is missing.
- `JWT_ACCESS_TOKEN_EXP_MINUTES` controls access token lifetime. The default is 10080
  (7 days) if unset.
- Generate a strong secret with:

  ```bash
  python -c "import secrets; print(secrets.token_hex(32))"
  ```

## Testing

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```
