# endingsignal Agent Guide

## Scope

`endingsignal` is the original repository with the public web surface, Python
backend, crawlers, and backfill tooling. Treat Docker-based local development
as the default path unless the task is clearly a quick local-only experiment.

## Repo map

- `app.py`: Flask entrypoint
- `views/`, `services/`, `crawlers/`: backend app and crawler logic
- `scripts/start_web.py`: preferred local app launcher
- `static/`, `templates/`, `frontend/`: public web assets
- `package.json`: root Vite build for the public app
- `tests/`: backend and policy verification
- `docs/`: repo notes and audits

## Setup and run

Preferred Docker path:

```bash
docker compose --profile with-db up --build
```

Local path:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
npm install
npm run dev
python scripts/start_web.py
```

## Validation

Backend:

```bash
PYTHONPATH=. pytest -q
```

Frontend build:

```bash
npm run build
```

If a change touches only a narrow backend area, run the smallest relevant test
slice instead of always running the full suite first.

## Durable rules

- Prefer Docker for day-to-day flows unless the task needs direct local runs.
- Do not log secrets, tokens, cookies, or raw DB credentials.
- Preserve DB init lock-safety behavior and timeout handling.
- Keep crawler transactions short; do not hold a transaction open across remote
  network work.
- If you touch `scripts/start_web.py`, `init_db.py`, or DB setup logic, stay
  compatible with the documented env var controls in `README.md`.
- If you touch public web assets or root Vite code, run `npm run build`.
- Be careful with backfill flows; avoid turning a web-shell task into a
  Playwright-heavy or memory-heavy run unless the user asked for that path.

## Preferred Codex workflows

For recurring maintenance in this repo, prefer the shared repo skills in
`.agents/skills`:

- `$commit-pulse`: summarize the last day of meaningful repo changes
- `$codex-upkeep`: improve repo guidance and repeated Codex workflows
- `$green-prs`: inspect open PRs, CI failures, stale branches, and conflicts
- `$sentry-issue-triage`: investigate one production issue at a time when
  issue data is available

## Review guidelines

- Treat auth regressions, DB init safety regressions, crawler transaction bugs,
  and broken public web builds as high severity.
- Prefer minimal, reviewable diffs over broad cleanup when working near DB
  setup, crawler orchestration, or production-serving paths.
- If instructions keep repeating across turns, update this file or
  `.agents/skills` instead of repeating them in prompts.
