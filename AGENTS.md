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
- For direct questions or narrow follow-ups, answer or act on the current-turn
  request first. Do not prepend prior progress recaps or stop work to restate
  old context unless the user explicitly asked for a progress summary or the
  immediate answer would be unclear without one.

## Preferred Codex workflows

For recurring maintenance in this repo, prefer the shared repo skills in
`.agents/skills`:

- `$commit-pulse`: summarize the last day of meaningful repo changes
- `$codex-upkeep`: improve repo guidance and repeated Codex workflows
- `$green-prs`: inspect open PRs, CI failures, stale branches, and conflicts
- `$sentry-issue-triage`: investigate one production issue at a time when
  issue data is available

## OTT guidance

For OTT collection, verification, season promotion, and DB cleanup work, read
and follow [ott-codex-guide.md](/C:/Users/lucas/endingsignal/docs/ott-codex-guide.md)
before changing code or running a full verification sync.

Short version:

- Favor durable rules over one-off fixes.
- Treat official platform pages as the top source of truth.
- Use TMDb aggressively for genre/cast and carefully for dates.
- Use IMDb as a search fallback when TMDb misses a global title.
- Do not force cast lists to exactly four names; four is a cap, not a target.
- If a season is inferred rather than directly proven, do not confidently
  invent an end date.
- When the user asks for a result summary, write it directly in chat unless
  they explicitly ask for a file.

## Review guidelines

- Treat auth regressions, DB init safety regressions, crawler transaction bugs,
  and broken public web builds as high severity.
- Prefer minimal, reviewable diffs over broad cleanup when working near DB
  setup, crawler orchestration, or production-serving paths.
- If instructions keep repeating across turns, update this file or
  `.agents/skills` instead of repeating them in prompts.
