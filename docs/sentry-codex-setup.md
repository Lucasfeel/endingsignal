# Sentry Codex Setup

This repository now includes a Codex-ready Sentry setup with three parts:

- optional backend Sentry initialization in the Flask app
- browser Sentry injection for the public page and the admin console
- `python scripts/sentry_triage_snapshot.py` for fetching the highest-priority issue from the official Sentry API
- a stronger `$sentry-issue-triage` skill that prefers the official `sentry` MCP server and falls back to the helper script when needed

## Local secrets

Copy `.env.sentry.example` to `.env.sentry.local` and fill the values locally.
For background worktree automations and the official Sentry MCP server, prefer
`C:\Users\lucas\.codex\sentry.env` so the same secrets work across Codex.

Required for Codex triage:

- `SENTRY_ACCESS_TOKEN`
- `SENTRY_ORG_SLUG`
- `SENTRY_PROJECT_SLUG` or `SENTRY_PROJECT_SLUGS`

Optional for runtime error capture:

- `SENTRY_API_DSN`
- `SENTRY_FRONTEND_DSN`
- `SENTRY_DSN`
- `SENTRY_ENVIRONMENT`
- `SENTRY_RELEASE`
- `SENTRY_TRACES_SAMPLE_RATE`
- `SENTRY_PROFILE_SESSION_SAMPLE_RATE`
- `SENTRY_PROFILES_SAMPLE_RATE`
- `SENTRY_FRONTEND_TRACES_SAMPLE_RATE`
- `SENTRY_FRONTEND_REPLAYS_SESSION_SAMPLE_RATE`
- `SENTRY_FRONTEND_REPLAYS_ON_ERROR_SAMPLE_RATE`
- `SENTRY_SEND_DEFAULT_PII`

## Validate the connection

```powershell
python scripts/sentry_triage_snapshot.py --doctor
```

If the connection is healthy, fetch the top issue context:

```powershell
python scripts/sentry_triage_snapshot.py --markdown --output-dir output/sentry
```

The helper writes:

- `output/sentry/top-issue.json`
- `output/sentry/top-issue.md`

## How Codex should use this

The repository-local `$sentry-issue-triage` skill should:

1. use the official `sentry` MCP server first when it is connected
2. fall back to the doctor check and local snapshot helper when MCP is unavailable
3. inspect the local codebase against the issue data
4. make the smallest high-confidence fix
5. stop with a concise triage note when confidence is low

## Frontend coverage

- `endingsignal-api` is the Flask backend project
- `endingsignal-web` receives browser and admin-side JavaScript errors
- template pages load the Sentry JavaScript loader before app scripts
- 5xx and network failures from the browser/admin API wrappers are reported to Sentry

## Security notes

- `SENTRY_ACCESS_TOKEN` stays local-only and should never be committed.
- `SENTRY_SEND_DEFAULT_PII` defaults to `0` to avoid broad user-data capture.
- The helper intentionally avoids printing request headers, bodies, and user objects from Sentry events.
