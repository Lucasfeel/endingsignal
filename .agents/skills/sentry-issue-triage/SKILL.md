---
name: sentry-issue-triage
description: Investigate one production issue at a time using Sentry issue data, stack traces, or exported logs, and implement a fix only when confidence is high. Use when the user wants background bug triage from Sentry-like issue data.
---

# Sentry Issue Triage

Use this skill for one-at-a-time production issue investigation backed by the
official Sentry MCP server when available, with a local Sentry API helper as a
fallback.

## Required local config

- preferred: `SENTRY_ACCESS_TOKEN`
- compatibility alias: `SENTRY_AUTH_TOKEN`
- `SENTRY_ORG_SLUG`
- `SENTRY_PROJECT_SLUG` or `SENTRY_PROJECT_SLUGS`
- optional `.env.sentry.local`
- for worktree automations, prefer `C:\Users\lucas\.codex\sentry.env` or
  `C:\Users\lucas\.codex\sentry.<repo-name>.env`

## Workflow

1. If the `sentry` MCP server is connected, use it first.
   - Find the top unresolved issue with MCP search/list tools.
   - Pull issue details, related events, and Seer analysis when available.
2. If the MCP server is unavailable or unauthenticated, run
   `python scripts/sentry_triage_snapshot.py --doctor`.
   - If config or API access is missing, stop and say exactly what is missing.
3. In the fallback path, fetch issue context with:
   - `python scripts/sentry_triage_snapshot.py --markdown --output-dir output/sentry`
   - or pass `--issue-id` / `--issue-url` when the user gives a specific issue
4. Identify the likely root cause from the Sentry data plus local code context.
5. Decide whether the issue is safe to fix automatically.
6. If confidence is high, implement the smallest reasonable fix and verify it.
7. If confidence is low, stop with a concise triage summary instead of forcing
   a code change.

## Rules

- Work one issue at a time.
- Prefer high-signal fixes over broad defensive rewrites.
- Treat Sentry issue data as the source of truth for issue metadata.
- Do not print, log, or commit Sentry tokens.
- Avoid echoing raw user objects, headers, or request bodies from Sentry events
  unless the user explicitly asks for them.
- If the repository has no accessible issue data, say what is missing and stop.
- Avoid retrying the same failed approach repeatedly.
