---
name: sentry-issue-triage
description: Investigate one production issue at a time using Sentry issue data, stack traces, or exported logs, and implement a fix only when confidence is high. Use when the user wants background bug triage from Sentry-like issue data.
---

# Sentry Issue Triage

Use this skill for one-at-a-time production issue investigation.

## Workflow

1. Confirm the issue input exists:
   - Sentry issue link or export
   - stack trace
   - logs
   - equivalent production failure artifact
2. Identify the likely root cause from issue data plus local code context.
3. Decide whether the issue is safe to fix automatically.
4. If confidence is high, implement the smallest reasonable fix and verify it.
5. If confidence is low, stop with a concise triage summary instead of forcing
   a code change.

## Rules

- Work one issue at a time.
- Prefer high-signal fixes over broad defensive rewrites.
- If the repository has no accessible issue data, say what is missing and stop.
- Avoid retrying the same failed approach repeatedly.
