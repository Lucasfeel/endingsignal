---
name: agents-md-update
description: Review repeated misunderstandings, missing shorthand, and recurring guidance gaps, then make small durable updates to AGENTS.md. Use when the user wants AGENTS drift checks or recurring instruction cleanup.
---

# AGENTS.md Update

Use this skill to keep `AGENTS.md` aligned with how the repository is actually
worked on.

## Goals

- Capture repeated misunderstandings before they happen again
- Add missing shorthand or routing guidance that would save future turns
- Codify recurring review feedback or operating rules

## Workflow

1. Inspect recent work and recent Codex friction visible from local context.
2. Look for durable guidance gaps such as:
   - repeated misunderstandings
   - shorthand the agent did not understand
   - recurring review feedback
   - repeated wasted reading or navigation
3. Update the nearest relevant `AGENTS.md` with the smallest precise rule that
   would have prevented the issue.
4. Summarize:
   - what misunderstanding or gap was observed
   - what was added or adjusted
   - why it should reduce future friction

## Rules

- Keep changes small and durable.
- Prefer repo-specific operational guidance over broad style prose.
- Do not rewrite large sections for one isolated incident.
- Do not modify skills here unless the user explicitly asks for skill changes.
- If no durable `AGENTS.md` update is justified, say so explicitly and stop.
