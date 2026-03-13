---
name: codex-upkeep
description: Review repeated Codex friction in this repository and make small durable improvements to AGENTS.md or repo-local skills. Use when the user wants upskill behavior, AGENTS drift checks, or recurring workflow cleanup.
---

# Codex Upkeep

Use this skill to make Codex easier to use in this repository over time.

## Goals

- Find repeated misunderstandings or missing guidance
- Improve repo-local skills when they keep needing the same steering
- Keep `AGENTS.md` and `.agents/skills` aligned with real usage

## Workflow

1. Look for recurring friction in the current task, recent repo usage, or
   recent skill failures that are visible from local context.
2. Classify the gap:
   - `AGENTS.md` if the rule is durable and repo-wide
   - repo-local skill if the workflow is repeatable and method-heavy
   - neither if the issue was clearly one-off
3. Make the smallest durable change that solves the repeated problem.
4. Summarize:
   - what friction was observed
   - what was updated
   - why the change should help next time

## Rules

- Do not rewrite large sections of guidance for one isolated incident.
- Prefer small precise edits over broad style churn.
- If no durable improvement is justified, say that explicitly and stop.
- Keep skill descriptions trigger-friendly and narrow in scope.
