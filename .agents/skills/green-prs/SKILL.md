---
name: green-prs
description: Inspect open pull requests for stale branches, CI failures, and straightforward merge conflicts, then make the smallest safe fix first. Use when the user wants PR maintenance, CI cleanup, or branch refresh work.
---

# Green PRs

Use this skill to keep open pull requests reviewable and merge-ready.

## Workflow

1. Inspect open pull requests for the current repository.
2. Prioritize problems in this order:
   - stale base branch
   - deterministic CI failure
   - straightforward merge conflict
3. Use the smallest safe fix first.
4. Re-run or summarize the most relevant verification.

## Tooling guidance

- Prefer GitHub context when available.
- If the task is specifically about failing GitHub Actions checks, use
  `gh-fix-ci`.
- If the task is specifically about addressing review comments, use
  `gh-address-comments`.

## Rules

- Do not force-merge or bypass review expectations.
- Do not make speculative product changes just to get CI green.
- If a failure needs human judgment, stop with a concise explanation.
- If the repo is dirty locally, avoid touching unrelated work.
