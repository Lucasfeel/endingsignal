---
name: commit-pulse
description: Summarize the last 24 hours of repository changes as a morning brief grouped by workstream, owner, and risk. Use when the user wants a daily pulse, commit summary, standup brief, or concise repo update.
---

# Commit Pulse

Create a short, high-signal brief of the last day of meaningful repository
changes.

## Workflow

1. Work from the repository root.
2. Use Git first:
   - inspect commits from roughly the last 24 hours
   - identify the files and directories touched
   - group related commits into workstreams instead of listing them one by one
3. Use GitHub metadata if available and helpful, but do not block on it.
4. Prefer plain language over raw commit output.

## Output requirements

- Group by workstream, not by commit hash.
- For each workstream, say:
  - what changed
  - who worked on it when that is clear
  - what the user should know
  - any risk, follow-up, or unfinished edge
- Keep it concise and scannable.
- Do not dump raw commit logs unless the user explicitly asks.

## Rules

- Ignore unrelated local dirty files unless the user asked about them.
- If there were no meaningful changes in scope, say so plainly.
- Prefer the current repository scope over cross-repo summaries unless the user
  explicitly asks for multiple repos.
