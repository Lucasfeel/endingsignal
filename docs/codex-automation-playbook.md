# Codex Cloud Playbook

This repo is prepared for the public Codex Cloud workflow documented at
`chatgpt.com/codex` and for GitHub comment delegation with `@codex`.

The important distinction is:

- Codex app automations are the documented recurring scheduler, but they require
  the app to stay running on a machine with the repo on disk.
- Codex Cloud runs tasks in OpenAI's cloud environment and can keep working
  while your computer is off.
- Public docs clearly cover Cloud tasks, GitHub reviews, and `@codex` task
  delegation. They do not currently document a public recurring scheduler inside
  Codex Cloud itself.

## One-time setup

1. Open [Codex](https://chatgpt.com/codex).
2. Connect GitHub.
3. Create an environment for `Lucasfeel/endingsignal`.
4. Confirm the environment can install dependencies and run the checks listed in
   `AGENTS.md`.
5. In [Code review settings](https://chatgpt.com/codex/settings/code-review),
   enable Codex review for this repository.
6. Optional: turn on automatic reviews if you want every PR reviewed without an
   explicit comment.

## How this repo is meant to work with Codex Cloud

- `AGENTS.md` gives Codex the repo map, setup commands, validation commands,
  and review rules.
- `.agents/skills/` contains reusable methods that are helpful when Codex is
  working in the repo.
- GitHub pull request comments are the main off-machine trigger surface:
  - `@codex review`
  - `@codex fix the CI failures`
  - `@codex update AGENTS.md for this recurring issue`

## Video pattern equivalents

### 1. Commit pulse

Run this as a background task in Codex Cloud:

```md
Use $commit-pulse for this repository and summarize the last 24 hours of
meaningful work. Group by workstream, not commit-by-commit. Call out risks and
follow-up items.
```

### 2. Codex upkeep

Run this as a background task in Codex Cloud:

```md
Use $codex-upkeep for this repository.

Review AGENTS.md and repo-local skills for repeated friction or stale guidance.
Make only small durable improvements. If no durable change is justified, say so
plainly and stop.
```

You can also delegate the same idea from GitHub on a PR or issue:

```md
@codex review our recurring guidance gaps and update AGENTS.md if a small durable
improvement is clearly justified
```

### 3. Green PRs

For PR review, use GitHub-native Codex review:

```md
@codex review
```

For a more task-oriented pass:

```md
@codex check this PR for stale assumptions, deterministic CI failures, and the
smallest safe fixes needed to keep it green
```

### 4. Sentry issue triage

Public docs do not currently document a first-class Sentry integration in Codex
Cloud. The closest public workflow is to launch a Cloud task with the Sentry
issue link or pasted issue context:

```md
Use $sentry-issue-triage for this repository.

Investigate this Sentry issue using the linked issue details, stack traces, and
logs. Work one issue at a time. Only implement a fix if confidence is high;
otherwise leave a concise triage summary.
```

## Repo-specific note

This repo does not currently show a strong in-repo Sentry integration surface.
That means the Sentry pattern here is best treated as a cloud task with manual
issue context rather than a native integration.
