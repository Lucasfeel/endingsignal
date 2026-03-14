---
name: upskill
description: Review recent skill usage, script friction, and repeated workflow steering, then improve skills or helper scripts when a durable pattern is clear. Use when the user wants overnight upskilling or skill maintenance.
---

# Upskill

Use this skill to make recurring Codex workflows smarter over time.

## Goals

- Find cases where a skill was invoked but was not helpful enough
- Fix scripts, commands, or instructions that repeatedly slow work down
- Improve skill trigger descriptions and workflows when the pattern is durable

## Workflow

1. Review recent Codex usage that is visible from local context, especially:
   - recent sessions
   - recent skill invocations
   - repeated command or script failures
   - places where extra steering was needed to finish a recurring task
2. Focus on skill-level fixes first:
   - tighten trigger descriptions
   - remove ambiguous instructions
   - add a missing step that keeps recurring
   - improve or repair a helper script when the same failure keeps showing up
3. Make the smallest durable change that improves the next run.
4. Summarize:
   - what friction was found
   - what changed
   - why the change should help next time

## Rules

- Prefer improving skills and helper scripts over broad repo guidance.
- Do not edit `AGENTS.md` here unless the user explicitly asks for that.
- Do not create a new skill unless the workflow is clearly recurring.
- If no durable skill improvement is justified, say that explicitly and stop.
