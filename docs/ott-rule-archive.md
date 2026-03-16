# OTT Rule Archive

As of 2026-03-15, the active OTT ingestion path is intentionally limited to
official-source collection only.

Active collection fields:

- title
- release_start_at
- platform_url
- cast

Operational rules:

- Keep `release_end_at` empty unless an operator fills it in manually.
- Keep `release_end_status` as `unknown` unless an operator updates it.
- Skip items without an official platform URL.
- Skip items whose `platform_content_id` has already been collected once,
  including records later hidden or deleted in the admin UI.

Legacy public-web, TMDb, season-upgrade, and end-date inference helpers remain
in code for reference only and are not part of the active OTT verification
flow.
