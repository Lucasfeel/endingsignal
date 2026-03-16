# Public Web PostHog Plan

This repo sends PostHog events only from the public web shell.
Admin stays excluded on purpose.

## Cost-first defaults

- No autocapture
- No pageview/pageleave capture
- No session replay
- No surveys
- No identify calls
- `person_profiles: identified_only`

This keeps the initial rollout focused on a small custom event set.

## Event set

Common context added to every tracked public event:

- `entry_tab`
- `entry_filter`
- `event_family`
- `journey_stage`
- `auth_state`
- `route_kind`
- `is_authenticated`
- `auth_provider`
- `user_role`
- `entry_sources`
- `entry_source_count`
- `search_input_length`
- `webtoon_filter`
- `novel_filter`
- `ott_filter`
- `my_view_mode`

Naming note:

- `route_kind` describes the current UI surface, such as `search`, `browse`, or `content`.
- `entry_*` describes the browse context the user came from, which is often more useful than calling that state `active_*` when the current route is an overlay or detail page.
- `event_family` is a low-cardinality grouping like `search`, `content`, `subscription`, `overlay`, or `navigation`.
- `journey_stage` is an AI-friendly funnel phase like `browse`, `search`, `evaluate`, `convert`, or `account`.
- `auth_state` is a stable label (`anonymous` or `authenticated`) that is easier for models to reason over than raw booleans alone.

| PostHog event | Source UI event | Properties |
| --- | --- | --- |
| `public_tab_selected` | `nav_tab_selected` | `tab_from`, `tab_to` |
| `public_content_opened` | `content_opened` | `content_source`, `content_type`, `content_status`, `from_tab`, `authors_count`, `genre_count`, `platform_count`, `has_content_url`, `has_thumbnail`, `is_upcoming`, `release_end_status`, `trigger`, `weekday_count` |
| `public_search_submitted` | `search_submitted` | `from_tab`, `query_length`, `query_word_count`, `search_trigger`, `used_recent_search` |
| `public_subscription_clicked` | `subscription_cta_clicked` | `action`, `content_source`, `content_type`, `content_status`, `from_tab`, `authors_count`, `genre_count`, `has_content_url`, `is_subscribed_before_click`, `requires_auth`, `is_upcoming`, `release_end_status`, `trigger` |
| `public_overlay_opened` | `overlay_opened` | `overlay`, `from_tab`, `entrypoint` |
| `public_overlay_closed` | `overlay_closed` | `overlay`, `return_to`, `close_reason` |

## Data minimization

- Do not send raw search text.
- Do not send titles.
- Do not send user identifiers.
- Do not send content IDs unless a later product question actually needs them.

If a later dashboard needs deeper drill-down, add properties deliberately instead of turning on broad autocapture.
