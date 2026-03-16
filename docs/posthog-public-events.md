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

| PostHog event | Source UI event | Properties |
| --- | --- | --- |
| `public_tab_selected` | `nav_tab_selected` | `tab_from`, `tab_to`, `path` |
| `public_content_opened` | `content_opened` | `content_source`, `content_type`, `from_tab`, `path` |
| `public_search_submitted` | `search_submitted` | `from_tab`, `query_length`, `path` |
| `public_subscription_clicked` | `subscription_cta_clicked` | `action`, `content_source`, `content_type`, `from_tab`, `requires_auth`, `path` |
| `public_overlay_opened` | `overlay_opened` | `overlay`, `from_tab`, `path` |
| `public_overlay_closed` | `overlay_closed` | `overlay`, `return_to`, `path` |

## Data minimization

- Do not send raw search text.
- Do not send titles.
- Do not send user identifiers.
- Do not send content IDs unless a later product question actually needs them.

If a later dashboard needs deeper drill-down, add properties deliberately instead of turning on broad autocapture.
