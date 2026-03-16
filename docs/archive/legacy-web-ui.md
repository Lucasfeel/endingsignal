# Legacy Web UI Archive

This document marks the pre-React public/admin UI before the first big-bang
refactor slice.

## Reference point

- Git tag: `pre-react-refactor`
- Legacy runtime files:
  - `static/app.js`
  - `static/admin.js`
- Legacy templates:
  - `templates/index.html`
  - `templates/admin.html`

## Route map

### Public shell

- `/`
- The legacy UI mostly used in-page overlays instead of route-based views.
- Main interaction clusters inside `static/app.js`:
  - recommendations / home
  - search overlay
  - browse filters
  - detail modal
  - subscriptions
  - my page

### Admin shell

- `/admin`
- `/admin/contents/new`
- Main interaction clusters inside `static/admin.js`:
  - content add/edit
  - deleted content
  - publication and completion changes
  - audit logs
  - CDC events
  - crawler reports
  - daily notification reports

## Notes

- The React refactor keeps the existing backend contracts first and rebuilds the
  UI/runtime around them.
- Crawlers, collection, backfill, and DB init flows are out of scope for the
  front-end rewrite.
