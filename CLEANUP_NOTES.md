# Cleanup Notes (2026-01-06)

## Removed
- `services/notification_service.py`: no runtime imports or entrypoint references; notification sending handled elsewhere if reintroduced.
- TypeScript client scaffold (`src/`, `package.json`, `tsconfig.json`): PWA runs from `templates/index.html` + `static/app.js` and no build step consumes these files.
- `docs/FE_SPEC_CANVAS_v8.1.md`: superseded by the canonical v10 frontend spec added in this update.
- `test_email.py`: manual dev check for email providers; not called from app or workflows.

## Archived
- `docs/archive/IMPLEMENTATION_AUDIT.md` and `docs/archive/TEST_RESULTS.md`: kept for historical reference; not part of runtime.
- `scripts/archive/v2_meta_structure.py` and `scripts/archive/backfill_content_urls.py`: one-off migration/backfill utilities not invoked by any scheduler or entrypoint.

## Notes
- Email/reporting utilities (`services/email.py`, `report_sender.py`, SMTP/SendGrid adapters) were retained because `.github/workflows/crawler.yml` still schedules them alongside crawler runs.
- GitHub Actions crawler workflow remains unchanged pending confirmation of operational scheduler preferences.
