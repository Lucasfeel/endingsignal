# Ending Signal PWA – FE Spec (Canvas) v8.1

## API wrapper behavior
- Authentication endpoints are mixed-shape:
  - `POST /api/auth/login` returns `{access_token, token_type, expires_in, user}` (no `success` wrapper).
  - `GET /api/auth/me` returns `{success: true, user}`.
  - `GET /api/me/subscriptions` returns `{success: true, data: [...]}`.
- Public contents APIs (`/api/contents/*`, `/api/status`) return raw arrays/objects without `success` flags.
- Error envelopes on auth/subscriptions/admin use `{success: false, error: {code, message}}`.

## Auth interactions
- Logout flows may be invoked without an Authorization header; the client should tolerate optional auth on logout endpoints.
- Token lifecycle must surface `TOKEN_EXPIRED` to trigger re-login in the UI.
- `201 Created` responses on registration carry the payload directly (no `success` wrapper).

## Contents payloads
- `contents` records return `{content_id, title, status, meta, source}`.
- `content_type` is **not** included in public contents APIs; the client must infer it from tab/context.
- `contents/ongoing` includes `status` values in `('연재중', '휴재')`.
- `meta` can be `null` or a JSON string; the client must normalize it to an object for rendering safety.
