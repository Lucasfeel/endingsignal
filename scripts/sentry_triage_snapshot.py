"""Fetch Sentry issue context for Codex triage.

Usage:
    python scripts/sentry_triage_snapshot.py --doctor
    python scripts/sentry_triage_snapshot.py --markdown
    python scripts/sentry_triage_snapshot.py --issue-id 123456 --json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUERY = "is:unresolved"
DEFAULT_SORT = "freq"
ISSUE_URL_RE = re.compile(r"/issues/(?P<issue_id>\d+)(?:/|$)")


def _load_local_env() -> None:
    candidates = [
        ROOT / ".env",
        ROOT / ".env.local",
        ROOT / ".env.sentry.local",
        Path.home() / ".codex" / f"sentry.{ROOT.name}.env",
        Path.home() / ".codex" / "sentry.env",
    ]
    for env_path in candidates:
        if env_path.exists():
            load_dotenv(env_path, override=False)


def _env_csv(name: str) -> list[str]:
    raw = os.getenv(name, "")
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return list(dict.fromkeys(values))


def _get_auth_token() -> str:
    return (os.getenv("SENTRY_ACCESS_TOKEN") or os.getenv("SENTRY_AUTH_TOKEN") or "").strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


class SentryApiError(RuntimeError):
    """Raised when the Sentry API cannot satisfy a request."""


class SentryClient:
    def __init__(self, base_url: str, auth_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {auth_token}",
                "Accept": "application/json",
                "User-Agent": "codex-sentry-triage/1.0",
            }
        )

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=(10, 30))
        if response.status_code >= 400:
            snippet = (response.text or response.reason or "").strip()
            if len(snippet) > 500:
                snippet = f"{snippet[:500]}..."
            raise SentryApiError(f"Sentry API {response.status_code} for {path}: {snippet}")
        return response.json()


def _extract_issue_id_from_url(issue_url: str | None) -> str | None:
    if not issue_url:
        return None
    match = ISSUE_URL_RE.search(issue_url)
    if not match:
        return None
    return match.group("issue_id")


def _collect_candidate_issues(
    client: SentryClient,
    org_slug: str,
    project_slugs: list[str],
    query: str,
    limit: int,
    sort: str,
) -> list[dict[str, Any]]:
    if not project_slugs:
        records = client.get_json(
            f"/api/0/organizations/{org_slug}/issues/",
            params={"query": query, "limit": limit, "sort": sort},
        )
        return [record for record in records if isinstance(record, dict)]

    deduped: dict[str, dict[str, Any]] = {}
    for project_slug in project_slugs:
        records = client.get_json(
            f"/api/0/projects/{org_slug}/{project_slug}/issues/",
            params={"query": query, "limit": limit, "sort": sort},
        )
        for record in records:
            if not isinstance(record, dict):
                continue
            record.setdefault("_project_slug", project_slug)
            issue_id = str(record.get("id") or "")
            if issue_id:
                deduped[issue_id] = record
    return list(deduped.values())


def _issue_sort_key(issue: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        _safe_int(issue.get("count")),
        _safe_int(issue.get("userCount")),
        str(issue.get("lastSeen") or ""),
        str(issue.get("id") or ""),
    )


def _extract_tags(event: dict[str, Any] | None) -> dict[str, str]:
    if not event:
        return {}

    allowed_keys = {"environment", "level", "os", "release", "runtime", "trace", "transaction"}
    tags: dict[str, str] = {}
    for tag in event.get("tags") or []:
        if not isinstance(tag, dict):
            continue
        key = str(_coalesce(tag.get("key"), tag.get("name")) or "").strip()
        value = str(tag.get("value") or "").strip()
        if key and value and key in allowed_keys:
            tags[key] = value
    return tags


def _extract_stack_frames(event: dict[str, Any] | None) -> list[str]:
    if not event:
        return []

    entries = event.get("entries") or []
    frames: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("type") != "exception":
            continue
        values = ((entry.get("data") or {}).get("values")) or []
        for exception in values:
            if not isinstance(exception, dict):
                continue
            stacktrace = exception.get("stacktrace") or {}
            current_frames = stacktrace.get("frames") or []
            if current_frames:
                frames.extend(frame for frame in current_frames if isinstance(frame, dict))

    if not frames:
        return []

    preferred = [frame for frame in frames if frame.get("in_app")]
    selected = preferred[-8:] if preferred else frames[-6:]
    rendered: list[str] = []
    for frame in selected:
        location = _coalesce(
            frame.get("absPath"),
            frame.get("filename"),
            frame.get("module"),
            "unknown",
        )
        function_name = _coalesce(frame.get("function"), "<unknown>")
        line_number = _coalesce(frame.get("lineno"), "?")
        rendered.append(f"{location}:{line_number} in {function_name}")
    return rendered


def _build_snapshot(
    issue: dict[str, Any],
    latest_event: dict[str, Any] | None,
    *,
    query: str,
) -> dict[str, Any]:
    project_info = issue.get("project") or {}
    issue_id = str(issue.get("id") or "")
    snapshot = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "query": query,
        "issue": {
            "id": issue_id,
            "short_id": _coalesce(issue.get("shortId"), issue.get("shareId"), issue_id),
            "title": _coalesce(issue.get("title"), issue.get("metadata", {}).get("title"), "(untitled issue)"),
            "status": _coalesce(issue.get("status"), "unknown"),
            "level": _coalesce(issue.get("level"), latest_event.get("level") if latest_event else None, "unknown"),
            "count": _safe_int(issue.get("count")),
            "user_count": _safe_int(issue.get("userCount")),
            "first_seen": issue.get("firstSeen"),
            "last_seen": issue.get("lastSeen"),
            "culprit": _coalesce(issue.get("culprit"), latest_event.get("culprit") if latest_event else None),
            "permalink": _coalesce(issue.get("permalink"), issue.get("webUrl")),
            "project_slug": _coalesce(project_info.get("slug"), issue.get("_project_slug")),
            "project_name": _coalesce(project_info.get("name"), project_info.get("slug")),
        },
        "latest_event": {
            "event_id": latest_event.get("eventID") if latest_event else None,
            "title": latest_event.get("title") if latest_event else None,
            "message": _coalesce(
                latest_event.get("message") if latest_event else None,
                latest_event.get("logentry", {}).get("formatted") if latest_event else None,
            ),
            "platform": latest_event.get("platform") if latest_event else None,
            "date_created": latest_event.get("dateCreated") if latest_event else None,
            "tags": _extract_tags(latest_event),
            "stack_frames": _extract_stack_frames(latest_event),
        },
    }
    return snapshot


def _render_markdown(snapshot: dict[str, Any]) -> str:
    issue = snapshot["issue"]
    event = snapshot["latest_event"]
    lines = [
        "# Sentry Triage Snapshot",
        "",
        f"- Query: `{snapshot['query']}`",
        f"- Issue: `{issue['short_id']}`",
        f"- Title: {issue['title']}",
        f"- Project: `{issue['project_slug'] or 'unknown'}`",
        f"- Level: `{issue['level']}`",
        f"- Status: `{issue['status']}`",
        f"- Count: `{issue['count']}`",
        f"- Users: `{issue['user_count']}`",
        f"- First seen: `{issue['first_seen'] or 'unknown'}`",
        f"- Last seen: `{issue['last_seen'] or 'unknown'}`",
        f"- Culprit: `{issue['culprit'] or 'unknown'}`",
    ]
    if issue["permalink"]:
        lines.append(f"- Link: {issue['permalink']}")

    lines.extend(
        [
            "",
            "## Latest Event",
            "",
            f"- Event ID: `{event['event_id'] or 'unknown'}`",
            f"- Occurred at: `{event['date_created'] or 'unknown'}`",
            f"- Platform: `{event['platform'] or 'unknown'}`",
        ]
    )
    if event["title"]:
        lines.append(f"- Event title: {event['title']}")
    if event["message"]:
        lines.append(f"- Message: {event['message']}")

    if event["tags"]:
        lines.extend(["", "## Selected Tags", ""])
        for key, value in sorted(event["tags"].items()):
            lines.append(f"- `{key}`: `{value}`")

    if event["stack_frames"]:
        lines.extend(["", "## Relevant Stack Frames", ""])
        for index, frame in enumerate(event["stack_frames"], start=1):
            lines.append(f"{index}. `{frame}`")

    return "\n".join(lines)


def _write_outputs(snapshot: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "top-issue.json").write_text(
        json.dumps(snapshot, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "top-issue.md").write_text(_render_markdown(snapshot), encoding="utf-8")


def _doctor_payload(base_url: str, org_slug: str | None, project_slugs: list[str], query: str, sort: str) -> dict[str, Any]:
    token = _get_auth_token()
    missing = []
    if not token:
        missing.append("SENTRY_ACCESS_TOKEN")
    if not org_slug:
        missing.append("SENTRY_ORG_SLUG")

    payload: dict[str, Any] = {
        "ok": False,
        "base_url": base_url,
        "org_slug": org_slug,
        "project_slugs": project_slugs,
        "query": query,
        "sort": sort,
        "access_token": "set" if token else "missing",
        "missing": missing,
    }
    if missing:
        return payload

    client = SentryClient(base_url, token)
    try:
        issues = _collect_candidate_issues(client, org_slug, project_slugs, query, limit=1, sort=sort)
    except Exception as exc:  # pragma: no cover - network and credential dependent
        payload["error"] = str(exc)
        return payload

    payload["ok"] = True
    payload["reachable"] = True
    payload["issue_sample_count"] = len(issues)
    if issues:
        top_issue = sorted(issues, key=_issue_sort_key, reverse=True)[0]
        payload["top_issue"] = {
            "id": top_issue.get("id"),
            "short_id": top_issue.get("shortId"),
            "title": top_issue.get("title"),
        }
    return payload


def main() -> int:
    _load_local_env()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue-id", help="Fetch a specific Sentry issue by numeric issue id.")
    parser.add_argument("--issue-url", help="Fetch a specific Sentry issue by Sentry issue URL.")
    parser.add_argument("--query", default=os.getenv("SENTRY_ISSUE_QUERY", DEFAULT_QUERY))
    parser.add_argument("--sort", default=os.getenv("SENTRY_ISSUE_SORT", DEFAULT_SORT))
    parser.add_argument("--limit", type=int, default=5, help="Per-project issue fetch limit.")
    parser.add_argument("--project", action="append", default=[], help="Override a project slug. Repeat to add more.")
    parser.add_argument("--markdown", action="store_true", help="Print Markdown output.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    parser.add_argument("--doctor", action="store_true", help="Validate local Sentry configuration and API access.")
    parser.add_argument(
        "--output-dir",
        help="Optional directory where top-issue.json and top-issue.md should be written.",
    )
    args = parser.parse_args()

    base_url = (os.getenv("SENTRY_BASE_URL") or "https://sentry.io").strip().rstrip("/")
    org_slug = (os.getenv("SENTRY_ORG_SLUG") or "").strip() or None
    configured_project_slugs = _env_csv("SENTRY_PROJECT_SLUGS")
    if configured_project_slugs:
        project_slugs = list(dict.fromkeys(args.project + configured_project_slugs))
    else:
        project_slugs = list(dict.fromkeys(args.project + _env_csv("SENTRY_PROJECT_SLUG")))

    if args.doctor:
        print(json.dumps(_doctor_payload(base_url, org_slug, project_slugs, args.query, args.sort), indent=2, ensure_ascii=False))
        return 0

    token = _get_auth_token()
    missing = [name for name, value in (("SENTRY_ACCESS_TOKEN", token), ("SENTRY_ORG_SLUG", org_slug)) if not value]
    if missing:
        print(
            json.dumps(
                {
                    "ok": False,
                    "missing": missing,
                    "hint": "Add the missing values to .env.sentry.local or your shell environment.",
                },
                indent=2,
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    client = SentryClient(base_url, token)
    issue_id = args.issue_id or _extract_issue_id_from_url(args.issue_url)

    try:
        if issue_id:
            issue = client.get_json(f"/api/0/issues/{issue_id}/")
        else:
            candidates = _collect_candidate_issues(
                client,
                org_slug,
                project_slugs,
                query=args.query,
                limit=max(1, args.limit),
                sort=args.sort,
            )
            if not candidates:
                raise SentryApiError("No issues matched the current Sentry query.")
            issue = sorted(candidates, key=_issue_sort_key, reverse=True)[0]
            issue_id = str(issue.get("id") or "")

        latest_event = None
        if issue_id:
            try:
                latest_event = client.get_json(f"/api/0/issues/{issue_id}/events/latest/")
            except SentryApiError:
                latest_event = None

        snapshot = _build_snapshot(issue, latest_event, query=args.query)
        if args.output_dir:
            _write_outputs(snapshot, Path(args.output_dir))

        if args.json:
            print(json.dumps(snapshot, indent=2, ensure_ascii=False))
            return 0

        print(_render_markdown(snapshot))
        return 0
    except Exception as exc:  # pragma: no cover - network and credential dependent
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
