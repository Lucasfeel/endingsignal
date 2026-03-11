from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import error, request

import psycopg2.extras
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from database import create_standalone_connection

SUMMARY_SQL = """
SELECT
    source,
    content_type,
    COUNT(*)::int AS total_count,
    COUNT(*) FILTER (WHERE COALESCE(is_deleted, FALSE) = FALSE)::int AS active_count,
    COUNT(*) FILTER (WHERE COALESCE(is_deleted, FALSE) = TRUE)::int AS deleted_count
FROM contents
GROUP BY source, content_type
ORDER BY source, content_type
"""

SELECT_COLUMNS_SQL = """
SELECT
    content_id,
    source,
    content_type,
    title,
    normalized_title,
    normalized_authors,
    status,
    meta,
    is_deleted,
    deleted_at,
    deleted_reason,
    deleted_by,
    created_at,
    updated_at,
    search_document
FROM contents
"""


def _serialize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _serialize_value(raw_value) for key, raw_value in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _parse_csv_values(raw_value: str) -> List[str]:
    return [item.strip() for item in str(raw_value or "").split(",") if item.strip()]


def _build_filters(*, sources: Sequence[str], content_types: Sequence[str]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if sources:
        clauses.append("source = ANY(%s)")
        params.append(list(sources))
    if content_types:
        clauses.append("content_type = ANY(%s)")
        params.append(list(content_types))

    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def _load_local_summary(conn, *, sources: Sequence[str], content_types: Sequence[str]) -> Dict[str, Any]:
    where_sql, params = _build_filters(sources=sources, content_types=content_types)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute(SUMMARY_SQL.replace("FROM contents", f"FROM contents{where_sql}"), params)
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()

    return {
        "rows": rows,
        "total_count": sum(int(row.get("total_count") or 0) for row in rows),
        "active_count": sum(int(row.get("active_count") or 0) for row in rows),
        "deleted_count": sum(int(row.get("deleted_count") or 0) for row in rows),
    }


def _count_local_rows(conn, *, sources: Sequence[str], content_types: Sequence[str]) -> int:
    where_sql, params = _build_filters(sources=sources, content_types=content_types)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM contents{where_sql}", params)
        row = cursor.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        cursor.close()


def _iter_local_rows(
    conn,
    *,
    batch_size: int,
    sources: Sequence[str],
    content_types: Sequence[str],
    limit: int,
):
    where_sql, params = _build_filters(sources=sources, content_types=content_types)
    named_cursor = conn.cursor(
        name=f"content_sync_{int(datetime.now().timestamp())}",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    sql = f"{SELECT_COLUMNS_SQL}{where_sql} ORDER BY source, content_type, content_id"
    if limit > 0:
        sql += " LIMIT %s"
        params = [*params, limit]
    named_cursor.execute(sql, params)
    try:
        while True:
            rows = named_cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [_serialize_value(dict(row)) for row in rows]
    finally:
        named_cursor.close()


def _request_json(
    *,
    method: str,
    url: str,
    token: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 120,
) -> Dict[str, Any]:
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            return json.loads(body or "{}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} calling {url}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"request to {url} failed: {exc}") from exc


def _build_output_path() -> Path:
    output_dir = ROOT_DIR / "output" / "content-sync"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return output_dir / f"content-sync-{timestamp}.json"


def main() -> int:
    load_dotenv(ROOT_DIR / ".env")

    parser = argparse.ArgumentParser(description="Upsert local contents rows into the remote app database.")
    parser.add_argument("--remote-base-url", default=os.getenv("VERIFIED_SYNC_API_BASE_URL", "").strip())
    parser.add_argument("--token", default=os.getenv("VERIFIED_SYNC_INTERNAL_TOKEN", "").strip())
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--sources", default="")
    parser.add_argument("--content-types", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    if not args.remote_base_url:
        print("ERROR: --remote-base-url or VERIFIED_SYNC_API_BASE_URL is required", file=sys.stderr)
        return 1
    if not args.token:
        print("ERROR: --token or VERIFIED_SYNC_INTERNAL_TOKEN is required", file=sys.stderr)
        return 1

    batch_size = max(1, min(int(args.batch_size), 1000))
    remote_base_url = args.remote_base_url.rstrip("/")
    sources = _parse_csv_values(args.sources)
    content_types = _parse_csv_values(args.content_types)

    conn = create_standalone_connection()
    conn.autocommit = False

    try:
        local_summary = _load_local_summary(conn, sources=sources, content_types=content_types)
        total_rows = _count_local_rows(conn, sources=sources, content_types=content_types)
        if args.limit and args.limit > 0:
            total_rows = min(total_rows, int(args.limit))

        remote_before = _request_json(
            method="GET",
            url=f"{remote_base_url}/api/internal/content-sync/summary",
            token=args.token,
        )

        inserted_count = 0
        updated_count = 0
        sent_count = 0
        batch_count = 0

        for batch in _iter_local_rows(
            conn,
            batch_size=batch_size,
            sources=sources,
            content_types=content_types,
            limit=max(0, int(args.limit or 0)),
        ):
            response = _request_json(
                method="POST",
                url=f"{remote_base_url}/api/internal/content-sync/upsert-batch",
                token=args.token,
                payload={"rows": batch},
            )
            result = response.get("result") or {}
            batch_count += 1
            sent_count += int(result.get("received_count") or 0)
            inserted_count += int(result.get("inserted_count") or 0)
            updated_count += int(result.get("updated_count") or 0)
            print(
                f"batch={batch_count} sent={sent_count}/{total_rows} "
                f"inserted={inserted_count} updated={updated_count}"
            )

        remote_after = _request_json(
            method="GET",
            url=f"{remote_base_url}/api/internal/content-sync/summary",
            token=args.token,
        )

        report = {
            "remote_base_url": remote_base_url,
            "filters": {
                "sources": sources,
                "content_types": content_types,
                "limit": int(args.limit or 0),
                "batch_size": batch_size,
            },
            "local_summary": local_summary,
            "remote_summary_before": remote_before.get("summary"),
            "remote_summary_after": remote_after.get("summary"),
            "sync_result": {
                "total_rows_targeted": total_rows,
                "batches": batch_count,
                "sent_count": sent_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
            },
            "completed_at": datetime.now().isoformat(),
        }

        output_path = Path(args.output) if args.output else _build_output_path()
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"report={output_path}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
