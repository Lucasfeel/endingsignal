"""
One-off backfill for populating meta.common.content_url for existing contents.

Runbook examples:
- python scripts/backfill_content_urls.py --dry-run
- python scripts/backfill_content_urls.py --batch-size 200 --limit 1000
"""

import argparse
import json
import sys
import urllib.parse

from dotenv import load_dotenv

from database import create_standalone_connection, get_cursor
from utils.record import read_field


def get_field(row, key):
    return read_field(row, key)


def infer_url(source, content_id, title=None):
    if not source:
        return None

    normalized = source.lower()
    if normalized in {"naver_webtoon", "naver webtoon", "naver"}:
        return f"https://m.comic.naver.com/webtoon/list?titleId={content_id}"

    if normalized in {"kakaowebtoon", "kakao_webtoon", "kakao webtoon", "kakao"}:
        if not title:
            return None
        encoded = urllib.parse.quote(title, safe="")
        return f"https://webtoon.kakao.com/content/{encoded}/{content_id}"

    return None


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill missing content URLs into contents.meta.common.content_url")
    parser.add_argument("--dry-run", action="store_true", help="Run without performing updates")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows to inspect")
    parser.add_argument("--batch-size", type=int, default=200, help="Number of updates per commit")
    return parser.parse_args()


def main():
    load_dotenv()
    args = parse_args()

    conn = None
    updated = 0
    skipped = 0

    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)

        base_query = (
            "SELECT content_id, source, title, meta "
            "FROM contents "
            "WHERE (meta IS NULL) "
            "OR (meta->'common' IS NULL) "
            "OR (NULLIF(meta->'common'->>'content_url', '') IS NULL) "
            "ORDER BY content_id, source"
        )
        params = []
        if args.limit:
            base_query += " LIMIT %s"
            params.append(args.limit)

        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        print(f"[INFO] Found {len(rows)} candidate rows for backfill.")

        updates = []

        def flush_updates():
            nonlocal updates, updated
            if args.dry_run or not updates:
                updates = []
                return
            cursor.executemany(
                "UPDATE contents SET meta=%s WHERE content_id=%s AND source=%s",
                updates,
            )
            conn.commit()
            updated += cursor.rowcount
            updates = []

        for row in rows:
            meta = get_field(row, "meta")
            if meta is None:
                meta = {}
            elif not isinstance(meta, dict):
                try:
                    meta = dict(meta)
                except Exception:
                    meta = {}

            common = meta.get("common") or {}
            existing = common.get("content_url")
            if existing and str(existing).strip():
                skipped += 1
                continue

            content_id = get_field(row, "content_id")
            source = get_field(row, "source")
            title = get_field(row, "title")

            url = infer_url(source, content_id, title)
            if not url:
                skipped += 1
                continue

            common["content_url"] = url
            meta["common"] = common

            updates.append((json.dumps(meta), content_id, source))

            if len(updates) >= args.batch_size:
                flush_updates()

        flush_updates()

        print(f"[INFO] Backfill completed. to_update={len(rows) - skipped}, skipped={skipped}, updated={updated}.")
        if args.dry_run:
            print("[INFO] Dry run enabled; no database updates were committed.")

    except Exception as exc:
        if conn:
            conn.rollback()
        print(f"[ERROR] Backfill failed: {exc}", file=sys.stderr)
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
