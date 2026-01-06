import os
import sys
from typing import Dict, Tuple

from database import create_standalone_connection, get_cursor


SOURCE = "kakaowebtoon"


def _fetch_counts(cursor) -> Dict[str, int]:
    cursor.execute("SELECT source, COUNT(*) AS cnt FROM contents GROUP BY source")
    contents_counts = {row["source"]: int(row["cnt"]) for row in cursor.fetchall()}

    cursor.execute("SELECT source, COUNT(*) AS cnt FROM subscriptions GROUP BY source")
    subscription_counts = {row["source"]: int(row["cnt"]) for row in cursor.fetchall()}

    return {
        "contents_total": contents_counts,
        "subscriptions_total": subscription_counts,
    }


def _purge(conn) -> Tuple[int, int]:
    cursor = get_cursor(conn)
    try:
        cursor.execute("DELETE FROM subscriptions WHERE source=%s", (SOURCE,))
        deleted_subscriptions = cursor.rowcount or 0
        cursor.execute("DELETE FROM contents WHERE source=%s", (SOURCE,))
        deleted_contents = cursor.rowcount or 0
        conn.commit()
        return deleted_contents, deleted_subscriptions
    finally:
        cursor.close()


def main():
    if os.getenv("KAKAO_LEGACY_PURGE", "") != "YES":
        sys.stderr.write("KAKAO_LEGACY_PURGE=YES is required to run this cleanup.\n")
        sys.exit(1)

    conn = create_standalone_connection()
    try:
        cursor = get_cursor(conn)
        counts_before = _fetch_counts(cursor)
        print("Counts before purge:", counts_before)
        cursor.close()

        deleted_contents, deleted_subscriptions = _purge(conn)
        print(
            f"Deleted contents rows: {deleted_contents}, subscriptions rows: {deleted_subscriptions}"
        )

        cursor = get_cursor(conn)
        counts_after = _fetch_counts(cursor)
        print("Counts after purge:", counts_after)
        cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
