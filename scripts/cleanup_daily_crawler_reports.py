from datetime import timedelta
import os
import sys

from app import app
from database import get_db, get_cursor
from utils.time import now_kst_naive


def _parse_keep_days(value):
    if value is None:
        return 14
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 14
    return max(1, min(parsed, 365))


def main():
    keep_days_input = os.getenv('KEEP_DAYS')
    if len(sys.argv) > 1:
        keep_days_input = sys.argv[1]
    keep_days = _parse_keep_days(keep_days_input)
    cutoff = now_kst_naive() - timedelta(days=keep_days)

    with app.app_context():
        conn = get_db()
        cursor = get_cursor(conn)
        try:
            cursor.execute(
                "DELETE FROM daily_crawler_reports WHERE created_at < %s",
                (cutoff,),
            )
            deleted_count = cursor.rowcount
            conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    print(
        "LOG: deleted {} rows from daily_crawler_reports (cutoff={}, keep_days={})".format(
            deleted_count,
            cutoff.isoformat(),
            keep_days,
        )
    )


if __name__ == '__main__':
    sys.exit(main())
