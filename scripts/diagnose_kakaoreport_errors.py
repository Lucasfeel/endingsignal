"""Quick diagnostic to print the latest Kakao crawler errors from daily_crawler_reports."""

import json

from database import create_standalone_connection, get_cursor


def main(limit: int = 5):
    conn = create_standalone_connection()
    cursor = get_cursor(conn)
    cursor.execute(
        """
        SELECT id, created_at, report_data
        FROM daily_crawler_reports
        WHERE lower(crawler_name) LIKE 'kakaowebtoon%'
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )

    for row in cursor.fetchall():
        report = row.get("report_data") or {}
        fetch_meta = (report.get("cdc_info") or {}).get("fetch_meta") or {}
        errors = fetch_meta.get("errors") or []
        request_samples = fetch_meta.get("request_samples") or []
        status = report.get("status") or fetch_meta.get("status")
        summary = report.get("summary") or fetch_meta.get("summary")
        print(f"ID={row.get('id')} created_at={row.get('created_at')} status={status}")
        print(f"  summary={summary}")
        print(f"  errors={json.dumps(errors)[:400]}")
        print(f"  request_samples={json.dumps(request_samples)[:400]}")
        print("-")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
