import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from database import create_standalone_connection
from run_all_crawlers import run_cli
from services.db_maintenance_service import run_db_maintenance

OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "db-maintenance-runs"


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run() -> int:
    conn = create_standalone_connection()
    try:
        summary = run_db_maintenance(conn)
    finally:
        conn.close()

    run_stamp = str(summary.get("checked_at") or "unknown").replace(":", "").replace("-", "")
    artifact_dir = OUTPUT_DIR / run_stamp
    _write_json(artifact_dir / "maintenance-summary.json", summary)
    print(
        "db maintenance complete: "
        f"reports_deleted={summary['daily_crawler_reports']['deleted_count']} "
        f"notification_logs_deleted={summary['notification_log']['deleted_count']} "
        f"cdc_events_deleted={summary['cdc']['events_deleted']}",
        flush=True,
    )
    return 0


async def main() -> int:
    return _run()


if __name__ == "__main__":
    sys.exit(run_cli(main, "db-maintenance"))
