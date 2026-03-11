import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from database import create_standalone_connection
from run_all_crawlers import (
    normalize_runtime_status,
    run_cli,
    run_completion_notification_dispatch,
    run_scheduled_completion_cdc,
    run_scheduled_publication_cdc,
)
from services.verified_sync_service import (
    CLOUD_DISPATCH_PIPELINE,
    build_run_context,
    get_next_attempt_no,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "cloud-dispatch-runs"


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_cloud_dispatch() -> int:
    conn = create_standalone_connection()
    try:
        attempt_no = get_next_attempt_no(conn, pipeline=CLOUD_DISPATCH_PIPELINE)
    finally:
        conn.close()

    run_context = build_run_context(
        pipeline=CLOUD_DISPATCH_PIPELINE,
        enabled_sources=[
            "scheduled_completion_cdc",
            "scheduled_publication_cdc",
            "completion_notification_dispatch",
        ],
        attempt_no=attempt_no,
    )

    print(
        f"cloud dispatch start: run_id={run_context['run_id']} attempt_no={run_context['attempt_no']}",
        flush=True,
    )

    action_reports = [
        run_scheduled_completion_cdc(report_context=run_context),
        run_scheduled_publication_cdc(report_context=run_context),
        run_completion_notification_dispatch(report_context=run_context),
    ]

    final_status = "ok"
    for report in action_reports:
        status = normalize_runtime_status((report or {}).get("status"))
        if status == "error":
            final_status = "error"
            break
        if status == "warn":
            final_status = "warn"

    artifact_dir = OUTPUT_DIR / str(run_context["run_id"]).replace(":", "_")
    _write_json(
        artifact_dir / "dispatch-summary.json",
        {
            "run_context": run_context,
            "final_status": final_status,
            "action_reports": action_reports,
        },
    )
    return 1 if final_status == "error" else 0


async def main() -> int:
    return _run_cloud_dispatch()


if __name__ == "__main__":
    sys.exit(run_cli(main, "cloud-dispatch"))
