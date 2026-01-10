from database import get_cursor
from services.cdc_event_service import record_content_completed_event
from services.final_state_resolver import resolve_final_state
from utils.time import now_kst_naive

# Optional: UI-friendly final state payload builder (may not exist on some branches)
try:
    from services.final_state_payload import build_final_state_payload
except Exception:
    build_final_state_payload = None


_DEF_NOT_FOUND = {"error": "CONTENT_NOT_FOUND"}


def _serialize_override_row(row):
    if not row:
        return None
    return {
        "id": row["id"],
        "content_id": row["content_id"],
        "source": row["source"],
        "override_status": row["override_status"],
        "override_completed_at": row["override_completed_at"],
        "reason": row["reason"],
        "admin_id": row["admin_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def upsert_override_and_record_event(
    conn,
    *,
    admin_id,
    content_id,
    source,
    override_status,
    override_completed_at,
    reason,
    now=None,
):
    cursor = get_cursor(conn)
    effective_now = now if now is not None else now_kst_naive()

    # Verify content exists
    cursor.execute(
        "SELECT status FROM contents WHERE content_id = %s AND source = %s",
        (content_id, source),
    )
    content_row = cursor.fetchone()
    if content_row is None:
        cursor.close()
        return _DEF_NOT_FOUND

    # Load existing override for transition comparison
    cursor.execute(
        """
        SELECT override_status, override_completed_at
        FROM admin_content_overrides
        WHERE content_id = %s AND source = %s
        """,
        (content_id, source),
    )
    existing_override = cursor.fetchone()

    previous_final_state = resolve_final_state(
        content_row["status"], existing_override, now=effective_now
    )

    # UPSERT override
    cursor.execute(
        """
        INSERT INTO admin_content_overrides (
            content_id,
            source,
            override_status,
            override_completed_at,
            reason,
            admin_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (content_id, source) DO UPDATE SET
            override_status = EXCLUDED.override_status,
            override_completed_at = EXCLUDED.override_completed_at,
            reason = EXCLUDED.reason,
            admin_id = EXCLUDED.admin_id,
            updated_at = NOW()
        RETURNING id, content_id, source, override_status, override_completed_at, reason, admin_id, created_at, updated_at
        """,
        (content_id, source, override_status, override_completed_at, reason, admin_id),
    )
    override_row = cursor.fetchone()

    # Compute new final state (scheduled completion rules apply here)
    new_override = {
        "override_status": override_status,
        "override_completed_at": override_completed_at,
    }
    new_final_state = resolve_final_state(
        content_row["status"], new_override, now=effective_now
    )

    # Optional UI-friendly final payload
    final_state_payload = None
    if build_final_state_payload is not None:
        final_state_payload = build_final_state_payload(
            content_row["status"], new_override, now=effective_now
        )

    # IMPORTANT: if override_completed_at is future, new_final_state won't be completed, so no event is recorded.
    event_recorded = False
    if previous_final_state.get("final_status") != "완결" and new_final_state.get("final_status") == "완결":
        event_recorded = record_content_completed_event(
            conn,
            content_id=content_id,
            source=source,
            final_completed_at=new_final_state.get("final_completed_at"),
            resolved_by="override",
        )

    cursor.close()

    result = {
        "override": _serialize_override_row(override_row),
        "previous_final_state": previous_final_state,
        "new_final_state": new_final_state,
        "event_recorded": event_recorded,
    }

    # Add UI payload only if builder exists
    if final_state_payload is not None:
        result["final_state"] = final_state_payload

    return result
