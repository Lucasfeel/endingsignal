from utils.record import read_field
from utils.time import now_kst_naive
from services.final_state_resolver import resolve_final_state


def _iso(dt):
    if dt is None:
        return None
    return dt.isoformat() if hasattr(dt, "isoformat") else str(dt)


def build_final_state_payload(contents_status, override_record=None, now=None):
    """
    Build a UI-friendly payload that contains:
    - raw_status (contents.status)
    - final_status/final_completed_at/resolved_by (resolved final state)
    - scheduled completion helpers for UI display

    Scheduled completion definition (v3.5):
    - override exists
    - override_status == "완결"
    - override_completed_at is not None
    - now < override_completed_at  => scheduled/pending
    """
    effective_now = now if now is not None else now_kst_naive()

    override_status = None
    override_completed_at = None
    if override_record:
        override_status = read_field(override_record, "override_status")
        override_completed_at = read_field(override_record, "override_completed_at")

    final_state = resolve_final_state(contents_status, override_record, now=effective_now)

    is_scheduled = (
        override_record is not None
        and override_status == "완결"
        and override_completed_at is not None
        and effective_now < override_completed_at
    )

    payload = {
        # Raw (crawler/DB) status
        "raw_status": contents_status,
        # Final state (resolver output)
        "final_status": final_state.get("final_status"),
        "final_completed_at": _iso(final_state.get("final_completed_at")),
        "resolved_by": final_state.get("resolved_by"),
        # Override info (optional but useful for admin UI)
        "override_status": override_status,
        "override_completed_at": _iso(override_completed_at),
        # Scheduled completion helpers
        "is_scheduled_completion": bool(is_scheduled),
        "scheduled_completed_at": _iso(override_completed_at) if is_scheduled else None,
    }

    return payload
