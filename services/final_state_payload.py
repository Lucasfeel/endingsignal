from services.final_state_resolver import resolve_final_state
from utils.record import read_field
from utils.time import now_kst_naive


def _serialize_datetime(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value if value is None else value


def build_final_state_payload(content_status, override=None, now=None):
    """Build a payload describing raw and resolved content state.

    Args:
        content_status (str): Raw status from contents.status.
        override: Mapping/row containing ``override_status`` and
            ``override_completed_at`` (optional).
        now (datetime, optional): Naive datetime used for scheduled comparisons.
            Defaults to ``now_kst_naive()``.

    Returns:
        dict: Payload containing raw/final fields and scheduled completion helpers.
    """

    effective_now = now if now is not None else now_kst_naive()
    override_status = read_field(override, "override_status")
    override_completed_at = read_field(override, "override_completed_at")

    final_state = resolve_final_state(content_status, override, now=effective_now)
    final_completed_at = _serialize_datetime(final_state.get("final_completed_at"))

    is_scheduled_completion = (
        override is not None
        and override_status == "완결"
        and override_completed_at is not None
        and effective_now < override_completed_at
    )

    scheduled_completed_at = None
    if is_scheduled_completion:
        scheduled_completed_at = _serialize_datetime(override_completed_at)

    return {
        "raw_status": content_status,
        "final_status": final_state.get("final_status"),
        "final_completed_at": final_completed_at,
        "resolved_by": final_state.get("resolved_by"),
        "is_scheduled_completion": is_scheduled_completion,
        "scheduled_completed_at": scheduled_completed_at,
    }
