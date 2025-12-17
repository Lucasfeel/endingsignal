from database import get_cursor
from services.cdc_constants import RESOLVED_BY_OVERRIDE, STATUS_COMPLETED
from services.cdc_event_service import record_content_completed_event
from services.final_state_resolver import resolve_final_state


def upsert_override_and_record_event(
    conn,
    *,
    admin_id,
    content_id,
    source,
    override_status,
    override_completed_at,
    reason,
):
    """
    Upsert an admin content override and record CDC event if completion is newly reached.

    Returns a dict containing override row, final state snapshots, and event status.
    """
    cursor = get_cursor(conn)

    cursor.execute(
        "SELECT status FROM contents WHERE content_id = %s AND source = %s",
        (content_id, source),
    )
    content_row = cursor.fetchone()
    if content_row is None:
        cursor.close()
        return {
            'success': False,
            'error': 'CONTENT_NOT_FOUND',
        }

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
        content_row['status'], existing_override
    )

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

    new_override = {
        'override_status': override_status,
        'override_completed_at': override_completed_at,
    }
    new_final_state = resolve_final_state(content_row['status'], new_override)

    event_recorded = False
    if previous_final_state.get('final_status') != STATUS_COMPLETED and new_final_state.get('final_status') == STATUS_COMPLETED:
        event_recorded = record_content_completed_event(
            conn,
            content_id=content_id,
            source=source,
            final_completed_at=new_final_state.get('final_completed_at'),
            resolved_by=RESOLVED_BY_OVERRIDE,
        )

    conn.commit()
    cursor.close()

    return {
        'success': True,
        'override': override_row,
        'previous_final_state': previous_final_state,
        'new_final_state': new_final_state,
        'event_recorded': event_recorded,
    }
