"""Final state resolver for content completion detection.

This module consolidates crawler-provided statuses with optional admin
overrides to determine the authoritative ("final") state of a piece of
content. It is intentionally side-effect free so it can be reused in
CDC snapshot and current-state calculations.
"""

from datetime import datetime


def resolve_final_state(content_status, override=None, now=None):
    """Resolve the final status for a content item.

    Args:
        content_status: Status provided by the crawler or existing DB value.
        override: Optional override row/dict containing ``override_status`` and
            ``override_completed_at``.
        now: Optional naive datetime for deterministic comparisons. Defaults to
            ``datetime.now()``.

    Returns:
        dict: ``{"final_status", "final_completed_at", "resolved_by"}``
    """
    effective_now = now if now is not None else datetime.now()

    if not override:
        return {
            'final_status': content_status,
            'final_completed_at': None,
            'resolved_by': 'crawler',
        }

    override_status = override.get('override_status')
    override_completed_at = override.get('override_completed_at')

    if override_status != '완결':
        return {
            'final_status': override_status,
            'final_completed_at': None,
            'resolved_by': 'override',
        }

    if override_completed_at is None:
        return {
            'final_status': '완결',
            'final_completed_at': None,
            'resolved_by': 'override',
        }

    if effective_now < override_completed_at:
        return {
            'final_status': content_status,
            'final_completed_at': None,
            'resolved_by': 'crawler',
        }

    return {
        'final_status': '완결',
        'final_completed_at': override_completed_at,
        'resolved_by': 'override',
    }
