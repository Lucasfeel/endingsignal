"""Final state resolver for content completion detection.

This module consolidates crawler-provided statuses with optional admin
overrides to determine the authoritative ("final") state of a piece of
content. It is intentionally side-effect free so it can be reused in
CDC snapshot and current-state calculations.
"""

def resolve_final_state(content_status, override=None):
    """Resolve the final status for a content item.

    Args:
        content_status: Status provided by the crawler or existing DB value.
        override: Optional override row/dict containing ``override_status`` and
            ``override_completed_at``.

    Returns:
        dict: ``{"final_status", "final_completed_at", "resolved_by"}``
    """
    if override:
        return {
            'final_status': override.get('override_status'),
            'final_completed_at': override.get('override_completed_at'),
            'resolved_by': 'override',
        }

    return {
        'final_status': content_status,
        'final_completed_at': None,
        'resolved_by': 'crawler',
    }
