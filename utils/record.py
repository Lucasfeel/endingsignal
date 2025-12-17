"""Utility helpers for working with mapping-like records."""


def read_field(obj, key, default=None):
    """Safely read a field from dict-like or row-like objects.

    Args:
        obj: Mapping/row-like object or None.
        key: Key to retrieve.
        default: Default value when key/object is unavailable.
    """
    if obj is None:
        return default
    if hasattr(obj, "get"):
        return obj.get(key, default)
    try:
        return obj[key]
    except Exception:
        return default
