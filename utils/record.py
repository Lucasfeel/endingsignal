"""Utility helpers for safe record access."""


def read_field(obj, key, default=None):
    """Safely read a field from mapping- or index-like objects.

    Args:
        obj: Mapping-like object, sequence, or any object supporting ``get`` or
            ``__getitem__``.
        key: Key/index to read.
        default: Value to return when the key is not present or the object is
            ``None``.
    """
    if obj is None:
        return default
    if hasattr(obj, "get"):
        return obj.get(key, default)
    try:
        return obj[key]
    except Exception:
        return default
