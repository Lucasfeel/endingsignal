"""Text normalization utilities for search and indexing."""

import re
import unicodedata

_WS_RE = re.compile(r"\s+", re.UNICODE)


def normalize_search_text(value):
    """Normalize text for whitespace-insensitive search comparisons."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = _WS_RE.sub("", text)
    return text.lower()
