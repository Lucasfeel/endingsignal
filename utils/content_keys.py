from typing import Optional, Tuple
from urllib.parse import quote, unquote


def build_content_key(content_id: str, source: str) -> str:
    raw = f"{source}:{content_id}"
    return quote(raw, safe="")


def parse_content_key(content_key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not content_key:
        return None, None

    raw = unquote(str(content_key)).strip()
    if ":" not in raw:
        return None, None

    source, content_id = raw.split(":", 1)
    source = source.strip()
    content_id = content_id.strip()
    if not source or not content_id:
        return None, None

    return content_id, source
