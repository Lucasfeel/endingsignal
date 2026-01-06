import datetime
from typing import Any, Dict, List, Optional


def now_iso() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


def redact_headers(headers: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    headers = headers or {}
    redacted = {
        "has_cookie_header": "Cookie" in headers,
        "has_authorization_header": any(
            key.lower() == "authorization" for key in headers.keys()
        ),
        "header_keys": sorted(headers.keys()),
    }
    if redacted["has_cookie_header"]:
        redacted["header_keys"] = [k for k in redacted["header_keys"] if k.lower() != "cookie"]
    return redacted


def redact_cookies(cookie_header: Optional[str]) -> Dict[str, Any]:
    result: Dict[str, Any] = {"has_cookie_header": bool(cookie_header)}
    if not cookie_header:
        result["cookie_count"] = 0
        result["cookie_names"] = []
        return result

    cookie_names: List[str] = []
    for segment in cookie_header.split(";"):
        name = segment.split("=", 1)[0].strip()
        if name:
            cookie_names.append(name)
    result["cookie_count"] = len(cookie_names)
    result["cookie_names"] = cookie_names[:10]
    return result


def append_error(fetch_meta: Dict[str, Any], code: str, message: str, context: Optional[Dict[str, Any]] = None) -> None:
    errors = fetch_meta.setdefault("errors", [])
    entry = {
        "ts": now_iso(),
        "code": code,
        "message": message,
    }
    if context:
        entry["context"] = context
    errors.append(entry)


def add_request_sample(fetch_meta: Dict[str, Any], sample: Dict[str, Any], max_samples: int = 5) -> None:
    samples = fetch_meta.setdefault("request_samples", [])
    samples.append(sample)
    if len(samples) > max_samples:
        fetch_meta["request_samples"] = samples[-max_samples:]
