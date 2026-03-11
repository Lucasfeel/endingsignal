import os
from typing import Any, Dict, Optional

import requests


DEFAULT_TIMEOUT_SECONDS = float(os.getenv("AIT_API_TIMEOUT_SECONDS", "10"))


class AppsInTossApiError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int = 502, payload: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.payload = payload


class AppsInTossApiDisabledError(AppsInTossApiError):
    def __init__(self, message: str):
        super().__init__("AIT_API_DISABLED", message, status_code=503)


def _base_url() -> str:
    return (os.getenv("AIT_API_BASE_URL") or "https://apps-in-toss-api.toss.im").rstrip("/")


def _build_cert():
    cert_path = (os.getenv("AIT_MTLS_CERT_PATH") or "").strip()
    key_path = (os.getenv("AIT_MTLS_KEY_PATH") or "").strip()
    if cert_path and key_path:
        return cert_path, key_path
    raise AppsInTossApiDisabledError(
        "Apps-in-Toss mTLS certificate is not configured. "
        "Set AIT_MTLS_CERT_PATH and AIT_MTLS_KEY_PATH."
    )


def _build_verify():
    ca_bundle_path = (os.getenv("AIT_CA_BUNDLE_PATH") or "").strip()
    if ca_bundle_path:
        return ca_bundle_path
    return True


def request_json(
    method: str,
    path: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    url = f"{_base_url()}{path}"
    merged_headers = {"Content-Type": "application/json"}
    if headers:
        merged_headers.update(headers)

    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=merged_headers,
            json=json_body,
            cert=_build_cert(),
            verify=_build_verify(),
            timeout=timeout or DEFAULT_TIMEOUT_SECONDS,
        )
    except AppsInTossApiError:
        raise
    except requests.RequestException as exc:
        raise AppsInTossApiError(
            "AIT_API_REQUEST_FAILED",
            "Failed to communicate with Apps-in-Toss API.",
            payload={"error": str(exc), "url": url},
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise AppsInTossApiError(
            "AIT_API_INVALID_RESPONSE",
            "Apps-in-Toss API returned a non-JSON response.",
            status_code=response.status_code,
            payload={"text": response.text[:500]},
        ) from exc

    if response.ok:
        return payload

    raise AppsInTossApiError(
        "AIT_API_HTTP_ERROR",
        "Apps-in-Toss API request failed.",
        status_code=response.status_code,
        payload=payload,
    )
