"""Optional Sentry initialization for Flask services."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlparse

try:
    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
except ImportError:  # pragma: no cover - keeps local dev working without the extra package
    sentry_sdk = None
    FlaskIntegration = None
    LoggingIntegration = None


_TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "on"}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in _TRUTHY_VALUES


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_value(*names: str) -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return ""


def _default_environment() -> str:
    return _env_value("SENTRY_ENVIRONMENT", "FLASK_ENV") or "development"


def _default_trace_rate() -> float:
    environment = _default_environment().lower()
    return 1.0 if environment in {"development", "dev", "local", "test", "testing"} else 0.2


def _default_profile_rate() -> float:
    environment = _default_environment().lower()
    return 1.0 if environment in {"development", "dev", "local", "test", "testing"} else 0.1


def _public_key_from_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    return parsed.username or ""


def _before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    tags = event.setdefault("tags", {})
    tags.setdefault("service", os.getenv("SENTRY_SERVICE_NAME", "unknown"))
    return event


def init_sentry(service_name: str, *dsn_env_names: str) -> bool:
    dsn = _env_value(*(dsn_env_names or ("SENTRY_API_DSN", "SENTRY_DSN")))
    if not dsn or sentry_sdk is None:
        return False

    os.environ.setdefault("SENTRY_SERVICE_NAME", service_name)
    sentry_sdk.init(
        dsn=dsn,
        integrations=[
            FlaskIntegration(),
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
        environment=_default_environment(),
        release=_env_value("SENTRY_API_RELEASE", "SENTRY_RELEASE") or None,
        traces_sample_rate=_env_float("SENTRY_TRACES_SAMPLE_RATE", _default_trace_rate()),
        profile_session_sample_rate=_env_float(
            "SENTRY_PROFILE_SESSION_SAMPLE_RATE",
            _env_float("SENTRY_PROFILES_SAMPLE_RATE", _default_profile_rate()),
        ),
        profile_lifecycle=_env_value("SENTRY_PROFILE_LIFECYCLE") or "trace",
        send_default_pii=_env_flag("SENTRY_SEND_DEFAULT_PII", default=False),
        debug=_env_flag("SENTRY_DEBUG", default=False),
        before_send=_before_send,
    )
    sentry_sdk.set_tag("service", service_name)
    return True


def frontend_template_context(repo_name: str) -> dict[str, Any]:
    dsn = _env_value("SENTRY_FRONTEND_DSN", "SENTRY_WEB_DSN")
    public_key = _public_key_from_dsn(dsn)
    environment = _env_value("SENTRY_FRONTEND_ENVIRONMENT", "SENTRY_ENVIRONMENT", "FLASK_ENV") or "development"
    environment_key = environment.lower()

    return {
        "sentry_frontend_enabled": bool(public_key),
        "sentry_frontend_public_key": public_key,
        "sentry_frontend_environment": environment,
        "sentry_frontend_release": _env_value("SENTRY_FRONTEND_RELEASE", "SENTRY_RELEASE"),
        "sentry_frontend_traces_sample_rate": _env_float(
            "SENTRY_FRONTEND_TRACES_SAMPLE_RATE",
            1.0 if environment_key in {"development", "dev", "local", "test", "testing"} else 0.2,
        ),
        "sentry_frontend_replays_session_sample_rate": _env_float(
            "SENTRY_FRONTEND_REPLAYS_SESSION_SAMPLE_RATE",
            0.1 if environment_key in {"development", "dev", "local", "test", "testing"} else 0.05,
        ),
        "sentry_frontend_replays_on_error_sample_rate": _env_float(
            "SENTRY_FRONTEND_REPLAYS_ON_ERROR_SAMPLE_RATE",
            1.0,
        ),
        "sentry_frontend_repo": repo_name,
    }
