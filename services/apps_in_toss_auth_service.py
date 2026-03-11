import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg2

from database import get_db, get_cursor
from services.auth_service import create_access_token, hash_password
from services.mtls_http import AppsInTossApiError, request_json

GENERATE_TOKEN_PATH = "/api-partner/v1/apps-in-toss/user/oauth2/generate-token"
LOGIN_ME_PATH = "/api-partner/v1/apps-in-toss/user/oauth2/login-me"
DEFAULT_MOCK_USER_KEY = "443731104"
DEFAULT_DISPLAY_NAME = "토스 사용자"


class AppsInTossLoginError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int = 400, payload: Any = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.payload = payload


def _is_mock_login_enabled() -> bool:
    raw = (os.getenv("AIT_LOGIN_MOCK_ENABLED") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "t", "yes", "y", "on"}
    return os.getenv("FLASK_ENV") in {"development", "test"} or bool(os.getenv("PYTEST_CURRENT_TEST"))


def _synthetic_email(user_key: str) -> str:
    return f"toss-user-{user_key}@apps-in-toss.local"


def _resolve_display_name(profile: Dict[str, Any]) -> str:
    display_name = (profile.get("displayName") or "").strip()
    if display_name:
        return display_name
    if profile.get("mock") is True:
        return (profile.get("name") or DEFAULT_DISPLAY_NAME).strip() or DEFAULT_DISPLAY_NAME
    return DEFAULT_DISPLAY_NAME


def _build_mock_profile(mock_user_key: Optional[str] = None) -> Dict[str, Any]:
    user_key = str(mock_user_key or os.getenv("AIT_LOGIN_MOCK_USER_KEY") or DEFAULT_MOCK_USER_KEY)
    return {
        "userKey": user_key,
        "scope": "user_key",
        "agreedTerms": [],
        "displayName": os.getenv("AIT_LOGIN_MOCK_NAME") or DEFAULT_DISPLAY_NAME,
        "mock": True,
    }


def _exchange_authorization_code(authorization_code: str, referrer: str) -> Dict[str, Any]:
    try:
        token_payload = request_json(
            "POST",
            GENERATE_TOKEN_PATH,
            json_body={
                "authorizationCode": authorization_code,
                "referrer": referrer,
            },
        )
    except AppsInTossApiError as exc:
        raise AppsInTossLoginError(
            "AUTH_EXCHANGE_FAILED",
            "Failed to exchange Apps-in-Toss authorization code.",
            status_code=exc.status_code,
            payload=exc.payload,
        ) from exc

    success_payload = token_payload.get("success") or {}
    access_token = success_payload.get("accessToken")
    if token_payload.get("resultType") != "SUCCESS" or not access_token:
        raise AppsInTossLoginError(
            "AUTH_EXCHANGE_FAILED",
            "Apps-in-Toss authorization code exchange was rejected.",
            payload=token_payload,
        )

    try:
        profile_payload = request_json(
            "GET",
            LOGIN_ME_PATH,
            headers={"Authorization": f"Bearer {access_token}"},
        )
    except AppsInTossApiError as exc:
        raise AppsInTossLoginError(
            "AUTH_PROFILE_FAILED",
            "Failed to retrieve Apps-in-Toss user profile.",
            status_code=exc.status_code,
            payload=exc.payload,
        ) from exc

    success_profile = profile_payload.get("success") or {}
    user_key = success_profile.get("userKey")
    if profile_payload.get("resultType") != "SUCCESS" or user_key in (None, ""):
        raise AppsInTossLoginError(
            "AUTH_PROFILE_FAILED",
            "Apps-in-Toss user profile was missing a userKey.",
            payload=profile_payload,
        )

    return success_profile


def ensure_user_for_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    user_key = str(profile.get("userKey") or "").strip()
    if not user_key:
        raise AppsInTossLoginError("USER_KEY_MISSING", "Apps-in-Toss userKey is required.")

    conn = get_db()
    cursor = get_cursor(conn)
    placeholder_hash = hash_password(secrets.token_urlsafe(24))
    synthetic_email = _synthetic_email(user_key)
    display_name = _resolve_display_name(profile)
    profile_json = json.dumps(profile, ensure_ascii=False)

    try:
        cursor.execute(
            """
            INSERT INTO users (
                email,
                password_hash,
                role,
                is_active,
                created_at,
                updated_at,
                last_login_at,
                user_key,
                auth_provider,
                display_name,
                profile_json
            )
            VALUES (%s, %s, 'user', TRUE, NOW(), NOW(), NOW(), %s, 'apps_in_toss', %s, %s::jsonb)
            ON CONFLICT (user_key)
            DO UPDATE SET
                is_active = TRUE,
                updated_at = NOW(),
                last_login_at = NOW(),
                auth_provider = 'apps_in_toss',
                display_name = EXCLUDED.display_name,
                profile_json = EXCLUDED.profile_json,
                email = CASE
                    WHEN users.email IS NULL OR users.email = '' OR users.email LIKE 'toss-user-%%@apps-in-toss.local'
                    THEN EXCLUDED.email
                    ELSE users.email
                END
            RETURNING id, email, role, user_key, auth_provider, display_name
            """,
            (synthetic_email, placeholder_hash, user_key, display_name, profile_json),
        )
        row = cursor.fetchone()
        conn.commit()
    except psycopg2.Error as exc:
        conn.rollback()
        raise AppsInTossLoginError(
            "USER_UPSERT_FAILED",
            "Failed to persist Apps-in-Toss user.",
            status_code=500,
            payload={"error": str(exc)},
        ) from exc
    finally:
        cursor.close()

    return {
        "id": row["id"],
        "email": row["email"],
        "role": row["role"],
        "user_key": str(row["user_key"]),
        "auth_provider": row["auth_provider"],
        "display_name": row["display_name"] or DEFAULT_DISPLAY_NAME,
    }


def login_with_authorization_code(
    *,
    authorization_code: Optional[str],
    referrer: Optional[str],
    mock_user_key: Optional[str] = None,
) -> Dict[str, Any]:
    if mock_user_key or (_is_mock_login_enabled() and not authorization_code):
        profile = _build_mock_profile(mock_user_key=mock_user_key)
    else:
        if not authorization_code:
            raise AppsInTossLoginError(
                "AUTHORIZATION_CODE_REQUIRED",
                "authorizationCode is required.",
            )
        profile = _exchange_authorization_code(
            authorization_code=authorization_code,
            referrer=(referrer or "DEFAULT").strip() or "DEFAULT",
        )

    user = ensure_user_for_profile(profile)
    token, expires_in = create_access_token(user)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    return {
        "accessToken": token,
        "expiresIn": expires_in,
        "expiresAt": expires_at.isoformat(),
        "me": {
            "id": user["id"],
            "userKey": user["user_key"],
            "displayName": user["display_name"],
            "authProvider": user["auth_provider"],
        },
    }
