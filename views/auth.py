from flask import Blueprint, g, jsonify, request
import psycopg2

from services.apps_in_toss_auth_service import (
    AppsInTossLoginError,
    login_with_authorization_code,
)
from services.auth_service import (
    authenticate_user,
    change_password,
    create_access_token,
    is_valid_email,
    is_valid_password,
    register_user,
)
from utils.auth import AuthConfigError, _error_response, admin_required, login_required

auth_bp = Blueprint("auth", __name__)


def _handle_apps_in_toss_login():
    data = request.get_json() or {}
    authorization_code = data.get("authorizationCode")
    referrer = data.get("referrer") or "DEFAULT"
    mock_user_key = data.get("mockUserKey")

    try:
        payload = login_with_authorization_code(
            authorization_code=authorization_code,
            referrer=referrer,
            mock_user_key=mock_user_key,
        )
        return jsonify(payload), 200
    except AppsInTossLoginError as exc:
        return _error_response(exc.status_code, exc.code, exc.message)
    except AuthConfigError as exc:
        return _error_response(503, exc.code, exc.message)
    except psycopg2.Error:
        return _error_response(500, "INTERNAL_ERROR", "Database error")
    except Exception:
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")


@auth_bp.route("/api/auth/register", methods=["POST"])
def register():
    data = request.get_json() or {}
    email = data.get("email")
    password = data.get("password")

    if not email or not password:
        return _error_response(400, "INVALID_INPUT", "email and password are required")
    if not is_valid_password(password):
        return _error_response(400, "PASSWORD_TOO_SHORT", "password must be at least 8 characters")
    if not is_valid_email(email):
        return _error_response(400, "INVALID_INPUT", "invalid email format")

    try:
        user, error = register_user(email, password)
        if error:
            return _error_response(409, "EMAIL_ALREADY_EXISTS", error)
        return jsonify({"success": True, "user_id": user["id"]}), 201
    except psycopg2.Error:
        return _error_response(500, "INTERNAL_ERROR", "Database error")
    except AuthConfigError:
        return _error_response(503, "JWT_SECRET_MISSING", "JWT secret is not configured")
    except Exception:
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")


@auth_bp.route("/api/auth/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    email = data.get("email")
    password = data.get("password")

    if not email or not password:
        return _error_response(400, "INVALID_INPUT", "email and password are required")

    try:
        user = authenticate_user(email, password)
        if not user:
            return _error_response(401, "INVALID_CREDENTIALS", "invalid credentials")

        token, expires_in = create_access_token(user)
        return (
            jsonify(
                {
                    "access_token": token,
                    "token_type": "bearer",
                    "expires_in": expires_in,
                    "user": user,
                }
            ),
            200,
        )
    except psycopg2.Error:
        return _error_response(500, "INTERNAL_ERROR", "Database error")
    except AuthConfigError:
        return _error_response(503, "JWT_SECRET_MISSING", "JWT secret is not configured")
    except Exception:
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")


@auth_bp.route("/v1/auth/login", methods=["POST"])
def login_v1():
    return _handle_apps_in_toss_login()


@auth_bp.route("/api/auth/logout", methods=["POST"])
def logout():
    return jsonify({"success": True}), 200


@auth_bp.route("/v1/auth/logout", methods=["POST"])
@login_required
def logout_v1():
    return jsonify({"success": True}), 200


@auth_bp.route("/api/auth/me", methods=["GET"])
@login_required
def me():
    return jsonify({"success": True, "user": g.current_user}), 200


@auth_bp.route("/v1/auth/me", methods=["GET"])
@login_required
def me_v1():
    return (
        jsonify(
            {
                "me": {
                    "id": g.current_user.get("id"),
                    "userKey": g.current_user.get("user_key"),
                    "displayName": g.current_user.get("display_name"),
                    "authProvider": g.current_user.get("auth_provider"),
                    "role": g.current_user.get("role"),
                }
            }
        ),
        200,
    )


@auth_bp.route("/api/auth/change-password", methods=["POST"])
@login_required
def change_password_route():
    data = request.get_json() or {}
    current_password = data.get("current_password")
    new_password = data.get("new_password")

    if not current_password or not new_password:
        return _error_response(
            400,
            "INVALID_INPUT",
            "current_password and new_password are required",
        )

    if not is_valid_password(new_password):
        return _error_response(400, "WEAK_PASSWORD", "password must be at least 8 characters")

    try:
        ok, code, message = change_password(
            g.current_user.get("id"), current_password, new_password
        )
        if not ok:
            status = 403 if code == "INVALID_PASSWORD" else 401 if code == "UNAUTHORIZED" else 400
            return _error_response(status, code or "CHANGE_PASSWORD_FAILED", message or "failed to change password")

        return jsonify({"ok": True}), 200
    except psycopg2.Error:
        return _error_response(500, "INTERNAL_ERROR", "Database error")
    except AuthConfigError:
        return _error_response(503, "JWT_SECRET_MISSING", "JWT secret is not configured")
    except Exception:
        return _error_response(500, "INTERNAL_ERROR", "Internal server error")


@auth_bp.route("/api/auth/admin/ping", methods=["GET"])
@login_required
@admin_required
def admin_ping():
    return jsonify({"success": True, "message": "admin ok"}), 200
