from flask import Blueprint, jsonify, request, g
import psycopg2

from services.auth_service import (
    authenticate_user,
    create_access_token,
    is_valid_email,
    is_valid_password,
    register_user,
    change_password,
)
from utils.auth import AuthConfigError, _error_response, admin_required, login_required

auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/api/auth/register', methods=['POST'])
def register():
    data = request.get_json() or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return _error_response(400, 'INVALID_INPUT', '이메일과 비밀번호가 필요합니다.')
    if not is_valid_password(password):
        return _error_response(400, 'PASSWORD_TOO_SHORT', '비밀번호는 8자 이상이어야 합니다.')
    if not is_valid_email(email):
        return _error_response(400, 'INVALID_INPUT', '올바른 이메일 형식이 아닙니다.')

    try:
        user, error = register_user(email, password)
        if error:
            return _error_response(409, 'EMAIL_ALREADY_EXISTS', error)
        return jsonify({'success': True, 'user_id': user['id']}), 201
    except psycopg2.Error:
        return _error_response(500, 'INTERNAL_ERROR', '데이터베이스 오류가 발생했습니다.')
    except AuthConfigError:
        return _error_response(503, 'JWT_SECRET_MISSING', '서버 설정(JWT_SECRET)이 누락되었습니다.')
    except Exception:
        return _error_response(500, 'INTERNAL_ERROR', '서버 오류가 발생했습니다.')


@auth_bp.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json() or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return _error_response(400, 'INVALID_INPUT', '이메일과 비밀번호가 필요합니다.')

    try:
        user = authenticate_user(email, password)
        if not user:
            return _error_response(401, 'INVALID_CREDENTIALS', '이메일 또는 비밀번호가 올바르지 않습니다.')

        token, expires_in = create_access_token(user)
        return (
            jsonify(
                {
                    'access_token': token,
                    'token_type': 'bearer',
                    'expires_in': expires_in,
                    'user': user,
                }
            ),
            200,
        )
    except psycopg2.Error:
        return _error_response(500, 'INTERNAL_ERROR', '데이터베이스 오류가 발생했습니다.')
    except AuthConfigError:
        return _error_response(503, 'JWT_SECRET_MISSING', '서버 설정(JWT_SECRET)이 누락되었습니다.')
    except Exception:
        return _error_response(500, 'INTERNAL_ERROR', '서버 오류가 발생했습니다.')


@auth_bp.route('/api/auth/logout', methods=['POST'])
def logout():
    return jsonify({'success': True}), 200


@auth_bp.route('/api/auth/me', methods=['GET'])
@login_required
def me():
    return jsonify({'success': True, 'user': g.current_user}), 200


@auth_bp.route('/api/auth/change-password', methods=['POST'])
@login_required
def change_password_route():
    data = request.get_json() or {}
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password:
        return _error_response(400, 'INVALID_INPUT', '현재 비밀번호와 새 비밀번호가 필요합니다.')

    if not is_valid_password(new_password):
        return _error_response(400, 'WEAK_PASSWORD', '비밀번호는 8자 이상이어야 합니다.')

    try:
        ok, code, message = change_password(
            g.current_user.get('id'), current_password, new_password
        )
        if not ok:
            status = 403 if code == 'INVALID_PASSWORD' else 401 if code == 'UNAUTHORIZED' else 400
            return _error_response(status, code or 'CHANGE_PASSWORD_FAILED', message or '비밀번호 변경에 실패했습니다.')

        return jsonify({'ok': True}), 200
    except psycopg2.Error:
        return _error_response(500, 'INTERNAL_ERROR', '데이터베이스 오류가 발생했습니다.')
    except AuthConfigError:
        return _error_response(503, 'JWT_SECRET_MISSING', '서버 설정(JWT_SECRET)이 누락되었습니다.')
    except Exception:
        return _error_response(500, 'INTERNAL_ERROR', '서버 오류가 발생했습니다.')


@auth_bp.route('/api/auth/admin/ping', methods=['GET'])
@login_required
@admin_required
def admin_ping():
    return jsonify({'success': True, 'message': 'admin ok'}), 200
