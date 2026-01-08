import os
from functools import wraps

import jwt
from flask import jsonify, request, g
from jwt import ExpiredSignatureError, InvalidIssuerError, InvalidTokenError

JWT_ISSUER = 'ending-signal'


class AuthConfigError(Exception):
    def __init__(self, code="JWT_SECRET_MISSING", message="JWT_SECRET is not configured."):
        super().__init__(message)
        self.code = code
        self.message = message


_DEV_FALLBACK_SECRET = None


def get_jwt_secret():
    global _DEV_FALLBACK_SECRET

    env_secret = os.getenv('JWT_SECRET')
    if env_secret:
        return env_secret

    flask_env = os.getenv('FLASK_ENV')
    allow_insecure_dev = flask_env in {'development', 'test'}
    if os.getenv('ALLOW_INSECURE_JWT_DEV') == '1':
        allow_insecure_dev = True
    if os.getenv('PYTEST_CURRENT_TEST'):
        allow_insecure_dev = True
    if allow_insecure_dev:
        if _DEV_FALLBACK_SECRET is None:
            _DEV_FALLBACK_SECRET = os.urandom(24).hex()
            print(
                '[WARN] JWT_SECRET is not configured; using insecure dev secret. '
                'Tokens will be invalidated on server restart.'
            )
        return _DEV_FALLBACK_SECRET

    raise AuthConfigError()


def _error_response(status_code: int, code: str, message: str):
    return (
        jsonify({'success': False, 'error': {'code': code, 'message': message}}),
        status_code,
    )


def _decode_token(token: str):
    secret = get_jwt_secret()

    return jwt.decode(
        token,
        secret,
        algorithms=['HS256'],
        issuer=JWT_ISSUER,
    )


def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return _error_response(401, 'AUTH_REQUIRED', 'Authentication required')

        token = auth_header.split(' ', 1)[1].strip()
        if not token:
            return _error_response(401, 'AUTH_REQUIRED', 'Authentication required')

        try:
            payload = _decode_token(token)
        except AuthConfigError as e:
            return _error_response(503, e.code, '서버 설정(JWT_SECRET)이 누락되었습니다.')
        except ExpiredSignatureError:
            return _error_response(401, 'TOKEN_EXPIRED', 'Token has expired')
        except InvalidIssuerError:
            return _error_response(401, 'INVALID_TOKEN', 'Invalid token issuer')
        except InvalidTokenError:
            return _error_response(401, 'INVALID_TOKEN', 'Invalid token')

        g.current_user = {
            'id': payload.get('uid'),
            'email': payload.get('email'),
            'role': payload.get('role'),
        }
        return func(*args, **kwargs)

    return wrapper


def admin_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        current_user = getattr(g, 'current_user', None)
        if not current_user or current_user.get('role') != 'admin':
            return _error_response(403, 'FORBIDDEN', 'Admin privileges required')
        return func(*args, **kwargs)

    return wrapper
