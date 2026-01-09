import datetime
import os
import re

import bcrypt
import jwt
import psycopg2

from database import create_standalone_connection, get_db, get_cursor
from utils.auth import JWT_ISSUER, get_jwt_secret

ACCESS_TOKEN_EXP_MINUTES = int(os.getenv('JWT_ACCESS_TOKEN_EXP_MINUTES', '10080'))


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email))


def is_valid_password(password: str) -> bool:
    return isinstance(password, str) and len(password) >= 8


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))


def create_access_token(user: dict):
    secret = get_jwt_secret()

    now = datetime.datetime.utcnow()
    expires_at = now + datetime.timedelta(minutes=ACCESS_TOKEN_EXP_MINUTES)
    payload = {
        'sub': f"user:{user['id']}",
        'uid': user['id'],
        'email': user['email'],
        'role': user['role'],
        'iat': now,
        'exp': expires_at,
        'iss': JWT_ISSUER,
    }
    token = jwt.encode(payload, secret, algorithm='HS256')
    return token, int((expires_at - now).total_seconds())


def register_user(email: str, password: str):
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute('SELECT id FROM users WHERE email = %s', (email,))
        if cursor.fetchone():
            return None, '이미 등록된 이메일입니다.'

        role = 'user'

        password_hash = hash_password(password)
        cursor.execute(
            'INSERT INTO users (email, password_hash, role) VALUES (%s, %s, %s) RETURNING id',
            (email, password_hash, role),
        )
        user_id = cursor.fetchone()[0]
        conn.commit()
        return {'id': user_id, 'email': email, 'role': role}, None
    except psycopg2.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


def authenticate_user(email: str, password: str):
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            'SELECT id, email, password_hash, role, is_active FROM users WHERE email = %s',
            (email,),
        )
        user = cursor.fetchone()
        if not user or not user['is_active']:
            return None
        if not verify_password(password, user['password_hash']):
            return None

        cursor.execute('UPDATE users SET last_login_at = NOW() WHERE id = %s', (user['id'],))
        conn.commit()
        return {'id': user['id'], 'email': user['email'], 'role': user['role']}
    except psycopg2.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


def change_password(user_id: int, current_password: str, new_password: str):
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute(
            'SELECT id, email, password_hash, is_active FROM users WHERE id = %s',
            (user_id,),
        )
        user = cursor.fetchone()
        if not user or not user['is_active']:
            return False, 'UNAUTHORIZED', '사용자를 찾을 수 없습니다.'

        if not verify_password(current_password, user['password_hash']):
            return False, 'INVALID_PASSWORD', '현재 비밀번호가 올바르지 않습니다.'

        if not is_valid_password(new_password):
            return False, 'WEAK_PASSWORD', '비밀번호는 8자 이상이어야 합니다.'

        cursor.execute(
            'UPDATE users SET password_hash = %s, updated_at = NOW() WHERE id = %s',
            (hash_password(new_password), user_id),
        )
        conn.commit()
        return True, None, None
    except psycopg2.Error:
        conn.rollback()
        raise
    finally:
        cursor.close()


def bootstrap_admin_from_env():
    admin_id = os.getenv("ADMIN_ID") or os.getenv("ADMIN_EMAIL")
    admin_password = os.getenv("ADMIN_PASSWORD")

    if not admin_id or not admin_password:
        return False, None

    if not is_valid_password(admin_password):
        raise ValueError("ADMIN_PASSWORD must be at least 8 characters.")

    conn = None
    cursor = None
    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        password_hash = hash_password(admin_password)
        cursor.execute(
            """
            INSERT INTO users (email, password_hash, role, is_active, created_at, updated_at)
            VALUES (%s, %s, 'admin', TRUE, NOW(), NOW())
            ON CONFLICT (email)
            DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                role = 'admin',
                is_active = TRUE,
                updated_at = NOW();
            """,
            (admin_id, password_hash),
        )
        conn.commit()
        return True, admin_id
    except psycopg2.Error:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
