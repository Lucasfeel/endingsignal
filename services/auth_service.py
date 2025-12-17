import os
import re
import datetime

import bcrypt
import jwt
import psycopg2

from database import get_db, get_cursor

JWT_SECRET = os.getenv('JWT_SECRET')
ACCESS_TOKEN_EXP_MINUTES = int(os.getenv('JWT_ACCESS_TOKEN_EXP_MINUTES', '20'))
JWT_ISSUER = 'ending-signal'


def is_valid_email(email: str) -> bool:
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email))


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))


def create_access_token(user: dict):
    if not JWT_SECRET:
        raise ValueError('JWT_SECRET is not configured')

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
    token = jwt.encode(payload, JWT_SECRET, algorithm='HS256')
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
