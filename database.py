# database.py

import psycopg2
import psycopg2.extras
from flask import g
import os

def get_db():
    """Application Context 내에서 유일한 DB 연결을 가져옵니다."""
    if 'db' not in g:
        g.db = psycopg2.connect(
            dbname=os.environ.get('DB_NAME'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            host=os.environ.get('DB_HOST'),
            port=os.environ.get('DB_PORT')
        )
    return g.db

def get_cursor(db):
    """지정된 DB 연결로부터 DictCursor를 반환합니다."""
    return db.cursor(cursor_factory=psycopg2.extras.DictCursor)

def close_db(exception=None):
    """요청(request)이 끝나면 자동으로 호출되어 DB 연결을 닫습니다."""
    db = g.pop('db', None)
    if db is not None:
        db.close()
