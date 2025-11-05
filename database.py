# database.py

import sqlite3
from flask import g
import config

def get_db():
    """Application Context 내에서 유일한 DB 연결을 가져옵니다."""
    if 'db' not in g:
        g.db = sqlite3.connect(config.DATABASE_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db

def close_db(exception=None):
    """요청(request)이 끝나면 자동으로 호출되어 DB 연결을 닫습니다."""
    db = g.pop('db', None)
    if db is not None:
        db.close()
