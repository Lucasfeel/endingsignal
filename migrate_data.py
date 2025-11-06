# migrate_data.py

import sqlite3
import psycopg2
import psycopg2.extras
import os
import json
from dotenv import load_dotenv

load_dotenv()

def migrate_data():
    """SQLite에서 PostgreSQL로 데이터를 마이그레이션합니다."""

    # --- 환경 변수 로드 ---
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')

    # --- 연결 확인 ---
    if not all([db_name, db_user, db_password, db_host, db_port]):
        print("데이터베이스 환경 변수가 모두 설정되지 않았습니다. .env 파일을 확인하세요.")
        return

    # --- SQLite 연결 ---
    try:
        sqlite_conn = sqlite3.connect('webtoons.db')
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cursor = sqlite_conn.cursor()
        print("SQLite DB에 성공적으로 연결되었습니다.")
    except sqlite3.Error as e:
        print(f"SQLite 연결 오류: {e}")
        return

    # --- PostgreSQL 연결 ---
    try:
        pg_conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        pg_cursor = pg_conn.cursor()
        print("PostgreSQL DB에 성공적으로 연결되었습니다.")
    except psycopg2.Error as e:
        print(f"PostgreSQL 연결 오류: {e}")
        if 'sqlite_conn' in locals():
            sqlite_conn.close()
        return

    try:
        # 1. 'contents' 테이블 마이그레이션
        print("\n'contents' 테이블 마이그레이션 시작...")
        sqlite_cursor.execute("SELECT * FROM contents")
        contents_data = sqlite_cursor.fetchall()

        if contents_data:
            contents_to_insert = []
            for row in contents_data:
                # meta 필드가 JSON 형식이 아니면 변환
                meta_data = row['meta']
                if isinstance(meta_data, str):
                    try:
                        meta_data = json.loads(meta_data)
                    except json.JSONDecodeError:
                        print(f"  - 경고: content_id {row['content_id']}의 meta 필드 JSON 파싱 실패. NULL로 처리합니다.")
                        meta_data = None

                contents_to_insert.append((
                    row['content_id'], row['source'], row['content_type'],
                    row['title'], row['status'], json.dumps(meta_data) if meta_data else None
                ))

            insert_query = "INSERT INTO contents (content_id, source, content_type, title, status, meta) VALUES %s ON CONFLICT DO NOTHING"
            psycopg2.extras.execute_values(
                pg_cursor, insert_query, contents_to_insert
            )
            print(f"  -> {len(contents_to_insert)}개 레코드 'contents' 테이블에 삽입 완료.")
        else:
            print("  -> 'contents' 테이블에 데이터가 없습니다.")

        # 2. 'subscriptions' 테이블 마이그레이션
        print("\n'subscriptions' 테이블 마이그레이션 시작...")
        sqlite_cursor.execute("SELECT * FROM subscriptions")
        subscriptions_data = sqlite_cursor.fetchall()

        if subscriptions_data:
            subscriptions_to_insert = [
                (row['email'], row['content_id'], row['source'])
                for row in subscriptions_data
            ]

            insert_query = "INSERT INTO subscriptions (email, content_id, source) VALUES %s ON CONFLICT DO NOTHING"
            psycopg2.extras.execute_values(
                pg_cursor, insert_query, subscriptions_to_insert
            )
            print(f"  -> {len(subscriptions_to_insert)}개 레코드 'subscriptions' 테이블에 삽입 완료.")
        else:
            print("  -> 'subscriptions' 테이블에 데이터가 없습니다.")

        # --- 변경사항 커밋 ---
        pg_conn.commit()
        print("\n모든 데이터 마이그레이션이 성공적으로 완료되었습니다.")

    except (Exception, psycopg2.Error) as e:
        print(f"\n마이그레이션 중 오류 발생: {e}")
        pg_conn.rollback()

    finally:
        # --- 연결 종료 ---
        sqlite_conn.close()
        pg_cursor.close()
        pg_conn.close()
        print("\n모든 DB 연결이 닫혔습니다.")

if __name__ == '__main__':
    migrate_data()
