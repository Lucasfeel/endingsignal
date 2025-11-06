# migrate_data.py
import sqlite3
import json
import sys
from dotenv import load_dotenv
from database import create_standalone_connection, get_cursor

def migrate_data():
    """
    Migrates data from the old SQLite database (webtoons.db) to the new PostgreSQL database.
    """
    print("--- Data Migration Started ---")

    # Load environment variables for the PostgreSQL connection
    load_dotenv()

    sqlite_conn = None
    pg_conn = None

    try:
        # Connect to the old SQLite database
        print("Connecting to SQLite database (webtoons.db)...")
        sqlite_conn = sqlite3.connect('webtoons.db')
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cursor = sqlite_conn.cursor()
        print("SQLite connection successful.")

        # Connect to the new PostgreSQL database
        print("Connecting to PostgreSQL database...")
        pg_conn = create_standalone_connection()
        pg_cursor = get_cursor(pg_conn)
        print("PostgreSQL connection successful.")

        # Fetch all data from the old webtoons table
        print("Fetching data from SQLite 'webtoons' table...")
        sqlite_cursor.execute("SELECT titleId, titleName, author, weekday, status FROM webtoons")
        old_webtoons = sqlite_cursor.fetchall()
        print(f"Found {len(old_webtoons)} records in SQLite database.")

        # Prepare data for insertion into the new 'contents' table
        inserts = []
        for row in old_webtoons:
            meta_data = {
                'author': row['author'],
                'weekday': row['weekday']
            }
            record = (
                row['titleId'],
                'naver_webtoon',  # source
                'webtoon',        # content_type
                row['titleName'],
                row['status'],
                json.dumps(meta_data)
            )
            inserts.append(record)

        # Insert data into the PostgreSQL 'contents' table
        if inserts:
            print(f"Inserting {len(inserts)} records into PostgreSQL 'contents' table...")
            pg_cursor.executemany(
                "INSERT INTO contents (content_id, source, content_type, title, status, meta) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (content_id, source) DO NOTHING",
                inserts
            )
            pg_conn.commit()
            print(f"{len(inserts)} records successfully inserted.")
        else:
            print("No new records to insert.")

        pg_cursor.close()

    except sqlite3.OperationalError as e:
        print(f"FATAL: SQLite error: {e}", file=sys.stderr)
        print("Please ensure 'webtoons.db' exists and the 'webtoons' table has the correct schema (titleId, titleName, author, weekday, status).", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: An unexpected error occurred: {e}", file=sys.stderr)
        if pg_conn:
            pg_conn.rollback()
        sys.exit(1)
    finally:
        if sqlite_conn:
            sqlite_conn.close()
            print("SQLite connection closed.")
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")
        print("--- Data Migration Finished ---")

if __name__ == '__main__':
    migrate_data()
