"""Database bootstrap script with readiness checks and idempotency."""

import sys
import time

import psycopg2

from database import create_standalone_connection, setup_database_standalone
from dotenv import load_dotenv
from services.auth_service import bootstrap_admin_from_env


def connect_with_retries(max_attempts: int = 10):
    """Connect to the database with basic retry/backoff for cold starts."""

    for attempt in range(1, max_attempts + 1):
        try:
            print(
                f"[CHECK] Connecting to database (attempt {attempt}/{max_attempts})..."
            )
            return create_standalone_connection()
        except psycopg2.OperationalError as exc:  # Retry only on connectivity issues
            if attempt == max_attempts:
                raise

            sleep_seconds = attempt
            print(
                f"[RETRY] Database not ready: {exc}. Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)


def database_already_initialized() -> bool:
    """Determine whether the schema already exists by checking the contents table."""

    conn = None
    cursor = None
    try:
        conn = connect_with_retries()
        cursor = conn.cursor()
        cursor.execute("SELECT to_regclass('public.contents') as t;")
        row = cursor.fetchone()
        return bool(row and row[0])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    print("==========================================")
    print("  DATABASE INITIALIZATION SCRIPT STARTED")
    print("==========================================")

    try:
        load_dotenv()

        already_initialized = database_already_initialized()
        if already_initialized:
            print("[INFO] Database already initialized. Running idempotent setup to apply latest schema...")
        else:
            print("[INFO] Database not initialized. Running setup...")
        setup_database_standalone()
        try:
            ran, admin_id = bootstrap_admin_from_env()
            if ran:
                print(f"[INFO] Admin bootstrap applied for ADMIN_ID={admin_id}")
            else:
                print("[INFO] ADMIN_ID/ADMIN_PASSWORD not set. Skipping admin bootstrap.")
        except ValueError as exc:
            print(
                f"[FATAL] Admin bootstrap misconfigured: {exc}",
                file=sys.stderr,
            )
            sys.exit(1)
        print("\n[SUCCESS] Database initialization complete.")
        print("==========================================")
        print("  DATABASE INITIALIZATION SCRIPT FINISHED")
        print("==========================================")
        sys.exit(0)
    except Exception as e:
        print(
            f"\n[FATAL] An error occurred during database initialization: {e}",
            file=sys.stderr,
        )
        print("==========================================")
        print("  DATABASE INITIALIZATION SCRIPT FAILED")
        print("==========================================")
        sys.exit(1)


if __name__ == "__main__":
    main()
