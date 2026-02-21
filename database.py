# database.py

import os
import re
import sys
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from flask import g
from psycopg2 import sql

REQUIRED_DB_ENV_VARS = ('DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT')
DB_INIT_APPLICATION_NAME = "endingsignal_init_db"
DB_INIT_ADVISORY_LOCK_NAME = "endingsignal:init_db"
DEFAULT_DB_INIT_LOCK_TIMEOUT = "5s"
DEFAULT_DB_INIT_ADVISORY_LOCK_WAIT_SECONDS = 60.0


class DatabaseUnavailableError(ValueError):
    """Raised when no usable database configuration is present."""


def has_database_config():
    database_url = (os.environ.get('DATABASE_URL') or '').strip()
    if database_url:
        return True
    return all((os.environ.get(var) or '').strip() for var in REQUIRED_DB_ENV_VARS)

def _create_connection():
    """
    Create a new database connection from environment configuration.
    Prefer DATABASE_URL, otherwise use individual DB_* variables.
    """
    db_timezone = os.environ.get('DB_TIMEZONE', '').strip() or 'Asia/Seoul'
    options = f"-c timezone={db_timezone}"
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url, options=options)

    # Validate local/deployment DB configuration when DATABASE_URL is absent.
    if not has_database_config():
        raise DatabaseUnavailableError(
            "Database configuration is missing. Set DATABASE_URL or "
            "DB_NAME/DB_USER/DB_PASSWORD/DB_HOST/DB_PORT."
        )

    return psycopg2.connect(
        dbname=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT'),
        options=options
    )

def get_db():
    """Return a request-scoped DB connection from Flask's application context."""
    if 'db' not in g:
        g.db = _create_connection()
    return g.db

def get_cursor(db):
    """Return a DictCursor for the provided DB connection."""
    return db.cursor(cursor_factory=psycopg2.extras.DictCursor)


@contextmanager
def managed_cursor(conn):
    cursor = get_cursor(conn)
    try:
        yield cursor
    finally:
        try:
            cursor.close()
        except Exception:
            pass

def close_db(exception=None):
    """Close the request-scoped DB connection if one exists."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def create_standalone_connection():
    """Create a standalone DB connection outside Flask request context."""
    return _create_connection()


def _read_db_init_timeouts():
    lock_timeout = (
        os.environ.get("DB_INIT_LOCK_TIMEOUT", DEFAULT_DB_INIT_LOCK_TIMEOUT).strip()
        or DEFAULT_DB_INIT_LOCK_TIMEOUT
    )
    statement_timeout = (os.environ.get("DB_INIT_STATEMENT_TIMEOUT") or "").strip() or None
    raw_wait_seconds = (
        os.environ.get("DB_INIT_ADVISORY_LOCK_WAIT_SECONDS") or ""
    ).strip()
    advisory_wait_seconds = DEFAULT_DB_INIT_ADVISORY_LOCK_WAIT_SECONDS
    if raw_wait_seconds:
        try:
            advisory_wait_seconds = float(raw_wait_seconds)
            if advisory_wait_seconds < 0:
                raise ValueError("must be non-negative")
        except ValueError:
            print(
                "WARN: [DB Setup] Invalid DB_INIT_ADVISORY_LOCK_WAIT_SECONDS "
                f"'{raw_wait_seconds}'. Using default "
                f"{DEFAULT_DB_INIT_ADVISORY_LOCK_WAIT_SECONDS:.0f}s.",
                file=sys.stderr,
            )
            advisory_wait_seconds = DEFAULT_DB_INIT_ADVISORY_LOCK_WAIT_SECONDS
    return lock_timeout, statement_timeout, advisory_wait_seconds


def configure_db_init_session(cursor, lock_timeout, statement_timeout=None):
    cursor.execute("SET application_name = %s", (DB_INIT_APPLICATION_NAME,))
    cursor.execute("SET lock_timeout = %s", (lock_timeout,))
    if statement_timeout:
        cursor.execute("SET statement_timeout = %s", (statement_timeout,))


def acquire_init_advisory_lock(conn, wait_seconds):
    deadline = time.monotonic() + max(wait_seconds, 0.0)
    while True:
        with managed_cursor(conn) as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s))",
                (DB_INIT_ADVISORY_LOCK_NAME,),
            )
            row = cursor.fetchone()
            has_lock = bool(row and row[0])

        try:
            conn.rollback()
        except Exception:
            pass

        if has_lock:
            return True

        if time.monotonic() >= deadline:
            return False

        time.sleep(min(1.0, max(deadline - time.monotonic(), 0.1)))


def release_init_advisory_lock(conn):
    try:
        conn.rollback()
    except Exception:
        pass

    with managed_cursor(conn) as cursor:
        cursor.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))",
            (DB_INIT_ADVISORY_LOCK_NAME,),
        )
        row = cursor.fetchone()
        return bool(row and row[0])


def column_exists(cursor, table, column, schema='public'):
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (schema, table, column),
    )
    return cursor.fetchone() is not None


def get_column_default(cursor, table, column, schema='public'):
    cursor.execute(
        """
        SELECT column_default
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        """,
        (schema, table, column),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return row[0]


def is_column_nullable(cursor, table, column, schema='public'):
    cursor.execute(
        """
        SELECT is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        """,
        (schema, table, column),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return row[0] == "YES"


def index_exists(cursor, index_name, schema='public'):
    cursor.execute(
        """
        SELECT 1
        FROM pg_class cls
        JOIN pg_namespace n ON n.oid = cls.relnamespace
        WHERE n.nspname = %s
          AND cls.relname = %s
          AND cls.relkind = 'i'
        LIMIT 1
        """,
        (schema, index_name),
    )
    return cursor.fetchone() is not None


def trigger_exists(cursor, table, trigger_name, schema='public'):
    cursor.execute(
        """
        SELECT 1
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND t.tgname = %s
          AND NOT t.tgisinternal
        LIMIT 1
        """,
        (schema, table, trigger_name),
    )
    return cursor.fetchone() is not None


def _canonicalize_default_expression(expression):
    if expression is None:
        return None
    normalized = expression.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"::[a-zA-Z0-9_ \[\]\.]+", "", normalized)
    while normalized.startswith("(") and normalized.endswith(")"):
        normalized = normalized[1:-1].strip()
    return normalized


def column_default_matches(existing_default, desired_default):
    existing_normalized = _canonicalize_default_expression(existing_default)
    desired_normalized = _canonicalize_default_expression(desired_default)
    if existing_normalized == desired_normalized:
        return True

    now_equivalents = {
        "now()",
        "current_timestamp",
        "transaction_timestamp()",
        "statement_timestamp()",
        "clock_timestamp()",
    }
    if desired_normalized in now_equivalents and existing_normalized in now_equivalents:
        return True

    return False


def ensure_column_exists(cursor, table, column, column_definition, schema='public'):
    if column_exists(cursor, table, column, schema=schema):
        print(
            "LOG: [DB Setup] Column "
            f"'{schema}.{table}.{column}' already exists. Skipping ALTER."
        )
        return False

    print(
        "LOG: [DB Setup] Adding missing column "
        f"'{schema}.{table}.{column}'."
    )
    cursor.execute(
        sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
            sql.SQL(column_definition),
        )
    )
    return True


def ensure_column_default(cursor, table, column, default_expression, schema='public'):
    current_default = get_column_default(cursor, table, column, schema=schema)
    if column_default_matches(current_default, default_expression):
        print(
            "LOG: [DB Setup] Default for "
            f"'{schema}.{table}.{column}' already set. Skipping ALTER."
        )
        return False

    print(
        "LOG: [DB Setup] Setting default for "
        f"'{schema}.{table}.{column}' to {default_expression}."
    )
    cursor.execute(
        sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
            sql.SQL(default_expression),
        )
    )
    return True


def ensure_column_not_null(cursor, table, column, schema='public'):
    nullable = is_column_nullable(cursor, table, column, schema=schema)
    if nullable is None:
        print(
            "WARN: [DB Setup] Column "
            f"'{schema}.{table}.{column}' does not exist. Skipping SET NOT NULL.",
            file=sys.stderr,
        )
        return False
    if not nullable:
        print(
            "LOG: [DB Setup] Column "
            f"'{schema}.{table}.{column}' is already NOT NULL. Skipping ALTER."
        )
        return False

    print(
        "LOG: [DB Setup] Setting column "
        f"'{schema}.{table}.{column}' to NOT NULL."
    )
    cursor.execute(
        sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
        )
    )
    return True


def ensure_updated_at_trigger(cursor, table, trigger_name, schema='public'):
    if trigger_exists(cursor, table, trigger_name, schema=schema):
        print(
            "LOG: [DB Setup] Trigger "
            f"'{trigger_name}' already exists on '{schema}.{table}'. Skipping CREATE TRIGGER."
        )
        return False

    print(
        "LOG: [DB Setup] Creating missing trigger "
        f"'{trigger_name}' on '{schema}.{table}'."
    )
    cursor.execute(
        sql.SQL(
            """
            CREATE TRIGGER {}
            BEFORE UPDATE ON {}.{}
            FOR EACH ROW
            EXECUTE PROCEDURE set_updated_at();
            """
        ).format(
            sql.Identifier(trigger_name),
            sql.Identifier(schema),
            sql.Identifier(table),
        )
    )
    return True


def create_index_if_missing(cursor, schema, index_name, create_sql):
    if index_exists(cursor, index_name, schema=schema):
        print(
            "LOG: [DB Setup] Index "
            f"'{schema}.{index_name}' already exists. Skipping CREATE INDEX."
        )
        return False

    cursor.execute(create_sql)
    return True


def is_lock_timeout_error(exc):
    if not exc:
        return False
    pgcode = getattr(exc, "pgcode", None)
    if pgcode == "55P03":
        return True
    return "lock timeout" in str(exc).lower()


def is_statement_timeout_error(exc):
    if not exc:
        return False
    pgcode = getattr(exc, "pgcode", None)
    if pgcode == "57014":
        return True
    return "statement timeout" in str(exc).lower()


def print_lock_diagnostics(conn):
    if not conn:
        return

    print(
        "ERROR: [DB Setup] Lock diagnostics (pg_stat_activity):",
        file=sys.stderr,
    )
    try:
        conn.rollback()
    except Exception:
        pass

    try:
        with managed_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT
                    pid,
                    usename,
                    state,
                    wait_event_type,
                    wait_event,
                    age(now(), xact_start) AS xact_age,
                    age(now(), query_start) AS query_age,
                    pg_blocking_pids(pid) AS blocking_pids,
                    LEFT(COALESCE(query, ''), 200) AS query_text
                FROM pg_stat_activity
                WHERE datname = current_database()
                ORDER BY COALESCE(xact_start, query_start) NULLS LAST
                """
            )
            rows = cursor.fetchall() or []

        for row in rows:
            print(
                "ERROR: [DB Setup] "
                f"pid={row['pid']} user={row['usename']} state={row['state']} "
                f"wait={row['wait_event_type']}/{row['wait_event']} "
                f"xact_age={row['xact_age']} query_age={row['query_age']} "
                f"blocking_pids={row['blocking_pids']} "
                f"query={row['query_text']}",
                file=sys.stderr,
            )
    except Exception as diag_exc:
        print(
            "ERROR: [DB Setup] Could not fetch lock diagnostics "
            f"(permissions or connection issue): {diag_exc}",
            file=sys.stderr,
        )


def setup_database_standalone():
    """Create and reconcile tables for standalone bootstrap/migration scripts."""
    conn = None
    cursor = None
    advisory_lock_acquired = False
    current_step = "startup"
    lock_timeout, statement_timeout, advisory_wait_seconds = _read_db_init_timeouts()
    try:
        current_step = "connect"
        print("LOG: [DB Setup] Attempting to connect to the database...")
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        print("LOG: [DB Setup] Connection successful.")

        current_step = "configure session"
        configure_db_init_session(
            cursor,
            lock_timeout=lock_timeout,
            statement_timeout=statement_timeout,
        )
        print(
            "LOG: [DB Setup] Session config applied "
            f"(application_name={DB_INIT_APPLICATION_NAME}, lock_timeout={lock_timeout}, "
            f"statement_timeout={statement_timeout or 'unset'})."
        )
        conn.commit()

        current_step = "acquire advisory lock"
        print(
            "LOG: [DB Setup] Waiting for DB init advisory lock "
            f"(max {advisory_wait_seconds:g}s)..."
        )
        if not acquire_init_advisory_lock(conn, advisory_wait_seconds):
            raise RuntimeError(
                "Could not acquire DB init advisory lock within "
                f"{advisory_wait_seconds:g}s. Another migration may be running."
            )
        advisory_lock_acquired = True
        print("LOG: [DB Setup] Advisory lock acquired.")

        current_step = "contents schema"
        # print("LOG: [DB Setup] Dropping existing tables (if any)...")
        # # cursor.execute("DROP TABLE IF EXISTS subscriptions;")
        # cursor.execute("DROP TABLE IF EXISTS contents;")
        # print("LOG: [DB Setup] Tables dropped.")

        current_step = "create contents table"
        print("LOG: [DB Setup] Creating 'contents' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contents (
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            content_type TEXT NOT NULL,
            title TEXT NOT NULL,
            normalized_title TEXT,
            normalized_authors TEXT,
            status TEXT NOT NULL,
            meta JSONB,
            PRIMARY KEY (content_id, source)
        )""")
        print("LOG: [DB Setup] 'contents' table created or already exists.")

        current_step = "ensure contents normalized columns"
        print("LOG: [DB Setup] Ensuring normalized search columns exist...")
        ensure_column_exists(cursor, "contents", "normalized_title", "TEXT")
        ensure_column_exists(cursor, "contents", "normalized_authors", "TEXT")

        current_step = "ensure contents soft-delete columns"
        print("LOG: [DB Setup] Ensuring soft-delete columns exist...")
        ensure_column_exists(cursor, "contents", "is_deleted", "BOOLEAN")
        ensure_column_default(cursor, "contents", "is_deleted", "FALSE")
        cursor.execute(
            """
            UPDATE contents
            SET is_deleted = FALSE
            WHERE is_deleted IS NULL;
            """
        )
        ensure_column_not_null(cursor, "contents", "is_deleted")
        ensure_column_exists(cursor, "contents", "deleted_at", "TIMESTAMP")
        ensure_column_exists(cursor, "contents", "deleted_reason", "TEXT")
        ensure_column_exists(cursor, "contents", "deleted_by", "INTEGER")

        current_step = "ensure contents timestamps"
        print("LOG: [DB Setup] Ensuring contents timestamps exist...")
        ensure_column_exists(cursor, "contents", "created_at", "TIMESTAMP")
        ensure_column_exists(cursor, "contents", "updated_at", "TIMESTAMP")
        ensure_column_default(cursor, "contents", "created_at", "NOW()")
        ensure_column_default(cursor, "contents", "updated_at", "NOW()")
        cursor.execute(
            """
            UPDATE contents
            SET created_at = NOW()
            WHERE created_at IS NULL;
            """
        )
        cursor.execute(
            """
            UPDATE contents
            SET updated_at = NOW()
            WHERE updated_at IS NULL;
            """
        )
        ensure_column_not_null(cursor, "contents", "created_at")
        ensure_column_not_null(cursor, "contents", "updated_at")
        current_step = "ensure set_updated_at function"
        cursor.execute(
            """
            CREATE OR REPLACE FUNCTION set_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )
        ensure_updated_at_trigger(cursor, "contents", "trg_contents_updated_at")

        current_step = "commit contents schema"
        print("LOG: [DB Setup] Committing contents schema phase...")
        conn.commit()
        print("LOG: [DB Setup] Contents schema phase committed.")

        current_step = "remaining schema"

        current_step = "create users table"
        print("LOG: [DB Setup] Creating 'users' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            last_login_at TIMESTAMP
        )""")
        print("LOG: [DB Setup] 'users' table created or already exists.")

        current_step = "ensure users updated_at column"
        print("LOG: [DB Setup] Ensuring 'users.updated_at' column exists and has defaults...")
        ensure_column_exists(cursor, "users", "updated_at", "TIMESTAMP")
        ensure_column_default(cursor, "users", "updated_at", "NOW()")
        cursor.execute(
            """
            UPDATE users
            SET updated_at = COALESCE(created_at, NOW())
            WHERE updated_at IS NULL;
            """
        )
        ensure_updated_at_trigger(cursor, "users", "trg_users_updated_at")

        current_step = "create subscriptions table"
        print("LOG: [DB Setup] Creating 'subscriptions' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            email TEXT,
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            UNIQUE(user_id, content_id, source)
        )""")
        print("LOG: [DB Setup] 'subscriptions' table created or already exists.")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_content_source ON subscriptions (content_id, source)"
        )
        current_step = "ensure subscriptions alert flags"
        print("LOG: [DB Setup] Ensuring subscription alert flags exist...")
        ensure_column_exists(
            cursor,
            "subscriptions",
            "wants_completion",
            "BOOLEAN NOT NULL DEFAULT FALSE",
        )
        ensure_column_exists(
            cursor,
            "subscriptions",
            "wants_publication",
            "BOOLEAN NOT NULL DEFAULT FALSE",
        )
        cursor.execute(
            """
            UPDATE subscriptions
            SET wants_completion = TRUE
            WHERE wants_completion = FALSE AND wants_publication = FALSE;
            """
        )

        print("LOG: [DB Setup] Creating 'content_types' table...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS content_types (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_content_types_name_ci ON content_types ((LOWER(name)))"
        )
        ensure_updated_at_trigger(cursor, "content_types", "trg_content_types_updated_at")
        print("LOG: [DB Setup] 'content_types' table created or already exists.")

        print("LOG: [DB Setup] Creating 'content_sources' table...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS content_sources (
                id SERIAL PRIMARY KEY,
                type_id INTEGER NOT NULL REFERENCES content_types(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(type_id, name)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_content_sources_type_id ON content_sources (type_id)"
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_content_sources_type_name_ci ON content_sources (type_id, LOWER(name))"
        )
        ensure_updated_at_trigger(cursor, "content_sources", "trg_content_sources_updated_at")
        print("LOG: [DB Setup] 'content_sources' table created or already exists.")

        print("LOG: [DB Setup] Seeding content type/source options (idempotent)...")
        seeded_types = ("\uc6f9\ud230", "\uc6f9\uc18c\uc124", "OTT")
        for type_name in seeded_types:
            cursor.execute(
                """
                INSERT INTO content_types (name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                """,
                (type_name,),
            )

        seeded_sources_by_type = {
            "\uc6f9\ud230": ("\ub124\uc774\ubc84\uc6f9\ud230", "\uce74\uce74\uc624\uc6f9\ud230"),
            "\uc6f9\uc18c\uc124": (
                "\ub124\uc774\ubc84 \uc2dc\ub9ac\uc988",
                "\uce74\uce74\uc624 \ud398\uc774\uc9c0",
                "\ubb38\ud53c\uc544",
                "\ub9ac\ub514",
            ),
            "OTT": (
                "\ub137\ud50c\ub9ad\uc2a4",
                "\ud2f0\ube59",
                "\ub514\uc988\ub2c8 \ud50c\ub7ec\uc2a4",
                "\uc6e8\uc774\ube0c",
                "\ub77c\ud504\ud154",
            ),
        }
        for type_name, source_names in seeded_sources_by_type.items():
            for source_name in source_names:
                cursor.execute(
                    """
                    WITH selected_type AS (
                        SELECT id
                        FROM content_types
                        WHERE name = %s
                    )
                    INSERT INTO content_sources (type_id, name)
                    SELECT id, %s
                    FROM selected_type
                    ON CONFLICT DO NOTHING
                    """,
                    (type_name, source_name),
                )
        print("LOG: [DB Setup] Creating 'admin_content_overrides' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_content_overrides (
            id SERIAL PRIMARY KEY,
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            override_status TEXT NOT NULL,
            override_completed_at TIMESTAMP,
            reason TEXT,
            admin_id INTEGER NOT NULL REFERENCES users(id),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(content_id, source)
        )""")
        print("LOG: [DB Setup] 'admin_content_overrides' table created or already exists.")
        ensure_updated_at_trigger(
            cursor,
            "admin_content_overrides",
            "trg_admin_content_overrides_updated_at",
        )

        print("LOG: [DB Setup] Creating 'admin_content_metadata' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_content_metadata (
            id SERIAL PRIMARY KEY,
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            public_at TIMESTAMP NULL,
            reason TEXT NULL,
            admin_id INTEGER NOT NULL REFERENCES users(id),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(content_id, source)
        )""")
        print("LOG: [DB Setup] 'admin_content_metadata' table created or already exists.")
        ensure_updated_at_trigger(
            cursor,
            "admin_content_metadata",
            "trg_admin_content_metadata_updated_at",
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_admin_content_metadata_public_at ON admin_content_metadata (public_at)"
        )

        print("LOG: [DB Setup] Creating 'admin_action_logs' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_action_logs (
            id SERIAL PRIMARY KEY,
            admin_id INTEGER NOT NULL REFERENCES users(id),
            action_type TEXT NOT NULL,
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NULL,
            payload JSONB NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )""")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_admin_action_logs_created_at ON admin_action_logs (created_at DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_admin_action_logs_content ON admin_action_logs (content_id, source)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_admin_action_logs_admin ON admin_action_logs (admin_id, created_at DESC)"
        )
        print("LOG: [DB Setup] 'admin_action_logs' table created or already exists.")

        print("LOG: [DB Setup] Creating 'cdc_events' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cdc_events (
            id SERIAL PRIMARY KEY,
            content_id TEXT NOT NULL,
            source TEXT NOT NULL,
            event_type TEXT NOT NULL,
            final_status TEXT NOT NULL,
            final_completed_at TIMESTAMP NULL,
            resolved_by TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT now(),
            UNIQUE(content_id, source, event_type)
        )
        """)
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cdc_events_source_created_at ON cdc_events (source, created_at)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cdc_events_created_at ON cdc_events (created_at DESC)"
        )
        print("LOG: [DB Setup] 'cdc_events' table created or already exists.")

        current_step = "create cdc_event_consumptions table"
        print("LOG: [DB Setup] Creating 'cdc_event_consumptions' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cdc_event_consumptions (
            id SERIAL PRIMARY KEY,
            consumer TEXT NOT NULL,
            event_id INTEGER NOT NULL REFERENCES cdc_events(id),
            status TEXT NOT NULL,
            reason TEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE(consumer, event_id),
            CONSTRAINT cdc_event_consumptions_status_check
                CHECK (status IN ('processed', 'skipped', 'failed'))
        )
        """)
        ensure_column_exists(cursor, "cdc_event_consumptions", "consumer", "TEXT")
        ensure_column_exists(cursor, "cdc_event_consumptions", "event_id", "INTEGER")
        ensure_column_exists(cursor, "cdc_event_consumptions", "status", "TEXT")
        ensure_column_exists(cursor, "cdc_event_consumptions", "reason", "TEXT")
        ensure_column_exists(cursor, "cdc_event_consumptions", "created_at", "TIMESTAMP")
        ensure_column_default(cursor, "cdc_event_consumptions", "created_at", "NOW()")
        ensure_column_not_null(cursor, "cdc_event_consumptions", "consumer")
        ensure_column_not_null(cursor, "cdc_event_consumptions", "event_id")
        ensure_column_not_null(cursor, "cdc_event_consumptions", "status")
        ensure_column_not_null(cursor, "cdc_event_consumptions", "created_at")
        cursor.execute(
            """
            DO $$
            DECLARE
                has_unique_constraint BOOLEAN;
                has_named_index BOOLEAN;
                named_index_is_unique BOOLEAN;
                constraint_name TEXT;
            BEGIN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE t.relname = 'cdc_event_consumptions'
                      AND n.nspname = current_schema()
                      AND c.contype = 'u'
                      AND c.conkey @> (
                          SELECT array_agg(attnum ORDER BY attnum)
                          FROM pg_attribute
                          WHERE attrelid = t.oid
                            AND attname IN ('consumer', 'event_id')
                      )
                      AND array_length(c.conkey, 1) = 2
                ) INTO has_unique_constraint;

                IF has_unique_constraint THEN
                    RETURN;
                END IF;

                SELECT EXISTS (
                    SELECT 1
                    FROM pg_class cls
                    JOIN pg_namespace n ON n.oid = cls.relnamespace
                    WHERE cls.relname = 'cdc_event_consumptions_consumer_event_id_key'
                      AND n.nspname = current_schema()
                ) INTO has_named_index;

                SELECT COALESCE(i.indisunique, FALSE)
                INTO named_index_is_unique
                FROM pg_class cls
                JOIN pg_index i ON i.indexrelid = cls.oid
                JOIN pg_namespace n ON n.oid = cls.relnamespace
                WHERE cls.relname = 'cdc_event_consumptions_consumer_event_id_key'
                  AND n.nspname = current_schema();

                IF has_named_index AND named_index_is_unique THEN
                    BEGIN
                        ALTER TABLE cdc_event_consumptions
                        ADD CONSTRAINT cdc_event_consumptions_consumer_event_id_key
                        UNIQUE USING INDEX cdc_event_consumptions_consumer_event_id_key;
                    EXCEPTION
                        WHEN duplicate_object OR duplicate_table THEN NULL;
                    END;
                ELSE
                    IF has_named_index AND NOT named_index_is_unique THEN
                        constraint_name := 'cdc_event_consumptions_consumer_event_id_uniq';
                    ELSE
                        constraint_name := 'cdc_event_consumptions_consumer_event_id_key';
                    END IF;

                    BEGIN
                        EXECUTE format(
                            'ALTER TABLE cdc_event_consumptions ADD CONSTRAINT %I UNIQUE (consumer, event_id)',
                            constraint_name
                        );
                    EXCEPTION
                        WHEN duplicate_object OR duplicate_table THEN NULL;
                    END;
                END IF;
            EXCEPTION
                WHEN duplicate_object OR duplicate_table THEN NULL;
            END $$;
            """
        )
        cursor.execute(
            """
            DO $$
            BEGIN
                ALTER TABLE cdc_event_consumptions
                ADD CONSTRAINT cdc_event_consumptions_event_id_fkey
                FOREIGN KEY (event_id) REFERENCES cdc_events(id);
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """
        )
        cursor.execute(
            """
            DO $$
            BEGIN
                ALTER TABLE cdc_event_consumptions
                ADD CONSTRAINT cdc_event_consumptions_status_check
                CHECK (status IN ('processed', 'skipped', 'failed'));
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cdc_event_consumptions_consumer_created_at
            ON cdc_event_consumptions (consumer, created_at DESC)
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cdc_event_consumptions_event_id ON cdc_event_consumptions (event_id)"
        )
        print("LOG: [DB Setup] 'cdc_event_consumptions' table created or already exists.")
        # === Table for crawler daily summary/report snapshots ===
        print("LOG: [DB Setup] Creating 'daily_crawler_reports' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_crawler_reports (
            id SERIAL PRIMARY KEY,
            crawler_name TEXT NOT NULL,
            status TEXT NOT NULL,
            report_data JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )""")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_crawler_reports_created_at ON daily_crawler_reports (created_at DESC)"
        )
        print("LOG: [DB Setup] 'daily_crawler_reports' table created or already exists.")
        # ================================================

        current_step = "commit remaining schema"
        print("LOG: [DB Setup] Committing remaining schema phase...")
        conn.commit()
        print("LOG: [DB Setup] Remaining schema phase committed.")

        current_step = "maintenance"

        current_step = "enable pg_trgm extension"
        print("LOG: [DB Setup] Enabling 'pg_trgm' extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        current_step = "create idx_contents_title_trgm"
        print("LOG: [DB Setup] Creating GIN index on contents.title...")
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_title_trgm",
            """
            CREATE INDEX idx_contents_title_trgm
            ON contents
            USING gin (title gin_trgm_ops);
            """,
        )

        current_step = "create idx_contents_authors_trgm"
        print("LOG: [DB Setup] Creating GIN index on contents meta authors...")
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_authors_trgm",
            """
            CREATE INDEX idx_contents_authors_trgm
            ON contents
            USING gin ((COALESCE(meta->'common'->>'authors', '')) gin_trgm_ops);
            """,
        )

        current_step = "create idx_contents_normalized_title_trgm"
        print("LOG: [DB Setup] Creating GIN index on contents.normalized_title...")
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_normalized_title_trgm",
            """
            CREATE INDEX idx_contents_normalized_title_trgm
            ON contents
            USING gin (normalized_title gin_trgm_ops);
            """,
        )

        current_step = "create idx_contents_normalized_authors_trgm"
        print("LOG: [DB Setup] Creating GIN index on contents.normalized_authors...")
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_normalized_authors_trgm",
            """
            CREATE INDEX idx_contents_normalized_authors_trgm
            ON contents
            USING gin (normalized_authors gin_trgm_ops);
            """,
        )

        current_step = "create active contents browse indexes"
        print("LOG: [DB Setup] Creating active browse list indexes...")
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_active_type_status_title_source_id",
            """
            CREATE INDEX idx_contents_active_type_status_title_source_id
            ON contents (content_type, status, title, source, content_id)
            WHERE COALESCE(is_deleted, FALSE) = FALSE;
            """,
        )
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_active_type_title_source_id",
            """
            CREATE INDEX idx_contents_active_type_title_source_id
            ON contents (content_type, title, source, content_id)
            WHERE COALESCE(is_deleted, FALSE) = FALSE;
            """,
        )
        create_index_if_missing(
            cursor,
            "public",
            "idx_contents_weekdays_gin",
            """
            CREATE INDEX idx_contents_weekdays_gin
            ON contents
            USING gin ((meta->'attributes'->'weekdays'));
            """,
        )

        current_step = "backfill normalized search fields"
        print("LOG: [DB Setup] Backfilling normalized search fields (idempotent)...")
        cursor.execute(
            """
            UPDATE contents
            SET normalized_title = regexp_replace(lower(COALESCE(title, '')), '\\s+', '', 'g')
            WHERE normalized_title IS NULL OR normalized_title = '';
            """
        )
        cursor.execute(
            """
            UPDATE contents
            SET normalized_authors = regexp_replace(lower(COALESCE(meta->'common'->>'authors', '')), '\\s+', '', 'g')
            WHERE normalized_authors IS NULL OR normalized_authors = '';
            """
        )

        print("LOG: [DB Setup] 'pg_trgm' setup complete.")

        current_step = "commit maintenance"
        print("LOG: [DB Setup] Committing changes...")
        conn.commit()
        print("LOG: [DB Setup] Changes committed.")
    except psycopg2.Error as e:
        print(
            "FATAL: [DB Setup] A database error occurred "
            f"during step '{current_step}': {e}",
            file=sys.stderr,
        )
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        if is_lock_timeout_error(e) or is_statement_timeout_error(e):
            print(
                "ERROR: [DB Setup] Migration failed due to lock/statement timeout. "
                f"Step: '{current_step}'.",
                file=sys.stderr,
            )
            print_lock_diagnostics(conn)
        raise
    except Exception as e:
        print(
            "FATAL: [DB Setup] An unexpected error occurred "
            f"during step '{current_step}': {e}",
            file=sys.stderr,
        )
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if advisory_lock_acquired and conn:
            try:
                unlocked = release_init_advisory_lock(conn)
                if unlocked:
                    print("LOG: [DB Setup] Advisory lock released.")
                else:
                    print(
                        "WARN: [DB Setup] Advisory lock was not held during release.",
                        file=sys.stderr,
                    )
            except Exception as unlock_exc:
                print(
                    f"WARN: [DB Setup] Failed to release advisory lock: {unlock_exc}",
                    file=sys.stderr,
                )
        if conn:
            conn.close()
            print("LOG: [DB Setup] Connection closed.")
