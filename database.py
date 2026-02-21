# database.py

import os
import random
import re
import sys
import time
from contextlib import contextmanager
from typing import Callable

import psycopg2
import psycopg2.extras
from flask import g
from psycopg2 import sql

REQUIRED_DB_ENV_VARS = ('DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT')
DB_INIT_APPLICATION_NAME = "endingsignal_init_db"
DB_INIT_ADVISORY_LOCK_NAME = "endingsignal:init_db"
DEFAULT_DB_INIT_LOCK_TIMEOUT = "5s"
DEFAULT_DB_INIT_ADVISORY_LOCK_WAIT_SECONDS = 60.0
DEFAULT_DB_INIT_DDL_RETRY_ATTEMPTS = 5
DEFAULT_DB_INIT_DDL_RETRY_BASE_DELAY_SECONDS = 1.0
DEFAULT_DB_INIT_STALE_DDL_MAX_AGE_SECONDS = 300
DEFAULT_DB_INIT_STALE_DDL_CLEANUP_ACTION = "cancel"
DEFAULT_DB_INIT_BACKFILL_BATCH_SIZE = 20000
DEFAULT_DB_INIT_STRICT_MAINTENANCE = False
PG_TIMEOUT_LITERAL_PATTERN = re.compile(r"^\d+(?:ms|s|min|h|d)?$", re.IGNORECASE)


class DatabaseUnavailableError(ValueError):
    """Raised when no usable database configuration is present."""


def has_database_config():
    database_url = (os.environ.get('DATABASE_URL') or '').strip()
    if database_url:
        return True
    return all((os.environ.get(var) or '').strip() for var in REQUIRED_DB_ENV_VARS)


def _read_pg_timeout_literal(env_name):
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return None
    if PG_TIMEOUT_LITERAL_PATTERN.fullmatch(raw):
        return raw
    print(
        f"WARN: Invalid {env_name} '{raw}'. Ignoring value.",
        file=sys.stderr,
    )
    return None


def _build_connection_kwargs():
    db_timezone = os.environ.get('DB_TIMEZONE', '').strip() or 'Asia/Seoul'
    option_parts = [f"-c timezone={db_timezone}"]

    idle_in_tx_timeout = _read_pg_timeout_literal("DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT")
    if idle_in_tx_timeout:
        option_parts.append(f"-c idle_in_transaction_session_timeout={idle_in_tx_timeout}")

    statement_timeout = _read_pg_timeout_literal("DB_STATEMENT_TIMEOUT")
    if statement_timeout:
        option_parts.append(f"-c statement_timeout={statement_timeout}")

    kwargs = {"options": " ".join(option_parts)}
    application_name = (os.environ.get("DB_APPLICATION_NAME") or "").strip()
    if application_name:
        kwargs["application_name"] = application_name
    return kwargs

def _create_connection():
    """
    Create a new database connection from environment configuration.
    Prefer DATABASE_URL, otherwise use individual DB_* variables.
    """
    connection_kwargs = _build_connection_kwargs()
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url, **connection_kwargs)

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
        **connection_kwargs
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


def _read_float_env(name, default, minimum=None):
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
        if minimum is not None and value < minimum:
            raise ValueError(f"must be >= {minimum}")
        return value
    except ValueError:
        print(
            f"WARN: [DB Setup] Invalid {name} '{raw}'. Using default {default}.",
            file=sys.stderr,
        )
        return default


def _read_int_env(name, default, minimum=None):
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        if minimum is not None and value < minimum:
            raise ValueError(f"must be >= {minimum}")
        return value
    except ValueError:
        print(
            f"WARN: [DB Setup] Invalid {name} '{raw}'. Using default {default}.",
            file=sys.stderr,
        )
        return default


def _read_bool_env(name, default):
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    print(
        f"WARN: [DB Setup] Invalid {name} '{raw}'. Using default {default}.",
        file=sys.stderr,
    )
    return default


def _read_db_init_timeouts():
    lock_timeout = (
        os.environ.get("DB_INIT_LOCK_TIMEOUT", DEFAULT_DB_INIT_LOCK_TIMEOUT).strip()
        or DEFAULT_DB_INIT_LOCK_TIMEOUT
    )
    statement_timeout = (os.environ.get("DB_INIT_STATEMENT_TIMEOUT") or "").strip() or None
    advisory_wait_seconds = _read_float_env(
        "DB_INIT_ADVISORY_LOCK_WAIT_SECONDS",
        DEFAULT_DB_INIT_ADVISORY_LOCK_WAIT_SECONDS,
        minimum=0.0,
    )
    return lock_timeout, statement_timeout, advisory_wait_seconds


def _read_db_init_settings():
    lock_timeout, statement_timeout, advisory_wait_seconds = _read_db_init_timeouts()
    cleanup_action = (
        os.environ.get("DB_INIT_STALE_DDL_CLEANUP_ACTION")
        or DEFAULT_DB_INIT_STALE_DDL_CLEANUP_ACTION
    ).strip().lower()
    if cleanup_action not in {"cancel", "terminate"}:
        print(
            "WARN: [DB Setup] Invalid DB_INIT_STALE_DDL_CLEANUP_ACTION "
            f"'{cleanup_action}'. Using default '{DEFAULT_DB_INIT_STALE_DDL_CLEANUP_ACTION}'.",
            file=sys.stderr,
        )
        cleanup_action = DEFAULT_DB_INIT_STALE_DDL_CLEANUP_ACTION

    return {
        "lock_timeout": lock_timeout,
        "statement_timeout": statement_timeout,
        "advisory_wait_seconds": advisory_wait_seconds,
        "ddl_retry_attempts": _read_int_env(
            "DB_INIT_DDL_RETRY_ATTEMPTS",
            DEFAULT_DB_INIT_DDL_RETRY_ATTEMPTS,
            minimum=1,
        ),
        "ddl_retry_base_delay_seconds": _read_float_env(
            "DB_INIT_DDL_RETRY_BASE_DELAY_SECONDS",
            DEFAULT_DB_INIT_DDL_RETRY_BASE_DELAY_SECONDS,
            minimum=0.0,
        ),
        "stale_ddl_max_age_seconds": _read_int_env(
            "DB_INIT_STALE_DDL_MAX_AGE_SECONDS",
            DEFAULT_DB_INIT_STALE_DDL_MAX_AGE_SECONDS,
            minimum=1,
        ),
        "stale_ddl_cleanup_action": cleanup_action,
        "backfill_batch_size": _read_int_env(
            "DB_INIT_BACKFILL_BATCH_SIZE",
            DEFAULT_DB_INIT_BACKFILL_BATCH_SIZE,
            minimum=1,
        ),
        "strict_maintenance": _read_bool_env(
            "DB_INIT_STRICT_MAINTENANCE",
            DEFAULT_DB_INIT_STRICT_MAINTENANCE,
        ),
    }


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


def table_exists(cursor, table, schema='public'):
    cursor.execute(
        "SELECT to_regclass(%s)",
        (f"{schema}.{table}",),
    )
    row = cursor.fetchone()
    return bool(row and row[0])


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


def _truncate_query(query_text, max_len=200):
    if query_text is None:
        return ""
    query = str(query_text)
    if len(query) <= max_len:
        return query
    return f"{query[:max_len]}..."


def find_stale_ddl_waiters(conn, max_age_seconds):
    if not conn:
        return []

    ddl_patterns = [
        "ALTER TABLE %",
        "CREATE INDEX %",
        "DROP TABLE %",
        "CREATE EXTENSION %",
    ]

    try:
        conn.rollback()
    except Exception:
        pass

    with managed_cursor(conn) as cursor:
        cursor.execute(
            """
            SELECT
                pid,
                usename,
                application_name,
                state,
                wait_event_type,
                wait_event,
                xact_start,
                query_start,
                age(now(), xact_start) AS xact_age,
                age(now(), query_start) AS query_age,
                LEFT(COALESCE(query, ''), 200) AS query_text
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND wait_event_type = 'Lock'
              AND query_start IS NOT NULL
              AND query_start < now() - (%s * interval '1 second')
              -- Keep wildcard patterns parameterized. Raw '%' in query text can
              -- interfere with psycopg2 placeholder parsing.
              AND query ILIKE ANY (%s::text[])
            ORDER BY query_start ASC
            """,
            (max_age_seconds, ddl_patterns),
        )
        rows = cursor.fetchall() or []

    try:
        conn.rollback()
    except Exception:
        pass
    return rows


def cleanup_stale_ddl_waiters(conn, max_age_seconds=None, cleanup_action=None):
    if not conn:
        return
    if max_age_seconds is None or cleanup_action is None:
        settings = _read_db_init_settings()
        if max_age_seconds is None:
            max_age_seconds = settings["stale_ddl_max_age_seconds"]
        if cleanup_action is None:
            cleanup_action = settings["stale_ddl_cleanup_action"]

    try:
        stale_waiters = find_stale_ddl_waiters(conn, max_age_seconds)
    except Exception as exc:
        print(
            f"WARN: [DB Setup] Could not inspect stale DDL waiters: {exc}",
            file=sys.stderr,
        )
        return

    if not stale_waiters:
        print("LOG: [DB Setup] No stale DDL lock waiters found.")
        return

    print(
        "WARN: [DB Setup] Found stale DDL lock waiters "
        f"(count={len(stale_waiters)}, action={cleanup_action}).",
        file=sys.stderr,
    )
    for row in stale_waiters:
        pid = row["pid"]
        action_fn = "pg_terminate_backend" if cleanup_action == "terminate" else "pg_cancel_backend"
        try:
            with managed_cursor(conn) as cursor:
                cursor.execute(
                    sql.SQL("SELECT {}(%s)").format(sql.SQL(action_fn)),
                    (pid,),
                )
                action_row = cursor.fetchone()
                action_result = bool(action_row and action_row[0])
            print(
                "WARN: [DB Setup] Stale DDL waiter cleanup attempted: "
                f"pid={pid} app={row['application_name']} state={row['state']} "
                f"query_age={row['query_age']} action={cleanup_action} result={action_result} "
                f"query={_truncate_query(row['query_text'])}",
                file=sys.stderr,
            )
        except Exception as exc:
            print(
                "WARN: [DB Setup] Failed stale DDL waiter cleanup: "
                f"pid={pid} action={cleanup_action} error={exc}",
                file=sys.stderr,
            )
        finally:
            try:
                conn.rollback()
            except Exception:
                pass


def print_relation_lock_report(conn, relation='contents'):
    if not conn:
        return

    try:
        conn.rollback()
    except Exception:
        pass

    print(
        f"ERROR: [DB Setup] Relation lock report for '{relation}':",
        file=sys.stderr,
    )
    try:
        with managed_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT
                    a.pid,
                    l.granted,
                    l.mode,
                    a.state,
                    a.application_name,
                    a.wait_event_type,
                    a.wait_event,
                    age(now(), a.xact_start) AS xact_age,
                    age(now(), a.query_start) AS query_age,
                    pg_blocking_pids(a.pid) AS blocking_pids,
                    LEFT(COALESCE(a.query, ''), 200) AS query_text
                FROM pg_locks l
                JOIN pg_class c ON c.oid = l.relation
                JOIN pg_stat_activity a ON a.pid = l.pid
                WHERE c.relname = %s
                  AND a.datname = current_database()
                ORDER BY l.granted ASC, a.query_start ASC NULLS LAST, a.pid ASC
                """,
                (relation,),
            )
            rows = cursor.fetchall() or []

        if not rows:
            print(
                "ERROR: [DB Setup] No relation-lock rows found for "
                f"'{relation}' in current database.",
                file=sys.stderr,
            )
            return

        blocker_counts = {}
        for row in rows:
            print(
                "ERROR: [DB Setup] "
                f"pid={row['pid']} granted={row['granted']} mode={row['mode']} "
                f"state={row['state']} app={row['application_name']} "
                f"wait={row['wait_event_type']}/{row['wait_event']} "
                f"xact_age={row['xact_age']} query_age={row['query_age']} "
                f"blocking_pids={row['blocking_pids']} "
                f"query={_truncate_query(row['query_text'])}",
                file=sys.stderr,
            )
            for blocker_pid in (row["blocking_pids"] or []):
                blocker_counts[blocker_pid] = blocker_counts.get(blocker_pid, 0) + 1

        if not blocker_counts:
            print(
                "ERROR: [DB Setup] Blocker summary: no blocking pids reported.",
                file=sys.stderr,
            )
            return

        top_blockers = sorted(
            blocker_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
        blocker_pid_list = [pid for pid, _count in top_blockers]
        try:
            with managed_cursor(conn) as cursor:
                cursor.execute(
                    """
                    SELECT
                        pid,
                        state,
                        application_name,
                        age(now(), query_start) AS query_age,
                        LEFT(COALESCE(query, ''), 200) AS query_text
                    FROM pg_stat_activity
                    WHERE pid = ANY(%s)
                    ORDER BY pid ASC
                    """,
                    (blocker_pid_list,),
                )
                blocker_rows = cursor.fetchall() or []
            blocker_by_pid = {row["pid"]: row for row in blocker_rows}
        except Exception:
            blocker_by_pid = {}

        print("ERROR: [DB Setup] Blocker summary:", file=sys.stderr)
        for blocker_pid, waiter_count in top_blockers:
            info = blocker_by_pid.get(blocker_pid)
            if not info:
                print(
                    f"ERROR: [DB Setup] blocker_pid={blocker_pid} waiter_count={waiter_count}",
                    file=sys.stderr,
                )
                continue
            print(
                "ERROR: [DB Setup] "
                f"blocker_pid={blocker_pid} waiter_count={waiter_count} "
                f"state={info['state']} app={info['application_name']} "
                f"query_age={info['query_age']} "
                f"query={_truncate_query(info['query_text'])}",
                file=sys.stderr,
            )
    except Exception as exc:
        print(
            "ERROR: [DB Setup] Could not build relation lock report: "
            f"{exc}",
            file=sys.stderr,
        )


def run_ddl_with_retry(
    conn,
    cursor,
    label,
    execute_fn: Callable[[], None],
    *,
    ddl_retry_attempts,
    ddl_retry_base_delay_seconds,
    stale_ddl_max_age_seconds,
    stale_ddl_cleanup_action,
    relation='contents',
):
    attempt = 0
    while attempt < ddl_retry_attempts:
        attempt += 1
        try:
            execute_fn()
            conn.commit()
            if attempt > 1:
                print(
                    "LOG: [DB Setup] DDL retry succeeded "
                    f"(label={label}, attempt={attempt}/{ddl_retry_attempts})."
                )
            return
        except Exception as exc:
            if not is_lock_timeout_error(exc):
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

            try:
                conn.rollback()
            except Exception:
                pass
            print(
                "WARN: [DB Setup] DDL lock timeout "
                f"(label={label}, attempt={attempt}/{ddl_retry_attempts}): {exc}",
                file=sys.stderr,
            )
            print_relation_lock_report(conn, relation=relation)
            print_lock_diagnostics(conn)
            cleanup_stale_ddl_waiters(
                conn,
                max_age_seconds=stale_ddl_max_age_seconds,
                cleanup_action=stale_ddl_cleanup_action,
            )

            if attempt >= ddl_retry_attempts:
                raise

            backoff = ddl_retry_base_delay_seconds * (2 ** (attempt - 1))
            jitter = random.uniform(0.0, max(ddl_retry_base_delay_seconds, 0.01))
            sleep_seconds = backoff + jitter
            print(
                "WARN: [DB Setup] Retrying DDL after backoff "
                f"{sleep_seconds:.2f}s (label={label}).",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)


def ensure_column_exists_with_retry(
    conn,
    cursor,
    table,
    column,
    column_definition,
    *,
    settings,
    schema='public',
    relation='contents',
):
    if column_exists(cursor, table, column, schema=schema):
        print(
            "LOG: [DB Setup] Column "
            f"'{schema}.{table}.{column}' already exists. Skipping ALTER."
        )
        return False

    print("LOG: [DB Setup] Adding missing column " f"'{schema}.{table}.{column}'.")

    def _execute():
        cursor.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(column),
                sql.SQL(column_definition),
            )
        )

    run_ddl_with_retry(
        conn,
        cursor,
        f"add column {schema}.{table}.{column}",
        _execute,
        ddl_retry_attempts=settings["ddl_retry_attempts"],
        ddl_retry_base_delay_seconds=settings["ddl_retry_base_delay_seconds"],
        stale_ddl_max_age_seconds=settings["stale_ddl_max_age_seconds"],
        stale_ddl_cleanup_action=settings["stale_ddl_cleanup_action"],
        relation=relation,
    )
    return True


def ensure_column_default_with_retry(
    conn,
    cursor,
    table,
    column,
    default_expression,
    *,
    settings,
    schema='public',
    relation='contents',
):
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

    def _execute():
        cursor.execute(
            sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(column),
                sql.SQL(default_expression),
            )
        )

    run_ddl_with_retry(
        conn,
        cursor,
        f"set default {schema}.{table}.{column}",
        _execute,
        ddl_retry_attempts=settings["ddl_retry_attempts"],
        ddl_retry_base_delay_seconds=settings["ddl_retry_base_delay_seconds"],
        stale_ddl_max_age_seconds=settings["stale_ddl_max_age_seconds"],
        stale_ddl_cleanup_action=settings["stale_ddl_cleanup_action"],
        relation=relation,
    )
    return True


def ensure_column_not_null_with_retry(
    conn,
    cursor,
    table,
    column,
    *,
    settings,
    schema='public',
    relation='contents',
):
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

    def _execute():
        cursor.execute(
            sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(column),
            )
        )

    run_ddl_with_retry(
        conn,
        cursor,
        f"set not null {schema}.{table}.{column}",
        _execute,
        ddl_retry_attempts=settings["ddl_retry_attempts"],
        ddl_retry_base_delay_seconds=settings["ddl_retry_base_delay_seconds"],
        stale_ddl_max_age_seconds=settings["stale_ddl_max_age_seconds"],
        stale_ddl_cleanup_action=settings["stale_ddl_cleanup_action"],
        relation=relation,
    )
    return True


def ensure_updated_at_trigger_with_retry(
    conn,
    cursor,
    table,
    trigger_name,
    *,
    settings,
    schema='public',
    relation='contents',
):
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

    def _execute():
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

    run_ddl_with_retry(
        conn,
        cursor,
        f"create trigger {trigger_name} on {schema}.{table}",
        _execute,
        ddl_retry_attempts=settings["ddl_retry_attempts"],
        ddl_retry_base_delay_seconds=settings["ddl_retry_base_delay_seconds"],
        stale_ddl_max_age_seconds=settings["stale_ddl_max_age_seconds"],
        stale_ddl_cleanup_action=settings["stale_ddl_cleanup_action"],
        relation=relation,
    )
    return True


def run_contents_backfill_in_batches(conn, cursor, *, settings):
    batch_size = settings["backfill_batch_size"]
    strict_maintenance = settings["strict_maintenance"]
    updates = [
        (
            "backfill contents.is_deleted",
            """
            UPDATE contents
            SET is_deleted = FALSE
            WHERE ctid IN (
                SELECT ctid
                FROM contents
                WHERE is_deleted IS NULL
                LIMIT %s
            )
            """,
        ),
        (
            "backfill contents.created_at",
            """
            UPDATE contents
            SET created_at = NOW()
            WHERE ctid IN (
                SELECT ctid
                FROM contents
                WHERE created_at IS NULL
                LIMIT %s
            )
            """,
        ),
        (
            "backfill contents.updated_at",
            """
            UPDATE contents
            SET updated_at = NOW()
            WHERE ctid IN (
                SELECT ctid
                FROM contents
                WHERE updated_at IS NULL
                LIMIT %s
            )
            """,
        ),
    ]

    for label, batch_sql in updates:
        total_updated = 0
        while True:
            try:
                cursor.execute(batch_sql, (batch_size,))
                updated = cursor.rowcount or 0
                conn.commit()
                total_updated += updated
                if updated == 0:
                    print(
                        "LOG: [DB Setup] Contents backfill complete "
                        f"({label}, total_updated={total_updated})."
                    )
                    break
            except psycopg2.Error as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if is_lock_timeout_error(exc) or is_statement_timeout_error(exc):
                    message = (
                        "WARN: [DB Setup] Contents backfill timed out "
                        f"({label}): {exc}"
                    )
                    if strict_maintenance:
                        print(message, file=sys.stderr)
                        raise
                    print(f"{message}. Continuing because DB_INIT_STRICT_MAINTENANCE=false.", file=sys.stderr)
                    break
                raise
def setup_database_standalone():
    """Create and reconcile tables for standalone bootstrap/migration scripts."""
    conn = None
    cursor = None
    advisory_lock_acquired = False
    current_step = "startup"
    settings = _read_db_init_settings()
    try:
        current_step = "connect"
        print("LOG: [DB Setup] Attempting to connect to the database...")
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        print("LOG: [DB Setup] Connection successful.")

        current_step = "configure session"
        configure_db_init_session(
            cursor,
            lock_timeout=settings["lock_timeout"],
            statement_timeout=settings["statement_timeout"],
        )
        print(
            "LOG: [DB Setup] Session config applied "
            f"(application_name={DB_INIT_APPLICATION_NAME}, "
            f"lock_timeout={settings['lock_timeout']}, "
            f"statement_timeout={settings['statement_timeout'] or 'unset'})."
        )
        conn.commit()

        current_step = "acquire advisory lock"
        print(
            "LOG: [DB Setup] Waiting for DB init advisory lock "
            f"(max {settings['advisory_wait_seconds']:g}s)..."
        )
        if not acquire_init_advisory_lock(conn, settings["advisory_wait_seconds"]):
            raise RuntimeError(
                "Could not acquire DB init advisory lock within "
                f"{settings['advisory_wait_seconds']:g}s. Another migration may be running."
            )
        advisory_lock_acquired = True
        print("LOG: [DB Setup] Advisory lock acquired.")

        current_step = "cleanup stale ddl waiters preflight"
        try:
            stale_waiters = find_stale_ddl_waiters(
                conn,
                settings["stale_ddl_max_age_seconds"],
            )
            if stale_waiters:
                print(
                    "WARN: [DB Setup] Detected stale DDL waiters before schema DDL. "
                    f"count={len(stale_waiters)}",
                    file=sys.stderr,
                )
                print_relation_lock_report(conn, relation="contents")
            cleanup_stale_ddl_waiters(
                conn,
                max_age_seconds=settings["stale_ddl_max_age_seconds"],
                cleanup_action=settings["stale_ddl_cleanup_action"],
            )
        except Exception as preflight_exc:
            if settings["strict_maintenance"]:
                raise
            print(
                "WARN: [DB Setup] Preflight stale-waiter cleanup failed and will be skipped "
                "because DB_INIT_STRICT_MAINTENANCE=false: "
                f"{preflight_exc}",
                file=sys.stderr,
            )
        finally:
            try:
                conn.rollback()
            except Exception:
                pass

        current_step = "contents schema ddl-only"

        current_step = "create contents table"
        if table_exists(cursor, "contents"):
            print("LOG: [DB Setup] 'contents' table already exists. Skipping CREATE TABLE.")
        else:
            print("LOG: [DB Setup] Creating 'contents' table...")
            run_ddl_with_retry(
                conn,
                cursor,
                "create contents table",
                lambda: cursor.execute(
                    """
                    CREATE TABLE contents (
                        content_id TEXT NOT NULL,
                        source TEXT NOT NULL,
                        content_type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        normalized_title TEXT,
                        normalized_authors TEXT,
                        status TEXT NOT NULL,
                        meta JSONB,
                        PRIMARY KEY (content_id, source)
                    )
                    """
                ),
                ddl_retry_attempts=settings["ddl_retry_attempts"],
                ddl_retry_base_delay_seconds=settings["ddl_retry_base_delay_seconds"],
                stale_ddl_max_age_seconds=settings["stale_ddl_max_age_seconds"],
                stale_ddl_cleanup_action=settings["stale_ddl_cleanup_action"],
                relation="contents",
            )
        print("LOG: [DB Setup] 'contents' table created or already exists.")

        current_step = "ensure contents normalized columns"
        print("LOG: [DB Setup] Ensuring normalized search columns exist...")
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "normalized_title",
            "TEXT",
            settings=settings,
            relation="contents",
        )
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "normalized_authors",
            "TEXT",
            settings=settings,
            relation="contents",
        )

        current_step = "ensure contents soft-delete columns"
        print("LOG: [DB Setup] Ensuring soft-delete columns exist...")
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "is_deleted",
            "BOOLEAN",
            settings=settings,
            relation="contents",
        )
        ensure_column_default_with_retry(
            conn,
            cursor,
            "contents",
            "is_deleted",
            "FALSE",
            settings=settings,
            relation="contents",
        )
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "deleted_at",
            "TIMESTAMP",
            settings=settings,
            relation="contents",
        )
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "deleted_reason",
            "TEXT",
            settings=settings,
            relation="contents",
        )
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "deleted_by",
            "INTEGER",
            settings=settings,
            relation="contents",
        )

        current_step = "ensure contents timestamps"
        print("LOG: [DB Setup] Ensuring contents timestamps exist...")
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "created_at",
            "TIMESTAMP",
            settings=settings,
            relation="contents",
        )
        ensure_column_exists_with_retry(
            conn,
            cursor,
            "contents",
            "updated_at",
            "TIMESTAMP",
            settings=settings,
            relation="contents",
        )
        ensure_column_default_with_retry(
            conn,
            cursor,
            "contents",
            "created_at",
            "NOW()",
            settings=settings,
            relation="contents",
        )
        ensure_column_default_with_retry(
            conn,
            cursor,
            "contents",
            "updated_at",
            "NOW()",
            settings=settings,
            relation="contents",
        )

        current_step = "ensure set_updated_at function"
        run_ddl_with_retry(
            conn,
            cursor,
            "create or replace set_updated_at function",
            lambda: cursor.execute(
                """
                CREATE OR REPLACE FUNCTION set_updated_at()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
            ),
            ddl_retry_attempts=settings["ddl_retry_attempts"],
            ddl_retry_base_delay_seconds=settings["ddl_retry_base_delay_seconds"],
            stale_ddl_max_age_seconds=settings["stale_ddl_max_age_seconds"],
            stale_ddl_cleanup_action=settings["stale_ddl_cleanup_action"],
            relation="contents",
        )
        ensure_updated_at_trigger_with_retry(
            conn,
            cursor,
            "contents",
            "trg_contents_updated_at",
            settings=settings,
            relation="contents",
        )

        current_step = "commit contents schema"
        # DDL statements above commit individually via run_ddl_with_retry.
        print("LOG: [DB Setup] Contents schema DDL phase complete.")

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

        current_step = "contents backfill and hardening"
        print("LOG: [DB Setup] Starting contents backfill/hardening phase...")
        run_contents_backfill_in_batches(conn, cursor, settings=settings)
        for column_name in ("is_deleted", "created_at", "updated_at"):
            try:
                ensure_column_not_null_with_retry(
                    conn,
                    cursor,
                    "contents",
                    column_name,
                    settings=settings,
                    relation="contents",
                )
            except psycopg2.Error as exc:
                if is_lock_timeout_error(exc) or is_statement_timeout_error(exc):
                    if settings["strict_maintenance"]:
                        raise
                    print(
                        "WARN: [DB Setup] Could not harden NOT NULL for "
                        f"contents.{column_name}: {exc}. "
                        "Continuing because DB_INIT_STRICT_MAINTENANCE=false.",
                        file=sys.stderr,
                    )
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue
                raise
        print("LOG: [DB Setup] Contents backfill/hardening phase complete.")

        current_step = "maintenance"
        try:
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
        except psycopg2.Error as maintenance_exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if (is_lock_timeout_error(maintenance_exc) or is_statement_timeout_error(maintenance_exc)) and not settings["strict_maintenance"]:
                print(
                    "WARN: [DB Setup] Maintenance phase timed out and will be skipped "
                    "because DB_INIT_STRICT_MAINTENANCE=false: "
                    f"{maintenance_exc}",
                    file=sys.stderr,
                )
                print_relation_lock_report(conn, relation="contents")
                print_lock_diagnostics(conn)
            else:
                raise
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
            print_relation_lock_report(conn, relation="contents")
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
