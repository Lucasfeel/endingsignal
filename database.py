# database.py

import psycopg2
import psycopg2.extras
from flask import g
from contextlib import contextmanager
import os
import sys

def _create_connection():
    """
    환경 변수를 기반으로 새로운 데이터베이스 연결을 생성합니다.
    DATABASE_URL이 있으면 우선 사용하고, 없으면 개별 변수를 사용합니다.
    """
    db_timezone = os.environ.get('DB_TIMEZONE', '').strip() or 'Asia/Seoul'
    options = f"-c timezone={db_timezone}"
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url, options=options)

    # 로컬 개발 환경을 위한 개별 변수 확인
    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']
    if not all(os.environ.get(var) for var in required_vars):
        raise ValueError("로컬 개발을 위해서는 DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT 환경 변수가 모두 필요합니다.")

    return psycopg2.connect(
        dbname=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT'),
        options=options
    )

def get_db():
    """Application Context 내에서 유일한 DB 연결을 가져옵니다."""
    if 'db' not in g:
        g.db = _create_connection()
    return g.db

def get_cursor(db):
    """지정된 DB 연결로부터 DictCursor를 반환합니다."""
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
    """요청(request)이 끝나면 자동으로 호출되어 DB 연결을 닫습니다."""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def create_standalone_connection():
    """Flask 컨텍스트 없이 독립적인 DB 연결을 생성합니다."""
    return _create_connection()

def setup_database_standalone():
    """독립 실행형 스크립트에서 테이블을 생성합니다."""
    conn = None
    try:
        print("LOG: [DB Setup] Attempting to connect to the database...")
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        print("LOG: [DB Setup] Connection successful.")

        # print("LOG: [DB Setup] Dropping existing tables (if any)...")
        # # cursor.execute("DROP TABLE IF EXISTS subscriptions;")
        # cursor.execute("DROP TABLE IF EXISTS contents;")
        # print("LOG: [DB Setup] Tables dropped.")

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

        print("LOG: [DB Setup] Ensuring normalized search columns exist...")
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS normalized_title TEXT"
        )
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS normalized_authors TEXT"
        )

        print("LOG: [DB Setup] Ensuring soft-delete columns exist...")
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN"
        )
        cursor.execute(
            "ALTER TABLE contents ALTER COLUMN is_deleted SET DEFAULT FALSE"
        )
        cursor.execute(
            """
            UPDATE contents
            SET is_deleted = FALSE
            WHERE is_deleted IS NULL;
            """
        )
        cursor.execute(
            "ALTER TABLE contents ALTER COLUMN is_deleted SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
        )
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS deleted_reason TEXT"
        )
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS deleted_by INTEGER"
        )

        print("LOG: [DB Setup] Ensuring contents timestamps exist...")
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS created_at TIMESTAMP"
        )
        cursor.execute(
            "ALTER TABLE contents ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"
        )
        cursor.execute(
            "ALTER TABLE contents ALTER COLUMN created_at SET DEFAULT NOW()"
        )
        cursor.execute(
            "ALTER TABLE contents ALTER COLUMN updated_at SET DEFAULT NOW()"
        )
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
        cursor.execute(
            "ALTER TABLE contents ALTER COLUMN created_at SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE contents ALTER COLUMN updated_at SET NOT NULL"
        )
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
        cursor.execute(
            "DROP TRIGGER IF EXISTS trg_contents_updated_at ON contents;"
        )
        cursor.execute(
            """
            CREATE TRIGGER trg_contents_updated_at
            BEFORE UPDATE ON contents
            FOR EACH ROW
            EXECUTE PROCEDURE set_updated_at();
            """
        )

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

        print("LOG: [DB Setup] Ensuring 'users.updated_at' column exists and has defaults...")
        cursor.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;
            """
        )
        cursor.execute(
            "ALTER TABLE users ALTER COLUMN updated_at SET DEFAULT NOW();"
        )
        cursor.execute(
            """
            UPDATE users
            SET updated_at = COALESCE(created_at, NOW())
            WHERE updated_at IS NULL;
            """
        )
        cursor.execute(
            "DROP TRIGGER IF EXISTS trg_users_updated_at ON users;"
        )
        cursor.execute(
            """
            CREATE TRIGGER trg_users_updated_at
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE PROCEDURE set_updated_at();
            """
        )

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
        print("LOG: [DB Setup] Ensuring subscription alert flags exist...")
        cursor.execute(
            "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS wants_completion BOOLEAN NOT NULL DEFAULT FALSE"
        )
        cursor.execute(
            "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS wants_publication BOOLEAN NOT NULL DEFAULT FALSE"
        )
        cursor.execute(
            """
            UPDATE subscriptions
            SET wants_completion = TRUE
            WHERE wants_completion = FALSE AND wants_publication = FALSE;
            """
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
        cursor.execute(
            "DROP TRIGGER IF EXISTS trg_admin_content_overrides_updated_at ON admin_content_overrides;"
        )
        cursor.execute(
            """
            CREATE TRIGGER trg_admin_content_overrides_updated_at
            BEFORE UPDATE ON admin_content_overrides
            FOR EACH ROW
            EXECUTE PROCEDURE set_updated_at();
            """
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
        cursor.execute(
            "DROP TRIGGER IF EXISTS trg_admin_content_metadata_updated_at ON admin_content_metadata;"
        )
        cursor.execute(
            """
            CREATE TRIGGER trg_admin_content_metadata_updated_at
            BEFORE UPDATE ON admin_content_metadata
            FOR EACH ROW
            EXECUTE PROCEDURE set_updated_at();
            """
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
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ADD COLUMN IF NOT EXISTS consumer TEXT"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ADD COLUMN IF NOT EXISTS event_id INTEGER"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ADD COLUMN IF NOT EXISTS status TEXT"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ADD COLUMN IF NOT EXISTS reason TEXT"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ALTER COLUMN created_at SET DEFAULT NOW()"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ALTER COLUMN consumer SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ALTER COLUMN event_id SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ALTER COLUMN status SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE cdc_event_consumptions ALTER COLUMN created_at SET NOT NULL"
        )
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

        # === 🚨 [신규] 통합 보고서 저장을 위한 테이블 생성 ===
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

        print("LOG: [DB Setup] Enabling 'pg_trgm' extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        print("LOG: [DB Setup] Creating GIN index on contents.title...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contents_title_trgm
            ON contents
            USING gin (title gin_trgm_ops);
        """)

        print("LOG: [DB Setup] Creating GIN index on contents meta authors...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contents_authors_trgm
            ON contents
            USING gin ((COALESCE(meta->'common'->>'authors', '')) gin_trgm_ops);
        """)

        print("LOG: [DB Setup] Creating GIN index on contents.normalized_title...")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_contents_normalized_title_trgm
            ON contents
            USING gin (normalized_title gin_trgm_ops);
            """
        )

        print("LOG: [DB Setup] Creating GIN index on contents.normalized_authors...")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_contents_normalized_authors_trgm
            ON contents
            USING gin (normalized_authors gin_trgm_ops);
            """
        )

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

        print("LOG: [DB Setup] Committing changes...")
        conn.commit()
        print("LOG: [DB Setup] Changes committed.")

        cursor.close()
    except psycopg2.Error as e:
        print(f"FATAL: [DB Setup] A database error occurred: {e}", file=sys.stderr)
        # Re-raise the exception to ensure the script exits with a non-zero status code
        raise
    except Exception as e:
        print(f"FATAL: [DB Setup] An unexpected error occurred: {e}", file=sys.stderr)
        raise
    finally:
        if conn:
            conn.close()
            print("LOG: [DB Setup] Connection closed.")
