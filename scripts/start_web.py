import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


load_dotenv()


TRUTHY_VALUES = {"1", "true", "yes", "y", "on"}
REQUIRED_DB_ENV_VARS = ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT")


def is_truthy(value):
    if value is None:
        return False
    return str(value).strip().lower() in TRUTHY_VALUES


def has_database_config():
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if database_url:
        return True
    return all((os.getenv(key) or "").strip() for key in REQUIRED_DB_ENV_VARS)


def should_run_db_init():
    if is_truthy(os.getenv("SKIP_DB_INIT")):
        return False
    if is_truthy(os.getenv("RUN_DB_INIT")):
        return True
    return has_database_config()


def build_db_init_env():
    env = os.environ.copy()
    explicit_backfill = env.get("DB_INIT_ENABLE_BACKFILL")
    if explicit_backfill is not None and str(explicit_backfill).strip():
        return env

    env["DB_INIT_ENABLE_BACKFILL"] = "1" if is_truthy(env.get("RUN_DB_INIT_WITH_BACKFILL")) else "0"
    return env


def run_db_init():
    init_env = build_db_init_env()
    print(
        "[startup] Running database init: python init_db.py "
        f"(DB_INIT_ENABLE_BACKFILL={init_env.get('DB_INIT_ENABLE_BACKFILL', '')})"
    )
    subprocess.run([sys.executable, "init_db.py"], check=True, env=init_env)


def resolve_bind():
    port = (os.getenv("PORT") or "5000").strip()
    bind = (os.getenv("GUNICORN_BIND") or f"0.0.0.0:{port}").strip()
    if ":" in bind:
        host, resolved_port = bind.rsplit(":", 1)
        return (host or "0.0.0.0").strip(), (resolved_port or port).strip(), bind
    return bind, port, bind


def build_gunicorn_command():
    _host, _port, bind = resolve_bind()
    workers = (os.getenv("WEB_CONCURRENCY") or "2").strip()
    timeout = (os.getenv("GUNICORN_TIMEOUT") or "120").strip()

    return [
        "gunicorn",
        "app:app",
        "--bind",
        bind,
        "--workers",
        workers,
        "--timeout",
        timeout,
    ]


def run_windows_dev_server():
    from app import app

    host, port, _bind = resolve_bind()
    app.run(host=host, port=int(port), debug=False, use_reloader=False)


def main():
    if should_run_db_init():
        run_db_init()
    else:
        print("[startup] Skipping database init (no DB config or SKIP_DB_INIT=1).")

    if os.name == "nt":
        print("[startup] Windows detected; using Flask development server instead of gunicorn.")
        run_windows_dev_server()
        return
    else:
        command = build_gunicorn_command()
    print("[startup] Starting web server:", " ".join(command))
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
