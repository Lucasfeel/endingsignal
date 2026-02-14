import os
import subprocess
import sys


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


def run_db_init():
    print("[startup] Running database init: python init_db.py")
    subprocess.run([sys.executable, "init_db.py"], check=True)


def build_gunicorn_command():
    port = (os.getenv("PORT") or "5000").strip()
    bind = (os.getenv("GUNICORN_BIND") or f"0.0.0.0:{port}").strip()
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


def main():
    if should_run_db_init():
        run_db_init()
    else:
        print("[startup] Skipping database init (no DB config or SKIP_DB_INIT=1).")

    command = build_gunicorn_command()
    print("[startup] Starting web server:", " ".join(command))
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
