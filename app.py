from pathlib import Path

from flask import Flask, render_template
from flask_compress import Compress
from flask_cors import CORS
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR / ".env.sentry.local")
load_dotenv(Path.home() / ".codex" / f"sentry.{BASE_DIR.name}.env")
load_dotenv(Path.home() / ".codex" / "sentry.env")

import config
from database import close_db
from utils.perf import init_request_perf, log_request_perf
from utils.sentry_setup import frontend_template_context, init_sentry
from views.admin import admin_bp
from views.auth import auth_bp
from views.contents import contents_bp
from views.internal_verified_sync import internal_verified_sync_bp
from views.status import status_bp
from views.subscriptions import subscriptions_bp

init_sentry("endingsignal-api", "SENTRY_API_DSN", "SENTRY_DSN")

app = Flask(__name__)
Compress(app)
if config.CORS_ALLOW_ORIGINS:
    CORS(
        app,
        origins=config.CORS_ALLOW_ORIGINS,
        supports_credentials=config.CORS_SUPPORTS_CREDENTIALS,
    )
else:
    CORS(app)

app.register_blueprint(contents_bp)
app.register_blueprint(subscriptions_bp)
app.register_blueprint(status_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(internal_verified_sync_bp)


@app.context_processor
def inject_sentry_context():
    return frontend_template_context("endingsignal")


@app.before_request
def before_request_perf():
    init_request_perf()


@app.after_request
def after_request_perf(response):
    return log_request_perf(response)


@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok"}, 200
