from flask import Flask, render_template
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

import config
from database import close_db
from views.admin import admin_bp
from views.auth import auth_bp
from views.contents import contents_bp
from views.status import status_bp
from views.subscriptions import subscriptions_bp

app = Flask(__name__)
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


@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok"}, 200
