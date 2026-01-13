# =====================================================================================
#  파일: app.py (웹 서버 및 API)
#  - [구조 개선] Flask Blueprint를 도입하여 API 라우트를 기능별 파일로 분리했습니다.
# =====================================================================================

from flask import Flask, render_template
from flask_cors import CORS
from dotenv import load_dotenv
import config

# --- 1. Blueprint 및 초기 설정 ---
# 환경 변수 로드
load_dotenv()

# Blueprint 임포트
from views.contents import contents_bp
from views.subscriptions import subscriptions_bp
from views.status import status_bp
from views.auth import auth_bp
from views.admin import admin_bp
from database import close_db

# --- 2. Flask 앱 생성 및 설정 ---
app = Flask(__name__)
if config.CORS_ALLOW_ORIGINS:
    CORS(
        app,
        origins=config.CORS_ALLOW_ORIGINS,
        supports_credentials=config.CORS_SUPPORTS_CREDENTIALS,
    )
else:
    CORS(app)

# Blueprint 등록
app.register_blueprint(contents_bp)
app.register_blueprint(subscriptions_bp)
app.register_blueprint(status_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)

# app 컨텍스트가 종료될 때마다 close_db를 호출하도록 설정
@app.teardown_appcontext
def teardown_db(exception):
    close_db(exception)

# --- 3. 기본 라우트 ---
@app.route('/')
def index():
    """웹 애플리케이션의 메인 페이지를 렌더링합니다."""
    return render_template('index.html')

# --- 4. 실행 ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
