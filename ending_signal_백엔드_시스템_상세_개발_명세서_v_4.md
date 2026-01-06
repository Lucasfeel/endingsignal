# Ending Signal 백엔드 시스템 상세 개발 명세서 (v4)

## 1. 런타임 엔트리포인트
- **Flask 웹 API**: `app.py`에서 Blueprint를 등록하고 `/`를 `templates/index.html`로 렌더링합니다. 프로세스는 `Procfile`을 통해 `python init_db.py && gunicorn app:app`으로 시작됩니다.【F:app.py†L6-L46】【F:Procfile†L1-L1】
- **크롤러 배치 실행**: `run_all_crawlers.py`가 네이버/카카오 크롤러를 병렬 실행 후 보고서를 저장합니다. 스케줄러나 GitHub Actions에서 직접 호출합니다.【F:run_all_crawlers.py†L1-L80】

## 2. 데이터베이스 스키마(실행 코드 기준)
- **contents**: `content_id`, `source`, `content_type`, `title`, `status`, `meta` 필드 외에 검색 최적화를 위한 `normalized_title`, `normalized_authors` 컬럼과 trigram 인덱스가 생성·백필됩니다.【F:database.py†L65-L229】
- **users**: 계정 정보에 `updated_at TIMESTAMP DEFAULT NOW()`가 포함되며, 초기화 시 누락된 값은 `created_at`으로 보정됩니다.【F:database.py†L88-L118】
- **기타 테이블**: `subscriptions`, `admin_content_overrides`, `cdc_events`, `daily_crawler_reports`가 동일 스크립트에서 생성됩니다.【F:database.py†L120-L178】

## 3. 인증 및 계정 API
- `POST /api/auth/register` / `POST /api/auth/login`: 이메일·비밀번호 기반 가입/로그인. 로그인 시 JWT(`bearer` 토큰)과 만료 시간, 사용자 정보가 반환됩니다.【F:views/auth.py†L17-L75】
- `POST /api/auth/logout`: 상태 저장 없이 `{ "success": true }`를 돌려주는 무상태 로그아웃 엔드포인트입니다.【F:views/auth.py†L77-L80】
- `GET /api/auth/me`: Bearer 토큰을 요구하며 현재 사용자 정보를 반환합니다.【F:views/auth.py†L82-L85】
- `POST /api/auth/change-password`: 인증된 사용자가 `current_password`, `new_password`를 제출하면 비밀번호를 검증 후 `users.updated_at`을 `NOW()`로 갱신합니다. 비밀번호 길이는 8자 이상이어야 합니다.【F:views/auth.py†L88-L115】【F:services/auth_service.py†L98-L121】
- `GET /api/auth/admin/ping`: 관리자 권한 확인용 간단한 핑 엔드포인트입니다.【F:views/auth.py†L118-L122】

## 4. 헬스체크
- `GET /api/status`: DB에서 `contents` 건수를 조회해 `{'status':'ok','content_count':...}` 형태로 반환합니다. 예외 시 `status: error`와 메시지를 포함합니다.【F:views/status.py†L8-L28】

## 5. 이메일 발송 옵션
- 이메일 제공자는 환경변수 `EMAIL_PROVIDER`로 선택하며 기본은 `smtp`입니다. SMTP 모드는 `SMTP_SERVER`/`SMTP_PORT`와 `EMAIL_ADDRESS`·`EMAIL_PASSWORD` 자격 증명을 요구합니다.【F:config.py†L60-L66】【F:services/smtp_service.py†L12-L35】
- SendGrid 모드는 `EMAIL_PROVIDER=sendgrid`로 선택하며 `SENDGRID_API_KEY`와 `EMAIL_ADDRESS`가 필요합니다.【F:services/sendgrid_service.py†L7-L28】
