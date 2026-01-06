# Ending Signal 프론트엔드 PWA 통합 개발 명세서 (v10)

## 1. 번들 및 자산 구조
- PWA는 Flask 루트 라우트(`/`)가 제공하는 `templates/index.html` 하나로 구성되며, Tailwind CDN 스타일과 내장 스타일 블록을 사용합니다.【F:templates/index.html†L1-L40】
- 동작 로직은 단일 ES 모듈 `static/app.js`에서 로드되며, 페이지 하단에서 `defer`로 불러옵니다.【F:templates/index.html†L377-L378】
- 별도의 TypeScript/Node 빌드 파이프라인은 사용하지 않습니다(이전 `src/`, `package.json`, `tsconfig.json` 제거). 모든 인터랙션은 순수 JS 자산으로 동작합니다.【F:CLEANUP_NOTES.md†L3-L10】

## 2. API 연동 규약
- `apiRequest` 헬퍼가 공통적으로 Authorization 헤더(Bearer 토큰)와 쿼리 문자열을 조립하여 REST 엔드포인트를 호출합니다.【F:static/app.js†L1292-L1345】
- 인증 플로우: 로그인/회원가입은 `/api/auth/login`, `/api/auth/register`에 POST하며, `/api/auth/me`로 토큰 검증 후 상태를 반영합니다. 로그아웃은 클라이언트 측 토큰/상태만 초기화합니다.【F:static/app.js†L1215-L1280】
- 비밀번호 변경: 마이페이지에서 `/api/auth/change-password`에 `current_password`, `new_password`를 전송합니다(8자 이상 검사 포함).【F:static/app.js†L2684-L2745】
- 구독 관리: `/api/me/subscriptions`에 대해 `GET`으로 목록을 불러오고, `POST`/`DELETE`로 구독 추가/삭제를 수행합니다.【F:static/app.js†L1476-L1601】
- 콘텐츠 조회: 메인 피드는 `/api/contents/{ongoing|completed|hiatus}` 페이징 응답을 받아 렌더링하며, 검색은 `/api/contents/search`에 쿼리(`q`, `type`, `source`)로 요청합니다.【F:static/app.js†L3324-L3369】【F:static/app.js†L2350-L2394】【F:static/app.js†L3548-L3577】

## 3. 주요 DOM 훅
- 루트 컨테이너는 `#app-root`이며, 헤더/필터/콘텐츠 그리드/모달 등 모든 UI 섹션이 정적 HTML로 선언된 뒤 `static/app.js`가 상태에 따라 토글합니다.【F:templates/index.html†L31-L376】
- 로그인/회원가입/비밀번호 변경 모달 입력 필드는 `#authEmail`, `#authPassword`, `#authPasswordConfirm`, `#myPagePwCurrent` 등 ID 기반 셀렉터를 사용하므로 DOM 구조를 변경할 때 동일 ID를 유지해야 합니다.【F:templates/index.html†L343-L374】
