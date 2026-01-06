# Archived scripts

이 디렉터리는 현재 배포 경로에서 실행되지 않는 마이그레이션/백필 스크립트를 보관합니다 (보존일: 2026-01-06).

- `v2_meta_structure.py`: 웹툰 `meta` 필드를 새로운 `common/attributes` 구조로 변환하는 단발성 마이그레이션.
- `backfill_content_urls.py`: `contents.meta.common.content_url`을 추론하여 보강하는 보조 스크립트.

필요 시 수동으로 실행하되, 최신 스키마와 충돌이 없는지 확인 후 사용하세요.
