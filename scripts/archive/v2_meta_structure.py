# migrations/v2_meta_structure.py
import os
import sys
import json
from dotenv import load_dotenv

# 프로젝트 루트를 Python 경로에 추가하여 프로젝트 모듈을 임포트할 수 있도록 함
# 이 스크립트는 프로젝트 루트 디렉토리에서 실행된다고 가정합니다.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import create_standalone_connection, get_cursor

def migrate_meta_structure():
    """
    'webtoon' 콘텐츠의 meta 필드 구조를 새로운 표준으로 마이그레이션합니다.
    - 기존: {"authors": [...], "weekdays": [...], "thumbnail_url": ...}
    - 신규: {"common": {"authors": [...], "thumbnail_url": ...}, "attributes": {"weekdays": [...]}}
    """
    conn = None
    updated_count = 0
    try:
        print("LOG: [Migration] meta 구조 마이그레이션을 시작합니다...")
        conn = create_standalone_connection()
        cursor = get_cursor(conn)

        print("LOG: [Migration] 마이그레이션할 웹툰 콘텐츠를 가져옵니다...")
        cursor.execute("SELECT content_id, source, meta FROM contents WHERE content_type = 'webtoon'")
        webtoons = cursor.fetchall()
        print(f"LOG: [Migration] 처리할 웹툰 레코드를 {len(webtoons)}개 찾았습니다.")

        if not webtoons:
            print("LOG: [Migration] 웹툰 레코드가 없어 마이그레이션할 내용이 없습니다.")
            return

        updates = []
        for webtoon in webtoons:
            # psycopg2는 JSONB를 자동으로 dict로 변환합니다.
            old_meta = webtoon['meta']

            if not old_meta:
                print(f"WARN: [Migration] meta 필드가 비어 있어 레코드({webtoon['content_id']}, {webtoon['source']})를 건너뜁니다.")
                continue

            # 이미 마이그레이션되었는지 확인
            if 'common' in old_meta and 'attributes' in old_meta:
                 print(f"INFO: [Migration] 이미 마이그레이션된 레코드({webtoon['content_id']}, {webtoon['source']})를 건너뜁니다.")
                 continue

            new_meta = {
                "common": {
                    "authors": old_meta.get("authors", []),
                    "thumbnail_url": old_meta.get("thumbnail_url") # 값이 없을 경우 None이 되도록 .get() 사용
                },
                "attributes": {
                    "weekdays": old_meta.get("weekdays", [])
                }
            }
            # executemany를 위한 레코드는 (json_string, content_id, source) 형태여야 합니다.
            updates.append((json.dumps(new_meta), webtoon['content_id'], webtoon['source']))

        if not updates:
            print("LOG: [Migration] 업데이트가 필요한 레코드가 없습니다.")
        else:
            print(f"LOG: [Migration] {len(updates)}개 레코드에 대한 업데이트를 적용합니다...")
            cursor.executemany(
                "UPDATE contents SET meta = %s WHERE content_id = %s AND source = %s",
                updates
            )
            updated_count = cursor.rowcount
            print(f"LOG: [Migration] {updated_count}개 레코드를 성공적으로 업데이트했습니다.")

        conn.commit()
        cursor.close()
        print(f"LOG: [Migration] 마이그레이션을 성공적으로 커밋했습니다. 총 업데이트 수: {updated_count}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"FATAL: [Migration] 오류가 발생했습니다: {e}", file=sys.stderr)
        # 실패를 알리기 위해 예외를 다시 발생시킴
        raise
    finally:
        if conn:
            conn.close()
            print("LOG: [Migration] 데이터베이스 연결을 닫았습니다.")

if __name__ == "__main__":
    print("==========================================")
    print("  마이그레이션 스크립트 (v2) 시작됨")
    print("==========================================")

    # .env 파일에서 환경 변수 로드
    # 이 스크립트가 루트에서 실행될 것을 가정하고, .env 파일은 루트에 있어야 합니다.
    load_dotenv()

    try:
        migrate_meta_structure()
        print("\n[SUCCESS] 마이그레이션 스크립트가 성공적으로 완료되었습니다.")
        print("==========================================")
        sys.exit(0)
    except Exception as e:
        print(f"\n[FATAL] 마이그레이션 스크립트가 실패했습니다.", file=sys.stderr)
        print("==========================================")
        sys.exit(1)
