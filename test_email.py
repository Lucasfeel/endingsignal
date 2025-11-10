# test_email.py
import os
import sys
import re
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from services.email import get_email_service
from database import create_standalone_connection, get_cursor

load_dotenv()

def is_valid_email(email):
    """서버 단에서 이메일 형식의 유효성을 검증합니다."""
    return re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email)

def run_test():
    """
    이메일 발송 기능 자체를 직접 테스트합니다.
    """
    try:
        email_service = get_email_service()
    except ValueError as e:
        print("="*60)
        print("❌ [오류] 이메일 서비스 초기화에 실패했습니다.")
        print(f"   원인: {e}")
        print("   .env 파일에 EMAIL_ADDRESS, EMAIL_PASSWORD 등 필요한 환경 변수가 모두 설정되었는지 확인해주세요.")
        print("="*60)
        return

    TEST_CONTENT_ID = '747269' # '전지적 독자 시점'
    TEST_SOURCE = 'naver_webtoon'
    TEST_RECIPIENT_EMAIL = "jules.testing.bot@gmail.com"

    if not is_valid_email(TEST_RECIPIENT_EMAIL):
        print("❌ [설정 필요] test_email.py 파일의 TEST_RECIPIENT_EMAIL 변수를 실제 이메일 주소로 변경해주세요.")
        return

    print("=== 이메일 발송 기능 테스트 시작 ===")

    conn = None
    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)
        cursor.execute("SELECT title FROM contents WHERE content_id = %s AND source = %s", (TEST_CONTENT_ID, TEST_SOURCE))
        result = cursor.fetchone()
    except Exception as e:
        print(f"❌ [오류] 데이터베이스 연결 또는 조회에 실패했습니다: {e}")
        print("   .env 파일의 데이터베이스 관련 환경 변수가 올바르게 설정되었는지 확인해주세요.")
        return
    finally:
        if conn:
            conn.close()

    if not result:
        print(f"❌ 오류: DB에서 콘텐츠 ID {TEST_CONTENT_ID} ({TEST_SOURCE})를 찾을 수 없습니다.")
        print("   'python -m crawlers.naver_webtoon_crawler'를 실행하여 DB에 데이터가 올바르게 수집되었는지 확인해주세요.")
        return

    content_title = result['title']
    print(f"테스트 대상 콘텐츠: '{content_title}' (ID: {TEST_CONTENT_ID})")
    print(f"테스트 메일 수신 주소: {TEST_RECIPIENT_EMAIL}")

    subject = f"[테스트] 콘텐츠 완결 알림: '{content_title}'가 완결되었습니다!"
    body = f"""안녕하세요! Ending Signal입니다.

이것은 이메일 발송 기능 테스트를 위해 전송된 메일입니다.
회원님께서 구독하신 콘텐츠 '{content_title}'가 완결될 경우, 이와 같은 형식으로 메일이 발송됩니다.

감사합니다.
"""

    print("\n이메일 발송을 시도합니다...")
    success = email_service.send_mail(TEST_RECIPIENT_EMAIL, subject, body)

    if success:
        print("✅ 이메일 발송 요청이 성공적으로 전송되었습니다.")
    else:
        print("❌ 이메일 발송 요청이 실패했습니다. 로그를 확인해주세요.")

    print("=== 테스트 종료 ===")


if __name__ == '__main__':
    run_test()
