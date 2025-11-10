# report_sender.py
import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
from database import create_standalone_connection, get_cursor
from services.email import get_email_service

def send_consolidated_report():
    load_dotenv()
    admin_email = os.getenv('ADMIN_EMAIL')
    if not admin_email:
        print("경고: 보고서를 수신할 ADMIN_EMAIL이 없습니다.", file=sys.stderr)
        return

    try:
        email_service = get_email_service()
    except ValueError as e:
        print(f"FATAL: 이메일 서비스 초기화 실패: {e}", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        conn = create_standalone_connection()
        cursor = get_cursor(conn)

        print("LOG: 일일 크롤러 보고서를 DB에서 조회합니다...")
        cursor.execute("SELECT id, crawler_name, status, report_data FROM daily_crawler_reports")
        reports = cursor.fetchall()

        if not reports:
            print("LOG: 발송할 보고서가 없습니다. 종료합니다.")
            return

        print(f"LOG: {len(reports)}개의 크롤러 보고서를 취합합니다.")

        # --- 1. 이메일 본문 생성 ---
        overall_status_icon = "✅"
        overall_status_text = "성공"
        body_lines = [f"안녕하세요, 관리자님.\n\n일일 콘텐츠 동기화 작업이 완료되었습니다.\n총 {len(reports)}개의 작업 결과가 보고되었습니다.\n"]

        for report in reports:
            name = report['crawler_name']
            status = report['status']
            data = report['report_data']

            if status == '실패':
                overall_status_icon = "❌"
                overall_status_text = "실패"

            body_lines.append(f"\n--- 🤖 {name} ({status}) ---")

            if status == '성공':
                body_lines.append(f"  - 실행 시간: {data.get('duration', 0):.2f}초")
                body_lines.append(f"  - 신규 등록: {data.get('new_webtoons', 0)}개")
                body_lines.append(f"  - 완결 알림: {data.get('total_notified', 0)}명")
                body_lines.append(f"  - 완결 내역: {len(data.get('completed_details', []))}건")
            else:
                body_lines.append(f"  - 오류: {data.get('error_message', '알 수 없는 오류')}")

        body = "\n".join(body_lines)
        now = datetime.now().strftime("%Y-%m-%d")
        subject = f"{overall_status_icon} [{overall_status_text}] 일일 통합 보고서 ({now})"

        # --- 2. 이메일 발송 ---
        print(f"LOG: 관리자({admin_email})에게 통합 보고서를 발송합니다...")
        email_service.send_mail(admin_email, subject, body)
        print("LOG: 통합 보고서 발송 완료.")

        # --- 3. (중요) 발송 완료 후 테이블 비우기 ---
        print("LOG: 'daily_crawler_reports' 테이블을 비웁니다 (TRUNCATE)...")
        cursor.execute("TRUNCATE TABLE daily_crawler_reports;")
        conn.commit()
        print("LOG: 테이블 비우기 완료.")

    except Exception as e:
        print(f"FATAL: 통합 보고서 발송기 실행 중 치명적 오류 발생: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("==========================================")
    print("  CONSOLIDATED REPORT SENDER STARTED")
    print("==========================================")
    send_consolidated_report()
    print("==========================================")
    print("  CONSOLIDATED REPORT SENDER FINISHED")
    print("==========================================")
