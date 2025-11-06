# services/notification_service.py

import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

def send_email(recipient_email, subject, body, smtp_server=None):
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        print("오류: 이메일 발송을 위한 환경 변수가 설정되지 않았습니다.")
        return False
    msg = MIMEText(body, _charset='utf-8')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email
    try:
        if smtp_server is None:
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipient_email, msg.as_string())
        else:
            smtp_server.sendmail(sender_email, recipient_email, msg.as_string())
        return True
    except Exception as e:
        print(f"오류: {recipient_email}에게 이메일 발송 실패 - {e}")
        return False

def send_completion_notifications(cursor, newly_completed_ids, all_content_today, source):
    if not newly_completed_ids:
        print("\n새롭게 완결된 콘텐츠가 없습니다.")
        return [], 0
    print(f"\n🔥 새로운 완결 콘텐츠 {len(newly_completed_ids)}개 발견! 알림 발송을 시작합니다.")
    completed_details, total_notified_users = [], 0
    sender_email = os.getenv('EMAIL_ADDRESS')
    sender_password = os.getenv('EMAIL_PASSWORD')
    if not sender_email or not sender_password:
        print("오류: 이메일 발송을 위한 환경 변수가 설정되지 않았습니다.")
        return [], 0
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp_server:
            smtp_server.starttls()
            smtp_server.login(sender_email, sender_password)
            for content_id in newly_completed_ids:
                content_data = all_content_today.get(content_id, {})
                title = content_data.get('titleName', f'ID {content_id}')

                cursor.execute("SELECT email FROM subscriptions WHERE content_id = %s AND source = %s", (content_id, source))
                subscribers = [row['email'] for row in cursor.fetchall()]

                print(f"--- '{title}'(ID:{content_id}) 완결 알림 발송 대상: {len(subscribers)}명 ---")
                if not subscribers:
                    completed_details.append(f"- '{title}' (ID:{content_id}) : 구독자 없음")
                    continue

                subject = f"콘텐츠 완결 알림: '{title}'가 완결되었습니다!"
                body = f"안녕하세요! Ending Signal입니다.\n\n회원님께서 구독하신 콘텐츠 '{title}'가 완결되었습니다.\n지금 바로 정주행을 시작해보세요!\n\n감사합니다."

                for email in subscribers:
                    send_email(email, subject, body, smtp_server)
                total_notified_users += len(subscribers)
                completed_details.append(f"- '{title}' (ID:{content_id}) : {len(subscribers)}명에게 알림 발송")
    except Exception as e:
        print(f"❌ 이메일 서버 연결 또는 발송 중 심각한 오류 발생: {e}")
    return completed_details, total_notified_users

def send_admin_report(report_data):
    admin_email = os.getenv('ADMIN_EMAIL')
    if not admin_email:
        print("경고: 보고서를 수신할 ADMIN_EMAIL 환경 변수가 설정되지 않았습니다.")
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if report_data['status'] == '성공':
        subject = f"✅ [성공] 웹툰 알리미 일일 보고서 ({now})"
        body = f"안녕하세요, 관리자님.\n웹툰 알리미 자동화 작업이 성공적으로 완료되었습니다.\n\n- 작업 시간: {now}\n- 실행 시간: {report_data['duration']:.2f}초\n- 신규 DB 등록 웹툰: {report_data.get('new_webtoons', 0)}개\n- 총 알림 발송 인원: {report_data.get('total_notified', 0)}명\n\n[금일 완결 처리 및 알림 발송 내역]\n"
        body += "\n".join(report_data['completed_details']) if report_data['completed_details'] else "없음"
    else:
        subject = f"❌ [실패] 웹툰 알리미 자동화 작업 오류 보고서 ({now})"
        body = f"안녕하세요, 관리자님.\n웹툰 알리미 자동화 작업 중 오류가 발생했습니다.\n\n- 작업 시간: {now}\n- 오류 내용:\n{report_data['error_message']}\n\nGitHub Actions 로그를 확인해주세요."
    send_email(admin_email, subject, body)
