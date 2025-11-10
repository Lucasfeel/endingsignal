# services/notification_service.py

import os
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
import config

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
            with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
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
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as smtp_server:
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

