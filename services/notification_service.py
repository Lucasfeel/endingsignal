# services/notification_service.py
from .email import get_email_service

def send_completion_notifications(cursor, newly_completed_ids, all_content_today, source):
    if not newly_completed_ids:
        print("\n새롭게 완결된 콘텐츠가 없습니다.")
        return [], 0

    try:
        email_service = get_email_service()
    except ValueError as e:
        print(f"❌ 이메일 서비스 초기화 실패: {e}")
        return [f"오류: {e}"], 0

    print(f"\n🔥 새로운 완결 콘텐츠 {len(newly_completed_ids)}개 발견! 알림 발송을 시작합니다.")
    completed_details, total_notified_users = [], 0

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
            email_service.send_mail(email, subject, body)

        total_notified_users += len(subscribers)
        completed_details.append(f"- '{title}' (ID:{content_id}) : {len(subscribers)}명에게 알림 발송")

    return completed_details, total_notified_users
