# services/notification_service.py
from database import get_cursor
from .email import get_email_service


def _extract_title(content_id, content_data):
    return (
        content_data.get('title')
        or content_data.get('titleName')
        or content_data.get('content', {}).get('title')
        or f'ID {content_id}'
    )


def send_completion_notifications(conn, newly_completed_items, all_content_today, source):
    """Send completion notifications for newly completed content.

    Args:
        conn: Active DB connection.
        newly_completed_items (list[tuple]): ``(content_id, source, final_completed_at, resolved_by)`` tuples.
        all_content_today (dict): Latest crawler content map used to extract titles.
        source (str): Source name for logging.

    Returns:
        tuple[list[str], int]: Human-readable details and total unique users notified.
    """

    if not newly_completed_items:
        print("\n새롭게 완결된 콘텐츠가 없습니다.")
        return [], 0

    try:
        email_service = get_email_service()
    except ValueError as e:
        print(f"❌ 이메일 서비스 초기화 실패: {e}")
        return [f"오류: {e}"], 0

    cursor = get_cursor(conn)

    print(f"\n🔥 새로운 완결 콘텐츠 {len(newly_completed_items)}개 발견! 알림 발송을 시작합니다.")
    completed_details, total_notified_users = [], 0

    for content_id, _, final_completed_at, resolved_by in newly_completed_items:
        content_data = all_content_today.get(content_id, {})
        title = _extract_title(content_id, content_data)

        cursor.execute(
            """
            SELECT DISTINCT u.id AS user_id, u.email
            FROM subscriptions s
            JOIN users u ON s.user_id = u.id
            WHERE s.content_id = %s AND s.source = %s
            """,
            (content_id, source),
        )
        subscribers = cursor.fetchall()

        print(f"--- '{title}'(ID:{content_id}) 완결 알림 발송 대상: {len(subscribers)}명 ---")
        if not subscribers:
            completed_details.append(f"- '{title}' (ID:{content_id}) : 구독자 없음")
            continue

        subject = f"콘텐츠 완결 알림: '{title}'가 완결되었습니다!"
        body_lines = [
            "안녕하세요! Ending Signal입니다.",
            f"회원님께서 구독하신 콘텐츠 '{title}'가 완결되었습니다.",
            "지금 바로 정주행을 시작해보세요!",
        ]
        if final_completed_at:
            body_lines.append(f"완결 시점: {final_completed_at}")
        body_lines.append(f"완결 판정 출처: {resolved_by}")
        body_lines.append("감사합니다.")
        body = "\n".join(body_lines)

        unique_user_ids = set()
        for subscriber in subscribers:
            user_id = subscriber['user_id']
            if user_id in unique_user_ids:
                continue

            email = subscriber['email']
            email_service.send_mail(email, subject, body)
            unique_user_ids.add(user_id)

        notified_count = len(unique_user_ids)
        total_notified_users += notified_count
        completed_details.append(
            f"- '{title}' (ID:{content_id}) : {notified_count}명에게 알림 발송"
        )

    cursor.close()
    return completed_details, total_notified_users
