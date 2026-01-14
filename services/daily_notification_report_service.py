def _format_duration(duration_seconds):
    if duration_seconds is None:
        return '-'
    return f"{duration_seconds:.2f}초"


def build_daily_notification_text(generated_at, stats, completed_items):
    duration_text = _format_duration(stats.get('duration_seconds'))
    new_contents_total = stats.get('new_contents_total', 0)
    total_recipients = stats.get('total_recipients', 0)

    lines = [
        '안녕하세요, 관리자님.',
        '자동화 작업이 성공적으로 완료되었습니다.',
        '',
        f"- 작업 시간: {generated_at}",
        f"- 실행 시간: {duration_text}",
        f"- 신규 DB 등록 콘텐츠: {new_contents_total}개",
        f"- 총 알림 발생 인원: {total_recipients}명",
        '',
        '[금일 완결 처리 및 알림 발생 내역]',
    ]

    if not completed_items:
        lines.append('- (없음)')
    else:
        for item in completed_items:
            title = item.get('title') or item.get('content_id') or '-'
            content_id = item.get('content_id') or '-'
            source = item.get('source') or '-'
            subscriber_count = item.get('subscriber_count') or 0
            if subscriber_count == 0:
                subscriber_text = '구독자 없음'
            else:
                subscriber_text = f"{subscriber_count}명"
            lines.append(
                f"- '{title}' (ID:{content_id} / {source}): {subscriber_text}"
            )

    return "\n".join(lines)
