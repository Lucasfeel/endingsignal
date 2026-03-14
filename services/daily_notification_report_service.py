def _format_duration(duration_seconds):
    if duration_seconds is None:
        return "-"
    return f"{duration_seconds:.2f}초"


def _format_dispatch_status(item):
    status = str(item.get("dispatch_status") or "").strip().lower()
    labels = {
        "processed": "처리완료",
        "skipped": "스킵",
        "failed": "실패",
        "deferred": "보류",
        "sent": "발송완료",
    }
    return labels.get(status, status or "미확정")


def build_daily_notification_text(generated_at, stats, completed_items):
    duration_text = _format_duration(stats.get("duration_seconds"))
    new_contents_total = stats.get("new_contents_total", 0)
    total_recipients = stats.get("total_recipients", 0)
    completed_total = stats.get("completed_total", 0)

    lines = [
        "안녕하세요 관리자님,",
        "일일 알림 리포트를 전달드립니다.",
        "",
        f"- 작업 시간: {generated_at}",
        f"- 실행 시간: {duration_text}",
        f"- 신규 DB 등록 콘텐츠: {new_contents_total}개",
        f"- 총 알림 대상 구독자: {total_recipients}명",
        f"- 완결 이벤트: {completed_total}건",
        (
            "- 디스패치 처리: "
            f"processed {stats.get('dispatch_processed_events', 0)} / "
            f"deferred {stats.get('dispatch_deferred_events', 0)} / "
            f"failed {stats.get('dispatch_failed_events', 0)} / "
            f"skipped {stats.get('dispatch_skipped_events', 0)}"
        ),
        (
            "- 알림 로그: "
            f"sent {stats.get('dispatch_log_sent_total', 0)} / "
            f"failed {stats.get('dispatch_log_failed_total', 0)} / "
            f"pending {stats.get('dispatch_log_pending_total', 0)}"
        ),
        (
            "- 복구/재사용: "
            f"retried {stats.get('dispatch_retried_notifications', 0)} / "
            f"already_sent {stats.get('dispatch_already_sent_notifications', 0)}"
        ),
        "",
        "[완결 처리 및 알림 상태]",
    ]

    if not completed_items:
        lines.append("- (없음)")
    else:
        for item in completed_items:
            title = item.get("title") or item.get("content_id") or "-"
            content_id = item.get("content_id") or "-"
            source = item.get("source") or "-"
            subscriber_count = item.get("subscriber_count") or 0
            subscriber_text = "구독자 없음" if subscriber_count == 0 else f"{subscriber_count}명"
            dispatch_status = _format_dispatch_status(item)
            dispatch_bits = [
                f"dispatch={dispatch_status}",
                f"sent={item.get('dispatch_sent_count', 0)}",
                f"failed={item.get('dispatch_failed_count', 0)}",
                f"pending={item.get('dispatch_pending_count', 0)}",
            ]
            if item.get("notification_excluded"):
                dispatch_bits.append("excluded")
            reason = item.get("dispatch_reason")
            if reason:
                dispatch_bits.append(f"reason={reason}")
            lines.append(
                f"- '{title}' (ID:{content_id} / {source}): {subscriber_text}, "
                + ", ".join(dispatch_bits)
            )

    return "\n".join(lines)
