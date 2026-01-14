import json

STATUS_ALIASES = {
    'success': ['success', 'ok', '성공'],
    'warning': ['warning', 'warn', '경고'],
    'failure': ['failure', 'fail', '실패'],
}


def normalize_report_status(raw):
    if raw is None:
        return 'unknown'
    value = str(raw).strip()
    if not value:
        return 'unknown'
    lowered = value.lower()
    for normalized, aliases in STATUS_ALIASES.items():
        for alias in aliases:
            if lowered == alias.lower():
                return normalized
    return 'unknown'


def expand_status_filter(status_param):
    if not status_param:
        return None
    value = str(status_param).strip().lower()
    if value in STATUS_ALIASES:
        return list(STATUS_ALIASES[value])
    return None


def _parse_report_data(report_data):
    if isinstance(report_data, dict):
        return report_data
    if isinstance(report_data, str):
        try:
            parsed = json.loads(report_data)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _pick_first_key(data, keys):
    for key in keys:
        value = data.get(key)
        if value is not None and value != '':
            return key, value
    return None, None


def _format_duration_seconds(data):
    if data is None:
        return None
    duration = data.get('duration')
    if isinstance(duration, (int, float)):
        return float(duration)
    for key in ['duration_ms', 'elapsed_ms', 'runtime_ms']:
        value = data.get(key)
        if isinstance(value, (int, float)):
            return float(value) / 1000.0
    return None


def _format_error_message(data):
    if not isinstance(data, dict):
        return '알 수 없는 오류'
    error_message = data.get('error_message')
    if error_message:
        return str(error_message)
    _, value = _pick_first_key(
        data,
        ['error', 'message', 'exception', 'traceback', 'reason', 'detail'],
    )
    if value is None:
        return '알 수 없는 오류'
    return str(value)


def build_daily_summary(reports, range_label, date_label):
    counts = {
        'success': 0,
        'warning': 0,
        'failure': 0,
        'unknown': 0,
    }
    summaries = []

    for report in reports:
        status = report.get('status')
        normalized = normalize_report_status(status)
        counts[normalized] = counts.get(normalized, 0) + 1
        summaries.append((report, normalized))

    if counts['failure']:
        overall_status = 'failure'
    elif counts['warning']:
        overall_status = 'warning'
    elif not reports:
        overall_status = 'empty'
    else:
        overall_status = 'success'

    status_label_map = {
        'success': ('✅', '성공'),
        'warning': ('⚠️', '경고'),
        'failure': ('❌', '실패'),
        'empty': ('⚪️', '없음'),
    }
    icon, label = status_label_map.get(overall_status, ('⚪️', '없음'))
    subject_text = f"{icon} [{label}] 일일 통합 보고서 ({date_label})"

    body_lines = [
        '안녕하세요, 관리자님.',
        '',
        '일일 콘텐츠 동기화 작업이 완료되었습니다.',
        f'총 {len(reports)}개의 작업 결과가 보고되었습니다.',
    ]
    if range_label:
        body_lines.append(f'조회 범위: {range_label}')

    for report, normalized in summaries:
        data = _parse_report_data(report.get('report_data'))
        crawler_name = report.get('crawler_name') or '-'
        raw_status = report.get('status') or '-'
        body_lines.append('')
        body_lines.append(f"--- 🤖 {crawler_name} ({raw_status}) ---")

        if normalized == 'success':
            duration_seconds = _format_duration_seconds(data)
            if duration_seconds is None:
                duration_seconds = 0.0
            body_lines.append(f"  - 실행 시간: {duration_seconds:.2f}초")
            new_contents = data.get('new_webtoons', data.get('new_contents', 0))
            body_lines.append(f"  - 신규 등록: {new_contents}개")

            newly_completed_items = data.get('newly_completed_items', [])
            cdc_info = data.get('cdc_info') or {}
            resolved_by_counts = cdc_info.get('resolved_by_counts', {})

            newly_completed_count = cdc_info.get(
                'newly_completed_count',
                len(newly_completed_items) if isinstance(newly_completed_items, list) else 0,
            )
            inserted_event_count = cdc_info.get('cdc_events_inserted_count', 0)
            cdc_mode = cdc_info.get('cdc_mode', 'unknown')

            body_lines.append(f"  - 신규 완결: {newly_completed_count}건 (CDC 모드: {cdc_mode})")
            if resolved_by_counts:
                body_lines.append(f"  - 완결 판정 출처: {resolved_by_counts}")
            body_lines.append(f"  - CDC 이벤트 기록 수: {inserted_event_count}건")
        else:
            body_lines.append(f"  - 오류: {_format_error_message(data)}")

    summary_text = "\n".join(body_lines)

    return {
        'overall_status': overall_status,
        'subject_text': subject_text,
        'summary_text': summary_text,
        'counts': counts,
    }
