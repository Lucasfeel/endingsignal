(() => {
  const getAccessToken = () => {
    try {
      return localStorage.getItem('es_access_token');
    } catch (err) {
      return null;
    }
  };

  const showToast = (message, { type = 'info' } = {}) => {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = 'flex justify-center transition-opacity duration-300';

    const inner = document.createElement('div');
    const baseClasses =
      'pointer-events-auto rounded-xl border px-4 py-2 text-sm shadow-lg backdrop-blur';
    const typeClasses =
      type === 'error'
        ? 'border-red-400/40 bg-red-500/20 text-red-100'
        : type === 'success'
          ? 'border-emerald-300/40 bg-emerald-500/20 text-emerald-50'
          : 'border-white/20 bg-white/10 text-white';
    inner.className = `${baseClasses} ${typeClasses}`;
    inner.textContent = message;

    toast.appendChild(inner);
    container.appendChild(toast);

    const removeToast = () => {
      toast.classList.add('opacity-0');
      setTimeout(() => toast.remove(), 300);
    };

    setTimeout(removeToast, 2500);
  };

  const setButtonDisabled = (button, disabled) => {
    if (!button) return;
    button.disabled = disabled;
    button.classList.toggle('opacity-60', disabled);
    button.classList.toggle('cursor-not-allowed', disabled);
  };

  const withLoading = async (button, label, action) => {
    if (!button) {
      return action();
    }
    if (button.disabled) return null;
    const originalText = button.dataset.originalText || button.textContent;
    if (!button.dataset.originalText) {
      button.dataset.originalText = originalText;
    }
    setButtonDisabled(button, true);
    if (label) button.textContent = label;
    try {
      return await action();
    } finally {
      button.textContent = originalText;
      setButtonDisabled(button, false);
    }
  };

  const apiRequest = async (method, path, { token, body } = {}) => {
    const headers = {
      Accept: 'application/json',
    };

    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    if (body) {
      headers['Content-Type'] = 'application/json';
    }

    const response = await fetch(path, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    const contentType = response.headers.get('content-type') || '';
    let payload = null;
    if (contentType.includes('application/json')) {
      try {
        payload = await response.json();
      } catch (err) {
        payload = null;
      }
    } else {
      try {
        payload = await response.text();
      } catch (err) {
        payload = null;
      }
    }

    if (!response.ok) {
      const message =
        payload?.error?.message ||
        payload?.message ||
        payload?.error ||
        payload?.detail ||
        response.statusText ||
        '요청에 실패했습니다.';
      const error = {
        httpStatus: response.status,
        message,
      };
      if (payload?.error?.code) error.code = payload.error.code;
      if (payload?.code) error.code = payload.code;
      throw error;
    }

    if (payload?.success === false && payload?.error) {
      const error = {
        httpStatus: response.status,
        message: payload.error.message || '요청에 실패했습니다.',
      };
      if (payload.error.code) error.code = payload.error.code;
      throw error;
    }

    return payload;
  };

  const redirectToHome = (delay = 800) => {
    setTimeout(() => {
      window.location.replace('/');
    }, delay);
  };

  const extractUser = (payload) => {
    if (!payload) return null;
    if (payload.user) return payload.user;
    if (payload.data?.user) return payload.data.user;
    return payload.data || null;
  };

  const hideGateOverlay = () => {
    const overlay = document.getElementById('adminGateOverlay');
    if (overlay) overlay.classList.add('hidden');
  };

  const STATE = {
    token: null,
    user: null,
    tab: 'manage',
    manage: {
      results: [],
      selected: null,
      overridesMap: new Map(),
      publicationsMap: new Map(),
      publicationActionMap: new Map(),
    },
    deleted: {
      items: [],
      selected: null,
      q: '',
      limit: 50,
      offset: 0,
      lastCount: 0,
    },
    publications: {
      items: [],
      limit: 50,
      offset: 0,
      lastCount: 0,
    },
    missingCompletion: {
      items: [],
      limit: 50,
      offset: 0,
      lastCount: 0,
      source: 'all',
      contentType: 'all',
      q: '',
    },
    missingPublication: {
      items: [],
      limit: 50,
      offset: 0,
      lastCount: 0,
      source: 'all',
      contentType: 'all',
      q: '',
    },
    audit: {
      items: [],
      q: '',
      actionType: '',
      limit: 50,
      offset: 0,
      lastCount: 0,
    },
    cdcEvents: {
      items: [],
      q: '',
      eventType: '',
      source: '',
      contentId: '',
      createdFrom: '',
      createdTo: '',
      limit: 50,
      offset: 0,
      lastCount: 0,
    },
    crawlerReports: {
      items: [],
      crawlerName: '',
      status: '',
      createdFrom: '',
      createdTo: '',
      limit: 50,
      offset: 0,
      lastCount: 0,
      summaryText: '',
      dailyNotificationText: '',
    },
  };

  const parseKstNaiveIsoToEpoch = (value) => {
    if (!value) return null;
    if (typeof value === 'number') return value;
    if (typeof value !== 'string') return null;
    if (/[zZ]|[+-]\d{2}:\d{2}$/.test(value)) {
      const epoch = Date.parse(value);
      return Number.isNaN(epoch) ? null : epoch;
    }
    const match = value.match(
      /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/,
    );
    if (!match) return null;
    const [, year, month, day, hour, minute, second] = match;
    const utcEpoch = Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour) - 9,
      Number(minute),
      Number(second || 0),
    );
    return Number.isNaN(utcEpoch) ? null : utcEpoch;
  };

  const formatEpochAsKst = (epoch) => {
    if (epoch === null || epoch === undefined) return '-';
    const date = new Date(epoch);
    if (Number.isNaN(date.getTime())) return '-';
    const formatter = new Intl.DateTimeFormat('ko-KR', {
      timeZone: 'Asia/Seoul',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
    const parts = formatter.formatToParts(date);
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${values.year}-${values.month}-${values.day} ${values.hour}:${values.minute}`;
  };

  const getTodayKstDateString = () => {
    const formatter = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Seoul',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    });
    return formatter.format(new Date());
  };

  const toDatetimeLocalValueKst = (value) => {
    if (!value) return '';
    if (typeof value === 'string' && !(/[zZ]|[+-]\d{2}:\d{2}$/.test(value))) {
      return value.length >= 16 ? value.slice(0, 16) : value;
    }
    const epoch = parseKstNaiveIsoToEpoch(value);
    if (epoch === null) return '';
    const date = new Date(epoch);
    const formatter = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Seoul',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
    const parts = formatter.formatToParts(date);
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${values.year}-${values.month}-${values.day}T${values.hour}:${values.minute}`;
  };

  const formatTimestamp = (value) => {
    if (!value) return '-';
    const epoch = parseKstNaiveIsoToEpoch(value);
    if (epoch === null) return value;
    return formatEpochAsKst(epoch);
  };

  const parseMeta = (meta) => {
    if (!meta) return {};
    if (typeof meta === 'object') return meta;
    if (typeof meta === 'string') {
      try {
        const parsed = JSON.parse(meta);
        return parsed && typeof parsed === 'object' ? parsed : {};
      } catch (err) {
        return {};
      }
    }
    return {};
  };

  const tryParseJsonObject = (value) => {
    if (typeof value !== 'string') return null;
    try {
      const parsed = JSON.parse(value);
      if (parsed && typeof parsed === 'object') {
        return parsed;
      }
    } catch (err) {
      return null;
    }
    return null;
  };

  const formatSummaryValue = (value) => {
    if (value === null || value === undefined) return '-';
    let text = '';
    if (typeof value === 'number') {
      text = value.toLocaleString();
    } else if (typeof value === 'boolean') {
      text = value ? 'true' : 'false';
    } else if (typeof value === 'object') {
      try {
        text = JSON.stringify(value);
      } catch (err) {
        text = String(value);
      }
    } else {
      text = String(value);
    }
    const maxLength = 80;
    if (text.length > maxLength) {
      text = `${text.slice(0, maxLength - 3)}...`;
    }
    return text;
  };

  const pickFirstExistingKey = (obj, keys) => {
    if (!obj) return null;
    for (const key of keys) {
      const value = obj[key];
      if (value !== undefined && value !== null && value !== '') {
        return { key, value };
      }
    }
    return null;
  };

  const SUMMARY_LABELS = {
    cdc_events_inserted_count: 'CDC inserted',
    scheduled_publication_events_inserted_count: 'Pub inserted',
    scheduled_publication_due_count: 'Pub due',
    scheduled_completion_events_inserted_count: 'Comp inserted',
    scheduled_completion_due_count: 'Comp due',
    duration_ms: 'Duration(ms)',
    elapsed_ms: 'Elapsed(ms)',
    runtime_ms: 'Runtime(ms)',
  };

  const normalizeReportStatus = (raw) => {
    if (!raw && raw !== 0) return 'unknown';
    const value = String(raw).trim();
    if (!value) return 'unknown';
    const lowered = value.toLowerCase();
    if (['success', 'ok', '성공'].some((alias) => alias.toLowerCase() === lowered)) {
      return 'success';
    }
    if (['warning', 'warn', '경고'].some((alias) => alias.toLowerCase() === lowered)) {
      return 'warning';
    }
    if (['failure', 'fail', '실패'].some((alias) => alias.toLowerCase() === lowered)) {
      return 'failure';
    }
    return 'unknown';
  };

  const getStatusBadgeClasses = (statusValue) => {
    switch (statusValue) {
      case 'success':
        return 'inline-flex rounded-full border border-emerald-300/40 bg-emerald-500/10 px-2 py-0.5 text-[10px] text-emerald-100';
      case 'warning':
        return 'inline-flex rounded-full border border-amber-300/40 bg-amber-500/10 px-2 py-0.5 text-[10px] text-amber-100';
      case 'failure':
        return 'inline-flex rounded-full border border-red-400/40 bg-red-500/10 px-2 py-0.5 text-[10px] text-red-100';
      case 'empty':
        return 'inline-flex rounded-full border border-white/20 bg-white/5 px-2 py-0.5 text-[10px] text-white/60';
      default:
        return 'inline-flex rounded-full border border-white/20 bg-white/5 px-2 py-0.5 text-[10px] text-white/70';
    }
  };

  const formatReportSummary = (reportData, normalizedStatus) => {
    if (!reportData) return '-';
    let data = reportData;
    if (typeof data === 'string') {
      const parsed = tryParseJsonObject(data);
      if (parsed) {
        data = parsed;
      } else {
        return formatSummaryValue(data);
      }
    }
    if (!data || typeof data !== 'object') return formatSummaryValue(data);

    const pairs = [];
    const usedKeys = new Set();

    if (normalizedStatus === 'failure') {
      const errorPick = pickFirstExistingKey(data, [
        'error',
        'message',
        'exception',
        'traceback',
        'reason',
        'detail',
      ]);
      if (errorPick) {
        pairs.push({ label: 'Error', value: formatSummaryValue(errorPick.value) });
        usedKeys.add(errorPick.key);
      }
    }

    const countKeys = [
      'cdc_events_inserted_count',
      'scheduled_publication_events_inserted_count',
      'scheduled_publication_due_count',
      'scheduled_completion_events_inserted_count',
      'scheduled_completion_due_count',
    ];
    countKeys.forEach((key) => {
      if (data[key] !== undefined && data[key] !== null) {
        pairs.push({ label: SUMMARY_LABELS[key] || key, value: formatSummaryValue(data[key]) });
        usedKeys.add(key);
      }
    });

    const durationPick = pickFirstExistingKey(data, ['duration_ms', 'elapsed_ms', 'runtime_ms']);
    if (durationPick && !usedKeys.has(durationPick.key)) {
      pairs.push({
        label: SUMMARY_LABELS[durationPick.key] || durationPick.key,
        value: formatSummaryValue(durationPick.value),
      });
      usedKeys.add(durationPick.key);
    }

    ['inserted_count', 'due_count', 'status'].forEach((key) => {
      if (data[key] !== undefined && data[key] !== null && !usedKeys.has(key)) {
        pairs.push({ label: SUMMARY_LABELS[key] || key, value: formatSummaryValue(data[key]) });
        usedKeys.add(key);
      }
    });

    const noisyKeys = new Set(['traceback']);
    const remainingKeys = Object.keys(data)
      .filter((key) => !usedKeys.has(key) && !noisyKeys.has(key))
      .sort();
    remainingKeys.slice(0, 2).forEach((key) => {
      pairs.push({ label: SUMMARY_LABELS[key] || key, value: formatSummaryValue(data[key]) });
      usedKeys.add(key);
    });

    if (!pairs.length) return '-';
    return pairs
      .slice(0, 6)
      .map((pair) => `${pair.label}: ${pair.value}`)
      .join(' · ');
  };

  const getThumbnailUrl = (item) => {
    const meta = parseMeta(item?.meta);
    return (
      meta?.common?.thumbnail_url ||
      meta?.thumbnail_url ||
      meta?.common?.thumbnail ||
      item?.thumbnail_url ||
      item?.thumbnail ||
      item?.thumbnail_path ||
      ''
    );
  };

  const getAuthorsText = (item) => {
    const meta = parseMeta(item?.meta);
    const authors = meta?.common?.authors;
    if (Array.isArray(authors)) {
      return authors.filter(Boolean).join(', ');
    }
    if (typeof meta?.common?.author === 'string') {
      return meta.common.author;
    }
    return '';
  };

  const getKey = (item) => `${item.content_id}::${item.source}`;

  const AUDIT_ACTION_LABELS = {
    OVERRIDE_UPSERT: '완결 처리 저장',
    OVERRIDE_DELETE: '완결 처리 삭제',
    PUBLICATION_UPSERT: '공개일 저장',
    PUBLICATION_DELETE: '공개일 삭제',
    CONTENT_DELETE: '콘텐츠 삭제',
    CONTENT_RESTORE: '콘텐츠 복구',
  };

  const computeFinalState = (rawStatus, override) => {
    if (!override) {
      return {
        final_status: rawStatus,
        final_completed_at: null,
        resolved_by: 'crawler',
        is_scheduled_completion: false,
      };
    }

    const overrideStatus = override.override_status;
    const overrideCompletedAt = override.override_completed_at;

    if (overrideStatus && overrideStatus !== '완결') {
      return {
        final_status: overrideStatus,
        final_completed_at: null,
        resolved_by: 'override',
        is_scheduled_completion: false,
      };
    }

    if (!overrideCompletedAt) {
      return {
        final_status: '완결',
        final_completed_at: null,
        resolved_by: 'override',
        is_scheduled_completion: false,
      };
    }

    const scheduledEpoch = parseKstNaiveIsoToEpoch(overrideCompletedAt);
    const nowEpoch = Date.now();
    if (scheduledEpoch === null) {
      return {
        final_status: rawStatus,
        final_completed_at: null,
        resolved_by: 'crawler',
        is_scheduled_completion: false,
      };
    }

    if (nowEpoch < scheduledEpoch) {
      return {
        final_status: rawStatus,
        final_completed_at: null,
        resolved_by: 'crawler',
        is_scheduled_completion: true,
      };
    }

    return {
      final_status: '완결',
      final_completed_at: overrideCompletedAt,
      resolved_by: 'override',
      is_scheduled_completion: false,
    };
  };

  const setTab = (tabName) => {
    STATE.tab = tabName;
    const panels = {
      manage: document.getElementById('panelManage'),
      deleted: document.getElementById('panelDeleted'),
      publications: document.getElementById('panelPublications'),
      missingCompletion: document.getElementById('panelMissingCompletion'),
      missingPublication: document.getElementById('panelMissingPublication'),
      audit: document.getElementById('panelAudit'),
      cdcEvents: document.getElementById('panelCdcEvents'),
      crawlerReports: document.getElementById('panelCrawlerReports'),
      dailyNotificationReport: document.getElementById('panelDailyNotificationReport'),
    };
    const tabs = {
      manage: document.getElementById('tabManage'),
      deleted: document.getElementById('tabDeleted'),
      publications: document.getElementById('tabPublications'),
      missingCompletion: document.getElementById('tabMissingCompletion'),
      missingPublication: document.getElementById('tabMissingPublication'),
      audit: document.getElementById('tabAudit'),
      cdcEvents: document.getElementById('tabCdcEvents'),
      crawlerReports: document.getElementById('tabCrawlerReports'),
      dailyNotificationReport: document.getElementById('tabDailyNotificationReport'),
    };

    Object.entries(panels).forEach(([key, panel]) => {
      if (!panel) return;
      if (key === tabName) {
        panel.classList.remove('hidden');
      } else {
        panel.classList.add('hidden');
      }
    });

    Object.entries(tabs).forEach(([key, button]) => {
      if (!button) return;
      if (key === tabName) {
        button.classList.add('bg-white/10', 'text-white');
        button.classList.remove('text-white/70');
      } else {
        button.classList.remove('bg-white/10', 'text-white');
        button.classList.add('text-white/70');
      }
    });
  };

  const confirmState = {
    onConfirm: null,
  };

  const detailState = {
    jsonText: '',
    contentId: null,
    source: null,
  };

  const openConfirm = ({ title, message, onConfirm }) => {
    const modal = document.getElementById('confirmModal');
    const titleEl = document.getElementById('confirmTitle');
    const messageEl = document.getElementById('confirmMessage');
    if (!modal) return;

    if (titleEl) titleEl.textContent = title || '확인';
    if (messageEl) messageEl.textContent = message || '계속 진행할까요?';
    confirmState.onConfirm = typeof onConfirm === 'function' ? onConfirm : null;

    modal.classList.remove('hidden');
    modal.classList.add('flex');
  };

  const copyToClipboard = async (text) => {
    if (!text) throw new Error('EMPTY');
    if (navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const el = document.createElement('textarea');
    el.value = text;
    el.setAttribute('readonly', 'true');
    el.style.position = 'fixed';
    el.style.left = '-9999px';
    document.body.appendChild(el);
    el.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(el);
    if (!ok) throw new Error('CLIPBOARD_FAILED');
  };

  const closeConfirm = () => {
    const modal = document.getElementById('confirmModal');
    if (!modal) return;
    modal.classList.add('hidden');
    modal.classList.remove('flex');
    confirmState.onConfirm = null;
  };

  const openDetailModal = ({ title, subtitle, obj }) => {
    const modal = document.getElementById('detailModal');
    const titleEl = document.getElementById('detailTitle');
    const subtitleEl = document.getElementById('detailSubtitle');
    const preEl = document.getElementById('detailBodyPre');
    const copyIdBtn = document.getElementById('detailCopyIdBtn');
    const copyIdSourceBtn = document.getElementById('detailCopyIdSourceBtn');
    if (!modal || !preEl) return;

    let jsonText = '';
    try {
      jsonText = JSON.stringify(obj ?? {}, null, 2);
    } catch (err) {
      showToast('JSON 표시 중 오류가 발생했습니다.', { type: 'error' });
      return;
    }

    detailState.jsonText = jsonText;
    detailState.contentId = obj?.content_id || null;
    detailState.source = obj?.source || null;

    if (titleEl) titleEl.textContent = title || 'Details';
    if (subtitleEl) subtitleEl.textContent = subtitle || '';
    preEl.textContent = jsonText;

    const hasId = !!detailState.contentId && !!detailState.source;
    if (copyIdBtn) copyIdBtn.classList.toggle('hidden', !hasId);
    if (copyIdSourceBtn) copyIdSourceBtn.classList.toggle('hidden', !hasId);

    modal.classList.remove('hidden');
    modal.classList.add('flex');
  };

  const closeDetailModal = () => {
    const modal = document.getElementById('detailModal');
    if (!modal) return;
    modal.classList.add('hidden');
    modal.classList.remove('flex');
    detailState.jsonText = '';
    detailState.contentId = null;
    detailState.source = null;
  };

  const initConfirmModal = () => {
    const modal = document.getElementById('confirmModal');
    const okBtn = document.getElementById('confirmOkBtn');
    const cancelBtn = document.getElementById('confirmCancelBtn');
    if (!modal || !okBtn || !cancelBtn) return;

    okBtn.addEventListener('click', () => {
      const action = confirmState.onConfirm;
      if (!action) {
        closeConfirm();
        return;
      }
      const result = withLoading(okBtn, '처리중...', async () => {
        await action();
      });
      if (result && typeof result.finally === 'function') {
        result.finally(() => {
          closeConfirm();
        });
      } else {
        closeConfirm();
      }
    });

    cancelBtn.addEventListener('click', () => {
      closeConfirm();
    });

    modal.addEventListener('click', (event) => {
      if (event.target === modal) {
        closeConfirm();
      }
    });

    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && !modal.classList.contains('hidden')) {
        closeConfirm();
      }
    });
  };

  const renderManageResults = () => {
    const container = document.getElementById('manageResultsList');
    if (!container) return;
    container.innerHTML = '';

    if (!STATE.manage.results.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '검색 결과가 없습니다.';
      container.appendChild(empty);
      return;
    }

    STATE.manage.results.forEach((item) => {
      const card = document.createElement('button');
      card.type = 'button';
      const isSelected =
        STATE.manage.selected &&
        STATE.manage.selected.content_id === item.content_id &&
        STATE.manage.selected.source === item.source;
      card.className = `w-full rounded-xl border px-3 py-2 text-left text-sm transition hover:border-white/40 ${
        isSelected ? 'border-emerald-300/60 bg-emerald-500/10' : 'border-white/10 bg-black/30'
      }`;
      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      title.textContent = item.title || item.normalized_title || item.content_id;
      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      meta.textContent = `${item.source || '-'} · ${item.content_type || '-'} · ${item.status || '-'}`;

      card.appendChild(title);
      card.appendChild(meta);
      card.addEventListener('click', () => selectManageItem(item));
      container.appendChild(card);
    });
  };

  const renderFinalState = (rawItem, override) => {
    const box = document.getElementById('finalStateBox');
    const notice = document.getElementById('scheduledNotice');
    if (!box) return;
    box.innerHTML = '';

    if (!rawItem) {
      box.innerHTML = '<p class="text-xs text-white/40">선택된 콘텐츠가 없습니다.</p>';
      if (notice) notice.classList.add('hidden');
      return;
    }

    const finalState = computeFinalState(rawItem.status, override);
    const entries = [
      ['원본 상태', rawItem.status || '-'],
      ['최종 상태', finalState.final_status || '-'],
      ['완결 시각', finalState.final_completed_at ? formatTimestamp(finalState.final_completed_at) : '-'],
      ['결정 주체', finalState.resolved_by || '-'],
    ];

    entries.forEach(([label, value]) => {
      const row = document.createElement('div');
      row.className = 'flex items-center justify-between gap-4 rounded-lg border border-white/10 bg-black/30 px-3 py-2';
      const labelEl = document.createElement('span');
      labelEl.className = 'text-xs text-white/50';
      labelEl.textContent = label;
      const valueEl = document.createElement('span');
      valueEl.className = 'text-xs text-white/90';
      valueEl.textContent = value;
      row.appendChild(labelEl);
      row.appendChild(valueEl);
      box.appendChild(row);
    });

    if (notice) {
      if (finalState.is_scheduled_completion) {
        notice.classList.remove('hidden');
      } else {
        notice.classList.add('hidden');
      }
    }
  };

  const renderSelectedContent = () => {
    const item = STATE.manage.selected;
    const titleEl = document.getElementById('selectedTitle');
    const metaEl = document.getElementById('selectedMeta');
    const statusEl = document.getElementById('selectedStatus');
    const thumbEl = document.getElementById('selectedThumb');
    const thumbFallback = document.getElementById('selectedThumbFallback');
    const overrideInput = document.getElementById('overrideCompletedAt');
    const overrideReason = document.getElementById('overrideReason');
    const publicationInput = document.getElementById('publicationAt');
    const publicationReason = document.getElementById('publicationReason');

    if (!item) {
      if (titleEl) titleEl.textContent = '콘텐츠를 선택하세요';
      if (metaEl) metaEl.textContent = '';
      if (statusEl) statusEl.textContent = '';
      if (thumbEl) {
        thumbEl.classList.add('hidden');
        thumbEl.src = '';
      }
      if (thumbFallback) thumbFallback.classList.remove('hidden');
      if (overrideInput) overrideInput.value = '';
      if (overrideReason) overrideReason.value = '';
      if (publicationInput) publicationInput.value = '';
      if (publicationReason) publicationReason.value = '';
      renderFinalState(null, null);
      renderPublicationCdcStatus();
      return;
    }

    const title = item.title || item.normalized_title || item.content_id;
    if (titleEl) titleEl.textContent = title;
    if (metaEl) metaEl.textContent = `${item.source || '-'} · ${item.content_type || '-'}`;
    if (statusEl) statusEl.textContent = `현재 상태: ${item.status || '-'}`;

    const thumbnail = getThumbnailUrl(item);
    if (thumbEl && thumbnail) {
      thumbEl.src = thumbnail;
      thumbEl.classList.remove('hidden');
      if (thumbFallback) thumbFallback.classList.add('hidden');
    } else if (thumbEl) {
      thumbEl.classList.add('hidden');
      thumbEl.src = '';
      if (thumbFallback) thumbFallback.classList.remove('hidden');
    }

    const key = getKey(item);
    const override = STATE.manage.overridesMap.get(key);
    const publication = STATE.manage.publicationsMap.get(key);

    if (overrideInput) {
      overrideInput.value = override ? toDatetimeLocalValueKst(override.override_completed_at) : '';
    }
    if (overrideReason) {
      overrideReason.value = override?.reason || '';
    }
    if (publicationInput) {
      publicationInput.value = publication ? toDatetimeLocalValueKst(publication.public_at) : '';
    }
    if (publicationReason) {
      publicationReason.value = publication?.reason || '';
    }

    renderFinalState(item, override);
    renderPublicationCdcStatus();
  };

  const selectManageItem = (item) => {
    STATE.manage.selected = item;
    renderManageResults();
    renderSelectedContent();
  };

  const openLookupInManage = async (item) => {
    if (!item?.content_id || !item?.source) {
      showToast('콘텐츠 식별자를 찾지 못했습니다.', { type: 'error' });
      return;
    }

    const params = new URLSearchParams();
    params.set('content_id', item.content_id);
    params.set('source', item.source);

    try {
      const payload = await apiRequest('GET', `/api/admin/contents/lookup?${params.toString()}`, {
        token: STATE.token,
      });
      if (!payload?.content) {
        showToast('콘텐츠 정보를 불러오지 못했습니다.', { type: 'error' });
        return;
      }
      STATE.manage.results = [payload.content];
      STATE.manage.selected = payload.content;

      if (payload.override) {
        STATE.manage.overridesMap.set(getKey(payload.content), payload.override);
      } else {
        STATE.manage.overridesMap.delete(getKey(payload.content));
      }
      if (payload.publication) {
        STATE.manage.publicationsMap.set(getKey(payload.content), payload.publication);
      } else {
        STATE.manage.publicationsMap.delete(getKey(payload.content));
      }

      setTab('manage');
      renderManageResults();
      renderSelectedContent();
    } catch (err) {
      showToast(err.message || '콘텐츠 정보를 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const updateOverrideHelper = (payload) => {
    const helper = document.getElementById('overrideHelperText');
    if (!helper) return;
    if (!payload) {
      helper.textContent = '';
      return;
    }
    const eventRecorded = payload.event_recorded;
    helper.textContent =
      typeof eventRecorded === 'boolean'
        ? `CDC event recorded: ${eventRecorded ? 'yes' : 'no'}`
        : '';
  };

  const buildPublicationCdcUiMessage = (action) => {
    if (!action) {
      return { tone: 'info', text: '' };
    }

    if (action.kind === 'delete') {
      return { tone: 'success', text: '공개일이 삭제되었습니다.' };
    }

    const publicAtLabel = action.public_at ? ` · 공개일: ${formatTimestamp(action.public_at)}` : '';
    if (action.event_due_now === false) {
      return {
        tone: 'info',
        text: `공개일 저장됨 (도래 전: 배치에서 CDC 기록)${publicAtLabel}`,
      };
    }

    if (action.event_due_now === true && action.event_recorded === true) {
      if (action.event_inserted === true) {
        return {
          tone: 'success',
          text: `공개일 저장 + CDC 기록됨 (신규)${publicAtLabel}`,
        };
      }
      return {
        tone: 'success',
        text: `공개일 저장 + CDC 이미 존재 (멱등)${publicAtLabel}`,
      };
    }

    if (action.event_due_now === true && action.event_recorded === false) {
      const reason = action.event_skipped_reason || '사유 없음';
      return {
        tone: 'info',
        text: `공개일 저장 (CDC 스킵: ${reason})${publicAtLabel}`,
      };
    }

    return {
      tone: 'success',
      text: `공개일 저장됨${publicAtLabel}`,
    };
  };

  const renderPublicationCdcStatus = () => {
    const item = STATE.manage.selected;
    const el = document.getElementById('publicationCdcStatus');
    if (!el) return;

    if (!item) {
      el.textContent = '';
      el.classList.add('hidden');
      return;
    }

    const action = STATE.manage.publicationActionMap.get(getKey(item)) || null;
    if (!action) {
      el.textContent = '';
      el.classList.add('hidden');
      return;
    }

    const msg = buildPublicationCdcUiMessage(action);
    el.textContent = msg.text;
    el.classList.remove('hidden');
  };

  const performSearch = async () => {
    const input = document.getElementById('manageSearchInput');
    const typeSelect = document.getElementById('manageTypeSelect');
    const sourceSelect = document.getElementById('manageSourceSelect');

    const q = input?.value?.trim() || '';
    if (q.length < 2) {
      showToast('검색어는 2자 이상 입력해야 합니다.', { type: 'error' });
      return;
    }

    const params = new URLSearchParams();
    params.set('q', q);
    if (typeSelect && typeSelect.value !== 'all') params.set('type', typeSelect.value);
    if (sourceSelect && sourceSelect.value !== 'all') params.set('source', sourceSelect.value);

    try {
      const results = await apiRequest('GET', `/api/contents/search?${params.toString()}`);
      STATE.manage.results = Array.isArray(results) ? results.slice(0, 100) : [];
      STATE.manage.selected = null;
      renderManageResults();
      renderSelectedContent();
    } catch (err) {
      showToast(err.message || '검색 중 오류가 발생했습니다.', { type: 'error' });
    }
  };

  const saveOverride = async () => {
    const item = STATE.manage.selected;
    if (!item) {
      showToast('먼저 콘텐츠를 선택하세요.', { type: 'error' });
      return;
    }

    const completedAt = document.getElementById('overrideCompletedAt')?.value || null;
    const reason = document.getElementById('overrideReason')?.value?.trim() || null;

    try {
      const payload = await apiRequest('POST', '/api/admin/contents/override', {
        token: STATE.token,
        body: {
          content_id: item.content_id,
          source: item.source,
          override_status: '완결',
          override_completed_at: completedAt || null,
          reason,
        },
      });
      if (payload?.override) {
        STATE.manage.overridesMap.set(getKey(item), payload.override);
      }
      updateOverrideHelper(payload);
      renderSelectedContent();
      showToast('완결 처리가 저장되었습니다.', { type: 'success' });
    } catch (err) {
      showToast(err.message || '완결 처리 저장 실패', { type: 'error' });
    }
  };

  const deleteOverride = async () => {
    const item = STATE.manage.selected;
    if (!item) {
      showToast('먼저 콘텐츠를 선택하세요.', { type: 'error' });
      return;
    }

    const reason = document.getElementById('overrideReason')?.value?.trim();
    if (!reason) {
      showToast('삭제 사유를 입력해주세요.', { type: 'error' });
      return;
    }

    openConfirm({
      title: '완결 처리 삭제',
      message: '완결 처리 정보를 삭제할까요?',
      onConfirm: async () => {
        try {
          await apiRequest('DELETE', '/api/admin/contents/override', {
            token: STATE.token,
            body: {
              content_id: item.content_id,
              source: item.source,
              reason,
            },
          });
          STATE.manage.overridesMap.delete(getKey(item));
          updateOverrideHelper(null);
          renderSelectedContent();
          showToast('완결 처리 정보가 삭제되었습니다.', { type: 'success' });
        } catch (err) {
          showToast(err.message || '완결 처리 삭제 실패', { type: 'error' });
        }
      },
    });
  };

  const savePublication = async () => {
    const item = STATE.manage.selected;
    if (!item) {
      showToast('먼저 콘텐츠를 선택하세요.', { type: 'error' });
      return;
    }

    const publicAt = document.getElementById('publicationAt')?.value || '';
    if (!publicAt) {
      showToast('공개일을 입력해주세요.', { type: 'error' });
      return;
    }
    const reason = document.getElementById('publicationReason')?.value?.trim() || null;

    try {
      const payload = await apiRequest('POST', '/api/admin/contents/publication', {
        token: STATE.token,
        body: {
          content_id: item.content_id,
          source: item.source,
          public_at: publicAt,
          reason,
        },
      });
      if (payload?.publication) {
        STATE.manage.publicationsMap.set(getKey(item), payload.publication);
      }
      const action = {
        kind: 'save',
        public_at: payload?.publication?.public_at || publicAt || null,
        saved_at: new Date().toISOString(),
        event_due_now: payload?.event_due_now ?? null,
        event_recorded: payload?.event_recorded ?? null,
        event_inserted: payload?.event_inserted ?? null,
        event_skipped_reason: payload?.event_skipped_reason ?? null,
      };
      STATE.manage.publicationActionMap.set(getKey(item), action);
      renderSelectedContent();
      const message = buildPublicationCdcUiMessage(action);
      showToast(message.text, { type: message.tone });
    } catch (err) {
      showToast(err.message || '공개일 저장 실패', { type: 'error' });
    }
  };

  const deletePublication = async () => {
    const item = STATE.manage.selected;
    if (!item) {
      showToast('먼저 콘텐츠를 선택하세요.', { type: 'error' });
      return;
    }

    const reason = document.getElementById('publicationReason')?.value?.trim();
    if (!reason) {
      showToast('삭제 사유를 입력해주세요.', { type: 'error' });
      return;
    }

    openConfirm({
      title: '공개일 삭제',
      message: '공개일 정보를 삭제할까요?',
      onConfirm: async () => {
        try {
          await apiRequest('DELETE', '/api/admin/contents/publication', {
            token: STATE.token,
            body: {
              content_id: item.content_id,
              source: item.source,
              reason,
            },
          });
          STATE.manage.publicationsMap.delete(getKey(item));
          const action = {
            kind: 'delete',
            public_at: null,
            saved_at: new Date().toISOString(),
            event_due_now: null,
            event_recorded: null,
            event_inserted: null,
            event_skipped_reason: null,
          };
          STATE.manage.publicationActionMap.set(getKey(item), action);
          renderSelectedContent();
          const message = buildPublicationCdcUiMessage(action);
          showToast(message.text, { type: message.tone });
        } catch (err) {
          showToast(err.message || '공개일 삭제 실패', { type: 'error' });
        }
      },
    });
  };

  const softDelete = async () => {
    const item = STATE.manage.selected;
    if (!item) {
      showToast('먼저 콘텐츠를 선택하세요.', { type: 'error' });
      return;
    }

    const reason = document.getElementById('deleteReason')?.value?.trim();
    if (!reason) {
      showToast('삭제 사유를 입력해주세요.', { type: 'error' });
      return;
    }

    openConfirm({
      title: '콘텐츠 삭제',
      message: '이 콘텐츠를 삭제할까요?',
      onConfirm: async () => {
        try {
          const payload = await apiRequest('POST', '/api/admin/contents/delete', {
            token: STATE.token,
            body: {
              content_id: item.content_id,
              source: item.source,
              reason,
            },
          });
          const message =
            '콘텐츠 삭제 완료 (구독 유지됨 · 삭제 콘텐츠는 유저 알림이 발송되지 않음)';
          showToast(message, { type: 'success' });
          STATE.manage.results = STATE.manage.results.filter(
            (entry) => !(entry.content_id === item.content_id && entry.source === item.source),
          );
          STATE.manage.selected = null;
          renderManageResults();
          renderSelectedContent();
          if (STATE.tab === 'deleted') {
            loadDeleted();
          }
        } catch (err) {
          showToast(err.message || '콘텐츠 삭제 실패', { type: 'error' });
        }
      },
    });
  };

  const renderDeletedList = () => {
    const container = document.getElementById('deletedList');
    if (!container) return;
    container.innerHTML = '';

    if (!STATE.deleted.items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '삭제된 콘텐츠가 없습니다.';
      container.appendChild(empty);
      return;
    }

    STATE.deleted.items.forEach((item) => {
      const card = document.createElement('div');
      const isSelected =
        STATE.deleted.selected &&
        STATE.deleted.selected.content_id === item.content_id &&
        STATE.deleted.selected.source === item.source;
      card.className = `w-full rounded-xl border px-3 py-2 text-left text-sm transition hover:border-white/40 ${
        isSelected ? 'border-emerald-300/60 bg-emerald-500/10' : 'border-white/10 bg-black/30'
      }`;

      const row = document.createElement('div');
      row.className = 'flex items-start gap-3';

      const thumbWrap = document.createElement('div');
      thumbWrap.className =
        'flex h-12 w-12 items-center justify-center overflow-hidden rounded-lg border border-white/10 bg-black/40';
      const thumbImg = document.createElement('img');
      const thumbUrl = getThumbnailUrl(item);
      thumbImg.className = 'h-full w-full object-cover';
      if (thumbUrl) {
        thumbImg.src = thumbUrl;
      } else {
        thumbImg.classList.add('hidden');
        const fallback = document.createElement('span');
        fallback.className = 'text-[10px] text-white/40';
        fallback.textContent = 'No Image';
        thumbWrap.appendChild(fallback);
      }
      thumbWrap.appendChild(thumbImg);

      const contentBox = document.createElement('div');
      contentBox.className = 'flex-1';
      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      title.textContent = item.title || item.normalized_title || item.content_id;
      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      meta.textContent = `${item.source || '-'} · ${item.content_type || '-'} · 삭제: ${formatTimestamp(
        item.deleted_at,
      )}`;

      const reason = document.createElement('div');
      reason.className = 'mt-1 text-[11px] text-white/50';
      reason.textContent = `사유: ${item.deleted_reason || '-'}`;

      const extra = document.createElement('div');
      extra.className = 'mt-1 flex flex-wrap gap-2 text-[11px] text-white/50';
      const subscriptionCount = Number.isFinite(item.subscription_count)
        ? item.subscription_count
        : Number(item.subscription_count) || 0;
      const subscriptionBadge = document.createElement('span');
      subscriptionBadge.className = 'rounded-full border border-white/10 bg-black/40 px-2 py-0.5';
      subscriptionBadge.textContent = `구독: ${subscriptionCount}`;
      extra.appendChild(subscriptionBadge);

      if (item.override_status) {
        const overrideBadge = document.createElement('span');
        overrideBadge.className = 'rounded-full border border-white/10 bg-black/40 px-2 py-0.5';
        overrideBadge.textContent = `완결 override: ${item.override_status}`;
        extra.appendChild(overrideBadge);
      }

      if (item.override_completed_at) {
        const overrideDateBadge = document.createElement('span');
        overrideDateBadge.className = 'rounded-full border border-white/10 bg-black/40 px-2 py-0.5';
        overrideDateBadge.textContent = `override 시각: ${formatTimestamp(item.override_completed_at)}`;
        extra.appendChild(overrideDateBadge);
      }

      contentBox.appendChild(title);
      contentBox.appendChild(meta);
      contentBox.appendChild(reason);
      contentBox.appendChild(extra);

      const actions = document.createElement('div');
      actions.className = 'flex flex-col gap-2';
      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      openBtn.textContent = 'Open';
      openBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        openLookupInManage(item);
      });
      actions.appendChild(openBtn);

      row.appendChild(thumbWrap);
      row.appendChild(contentBox);
      row.appendChild(actions);

      card.appendChild(row);
      card.addEventListener('click', () => {
        STATE.deleted.selected = item;
        renderDeletedList();
        renderDeletedDetail();
      });
      container.appendChild(card);
    });
  };

  const renderDeletedDetail = () => {
    const container = document.getElementById('deletedDetail');
    if (!container) return;
    container.innerHTML = '';
    const item = STATE.deleted.selected;

    if (!item) {
      container.innerHTML = '<p class="text-xs text-white/40">선택된 콘텐츠가 없습니다.</p>';
      return;
    }

    const entries = [
      ['제목', item.title || item.normalized_title || item.content_id],
      ['소스', item.source || '-'],
      ['구독자 수', `${item.subscription_count ?? 0}`],
      ['완결 override', item.override_status || '-'],
      [
        'override 시각',
        item.override_completed_at ? formatTimestamp(item.override_completed_at) : '-',
      ],
      ['삭제 시각', item.deleted_at ? formatTimestamp(item.deleted_at) : '-'],
      ['삭제 사유', item.deleted_reason || '-'],
      ['삭제자', item.deleted_by || '-'],
    ];

    entries.forEach(([label, value]) => {
      const row = document.createElement('div');
      row.className = 'flex items-center justify-between gap-4 rounded-lg border border-white/10 bg-black/30 px-3 py-2';
      const labelEl = document.createElement('span');
      labelEl.className = 'text-xs text-white/50';
      labelEl.textContent = label;
      const valueEl = document.createElement('span');
      valueEl.className = 'text-xs text-white/90';
      valueEl.textContent = value;
      row.appendChild(labelEl);
      row.appendChild(valueEl);
      container.appendChild(row);
    });
  };

  const updateDeletedPagination = () => {
    const prevBtn = document.getElementById('deletedPrevBtn');
    const nextBtn = document.getElementById('deletedNextBtn');
    setButtonDisabled(prevBtn, STATE.deleted.offset === 0);
    setButtonDisabled(nextBtn, STATE.deleted.lastCount < STATE.deleted.limit);
  };

  const loadDeleted = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.deleted.limit);
    params.set('offset', STATE.deleted.offset);
    if (STATE.deleted.q) params.set('q', STATE.deleted.q);

    try {
      const payload = await apiRequest('GET', `/api/admin/contents/deleted?${params.toString()}`, {
        token: STATE.token,
      });
      STATE.deleted.items = payload?.deleted_contents || [];
      STATE.deleted.lastCount = STATE.deleted.items.length;
      STATE.deleted.selected = null;
      renderDeletedList();
      renderDeletedDetail();
      updateDeletedPagination();
    } catch (err) {
      showToast(err.message || '삭제 목록을 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const restoreDeleted = async () => {
    const item = STATE.deleted.selected;
    if (!item) {
      showToast('복구할 콘텐츠를 선택하세요.', { type: 'error' });
      return;
    }

    openConfirm({
      title: '콘텐츠 복구',
      message: '이 콘텐츠를 복구할까요?',
      onConfirm: async () => {
        try {
          await apiRequest('POST', '/api/admin/contents/restore', {
            token: STATE.token,
            body: {
              content_id: item.content_id,
              source: item.source,
            },
          });
          showToast('콘텐츠가 복구되었습니다.', { type: 'success' });
          STATE.deleted.items = STATE.deleted.items.filter(
            (entry) => !(entry.content_id === item.content_id && entry.source === item.source),
          );
          STATE.deleted.selected = null;
          renderDeletedList();
          renderDeletedDetail();
        } catch (err) {
          showToast(err.message || '복구 실패', { type: 'error' });
        }
      },
    });
  };

  const renderPublications = () => {
    const container = document.getElementById('publicationsList');
    if (!container) return;
    container.innerHTML = '';

    if (!STATE.publications.items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '최근 변경된 공개일이 없습니다.';
      container.appendChild(empty);
      return;
    }

    STATE.publications.items.forEach((item) => {
      const card = document.createElement('div');
      card.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80';

      const row = document.createElement('div');
      row.className = 'flex items-start gap-3';

      const thumbWrap = document.createElement('div');
      thumbWrap.className =
        'flex h-12 w-12 items-center justify-center overflow-hidden rounded-lg border border-white/10 bg-black/40';
      const thumbImg = document.createElement('img');
      const thumbUrl = getThumbnailUrl(item);
      thumbImg.className = 'h-full w-full object-cover';
      if (thumbUrl) {
        thumbImg.src = thumbUrl;
      } else {
        thumbImg.classList.add('hidden');
        const fallback = document.createElement('span');
        fallback.className = 'text-[10px] text-white/40';
        fallback.textContent = 'No Image';
        thumbWrap.appendChild(fallback);
      }
      thumbWrap.appendChild(thumbImg);

      const contentBox = document.createElement('div');
      contentBox.className = 'flex-1';
      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      title.textContent = item.title || item.normalized_title || item.content_id;
      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      meta.textContent = `${item.source || '-'} · ${item.content_type || '-'} · ${item.status || '-'}`;
      const detail = document.createElement('div');
      detail.className = 'mt-1 text-[11px] text-white/50';
      detail.textContent = `공개일: ${formatTimestamp(item.public_at)} · 사유: ${item.reason || '-'}`;

      if (item.is_deleted) {
        const badge = document.createElement('span');
        badge.className =
          'ml-2 inline-flex rounded-full border border-red-400/40 bg-red-500/10 px-2 py-0.5 text-[10px] text-red-100';
        badge.textContent = '삭제됨';
        detail.appendChild(badge);
      }

      contentBox.appendChild(title);
      contentBox.appendChild(meta);
      contentBox.appendChild(detail);

      const actions = document.createElement('div');
      actions.className = 'flex flex-col gap-2';
      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      openBtn.textContent = 'Open';
      openBtn.addEventListener('click', () => openLookupInManage(item));
      actions.appendChild(openBtn);

      row.appendChild(thumbWrap);
      row.appendChild(contentBox);
      row.appendChild(actions);

      card.appendChild(row);
      container.appendChild(card);
    });
  };

  const renderMissingList = ({ items, containerId, emptyText, detailBuilder }) => {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = '';

    if (!items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = emptyText;
      container.appendChild(empty);
      return;
    }

    items.forEach((item) => {
      const card = document.createElement('div');
      card.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80';

      const row = document.createElement('div');
      row.className = 'flex items-start gap-3';

      const thumbWrap = document.createElement('div');
      thumbWrap.className =
        'flex h-12 w-12 items-center justify-center overflow-hidden rounded-lg border border-white/10 bg-black/40';
      const thumbImg = document.createElement('img');
      const thumbUrl = getThumbnailUrl(item);
      thumbImg.className = 'h-full w-full object-cover';
      if (thumbUrl) {
        thumbImg.src = thumbUrl;
      } else {
        thumbImg.classList.add('hidden');
        const fallback = document.createElement('span');
        fallback.className = 'text-[10px] text-white/40';
        fallback.textContent = 'No Image';
        thumbWrap.appendChild(fallback);
      }
      thumbWrap.appendChild(thumbImg);

      const contentBox = document.createElement('div');
      contentBox.className = 'flex-1';
      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      title.textContent = item.title || item.normalized_title || item.content_id;
      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      meta.textContent = `${item.source || '-'} · ${item.content_type || '-'} · ${item.status || '-'}`;
      const detail = document.createElement('div');
      detail.className = 'mt-1 text-[11px] text-white/50';
      detail.textContent = detailBuilder(item);

      contentBox.appendChild(title);
      contentBox.appendChild(meta);
      contentBox.appendChild(detail);

      const actions = document.createElement('div');
      actions.className = 'flex flex-col gap-2';
      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      openBtn.textContent = 'Open';
      openBtn.addEventListener('click', () => openLookupInManage(item));
      actions.appendChild(openBtn);

      row.appendChild(thumbWrap);
      row.appendChild(contentBox);
      row.appendChild(actions);

      card.appendChild(row);
      container.appendChild(card);
    });
  };

  const renderMissingCompletionList = () => {
    renderMissingList({
      items: STATE.missingCompletion.items,
      containerId: 'missingCompletionList',
      emptyText: '완결일 미설정 콘텐츠가 없습니다.',
      detailBuilder: (item) => {
        const overrideStatus = item.override_status || '-';
        const overrideCompletedAt = item.override_completed_at
          ? formatTimestamp(item.override_completed_at)
          : '미설정';
        return `override: ${overrideStatus} · 완결일: ${overrideCompletedAt}`;
      },
    });
  };

  const renderMissingPublicationList = () => {
    renderMissingList({
      items: STATE.missingPublication.items,
      containerId: 'missingPublicationList',
      emptyText: '공개일 미설정 콘텐츠가 없습니다.',
      detailBuilder: () => '공개일: 미설정',
    });
  };

  const renderAuditList = () => {
    const container = document.getElementById('auditList');
    if (!container) return;
    container.innerHTML = '';

    if (!STATE.audit.items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '운영 로그가 없습니다.';
      container.appendChild(empty);
      return;
    }

    STATE.audit.items.forEach((item) => {
      const card = document.createElement('div');
      card.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80';

      const header = document.createElement('div');
      header.className = 'flex items-start justify-between gap-2';

      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      const titleText = item.title || `${item.content_id || '-'}::${item.source || '-'}`;
      title.textContent = titleText;

      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      openBtn.textContent = 'Open';
      openBtn.addEventListener('click', () => openLookupInManage(item));

      header.appendChild(title);
      header.appendChild(openBtn);

      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      const label = AUDIT_ACTION_LABELS[item.action_type] || item.action_type || '-';
      const adminLabel = item.admin_email || `ID ${item.admin_id || '-'}`;
      meta.textContent = `${label} · ${adminLabel} · ${formatTimestamp(item.created_at)}`;

      const reason = document.createElement('div');
      reason.className = 'mt-1 text-[11px] text-white/50';
      reason.textContent = `사유: ${item.reason || '-'}`;

      card.appendChild(header);
      card.appendChild(meta);
      card.appendChild(reason);
      container.appendChild(card);
    });
  };

  const renderCdcEventsList = () => {
    const container = document.getElementById('cdcEventsList');
    if (!container) return;
    container.innerHTML = '';

    if (!STATE.cdcEvents.items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = 'CDC 이벤트가 없습니다.';
      container.appendChild(empty);
      return;
    }

    STATE.cdcEvents.items.forEach((item) => {
      const card = document.createElement('div');
      card.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80';

      const header = document.createElement('div');
      header.className = 'flex items-start justify-between gap-2';

      const titleWrapper = document.createElement('div');
      titleWrapper.className = 'flex flex-wrap items-center gap-2';

      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      title.textContent = item.title || item.content_id || '-';

      const source = document.createElement('span');
      source.className = 'text-xs text-white/50';
      source.textContent = item.source || '-';

      titleWrapper.appendChild(title);
      titleWrapper.appendChild(source);

      if (item.is_deleted) {
        const deletedBadge = document.createElement('span');
        deletedBadge.className =
          'inline-flex rounded-full border border-red-400/40 bg-red-500/10 px-2 py-0.5 text-[10px] text-red-100';
        deletedBadge.textContent = 'DELETED';
        titleWrapper.appendChild(deletedBadge);
      }

      const actions = document.createElement('div');
      actions.className = 'flex items-center gap-2';

      const jsonBtn = document.createElement('button');
      jsonBtn.type = 'button';
      jsonBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      jsonBtn.textContent = 'JSON';
      jsonBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        const titleText = item.title || item.content_id || '-';
        openDetailModal({
          title: `CDC Event · ${item.event_type || '-'}`,
          subtitle: `${titleText} · ${item.source || '-'}`,
          obj: item,
        });
      });

      const openBtn = document.createElement('button');
      openBtn.type = 'button';
      openBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      openBtn.textContent = 'Open';
      openBtn.addEventListener('click', () =>
        openLookupInManage({ content_id: item.content_id, source: item.source }),
      );

      actions.appendChild(jsonBtn);
      actions.appendChild(openBtn);

      header.appendChild(titleWrapper);
      header.appendChild(actions);

      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      meta.textContent = `${item.event_type || '-'} · ${item.resolved_by || '-'} · ${formatTimestamp(
        item.created_at,
      )}`;

      const detail = document.createElement('div');
      detail.className = 'mt-1 text-[11px] text-white/50';
      detail.textContent = `final: ${item.final_status || '-'} · at: ${
        item.final_completed_at ? formatTimestamp(item.final_completed_at) : '-'
      }`;

      card.appendChild(header);
      card.appendChild(meta);
      card.appendChild(detail);
      container.appendChild(card);
    });
  };

  const renderCrawlerReportsList = () => {
    const container = document.getElementById('crawlerReportsList');
    if (!container) return;
    container.innerHTML = '';

    if (!STATE.crawlerReports.items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '배치 리포트가 없습니다.';
      container.appendChild(empty);
      return;
    }

    STATE.crawlerReports.items.forEach((item) => {
      const card = document.createElement('div');
      card.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80';

      const header = document.createElement('div');
      header.className = 'flex items-start justify-between gap-2';

      const title = document.createElement('div');
      title.className = 'font-semibold text-white';
      title.textContent = item.crawler_name || '-';

      const statusBadge = document.createElement('span');
      const statusValue = item.status || '-';
      const normalizedStatus = item.normalized_status || normalizeReportStatus(statusValue);
      statusBadge.className = getStatusBadgeClasses(normalizedStatus);
      statusBadge.textContent = statusValue;

      const right = document.createElement('div');
      right.className = 'flex items-center gap-2';

      const jsonBtn = document.createElement('button');
      jsonBtn.type = 'button';
      jsonBtn.className =
        'rounded-full border border-white/20 px-3 py-1 text-[11px] text-white/70 transition hover:border-white/40 hover:text-white';
      jsonBtn.textContent = 'JSON';
      jsonBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        openDetailModal({
          title: 'Crawler Report',
          subtitle: `${item.crawler_name || '-'} · ${formatTimestamp(item.created_at)}`,
          obj: item,
        });
      });

      right.appendChild(statusBadge);
      right.appendChild(jsonBtn);

      header.appendChild(title);
      header.appendChild(right);

      const meta = document.createElement('div');
      meta.className = 'mt-1 text-xs text-white/60';
      meta.textContent = `${statusValue} · ${formatTimestamp(item.created_at)}`;

      const summary = document.createElement('div');
      summary.className = 'mt-1 text-[11px] text-white/50';
      summary.textContent = formatReportSummary(item.report_data, normalizedStatus);

      card.appendChild(header);
      card.appendChild(meta);
      card.appendChild(summary);
      container.appendChild(card);
    });
  };

  const updateMissingCompletionPagination = () => {
    const prevBtn = document.getElementById('missingCompletionPrevBtn');
    const nextBtn = document.getElementById('missingCompletionNextBtn');
    setButtonDisabled(prevBtn, STATE.missingCompletion.offset === 0);
    setButtonDisabled(nextBtn, STATE.missingCompletion.lastCount < STATE.missingCompletion.limit);
  };

  const updateMissingPublicationPagination = () => {
    const prevBtn = document.getElementById('missingPublicationPrevBtn');
    const nextBtn = document.getElementById('missingPublicationNextBtn');
    setButtonDisabled(prevBtn, STATE.missingPublication.offset === 0);
    setButtonDisabled(nextBtn, STATE.missingPublication.lastCount < STATE.missingPublication.limit);
  };

  const loadPublications = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.publications.limit);
    params.set('offset', STATE.publications.offset);

    try {
      const payload = await apiRequest(
        'GET',
        `/api/admin/contents/publications?${params.toString()}`,
        { token: STATE.token },
      );
      STATE.publications.items = payload?.publications || [];
      STATE.publications.lastCount = STATE.publications.items.length;
      renderPublications();
      updatePublicationsPagination();
    } catch (err) {
      showToast(err.message || '공개일 목록을 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const loadMissingCompletion = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.missingCompletion.limit);
    params.set('offset', STATE.missingCompletion.offset);
    if (STATE.missingCompletion.source !== 'all') params.set('source', STATE.missingCompletion.source);
    if (STATE.missingCompletion.contentType !== 'all') {
      params.set('content_type', STATE.missingCompletion.contentType);
    }
    if (STATE.missingCompletion.q) params.set('q', STATE.missingCompletion.q);

    try {
      const payload = await apiRequest(
        'GET',
        `/api/admin/contents/missing-completion?${params.toString()}`,
        { token: STATE.token },
      );
      STATE.missingCompletion.items = payload?.items || [];
      STATE.missingCompletion.lastCount = STATE.missingCompletion.items.length;
      renderMissingCompletionList();
      updateMissingCompletionPagination();
    } catch (err) {
      showToast(err.message || '완결일 미설정 목록을 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const refreshMissingCompletion = async () => {
    const source = document.getElementById('missingCompletionSourceSelect')?.value || 'all';
    const contentType = document.getElementById('missingCompletionTypeSelect')?.value || 'all';
    const q = document.getElementById('missingCompletionSearchInput')?.value?.trim() || '';
    STATE.missingCompletion.source = source;
    STATE.missingCompletion.contentType = contentType;
    STATE.missingCompletion.q = q;
    STATE.missingCompletion.offset = 0;
    await loadMissingCompletion();
  };

  const loadMissingPublication = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.missingPublication.limit);
    params.set('offset', STATE.missingPublication.offset);
    if (STATE.missingPublication.source !== 'all') params.set('source', STATE.missingPublication.source);
    if (STATE.missingPublication.contentType !== 'all') {
      params.set('content_type', STATE.missingPublication.contentType);
    }
    if (STATE.missingPublication.q) params.set('q', STATE.missingPublication.q);

    try {
      const payload = await apiRequest(
        'GET',
        `/api/admin/contents/missing-publication?${params.toString()}`,
        { token: STATE.token },
      );
      STATE.missingPublication.items = payload?.items || [];
      STATE.missingPublication.lastCount = STATE.missingPublication.items.length;
      renderMissingPublicationList();
      updateMissingPublicationPagination();
    } catch (err) {
      showToast(err.message || '공개일 미설정 목록을 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const refreshMissingPublication = async () => {
    const source = document.getElementById('missingPublicationSourceSelect')?.value || 'all';
    const contentType = document.getElementById('missingPublicationTypeSelect')?.value || 'all';
    const q = document.getElementById('missingPublicationSearchInput')?.value?.trim() || '';
    STATE.missingPublication.source = source;
    STATE.missingPublication.contentType = contentType;
    STATE.missingPublication.q = q;
    STATE.missingPublication.offset = 0;
    await loadMissingPublication();
  };

  const loadAudit = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.audit.limit);
    params.set('offset', STATE.audit.offset);
    if (STATE.audit.q) params.set('q', STATE.audit.q);
    if (STATE.audit.actionType) params.set('action_type', STATE.audit.actionType);

    try {
      const payload = await apiRequest('GET', `/api/admin/audit/logs?${params.toString()}`, {
        token: STATE.token,
      });
      STATE.audit.items = payload?.logs || [];
      STATE.audit.lastCount = STATE.audit.items.length;
      renderAuditList();
      updateAuditPagination();
    } catch (err) {
      showToast(err.message || '운영 로그를 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const loadCdcEvents = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.cdcEvents.limit);
    params.set('offset', STATE.cdcEvents.offset);
    if (STATE.cdcEvents.q) params.set('q', STATE.cdcEvents.q);
    if (STATE.cdcEvents.eventType) params.set('event_type', STATE.cdcEvents.eventType);
    if (STATE.cdcEvents.source) params.set('source', STATE.cdcEvents.source);
    if (STATE.cdcEvents.contentId) params.set('content_id', STATE.cdcEvents.contentId);
    if (STATE.cdcEvents.createdFrom) params.set('created_from', STATE.cdcEvents.createdFrom);
    if (STATE.cdcEvents.createdTo) params.set('created_to', STATE.cdcEvents.createdTo);

    try {
      const payload = await apiRequest('GET', `/api/admin/cdc/events?${params.toString()}`, {
        token: STATE.token,
      });
      STATE.cdcEvents.items = payload?.events || [];
      STATE.cdcEvents.lastCount = STATE.cdcEvents.items.length;
      renderCdcEventsList();
      updateCdcPagination();
    } catch (err) {
      showToast(err.message || 'CDC 이벤트를 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const refreshCdcEventsFromInputs = async () => {
    STATE.cdcEvents.q = document.getElementById('cdcSearchInput')?.value?.trim() || '';
    STATE.cdcEvents.eventType = document.getElementById('cdcEventTypeSelect')?.value || '';
    STATE.cdcEvents.source = document.getElementById('cdcSourceSelect')?.value || '';
    STATE.cdcEvents.contentId = document.getElementById('cdcContentIdInput')?.value?.trim() || '';
    STATE.cdcEvents.createdFrom = document.getElementById('cdcCreatedFrom')?.value || '';
    STATE.cdcEvents.createdTo = document.getElementById('cdcCreatedTo')?.value || '';
    STATE.cdcEvents.offset = 0;
    await loadCdcEvents();
  };

  const loadCrawlerReports = async () => {
    const params = new URLSearchParams();
    params.set('limit', STATE.crawlerReports.limit);
    params.set('offset', STATE.crawlerReports.offset);
    if (STATE.crawlerReports.crawlerName) {
      params.set('crawler_name', STATE.crawlerReports.crawlerName);
    }
    if (STATE.crawlerReports.status) params.set('status', STATE.crawlerReports.status);
    if (STATE.crawlerReports.createdFrom) params.set('created_from', STATE.crawlerReports.createdFrom);
    if (STATE.crawlerReports.createdTo) params.set('created_to', STATE.crawlerReports.createdTo);

    try {
      const payload = await apiRequest(
        'GET',
        `/api/admin/reports/daily-crawler?${params.toString()}`,
        { token: STATE.token },
      );
      STATE.crawlerReports.items = payload?.reports || [];
      STATE.crawlerReports.lastCount = STATE.crawlerReports.items.length;
      renderCrawlerReportsList();
      updateReportsPagination();
    } catch (err) {
      showToast(err.message || '배치 리포트를 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const renderDailyNotificationReport = (payload) => {
    const summaryEl = document.getElementById('dailyNotificationSummary');
    const listEl = document.getElementById('dailyNotificationCompletedList');
    const copyBtn = document.getElementById('dailyNotificationCopyBtn');
    if (!summaryEl || !listEl) return;

    const stats = payload?.stats || {};
    const durationValue = stats.duration_seconds;
    const durationText =
      durationValue === null || durationValue === undefined
        ? '-'
        : `${Number(durationValue).toFixed(2)}초`;

    summaryEl.textContent = `작업 시간: ${payload?.generated_at || '-'} · 실행 시간: ${durationText} · 신규 DB 등록 콘텐츠: ${
      stats.new_contents_total ?? 0
    }개 · 총 알림 발생 인원: ${stats.total_recipients ?? 0}명 · 완결 이벤트: ${
      stats.completed_total ?? 0
    }건`;

    listEl.innerHTML = '';
    const items = payload?.completed_items || [];
    if (!items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '(없음)';
      listEl.appendChild(empty);
    } else {
      items.forEach((item) => {
        const card = document.createElement('div');
        card.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80';

        const header = document.createElement('div');
        header.className = 'flex flex-wrap items-center gap-2';

        const title = document.createElement('div');
        title.className = 'font-semibold text-white';
        title.textContent = item.title || item.content_id || '-';

        header.appendChild(title);

        if (item.notification_excluded) {
          const excludedBadge = document.createElement('span');
          excludedBadge.className =
            'inline-flex rounded-full border border-red-400/40 bg-red-500/10 px-2 py-0.5 text-[10px] text-red-100';
          excludedBadge.textContent = '삭제됨';
          header.appendChild(excludedBadge);
        }

        const meta = document.createElement('div');
        meta.className = 'mt-1 text-xs text-white/60';
        meta.textContent = `${item.content_id || '-'} · ${item.source || '-'} · ${
          item.subscriber_count ? `${item.subscriber_count}명` : '구독자 없음'
        }`;

        card.appendChild(header);
        card.appendChild(meta);
        listEl.appendChild(card);
      });
    }

    STATE.crawlerReports.dailyNotificationText = payload?.text_report || '';
    setButtonDisabled(copyBtn, !STATE.crawlerReports.dailyNotificationText);
  };

  const loadDailyNotificationReport = async () => {
    const dateInput = document.getElementById('dailyNotificationDateInput');
    const includeDeleted = document.getElementById('dailyNotificationIncludeDeleted');
    if (dateInput && !dateInput.value) {
      dateInput.value = getTodayKstDateString();
    }
    const params = new URLSearchParams();
    if (dateInput?.value) params.set('date', dateInput.value);
    if (includeDeleted?.checked) params.set('include_deleted', '1');

    const url = `/api/admin/reports/daily-notification?${params.toString()}`;
    try {
      const payload = await apiRequest('GET', url, { token: STATE.token });
      renderDailyNotificationReport(payload);
    } catch (err) {
      showToast(err.message || '일일 리포트를 불러오지 못했습니다.', { type: 'error' });
      STATE.crawlerReports.dailyNotificationText = '';
      const copyBtn = document.getElementById('dailyNotificationCopyBtn');
      setButtonDisabled(copyBtn, true);
    }
  };

  const renderDailySummary = (payload) => {
    const badgeEl = document.getElementById('dailySummaryBadge');
    const titleEl = document.getElementById('dailySummaryTitle');
    const metaEl = document.getElementById('dailySummaryMeta');
    const listEl = document.getElementById('dailySummaryList');
    const copyBtn = document.getElementById('dailySummaryCopyBtn');

    if (!listEl) return;

    const overallStatus = payload?.overall_status || 'empty';
    const statusLabelMap = {
      success: '성공',
      warning: '경고',
      failure: '실패',
      empty: '없음',
      unknown: '알 수 없음',
    };

    if (badgeEl) {
      badgeEl.className = getStatusBadgeClasses(overallStatus);
      badgeEl.textContent = statusLabelMap[overallStatus] || overallStatus;
    }
    if (titleEl) {
      titleEl.textContent = payload?.subject_text || 'Daily Consolidated Summary';
    }

    const rangeText = payload?.range
      ? `${payload.range.created_from || '-'} ~ ${payload.range.created_to || '-'}`
      : '-';
    const counts = payload?.counts || {};
    if (metaEl) {
      metaEl.textContent = `기간: ${rangeText} · 총 ${payload?.total_reports ?? 0}건 · 성공 ${
        counts.success ?? 0
      } / 경고 ${counts.warning ?? 0} / 실패 ${counts.failure ?? 0} / 미확인 ${
        counts.unknown ?? 0
      }`;
    }

    listEl.innerHTML = '';
    const items = payload?.items || [];
    if (!items.length) {
      const empty = document.createElement('div');
      empty.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/50';
      empty.textContent = '요약할 배치 리포트가 없습니다.';
      listEl.appendChild(empty);
    } else {
      items.forEach((item) => {
        const row = document.createElement('div');
        row.className = 'rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-xs text-white/70';

        const header = document.createElement('div');
        header.className = 'flex flex-wrap items-center gap-2';

        const name = document.createElement('span');
        name.className = 'text-sm font-semibold text-white';
        name.textContent = item.crawler_name || '-';

        const badge = document.createElement('span');
        const normalized = item.normalized_status || normalizeReportStatus(item.status);
        badge.className = getStatusBadgeClasses(normalized);
        badge.textContent = item.status || normalized;

        header.appendChild(name);
        header.appendChild(badge);

        const detail = document.createElement('div');
        detail.className = 'mt-1 text-[11px] text-white/50';
        detail.textContent = formatReportSummary(item.report_data, normalized);

        row.appendChild(header);
        row.appendChild(detail);
        listEl.appendChild(row);
      });
    }

    STATE.crawlerReports.summaryText = payload?.summary_text || '';
    setButtonDisabled(copyBtn, !STATE.crawlerReports.summaryText);
  };

  const loadDailySummary = async () => {
    const params = new URLSearchParams();
    if (STATE.crawlerReports.createdFrom) params.set('created_from', STATE.crawlerReports.createdFrom);
    if (STATE.crawlerReports.createdTo) params.set('created_to', STATE.crawlerReports.createdTo);

    const url = params.toString()
      ? `/api/admin/reports/daily-summary?${params.toString()}`
      : '/api/admin/reports/daily-summary';

    try {
      const payload = await apiRequest('GET', url, { token: STATE.token });
      renderDailySummary(payload);
    } catch (err) {
      showToast(err.message || '요약 정보를 불러오지 못했습니다.', { type: 'error' });
      STATE.crawlerReports.summaryText = '';
      const copyBtn = document.getElementById('dailySummaryCopyBtn');
      setButtonDisabled(copyBtn, true);
    }
  };

  const refreshCrawlerReportsFromInputs = async () => {
    STATE.crawlerReports.crawlerName =
      document.getElementById('reportsCrawlerNameInput')?.value?.trim() || '';
    STATE.crawlerReports.status = document.getElementById('reportsStatusSelect')?.value || '';
    STATE.crawlerReports.createdFrom = document.getElementById('reportsCreatedFrom')?.value || '';
    STATE.crawlerReports.createdTo = document.getElementById('reportsCreatedTo')?.value || '';
    STATE.crawlerReports.offset = 0;
    await Promise.all([loadCrawlerReports(), loadDailySummary()]);
  };

  const updateAuditPagination = () => {
    const prevBtn = document.getElementById('auditPrevBtn');
    const nextBtn = document.getElementById('auditNextBtn');
    setButtonDisabled(prevBtn, STATE.audit.offset === 0);
    setButtonDisabled(nextBtn, STATE.audit.lastCount < STATE.audit.limit);
  };

  const updateCdcPagination = () => {
    const prevBtn = document.getElementById('cdcPrevBtn');
    const nextBtn = document.getElementById('cdcNextBtn');
    setButtonDisabled(prevBtn, STATE.cdcEvents.offset === 0);
    setButtonDisabled(nextBtn, STATE.cdcEvents.lastCount < STATE.cdcEvents.limit);
  };

  const updateReportsPagination = () => {
    const prevBtn = document.getElementById('reportsPrevBtn');
    const nextBtn = document.getElementById('reportsNextBtn');
    setButtonDisabled(prevBtn, STATE.crawlerReports.offset === 0);
    setButtonDisabled(nextBtn, STATE.crawlerReports.lastCount < STATE.crawlerReports.limit);
  };

  const updatePublicationsPagination = () => {
    const prevBtn = document.getElementById('publicationsPrevBtn');
    const nextBtn = document.getElementById('publicationsNextBtn');
    setButtonDisabled(prevBtn, STATE.publications.offset === 0);
    setButtonDisabled(nextBtn, STATE.publications.lastCount < STATE.publications.limit);
  };

  const prefetchCaches = async () => {
    try {
      const [overridesPayload, publicationsPayload] = await Promise.all([
        apiRequest('GET', '/api/admin/contents/overrides?limit=200&offset=0', { token: STATE.token }),
        apiRequest('GET', '/api/admin/contents/publications?limit=200&offset=0', { token: STATE.token }),
      ]);
      const overrides = overridesPayload?.overrides || [];
      const publications = publicationsPayload?.publications || [];
      overrides.forEach((override) => {
        if (!override?.content_id || !override?.source) return;
        STATE.manage.overridesMap.set(`${override.content_id}::${override.source}`, override);
      });
      publications.forEach((publication) => {
        if (!publication?.content_id || !publication?.source) return;
        STATE.manage.publicationsMap.set(`${publication.content_id}::${publication.source}`, publication);
      });
    } catch (err) {
      showToast(err.message || '관리 데이터를 불러오지 못했습니다.', { type: 'error' });
    }
  };

  const bindEvents = () => {
    const manageSearchBtn = document.getElementById('manageSearchBtn');
    const manageSearchInput = document.getElementById('manageSearchInput');
    const tabManage = document.getElementById('tabManage');
    const tabDeleted = document.getElementById('tabDeleted');
    const tabPublications = document.getElementById('tabPublications');
    const tabMissingCompletion = document.getElementById('tabMissingCompletion');
    const tabMissingPublication = document.getElementById('tabMissingPublication');
    const tabAudit = document.getElementById('tabAudit');
    const tabCdcEvents = document.getElementById('tabCdcEvents');
    const tabCrawlerReports = document.getElementById('tabCrawlerReports');
    const tabDailyNotificationReport = document.getElementById('tabDailyNotificationReport');
    const deletedSearchBtn = document.getElementById('deletedSearchBtn');
    const deletedPrevBtn = document.getElementById('deletedPrevBtn');
    const deletedNextBtn = document.getElementById('deletedNextBtn');
    const publicationsPrevBtn = document.getElementById('publicationsPrevBtn');
    const publicationsNextBtn = document.getElementById('publicationsNextBtn');
    const missingCompletionRefreshBtn = document.getElementById('missingCompletionRefreshBtn');
    const missingCompletionPrevBtn = document.getElementById('missingCompletionPrevBtn');
    const missingCompletionNextBtn = document.getElementById('missingCompletionNextBtn');
    const missingPublicationRefreshBtn = document.getElementById('missingPublicationRefreshBtn');
    const missingPublicationPrevBtn = document.getElementById('missingPublicationPrevBtn');
    const missingPublicationNextBtn = document.getElementById('missingPublicationNextBtn');
    const auditSearchBtn = document.getElementById('auditSearchBtn');
    const auditPrevBtn = document.getElementById('auditPrevBtn');
    const auditNextBtn = document.getElementById('auditNextBtn');
    const cdcSearchBtn = document.getElementById('cdcSearchBtn');
    const cdcPrevBtn = document.getElementById('cdcPrevBtn');
    const cdcNextBtn = document.getElementById('cdcNextBtn');
    const reportsSearchBtn = document.getElementById('reportsSearchBtn');
    const reportsPrevBtn = document.getElementById('reportsPrevBtn');
    const reportsNextBtn = document.getElementById('reportsNextBtn');
    const dailyNotificationLoadBtn = document.getElementById('dailyNotificationLoadBtn');
    const dailyNotificationCopyBtn = document.getElementById('dailyNotificationCopyBtn');
    const dailySummaryCopyBtn = document.getElementById('dailySummaryCopyBtn');
    const dailySummaryCleanupBtn = document.getElementById('dailySummaryCleanupBtn');
    const dailySummaryKeepDaysInput = document.getElementById('dailySummaryKeepDaysInput');
    const detailCloseBtn = document.getElementById('detailCloseBtn');
    const detailCopyJsonBtn = document.getElementById('detailCopyJsonBtn');
    const detailCopyIdBtn = document.getElementById('detailCopyIdBtn');
    const detailCopyIdSourceBtn = document.getElementById('detailCopyIdSourceBtn');

    manageSearchBtn?.addEventListener('click', () =>
      withLoading(manageSearchBtn, '검색 중...', performSearch),
    );
    manageSearchInput?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        withLoading(manageSearchBtn, '검색 중...', performSearch);
      }
    });

    tabManage?.addEventListener('click', () => setTab('manage'));
    tabDeleted?.addEventListener('click', () => {
      setTab('deleted');
      loadDeleted();
    });
    tabPublications?.addEventListener('click', () => {
      setTab('publications');
      loadPublications();
    });
    tabMissingCompletion?.addEventListener('click', () => {
      setTab('missingCompletion');
      withLoading(missingCompletionRefreshBtn, '불러오는 중...', refreshMissingCompletion);
    });
    tabMissingPublication?.addEventListener('click', () => {
      setTab('missingPublication');
      withLoading(missingPublicationRefreshBtn, '불러오는 중...', refreshMissingPublication);
    });
    tabAudit?.addEventListener('click', () => {
      setTab('audit');
      loadAudit();
    });
    tabCdcEvents?.addEventListener('click', () => {
      setTab('cdcEvents');
      loadCdcEvents();
    });
    tabCrawlerReports?.addEventListener('click', () => {
      setTab('crawlerReports');
      loadCrawlerReports();
      loadDailySummary();
    });
    tabDailyNotificationReport?.addEventListener('click', () => {
      setTab('dailyNotificationReport');
      loadDailyNotificationReport();
    });

    document.getElementById('overrideSaveBtn')?.addEventListener('click', (event) =>
      withLoading(event.currentTarget, '저장 중...', saveOverride),
    );
    document.getElementById('overrideDeleteBtn')?.addEventListener('click', deleteOverride);
    document.getElementById('publicationSaveBtn')?.addEventListener('click', (event) =>
      withLoading(event.currentTarget, '저장 중...', savePublication),
    );
    document.getElementById('publicationDeleteBtn')?.addEventListener('click', deletePublication);
    document.getElementById('softDeleteBtn')?.addEventListener('click', softDelete);

    deletedSearchBtn?.addEventListener('click', () =>
      withLoading(deletedSearchBtn, '불러오는 중...', async () => {
        const q = document.getElementById('deletedSearchInput')?.value?.trim() || '';
        STATE.deleted.q = q;
        STATE.deleted.offset = 0;
        await loadDeleted();
      }),
    );
    deletedPrevBtn?.addEventListener('click', () =>
      withLoading(deletedPrevBtn, '불러오는 중...', async () => {
        if (STATE.deleted.offset === 0) return;
        STATE.deleted.offset = Math.max(0, STATE.deleted.offset - STATE.deleted.limit);
        await loadDeleted();
      }),
    );
    deletedNextBtn?.addEventListener('click', () =>
      withLoading(deletedNextBtn, '불러오는 중...', async () => {
        if (STATE.deleted.lastCount < STATE.deleted.limit) return;
        STATE.deleted.offset += STATE.deleted.limit;
        await loadDeleted();
      }),
    );
    document.getElementById('restoreBtn')?.addEventListener('click', restoreDeleted);

    publicationsPrevBtn?.addEventListener('click', () =>
      withLoading(publicationsPrevBtn, '불러오는 중...', async () => {
        if (STATE.publications.offset === 0) return;
        STATE.publications.offset = Math.max(0, STATE.publications.offset - STATE.publications.limit);
        await loadPublications();
      }),
    );
    publicationsNextBtn?.addEventListener('click', () =>
      withLoading(publicationsNextBtn, '불러오는 중...', async () => {
        if (STATE.publications.lastCount < STATE.publications.limit) return;
        STATE.publications.offset += STATE.publications.limit;
        await loadPublications();
      }),
    );

    missingCompletionRefreshBtn?.addEventListener('click', () =>
      withLoading(missingCompletionRefreshBtn, '불러오는 중...', refreshMissingCompletion),
    );
    document.getElementById('missingCompletionSearchInput')?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        withLoading(missingCompletionRefreshBtn, '불러오는 중...', refreshMissingCompletion);
      }
    });
    document.getElementById('missingCompletionSourceSelect')?.addEventListener('change', () => {
      withLoading(missingCompletionRefreshBtn, '불러오는 중...', refreshMissingCompletion);
    });
    document.getElementById('missingCompletionTypeSelect')?.addEventListener('change', () => {
      withLoading(missingCompletionRefreshBtn, '불러오는 중...', refreshMissingCompletion);
    });
    missingCompletionPrevBtn?.addEventListener('click', () =>
      withLoading(missingCompletionPrevBtn, '불러오는 중...', async () => {
        if (STATE.missingCompletion.offset === 0) return;
        STATE.missingCompletion.offset = Math.max(
          0,
          STATE.missingCompletion.offset - STATE.missingCompletion.limit,
        );
        await loadMissingCompletion();
      }),
    );
    missingCompletionNextBtn?.addEventListener('click', () =>
      withLoading(missingCompletionNextBtn, '불러오는 중...', async () => {
        if (STATE.missingCompletion.lastCount < STATE.missingCompletion.limit) return;
        STATE.missingCompletion.offset += STATE.missingCompletion.limit;
        await loadMissingCompletion();
      }),
    );

    missingPublicationRefreshBtn?.addEventListener('click', () =>
      withLoading(missingPublicationRefreshBtn, '불러오는 중...', refreshMissingPublication),
    );
    document.getElementById('missingPublicationSearchInput')?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        withLoading(missingPublicationRefreshBtn, '불러오는 중...', refreshMissingPublication);
      }
    });
    document.getElementById('missingPublicationSourceSelect')?.addEventListener('change', () => {
      withLoading(missingPublicationRefreshBtn, '불러오는 중...', refreshMissingPublication);
    });
    document.getElementById('missingPublicationTypeSelect')?.addEventListener('change', () => {
      withLoading(missingPublicationRefreshBtn, '불러오는 중...', refreshMissingPublication);
    });
    missingPublicationPrevBtn?.addEventListener('click', () =>
      withLoading(missingPublicationPrevBtn, '불러오는 중...', async () => {
        if (STATE.missingPublication.offset === 0) return;
        STATE.missingPublication.offset = Math.max(
          0,
          STATE.missingPublication.offset - STATE.missingPublication.limit,
        );
        await loadMissingPublication();
      }),
    );
    missingPublicationNextBtn?.addEventListener('click', () =>
      withLoading(missingPublicationNextBtn, '불러오는 중...', async () => {
        if (STATE.missingPublication.lastCount < STATE.missingPublication.limit) return;
        STATE.missingPublication.offset += STATE.missingPublication.limit;
        await loadMissingPublication();
      }),
    );

    auditSearchBtn?.addEventListener('click', () =>
      withLoading(auditSearchBtn, '불러오는 중...', async () => {
        const q = document.getElementById('auditSearchInput')?.value?.trim() || '';
        const actionType = document.getElementById('auditActionSelect')?.value || '';
        STATE.audit.q = q;
        STATE.audit.actionType = actionType;
        STATE.audit.offset = 0;
        await loadAudit();
      }),
    );
    auditPrevBtn?.addEventListener('click', () =>
      withLoading(auditPrevBtn, '불러오는 중...', async () => {
        if (STATE.audit.offset === 0) return;
        STATE.audit.offset = Math.max(0, STATE.audit.offset - STATE.audit.limit);
        await loadAudit();
      }),
    );
    auditNextBtn?.addEventListener('click', () =>
      withLoading(auditNextBtn, '불러오는 중...', async () => {
        if (STATE.audit.lastCount < STATE.audit.limit) return;
        STATE.audit.offset += STATE.audit.limit;
        await loadAudit();
      }),
    );

    cdcSearchBtn?.addEventListener('click', () =>
      withLoading(cdcSearchBtn, '불러오는 중...', refreshCdcEventsFromInputs),
    );
    document.getElementById('cdcSearchInput')?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        withLoading(cdcSearchBtn, '불러오는 중...', refreshCdcEventsFromInputs);
      }
    });
    cdcPrevBtn?.addEventListener('click', () =>
      withLoading(cdcPrevBtn, '불러오는 중...', async () => {
        if (STATE.cdcEvents.offset === 0) return;
        STATE.cdcEvents.offset = Math.max(0, STATE.cdcEvents.offset - STATE.cdcEvents.limit);
        await loadCdcEvents();
      }),
    );
    cdcNextBtn?.addEventListener('click', () =>
      withLoading(cdcNextBtn, '불러오는 중...', async () => {
        if (STATE.cdcEvents.lastCount < STATE.cdcEvents.limit) return;
        STATE.cdcEvents.offset += STATE.cdcEvents.limit;
        await loadCdcEvents();
      }),
    );

    reportsSearchBtn?.addEventListener('click', () =>
      withLoading(reportsSearchBtn, '불러오는 중...', refreshCrawlerReportsFromInputs),
    );
    document.getElementById('reportsCrawlerNameInput')?.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        withLoading(reportsSearchBtn, '불러오는 중...', refreshCrawlerReportsFromInputs);
      }
    });
    reportsPrevBtn?.addEventListener('click', () =>
      withLoading(reportsPrevBtn, '불러오는 중...', async () => {
        if (STATE.crawlerReports.offset === 0) return;
        STATE.crawlerReports.offset = Math.max(
          0,
          STATE.crawlerReports.offset - STATE.crawlerReports.limit,
        );
        await loadCrawlerReports();
      }),
    );
    reportsNextBtn?.addEventListener('click', () =>
      withLoading(reportsNextBtn, '불러오는 중...', async () => {
        if (STATE.crawlerReports.lastCount < STATE.crawlerReports.limit) return;
        STATE.crawlerReports.offset += STATE.crawlerReports.limit;
        await loadCrawlerReports();
      }),
    );

    const handleCopy = async (text) => {
      try {
        await copyToClipboard(text);
        showToast('복사되었습니다.', { type: 'success' });
      } catch (err) {
        showToast('복사에 실패했습니다.', { type: 'error' });
      }
    };

    dailyNotificationLoadBtn?.addEventListener('click', () =>
      withLoading(dailyNotificationLoadBtn, '불러오는 중...', loadDailyNotificationReport),
    );
    dailyNotificationCopyBtn?.addEventListener('click', () =>
      handleCopy(STATE.crawlerReports.dailyNotificationText),
    );

    dailySummaryCopyBtn?.addEventListener('click', () => handleCopy(STATE.crawlerReports.summaryText));
    dailySummaryCleanupBtn?.addEventListener('click', () => {
      const keepDaysValue = Number.parseInt(dailySummaryKeepDaysInput?.value || '', 10);
      const keepDays = Number.isNaN(keepDaysValue) ? null : keepDaysValue;
      openConfirm({
        title: '배치 리포트 정리',
        message: `보관 기간 ${keepDays ?? 14}일 이전의 리포트를 삭제할까요?`,
        onConfirm: async () => {
          const body = keepDays ? { keep_days: keepDays } : {};
          const payload = await apiRequest('POST', '/api/admin/reports/daily-crawler/cleanup', {
            token: STATE.token,
            body,
          });
          if (dailySummaryKeepDaysInput) {
            dailySummaryKeepDaysInput.value = String(payload?.keep_days ?? keepDays ?? 14);
          }
          showToast(`삭제 완료: ${payload?.deleted_count ?? 0}건`, { type: 'success' });
          await Promise.all([loadCrawlerReports(), loadDailySummary()]);
        },
      });
    });

    detailCloseBtn?.addEventListener('click', closeDetailModal);
    detailCopyJsonBtn?.addEventListener('click', () => handleCopy(detailState.jsonText));
    detailCopyIdBtn?.addEventListener('click', () => handleCopy(detailState.contentId));
    detailCopyIdSourceBtn?.addEventListener('click', () =>
      handleCopy(`${detailState.contentId}::${detailState.source}`),
    );
    document.getElementById('detailModal')?.addEventListener('click', (event) => {
      if (event.target?.id === 'detailModal') {
        closeDetailModal();
      }
    });
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        closeDetailModal();
      }
    });
  };

  document.addEventListener('DOMContentLoaded', async () => {
    const token = getAccessToken();
    if (!token) {
      showToast('로그인이 필요합니다.', { type: 'error' });
      redirectToHome();
      return;
    }

    try {
      const payload = await apiRequest('GET', '/api/auth/me', { token });
      const user = extractUser(payload);

      if (user?.role !== 'admin') {
        showToast('403: Admin 권한이 필요합니다.', { type: 'error' });
        redirectToHome();
        return;
      }

      STATE.token = token;
      STATE.user = user;

      const emailEl = document.getElementById('adminUserEmail');
      if (emailEl) {
        emailEl.textContent = user?.email ? `관리자: ${user.email}` : '관리자 인증 완료';
      }

      initConfirmModal();
      bindEvents();
      await prefetchCaches();
      hideGateOverlay();
    } catch (err) {
      const isForbidden = err?.httpStatus === 401 || err?.httpStatus === 403;
      const message = isForbidden
        ? '403: Admin 권한이 필요합니다.'
        : err?.message || '권한 확인 중 오류가 발생했습니다.';
      showToast(message, { type: 'error' });
      redirectToHome();
    }
  });
})();
