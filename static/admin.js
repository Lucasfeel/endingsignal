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
    };
    const tabs = {
      manage: document.getElementById('tabManage'),
      deleted: document.getElementById('tabDeleted'),
      publications: document.getElementById('tabPublications'),
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

  const closeConfirm = () => {
    const modal = document.getElementById('confirmModal');
    if (!modal) return;
    modal.classList.add('hidden');
    modal.classList.remove('flex');
    confirmState.onConfirm = null;
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
      renderSelectedContent();
      showToast('공개일이 저장되었습니다.', { type: 'success' });
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
          renderSelectedContent();
          showToast('공개일 정보가 삭제되었습니다.', { type: 'success' });
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
          const deletedCount = payload?.subscriptions_deleted;
          const message =
            typeof deletedCount === 'number'
              ? `콘텐츠 삭제 완료 (구독 ${deletedCount}건 정리)`
              : '콘텐츠 삭제 완료';
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

      contentBox.appendChild(title);
      contentBox.appendChild(meta);
      contentBox.appendChild(reason);

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
    const deletedSearchBtn = document.getElementById('deletedSearchBtn');
    const deletedPrevBtn = document.getElementById('deletedPrevBtn');
    const deletedNextBtn = document.getElementById('deletedNextBtn');
    const publicationsPrevBtn = document.getElementById('publicationsPrevBtn');
    const publicationsNextBtn = document.getElementById('publicationsNextBtn');

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
