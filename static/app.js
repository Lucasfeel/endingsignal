/* static/app.js
   CP2 + CP2.1 + CP4 + CP4.1 integrated (conflict-free)
   - apiRequest + normalizeMeta
   - authenticated subscriptions via /api/me/subscriptions
   - My Sub uses real data + final_state (scheduled completion supported)
   - toast-based UX (no alert) + hardened schema parsing
   - no backend changes
*/

const DEBUG_API = false;
const DEBUG_TOOLS = false;

function debugLog(...args) {
  if (DEBUG_API) console.log(...args);
}

const ICONS = {
  webtoon: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M21 4H3C1.9 4 1 4.9 1 6v13c0 1.1.9 2 2 2h18c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zM3 19V6h8v13H3zm18 0h-8V6h8v13z"/></svg>`,
  novel: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M7.127 22.562l-7.127 1.438 1.438-7.128 5.689 5.69zm1.414-1.414l11.228-11.225-5.69-5.692-11.227 11.227 5.689 5.69zm9.768-21.148l-2.816 2.817 5.691 5.691 2.816-2.819-5.691-5.689z"/></svg>`,
  ott: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 14.5v-9l6 4.5-6 4.5z"/></svg>`,
  series: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg>`,
  my: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>`,
};

const FALLBACK_THUMB = `data:image/svg+xml;utf8,${encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 400" preserveAspectRatio="xMidYMid slice"><rect width="300" height="400" fill="#1E1E1E"/><path d="M30 320h240v30H30z" fill="#2d2d2d"/><rect x="60" y="60" width="180" height="200" rx="12" fill="#2f2f2f"/><text x="150" y="175" text-anchor="middle" fill="#6b7280" font-family="sans-serif" font-size="20">No Image</text></svg>'
)}`;

const STATE = {
  activeTab: 'webtoon',
  lastBrowseTab: 'webtoon',
  filters: {
    webtoon: { source: 'all', day: 'mon' },
    novel: { source: 'all', day: 'mon' },
    ott: { source: 'all', genre: 'drama' },
    series: { sort: 'latest' },
    my: { viewMode: 'subscribing' },
  },
  search: {
    isOpen: false,
    q: '',
    isLoading: false,
    debounceTimer: null,
    requestSeq: 0,
    activeIndex: -1,
  },
  contents: {},
  isLoading: false,
  contentRequestSeq: 0,
  currentModalContent: null,

  auth: {
    isAuthenticated: false,
    user: null,
    isChecking: false,
    uiMode: 'login',
  },

  // subscriptions
  subscriptionsSet: new Set(),
  pendingSubOps: new Set(),
  mySubscriptions: [],
  subscriptionsLoadedAt: null,

  pagination: {
    completed: {
      cursor: null,
      legacyCursor: null,
      done: false,
      loading: false,
      items: [],
      totalLoaded: 0,
      requestSeq: 0,
      tabId: null,
      source: 'all',
      aspectClass: 'aspect-[3/4]',
    },
    hiatus: {
      cursor: null,
      legacyCursor: null,
      done: false,
      loading: false,
      items: [],
      totalLoaded: 0,
      requestSeq: 0,
      tabId: null,
      source: 'all',
      aspectClass: 'aspect-[3/4]',
    },
  },
  activePaginationCategory: null,
  rendering: {
    list: [],
    index: 0,
    batchSize: 60,
    scheduled: false,
    requestSeq: 0,
    aspectClass: 'aspect-[3/4]',
    tabId: 'webtoon',
  },
};

const UI = {
  bottomNav: document.getElementById('bottomNav'),
  contentGrid: document.getElementById('contentGridContainer'),
  contentCountIndicator: document.getElementById('contentCountIndicator'),
  contentLoadMoreBtn: document.getElementById('contentLoadMoreBtn'),
  contentGridSentinel: document.getElementById('contentGridSentinel'),
  l1Filter: document.getElementById('l1FilterContainer'),
  l2Filter: document.getElementById('l2FilterContainer'),
  filtersWrapper: document.getElementById('filtersWrapper'),
  mySubToggle: document.getElementById('mySubToggleContainer'),
  seriesSort: document.getElementById('seriesSortOptions'),
  seriesFooter: document.getElementById('seriesFooterButton'),
  toggleIndicator: document.getElementById('toggleIndicator'),
  header: document.getElementById('mainHeader'),
  profileButton: document.getElementById('profileButton'),
  profileButtonText: document.getElementById('profileButtonText'),
  profileMenu: document.getElementById('profileMenu'),
  profileMenuMy: document.getElementById('profileMenuMy'),
  profileMenuLogout: document.getElementById('profileMenuLogout'),
  headerSearchWrap: document.getElementById('headerSearchWrap'),
  searchButton: document.getElementById('searchButton'),
  searchInput: document.getElementById('searchInput'),
  searchClearBtn: document.getElementById('searchClearBtn'),
  searchPanel: document.getElementById('searchPanel'),
  searchResultsGrid: document.getElementById('searchResultsGrid'),
  searchEmptyState: document.getElementById('searchEmptyState'),
  searchLoadingState: document.getElementById('searchLoadingState'),
};

function renderEmptyState(containerEl, { title = '', message = '', actions = [] } = {}) {
  if (!containerEl) return;
  containerEl.innerHTML = '';

  const wrapper = document.createElement('div');
  wrapper.className =
    'w-full col-span-full flex flex-col items-center justify-center text-center py-12 px-4';

  if (title) {
    const titleEl = document.createElement('h3');
    titleEl.className = 'text-lg font-semibold text-white';
    titleEl.textContent = title;
    wrapper.appendChild(titleEl);
  }

  if (message) {
    const msgEl = document.createElement('p');
    msgEl.className = 'text-sm text-white/70 mt-2 max-w-md';
    msgEl.textContent = message;
    wrapper.appendChild(msgEl);
  }

  if (Array.isArray(actions) && actions.length) {
    const actionsWrap = document.createElement('div');
    actionsWrap.className = 'mt-6 flex flex-wrap items-center justify-center gap-3';

    actions.forEach((action) => {
      if (!action?.label || typeof action.onClick !== 'function') return;
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className =
        'px-4 py-2 rounded-full text-sm font-semibold transition focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-[#121212] spring-bounce';
      if (action.variant === 'primary')
        btn.className += ' bg-[#4F46E5] text-white hover:bg-[#4338CA] focus:ring-[#4F46E5]';
      else btn.className += ' bg-white/5 text-white hover:bg-white/10 border border-white/10 focus:ring-white/40';

      btn.textContent = action.label;
      btn.onclick = action.onClick;
      actionsWrap.appendChild(btn);
    });

    wrapper.appendChild(actionsWrap);
  }

  containerEl.appendChild(wrapper);
}

let contentGridObserver = null;

// Modal management
const modalStack = [];
const modalMeta = new Map();
let bodyOverflowBackup = '';

const isAnyModalOpen = () => modalStack.length > 0;
const getTopModal = () => modalStack[modalStack.length - 1] || null;

const getFocusableElements = (modalEl) => {
  if (!modalEl) return [];
  const selectors =
    'a[href], area[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), [tabindex]:not([tabindex="-1"])';
  return Array.from(modalEl.querySelectorAll(selectors)).filter((el) => {
    const isHidden = el.getAttribute('aria-hidden') === 'true';
    return !isHidden && el.offsetParent !== null && el.tabIndex >= 0;
  });
};

const focusFirstElement = (modalEl, initialFocusEl) => {
  const focusables = getFocusableElements(modalEl);
  const target =
    (initialFocusEl && modalEl.contains(initialFocusEl) && initialFocusEl) ||
    focusables[0] ||
    modalEl;
  requestAnimationFrame(() => {
    if (target && typeof target.focus === 'function') target.focus();
  });
};

const setupModalRoot = (modalEl) => {
  if (!modalEl || modalEl.dataset.modalSetup === '1') return;
  modalEl.dataset.modalSetup = '1';

  modalEl.addEventListener('click', (evt) => {
    if (!isAnyModalOpen()) return;
    const isOverlayClick =
      evt.target === modalEl || evt.target?.dataset?.modalOverlay === 'true';
    if (isOverlayClick && getTopModal() === modalEl) {
      closeModal(modalEl);
    }
  });
};

function openModal(modalEl, { initialFocusEl } = {}) {
  if (!modalEl) return;
  setupModalRoot(modalEl);
  if (modalStack.includes(modalEl)) return;

  const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;

  if (!isAnyModalOpen()) {
    bodyOverflowBackup = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
  }

  modalStack.push(modalEl);
  modalMeta.set(modalEl, { opener });

  modalEl.classList.remove('hidden');
  modalEl.setAttribute('aria-hidden', 'false');
  focusFirstElement(modalEl, initialFocusEl);
}

function closeModal(modalEl) {
  if (!modalEl) return;
  const idx = modalStack.indexOf(modalEl);
  if (idx === -1) return;

  modalStack.splice(idx, 1);
  const meta = modalMeta.get(modalEl) || {};
  modalMeta.delete(modalEl);

  modalEl.classList.add('hidden');
  modalEl.setAttribute('aria-hidden', 'true');

  if (!isAnyModalOpen()) {
    document.body.style.overflow = bodyOverflowBackup || '';
  }

  const opener = meta.opener;
  if (opener && document.contains(opener) && typeof opener.focus === 'function') {
    opener.focus();
  }
}

document.addEventListener(
  'keydown',
  (evt) => {
    if (!isAnyModalOpen()) return;
    const topModal = getTopModal();
    if (!topModal) return;

    if (evt.key === 'Escape') {
      evt.preventDefault();
      closeModal(topModal);
      return;
    }

    if (evt.key === 'Tab') {
      const focusables = getFocusableElements(topModal);
      if (!focusables.length) {
        evt.preventDefault();
        topModal.focus();
        return;
      }

      const current = document.activeElement;
      const currentIndex = focusables.indexOf(current);
      const lastIndex = focusables.length - 1;
      let nextIndex = currentIndex;

      if (evt.shiftKey) nextIndex = currentIndex <= 0 ? lastIndex : currentIndex - 1;
      else nextIndex = currentIndex === lastIndex ? 0 : currentIndex + 1;

      evt.preventDefault();
      focusables[nextIndex].focus();
    }
  },
  true
);

const contentKey = (c) => {
  const cid = String(c?.content_id ?? c?.contentId ?? c?.id ?? '')?.trim();
  const src = String(c?.source ?? '').trim();
  if (!cid && !src) return '';
  return `${src}::${cid}`;
};

const resetPaginationState = (category, { tabId, source, aspectClass, requestSeq }) => {
  const target = STATE.pagination?.[category];
  if (!target) return;

  target.cursor = null;
  target.legacyCursor = null;
  target.done = false;
  target.loading = false;
  target.items = [];
  target.totalLoaded = 0;
  target.tabId = tabId;
  target.source = source;
  target.aspectClass = aspectClass;
  target.requestSeq = requestSeq;
};

const setActivePaginationCategory = (category) => {
  STATE.activePaginationCategory = category;
};

const getActivePaginationCategory = () => STATE.activePaginationCategory;

const setCountIndicatorText = (text = '') => {
  if (UI.contentCountIndicator) {
    UI.contentCountIndicator.textContent = text || '';
  }
};

const updateCountIndicator = (category) => {
  const pg = STATE.pagination?.[category];
  if (!pg) {
    setCountIndicatorText('');
    return;
  }

  const loadingSuffix = pg.loading ? ' (불러오는 중...)' : '';
  setCountIndicatorText(`불러온 콘텐츠 ${pg.totalLoaded}${loadingSuffix}`);
};

const updateCountIndicatorForBatched = () => {
  const { list, index } = STATE.rendering;
  if (!list || !list.length) {
    setCountIndicatorText('');
    return;
  }
  const rendered = Math.min(index, list.length);
  setCountIndicatorText(`불러온 콘텐츠 ${rendered} / ${list.length}`);
};

const hideLoadMoreUI = () => {
  if (UI.contentLoadMoreBtn) UI.contentLoadMoreBtn.classList.add('hidden');
};

const updateLoadMoreUI = (category) => {
  const btn = UI.contentLoadMoreBtn;
  if (!btn) return;

  const pg = STATE.pagination?.[category];
  if (!pg || pg.done) {
    btn.classList.add('hidden');
    return;
  }

  btn.classList.remove('hidden');
  btn.disabled = Boolean(pg.loading);
  btn.textContent = pg.loading ? '불러오는 중...' : '더 불러오기';
};

const disconnectInfiniteObserver = () => {
  if (contentGridObserver) {
    contentGridObserver.disconnect();
    contentGridObserver = null;
  }
};

const setupInfiniteObserver = (category) => {
  disconnectInfiniteObserver();
  if (!UI.contentGridSentinel || !('IntersectionObserver' in window)) return;

  contentGridObserver = new IntersectionObserver((entries) => {
    const active = getActivePaginationCategory();
    if (!active || active !== category) return;

    if (entries.some((e) => e.isIntersecting)) {
      loadNextPage(active).catch((err) => console.warn('Pagination load failed', err));
    }
  },
  { root: null, rootMargin: '200px 0px' });

  contentGridObserver.observe(UI.contentGridSentinel);
};

/* =========================
   Toast helper (CP4.1)
   Requires #toastContainer in HTML (optional; gracefully degrades)
   ========================= */

function showToast(message, { type = 'info', duration = 2200 } = {}) {
  const container = document.getElementById('toastContainer');
  if (!container) {
    // graceful fallback (do not crash)
    console.warn('Toast container missing:', message);
    return;
  }

  const prefix =
    type === 'success' ? '[성공] ' : type === 'error' ? '[오류] ' : '[알림] ';

  const normalizedMessage = String(message ?? '').trim();
  const truncatedMessage =
    normalizedMessage.length > 400
      ? `${normalizedMessage.slice(0, 400)}…`
      : normalizedMessage;

  const toast = document.createElement('div');
  toast.className =
    'pointer-events-none w-full text-center transition-all duration-300 opacity-0 -translate-y-2';

  const inner = document.createElement('div');
  inner.className =
    'inline-flex px-4 py-2 rounded-xl bg-black/70 border border-white/10 shadow-xl backdrop-blur-md text-sm text-white';
  inner.textContent = `${prefix}${truncatedMessage}`;

  toast.appendChild(inner);

  container.appendChild(toast);

  requestAnimationFrame(() => {
    toast.classList.remove('opacity-0', '-translate-y-2');
    toast.classList.add('opacity-100', 'translate-y-0');
  });

  const remove = () => {
    toast.classList.remove('opacity-100', 'translate-y-0');
    toast.classList.add('opacity-0', '-translate-y-2');
    setTimeout(() => {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, 250);
  };

  setTimeout(remove, duration);
}

/* =========================
   Minimal auth helpers
   ========================= */

const getAccessToken = () => {
  try {
    return localStorage.getItem('es_access_token');
  } catch (e) {
    console.warn('Failed to read access token', e);
    return null;
  }
};

const setAccessToken = (token) => {
  try {
    if (token) localStorage.setItem('es_access_token', token);
  } catch (e) {
    console.warn('Failed to save access token', e);
  }
};

const clearAccessToken = () => {
  try {
    localStorage.removeItem('es_access_token');
  } catch (e) {
    console.warn('Failed to clear access token', e);
  }
};

const requireAuthOrPrompt = (_actionName) => {
  const token = getAccessToken();
  if (!token) {
    showToast('로그인이 필요합니다.', { type: 'error' });
    openAuthModal({ reason: _actionName || 'auth-required' });
    return false;
  }
  return true;
};

async function fetchMe() {
  const token = getAccessToken();
  if (!token) {
    STATE.auth.isAuthenticated = false;
    STATE.auth.user = null;
    updateProfileButtonState();
    return null;
  }

  STATE.auth.isChecking = true;

  try {
    const res = await apiRequest('GET', '/api/auth/me', { token });
    const user = res?.data?.user || res?.user || null;

    STATE.auth.user = user;
    STATE.auth.isAuthenticated = Boolean(user || token);
    updateProfileButtonState();
    return user;
  } catch (e) {
    if (e?.httpStatus === 401 || e?.httpStatus === 403) {
      STATE.auth.isAuthenticated = false;
      STATE.auth.user = null;
    }
    updateProfileButtonState();
    return null;
  } finally {
    STATE.auth.isChecking = false;
  }
}

async function login(email, password) {
  const res = await apiRequest('POST', '/api/auth/login', {
    body: { email, password },
  });

  const token = res?.data?.access_token || res?.access_token;
  if (!token) throw new Error('토큰을 받지 못했습니다.');

  setAccessToken(token);
  const user = res?.data?.user || res?.user || null;
  STATE.auth.isAuthenticated = true;
  STATE.auth.user = user;
  updateProfileButtonState();
  return res;
}

async function register(email, password) {
  const res = await apiRequest('POST', '/api/auth/register', {
    body: { email, password },
  });

  showToast('회원가입이 완료되었습니다. 로그인 중...', { type: 'success' });
  return login(email, password);
}

function logout({ silent = false } = {}) {
  clearAccessToken();
  STATE.auth.isAuthenticated = false;
  STATE.auth.user = null;

  STATE.subscriptionsSet = new Set();
  STATE.mySubscriptions = [];
  STATE.subscriptionsLoadedAt = null;

  if (!silent) showToast('로그아웃되었습니다', { type: 'info' });
  updateProfileButtonState();
  fetchAndRenderContent(STATE.activeTab);
}

/* =========================
   CP2: API Contract Baseline + CP2.1 hardening
   ========================= */

const isJsonResponse = (response) => {
  const contentType = response.headers.get('content-type');
  return contentType ? contentType.includes('application/json') : false;
};

const buildUrl = (path, queryObj = {}) => {
  const entries = Object.entries(queryObj).filter(
    ([, value]) => value !== undefined && value !== null
  );
  if (!entries.length) return path;

  const params = new URLSearchParams();
  entries.forEach(([key, value]) => params.append(key, String(value)));
  const queryString = params.toString();

  return queryString ? `${path}?${queryString}` : path;
};

async function apiRequest(method, path, { query, body, token } = {}) {
  const url = buildUrl(path, query);
  const headers = { Accept: 'application/json' };
  let serializedBody;

  if (body !== undefined) {
    headers['Content-Type'] = 'application/json';
    serializedBody = JSON.stringify(body);
  }

  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  debugLog('[apiRequest]', method, url, {
    hasBody: body !== undefined,
    hasToken: Boolean(token),
  });

  const response = await fetch(url, { method, headers, body: serializedBody });
  debugLog('[apiResponse]', response.status, response.ok);

  const buildError = async () => {
    let message = response.statusText || 'Request failed';
    let code;
    let handled = false;

    if (isJsonResponse(response)) {
      try {
        const json = await response.clone().json();
        if (json?.success === false && json?.error) {
          code = json.error.code;
          message = json.error.message || message;
        } else if (typeof json?.message === 'string') {
          message = json.message;
        }
        handled = true;
      } catch {
        // ignore parse failures
      }
    }

    // fallback for non-json error responses (e.g., HTML 404)
    if (!handled) {
      try {
        const statusCode = response.status || 'unknown';
        const text = await response.clone().text();
        if (text) console.warn('Non-JSON error body:', text);
        message = `Request failed (HTTP ${statusCode}).`;
      } catch {
        // ignore
      }
    }

    return { httpStatus: response.status, code, message };
  };

  if (!response.ok) {
    const errorObj = await buildError();

    if (response.status === 401 || response.status === 403) {
      logout({ silent: true });
      showToast('세션이 만료되었습니다. 다시 로그인해주세요.', { type: 'error' });
    }

    errorObj.httpStatus = errorObj.httpStatus || response.status;
    throw errorObj;
  }

  if (isJsonResponse(response)) {
    try {
      return await response.json();
    } catch {
      return null;
    }
  }

  // optional text fallback for OK non-json responses
  try {
    const text = await response.text();
    return text || null;
  } catch {
    return null;
  }
}

const normalizeMeta = (input) => {
  if (input === null || input === undefined) return {};
  if (typeof input === 'string') {
    try {
      const parsed = JSON.parse(input);
      return typeof parsed === 'object' && parsed !== null ? parsed : {};
    } catch {
      return {};
    }
  }
  if (typeof input === 'object') return input;
  return {};
};

/* =========================
   Schema safety (CP4.1)
   ========================= */

const safeString = (v, fallback = '') => (typeof v === 'string' ? v : fallback);
const safeBool = (v, fallback = false) =>
  typeof v === 'boolean' ? v : fallback;
const safeObj = (v) => (v && typeof v === 'object' ? v : {});

const normalizeSubscriptionItem = (item) => {
  if (!item || typeof item !== 'object') return null;

  const contentId = item.content_id || item.contentId || item.id;
  const source = safeString(item.source, '');
  if (!contentId || !source) return null;

  const fsRaw = safeObj(item.final_state);
  const finalStatus = safeString(
    fsRaw.final_status,
    safeString(fsRaw.raw_status, '')
  );

  const finalState = {
    ...fsRaw,
    final_status: finalStatus,
    raw_status: safeString(fsRaw.raw_status, ''),
    is_scheduled_completion: safeBool(fsRaw.is_scheduled_completion, false),
    scheduled_completed_at: safeString(fsRaw.scheduled_completed_at, ''),
    final_completed_at: safeString(fsRaw.final_completed_at, ''),
  };

  return {
    ...item,
    content_id: contentId,
    source,
    title: safeString(item.title, 'Untitled'),
    status: safeString(item.status, ''),
    meta: normalizeMeta(item.meta),
    final_state: finalState,
  };
};

/* =========================
   Subscriptions helpers/state (CP4 + CP4.1)
   ========================= */

const buildSubscriptionKey = (content) => {
  if (!content) return '';
  const source = content.source || '';
  const contentId = content.content_id || content.contentId || content.id;
  if (!source || !contentId) return '';
  return `${source}::${contentId}`;
};

const isSubscribed = (content) => {
  const key = buildSubscriptionKey(content);
  return key ? STATE.subscriptionsSet.has(key) : false;
};

async function loadSubscriptions({ force = false } = {}) {
  const token = getAccessToken();
  if (!token) {
    STATE.subscriptionsSet = new Set();
    STATE.mySubscriptions = [];
    STATE.subscriptionsLoadedAt = null;
    return [];
  }

  if (!force && STATE.subscriptionsLoadedAt) {
    return STATE.mySubscriptions;
  }

  const res = await apiRequest('GET', '/api/me/subscriptions', { token });
  if (!res || res.success !== true || !Array.isArray(res.data)) {
    throw new Error('구독 정보를 불러오지 못했습니다.');
  }

  const normalized = res.data
    .map((x) => normalizeSubscriptionItem(x))
    .filter(Boolean);

  const nextSet = new Set();
  normalized.forEach((item) => {
    const key = buildSubscriptionKey(item);
    if (key) nextSet.add(key);
  });

  STATE.subscriptionsSet = nextSet;
  STATE.mySubscriptions = normalized;
  STATE.subscriptionsLoadedAt = Date.now();

  return normalized;
}

async function subscribeContent(content) {
  const token = getAccessToken();
  if (!token) {
    showToast('로그인이 필요합니다. 로그인 후 이용해주세요.', { type: 'error' });
    return;
  }

  const contentId = content?.content_id || content?.contentId || content?.id;
  const source = content?.source;
  const key = buildSubscriptionKey({ ...content, content_id: contentId, source });

  if (!contentId || !source) {
    showToast('콘텐츠 정보가 없습니다.', { type: 'error' });
    return;
  }

  try {
    await apiRequest('POST', '/api/me/subscriptions', {
      body: { content_id: contentId, contentId, source },
      token,
    });

    if (key) STATE.subscriptionsSet.add(key);

    // best-effort refresh (does not block UX)
    loadSubscriptions({ force: true }).catch((err) =>
      console.warn('Failed to refresh subscriptions after subscribe', err)
    );

    showToast('구독이 추가되었습니다.', { type: 'success' });
  } catch (e) {
    if (key) STATE.subscriptionsSet.delete(key);
    showToast(e?.message || '구독에 실패했습니다.', { type: 'error' });
    throw e;
  }
}

async function unsubscribeContent(content) {
  const token = getAccessToken();
  if (!token) {
    showToast('로그인이 필요합니다. 로그인 후 이용해주세요.', { type: 'error' });
    return;
  }

  const contentId = content?.content_id || content?.contentId || content?.id;
  const source = content?.source;
  const key = buildSubscriptionKey({ ...content, content_id: contentId, source });

  if (!contentId || !source) {
    showToast('콘텐츠 정보가 없습니다.', { type: 'error' });
    return;
  }

  try {
    await apiRequest('DELETE', '/api/me/subscriptions', {
      body: { content_id: contentId, contentId, source },
      token,
    });

    if (key) STATE.subscriptionsSet.delete(key);

    loadSubscriptions({ force: true }).catch((err) =>
      console.warn('Failed to refresh subscriptions after unsubscribe', err)
    );

    showToast('구독이 해제되었습니다.', { type: 'success' });
  } catch (e) {
    if (key) STATE.subscriptionsSet.add(key);
    showToast(e?.message || '구독 해제에 실패했습니다.', { type: 'error' });
    throw e;
  }
}

const formatDateKST = (isoString) => {
  if (!isoString) return '';
  try {
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return isoString;

    const parts = new Intl.DateTimeFormat('ko-KR', {
      timeZone: 'Asia/Seoul',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    })
      .formatToParts(date)
      .reduce((acc, part) => {
        if (part.type !== 'literal') acc[part.type] = part.value;
        return acc;
      }, {});

    return `${parts.year}.${parts.month}.${parts.day}`;
  } catch (e) {
    console.warn('Failed to format date', isoString, e);
    return isoString;
  }
};

/* =========================
   App lifecycle
   ========================= */

document.addEventListener('DOMContentLoaded', async () => {
  setupAuthModalListeners();
  setupProfileButton();
  updateProfileButtonState();
  setupSearchHandlers();

  if (UI.contentLoadMoreBtn) {
    UI.contentLoadMoreBtn.addEventListener('click', () => {
      const active = getActivePaginationCategory();
      if (active) loadNextPage(active);
    });
  }

  try {
    // preload subscriptions so stars render correctly (if token exists)
    await fetchMe();
    await loadSubscriptions();
  } catch (e) {
    console.warn('Failed to preload subscriptions', e);
  }

  renderBottomNav();
  updateTab('webtoon');
  setupScrollEffect();
  setupSeriesSortHandlers();
});

function setupScrollEffect() {
  window.addEventListener('scroll', () => {
    const scrolled = window.scrollY > 10;
    if (!UI.filtersWrapper) return;

    UI.filtersWrapper.style.backgroundColor = scrolled
      ? 'rgba(18, 18, 18, 0.85)'
      : '#121212';
    UI.filtersWrapper.style.backdropFilter = scrolled ? 'blur(12px)' : 'none';

    if (scrolled) UI.filtersWrapper.classList.add('border-b', 'border-white/5');
    else UI.filtersWrapper.classList.remove('border-b', 'border-white/5');
  });
}

/* =========================
   Search modal
   ========================= */

const getSearchType = () =>
  STATE.activeTab === 'my'
    ? STATE.lastBrowseTab || 'webtoon'
    : STATE.activeTab || 'webtoon';

const getSearchSource = (type) => {
  if (['webtoon', 'novel', 'ott'].includes(type)) {
    const src = STATE.filters?.[type]?.source;
    return src || 'all';
  }
  return 'all';
};

const getAspectByType = (type) => {
  if (type === 'novel') return 'aspect-[1/1.4]';
  if (type === 'ott') return 'aspect-[2/3]';
  return 'aspect-[3/4]';
};

function showSearchEmpty(title, { message = '', actions = [] } = {}) {
  STATE.search.activeIndex = -1;
  setActiveSearchIndex(-1);
  if (UI.searchEmptyState) {
    UI.searchEmptyState.classList.remove('hidden');
    renderEmptyState(UI.searchEmptyState, {
      title,
      message,
      actions,
    });
  }
  if (UI.searchResultsGrid) UI.searchResultsGrid.innerHTML = '';
  if (UI.searchLoadingState) UI.searchLoadingState.classList.add('hidden');
}

const SEARCH_ACTIVE_CLASSES = ['ring-2', 'ring-white/50', 'bg-white/5'];

const getSearchResultElements = () => {
  if (!UI.searchResultsGrid) return [];
  return Array.from(UI.searchResultsGrid.querySelectorAll('[data-search-index]'));
};

function setActiveSearchIndex(nextIndex) {
  const elements = getSearchResultElements();
  const hasItems = elements.length > 0;

  if (!hasItems || nextIndex < 0) {
    elements.forEach((el) => {
      SEARCH_ACTIVE_CLASSES.forEach((cls) => el.classList.remove(cls));
      el.setAttribute('aria-selected', 'false');
    });
    STATE.search.activeIndex = -1;
    return;
  }

  const clampedIndex = Math.max(0, Math.min(nextIndex, elements.length - 1));

  elements.forEach((el, idx) => {
    const isActive = idx === clampedIndex;
    SEARCH_ACTIVE_CLASSES.forEach((cls) =>
      el.classList[isActive ? 'add' : 'remove'](cls)
    );
    el.setAttribute('aria-selected', isActive ? 'true' : 'false');
    if (isActive) {
      el.scrollIntoView({ block: 'nearest' });
    }
  });

  STATE.search.activeIndex = clampedIndex;
}

function openActiveSearchResult() {
  const elements = getSearchResultElements();
  const idx = STATE.search.activeIndex;
  if (!elements.length || idx < 0 || idx >= elements.length) return;
  const el = elements[idx];
  const content = el.__content;
  if (!content) return;
  closeInlineSearch();
  openSubscribeModal(content);
}

function renderSearchLoading(type) {
  const container = UI.searchLoadingState;
  if (!container) return;
  container.classList.remove('hidden');
  container.innerHTML = '';
  const aspectClass = getAspectByType(type);
  for (let i = 0; i < 6; i += 1) {
    const item = document.createElement('div');
    item.className = `${aspectClass} rounded-lg skeleton`;
    container.appendChild(item);
  }
}

function resetSearchUI({ preserveInput = false } = {}) {
  if (!preserveInput && UI.searchInput) UI.searchInput.value = '';
  STATE.search.q = UI.searchInput ? UI.searchInput.value.trim() : '';
  STATE.search.requestSeq += 1; // invalidate inflight requests
  STATE.search.activeIndex = -1;
  if (UI.searchResultsGrid) UI.searchResultsGrid.innerHTML = '';
  if (UI.searchLoadingState) UI.searchLoadingState.classList.add('hidden');
  if (STATE.search.q) {
    if (UI.searchEmptyState) UI.searchEmptyState.classList.add('hidden');
  } else {
    showSearchEmpty('검색어를 입력하세요.');
  }
}

function updateSearchInputVisibility(isOpen) {
  const input = UI.searchInput;
  if (!input) return;
  const collapsed = ['w-0', 'opacity-0', 'pointer-events-none', 'ml-0'];
  const expanded = ['w-[220px]', 'opacity-100', 'pointer-events-auto', 'ml-2'];

  if (isOpen) {
    collapsed.forEach((c) => input.classList.remove(c));
    expanded.forEach((c) => input.classList.add(c));
  } else {
    expanded.forEach((c) => input.classList.remove(c));
    collapsed.forEach((c) => input.classList.add(c));
  }

  if (UI.searchClearBtn) {
    const hasValue = Boolean(input.value);
    UI.searchClearBtn.classList.toggle('hidden', !isOpen || !hasValue);
  }
}

function closeInlineSearch({ clearInput = false } = {}) {
  STATE.search.isOpen = false;
  STATE.search.requestSeq += 1;
  if (STATE.search.debounceTimer) {
    clearTimeout(STATE.search.debounceTimer);
    STATE.search.debounceTimer = null;
  }

  STATE.search.activeIndex = -1;

  if (UI.searchPanel) UI.searchPanel.classList.add('hidden');
  if (UI.searchLoadingState) UI.searchLoadingState.classList.add('hidden');
  if (UI.searchEmptyState) UI.searchEmptyState.classList.add('hidden');
  if (UI.searchResultsGrid) UI.searchResultsGrid.innerHTML = '';

  if (UI.searchInput && clearInput) UI.searchInput.value = '';
  STATE.search.q = UI.searchInput ? UI.searchInput.value.trim() : '';
  updateSearchInputVisibility(false);
}

function openInlineSearch() {
  STATE.search.isOpen = true;
  updateSearchInputVisibility(true);
  if (UI.searchPanel) UI.searchPanel.classList.remove('hidden');
  resetSearchUI({ preserveInput: true });

  if (UI.searchInput) {
    UI.searchInput.focus();
    const value = UI.searchInput.value.trim();
    if (value) performSearch(value);
  }
}

function openSearchAndFocus() {
  if (!UI.searchInput) return;
  if (!STATE.search.isOpen) openInlineSearch();
  else {
    if (UI.searchPanel) UI.searchPanel.classList.remove('hidden');
    updateSearchInputVisibility(true);
  }

  requestAnimationFrame(() => {
    if (UI.searchInput) UI.searchInput.focus();
  });
}

function renderSearchResults(items, effectiveType) {
  const grid = UI.searchResultsGrid;
  if (!grid) return;

  if (UI.searchLoadingState) UI.searchLoadingState.classList.add('hidden');
  if (UI.searchEmptyState) UI.searchEmptyState.classList.add('hidden');

  STATE.search.activeIndex = -1;
  grid.innerHTML = '';
  grid.setAttribute('role', 'listbox');
  grid.setAttribute('aria-label', '검색 결과');
  const aspectClass = getAspectByType(effectiveType);

  const normalizedItems = Array.isArray(items) ? items : [];
  if (!normalizedItems.length) {
    const clearAction = {
      label: '검색어 지우기',
      variant: 'primary',
      onClick: () => {
        if (UI.searchInput) {
          UI.searchInput.value = '';
          resetSearchUI();
          UI.searchInput.focus();
          updateSearchInputVisibility(true);
        }
      },
    };
    showSearchEmpty('검색 결과가 없습니다', {
      message: '다른 키워드로 검색해보세요.',
      actions: [clearAction],
    });
    return;
  }

  normalizedItems.forEach((raw, idx) => {
    const normalized = {
      ...raw,
      meta: normalizeMeta(raw?.meta),
      title: safeString(raw?.title, ''),
      content_id: raw?.content_id || raw?.contentId || raw?.id,
      id: raw?.id || raw?.content_id || raw?.contentId,
      source: raw?.source || getSearchSource(effectiveType),
    };

    const card = createCard(normalized, effectiveType, aspectClass);
    card.dataset.searchIndex = String(idx);
    card.setAttribute('role', 'option');
    card.setAttribute('aria-selected', 'false');
    card.__content = normalized;
    card.addEventListener('mouseenter', () => setActiveSearchIndex(idx));
    card.onclick = () => {
      closeInlineSearch();
      openSubscribeModal(normalized);
    };
    grid.appendChild(card);
  });

  setActiveSearchIndex(normalizedItems.length ? 0 : -1);
}

function performSearch(q) {
  const query = (q || '').trim();
  const effectiveType = getSearchType();
  const source = getSearchSource(effectiveType);

  STATE.search.q = query;
  STATE.search.activeIndex = -1;
  setActiveSearchIndex(-1);

  if (!query) {
    showSearchEmpty('검색어를 입력하세요.');
    if (UI.searchLoadingState) UI.searchLoadingState.classList.add('hidden');
    if (UI.searchResultsGrid) UI.searchResultsGrid.innerHTML = '';
    return;
  }

  const seq = ++STATE.search.requestSeq;
  STATE.search.isLoading = true;

  if (UI.searchEmptyState) UI.searchEmptyState.classList.add('hidden');
  renderSearchLoading(effectiveType);
  if (UI.searchResultsGrid) UI.searchResultsGrid.innerHTML = '';

  apiRequest('GET', '/api/contents/search', {
    query: { q: query, type: effectiveType, source },
  })
    .then((res) => {
      if (seq !== STATE.search.requestSeq) return;
      const items = Array.isArray(res?.data) ? res.data : Array.isArray(res) ? res : [];
      renderSearchResults(items, effectiveType);
    })
    .catch((e) => {
      if (seq !== STATE.search.requestSeq) return;
      showToast(e?.message || '검색에 실패했습니다.', { type: 'error' });
      const clearAction = {
        label: '검색어 지우기',
        variant: 'primary',
        onClick: () => {
          if (UI.searchInput) {
            UI.searchInput.value = '';
            resetSearchUI();
            UI.searchInput.focus();
            updateSearchInputVisibility(true);
          }
        },
      };
      showSearchEmpty('검색 결과가 없습니다', {
        message: '다른 키워드로 검색해보세요.',
        actions: [clearAction],
      });
    })
    .finally(() => {
      if (seq !== STATE.search.requestSeq) return;
      STATE.search.isLoading = false;
      if (UI.searchLoadingState) UI.searchLoadingState.classList.add('hidden');
    });
}

function debouncedSearch(q) {
  if (STATE.search.debounceTimer) clearTimeout(STATE.search.debounceTimer);
  STATE.search.debounceTimer = setTimeout(() => performSearch(q), 300);
}

function setupSearchHandlers() {
  if (UI.searchButton)
    UI.searchButton.onclick = () => {
      if (STATE.search.isOpen) closeInlineSearch();
      else openInlineSearch();
    };

  document.addEventListener('keydown', (evt) => {
    if (isAnyModalOpen()) return;
    if (evt.key === 'Escape' && STATE.search.isOpen) {
      closeInlineSearch();
    } else if ((evt.ctrlKey || evt.metaKey) && evt.key.toLowerCase() === 'k') {
      evt.preventDefault();
      if (STATE.search.isOpen) closeInlineSearch();
      else openInlineSearch();
    }
  });

  document.addEventListener('click', (evt) => {
    if (!STATE.search.isOpen) return;
    if (UI.headerSearchWrap && UI.headerSearchWrap.contains(evt.target)) return;
    closeInlineSearch();
  });

  if (UI.searchInput) {
    UI.searchInput.addEventListener('input', (evt) => {
      updateSearchInputVisibility(true);
      STATE.search.activeIndex = -1;
      setActiveSearchIndex(-1);
      debouncedSearch(evt.target.value);
    });
    UI.searchInput.addEventListener('keydown', (evt) => {
      if (isAnyModalOpen()) return;
      const elements = getSearchResultElements();
      const hasResults = elements.length > 0;

      if (evt.key === 'ArrowDown') {
        if (!STATE.search.isOpen) openInlineSearch();
        if (hasResults) {
          evt.preventDefault();
          const nextIndex =
            STATE.search.activeIndex < 0
              ? 0
              : Math.min(STATE.search.activeIndex + 1, elements.length - 1);
          setActiveSearchIndex(nextIndex);
        }
      } else if (evt.key === 'ArrowUp') {
        if (hasResults) {
          evt.preventDefault();
          const current = STATE.search.activeIndex < 0 ? 0 : STATE.search.activeIndex;
          const nextIndex = Math.max(current - 1, 0);
          setActiveSearchIndex(nextIndex);
        }
      } else if (evt.key === 'Enter') {
        if (STATE.search.activeIndex >= 0 && STATE.search.activeIndex < elements.length) {
          evt.preventDefault();
          openActiveSearchResult();
        } else {
          performSearch(evt.target.value);
        }
      }
    });
  }

  if (UI.searchClearBtn)
    UI.searchClearBtn.onclick = (evt) => {
      evt.stopPropagation();
      resetSearchUI();
      if (UI.searchInput) {
        UI.searchInput.focus();
        updateSearchInputVisibility(true);
      }
    };
}

/* =========================
   Auth modal + profile
   ========================= */

const isProfileMenuOpen = () => UI.profileMenu && !UI.profileMenu.classList.contains('hidden');

function closeProfileMenu() {
  if (UI.profileMenu) UI.profileMenu.classList.add('hidden');
  if (UI.profileButton) UI.profileButton.setAttribute('aria-expanded', 'false');
}

function openProfileMenu() {
  if (!UI.profileMenu || !UI.profileButton) return;
  UI.profileMenu.classList.remove('hidden');
  UI.profileButton.setAttribute('aria-expanded', 'true');
  const firstItem = UI.profileMenu.querySelector('[role="menuitem"]');
  if (firstItem) firstItem.focus();
}

function toggleProfileMenu() {
  if (isProfileMenuOpen()) closeProfileMenu();
  else openProfileMenu();
}

function updateProfileButtonState() {
  const btn = UI.profileButton;
  const textEl = UI.profileButtonText;
  if (!btn || !textEl) return;

  btn.setAttribute('aria-expanded', isProfileMenuOpen() ? 'true' : 'false');

  const isAuth = STATE.auth.isAuthenticated;
  const user = STATE.auth.user;

  const baseClasses =
    'h-[32px] px-3 whitespace-nowrap rounded-full border border-white/10 flex items-center justify-center text-xs text-white spring-bounce hover:border-[#4F46E5] hover:shadow-[0_0_12px_rgba(79,70,229,0.4)]';
  btn.className = baseClasses + (isAuth ? ' bg-[#4F46E5]' : ' bg-[#2d2d2d]');

  if (isAuth && user) {
    const initial = safeString(user.email || user.id || 'M', 'M')
      .charAt(0)
      .toUpperCase();
    textEl.textContent = initial || 'M';
    btn.setAttribute('title', safeString(user.email, '로그아웃'));
  } else {
    textEl.textContent = 'Login';
    btn.setAttribute('title', 'Login');
    closeProfileMenu();
  }
}

function setupProfileButton() {
  const btn = UI.profileButton;
  if (!btn) return;

  btn.onclick = () => {
    if (STATE.auth.isAuthenticated) {
      toggleProfileMenu();
    } else {
      openAuthModal({ reason: 'profile' });
    }
  };

  document.addEventListener('click', (evt) => {
    if (!isProfileMenuOpen()) return;
    if (UI.profileButton?.contains(evt.target) || UI.profileMenu?.contains(evt.target)) return;
    closeProfileMenu();
  });

  document.addEventListener('keydown', (evt) => {
    if (evt.key === 'Escape' && isProfileMenuOpen()) {
      closeProfileMenu();
    }
  });

  if (UI.profileMenuMy) {
    UI.profileMenuMy.onclick = () => {
      closeProfileMenu();
      updateTab('my');
    };
  }

  if (UI.profileMenuLogout) {
    UI.profileMenuLogout.onclick = () => {
      closeProfileMenu();
      logout();
    };
  }
}

function applyAuthMode(mode = 'login') {
  STATE.auth.uiMode = mode;

  const titleEl = document.getElementById('authTitle');
  const submitBtn = document.getElementById('authSubmitBtn');
  const confirmRow = document.getElementById('authPasswordConfirmRow');
  const confirmInput = document.getElementById('authPasswordConfirm');
  const hintTextEl = document.getElementById('authModeHintText');
  const toggleBtn = document.getElementById('authToggleModeBtn');
  const errorEl = document.getElementById('authError');

  if (errorEl) errorEl.textContent = '';

  if (mode === 'register') {
    if (titleEl) titleEl.textContent = '회원가입';
    if (submitBtn) submitBtn.textContent = '회원가입';
    if (confirmRow) confirmRow.classList.remove('hidden');
    if (hintTextEl) hintTextEl.textContent = '이미 계정이 있나요?';
    if (toggleBtn) toggleBtn.textContent = '로그인';
  } else {
    if (titleEl) titleEl.textContent = '로그인';
    if (submitBtn) submitBtn.textContent = '로그인';
    if (confirmRow) confirmRow.classList.add('hidden');
    if (hintTextEl) hintTextEl.textContent = '계정이 없으신가요?';
    if (toggleBtn) toggleBtn.textContent = '회원가입';
    if (confirmInput) confirmInput.value = '';
  }
}

function openAuthModal(_opts = {}) {
  const modal = document.getElementById('authModal');
  const emailEl = document.getElementById('authEmail');
  const pwdEl = document.getElementById('authPassword');
  const confirmEl = document.getElementById('authPasswordConfirm');
  const errorEl = document.getElementById('authError');
  const closeBtn = document.getElementById('authCloseBtn');
  if (!modal) return;

  const mode = typeof _opts === 'string' ? _opts : _opts?.mode || 'login';

  closeProfileMenu();
  if (errorEl) errorEl.textContent = '';
  if (emailEl) emailEl.value = '';
  if (pwdEl) pwdEl.value = '';
  if (confirmEl) confirmEl.value = '';
  applyAuthMode(mode);
  openModal(modal, { initialFocusEl: closeBtn || emailEl || modal });
}

function closeAuthModal() {
  const modal = document.getElementById('authModal');
  if (modal) closeModal(modal);
}

function setupAuthModalListeners() {
  const submitBtn = document.getElementById('authSubmitBtn');
  const cancelBtn = document.getElementById('authCloseBtn');
  const toggleBtn = document.getElementById('authToggleModeBtn');
  const emailEl = document.getElementById('authEmail');
  const pwdEl = document.getElementById('authPassword');
  const confirmEl = document.getElementById('authPasswordConfirm');
  const errorEl = document.getElementById('authError');

  if (cancelBtn) cancelBtn.onclick = () => closeAuthModal();
  if (toggleBtn)
    toggleBtn.onclick = () => {
      const nextMode = STATE.auth.uiMode === 'login' ? 'register' : 'login';
      applyAuthMode(nextMode);
    };

  const handleSubmit = async () => {
    if (!emailEl || !pwdEl) return;
    const email = emailEl.value.trim();
    const password = pwdEl.value;
    const mode = STATE.auth.uiMode || 'login';

    if (!/.+@.+\..+/.test(email)) {
      if (errorEl) errorEl.textContent = '유효한 이메일을 입력해주세요.';
      return;
    }

    if (!password || password.length < 8) {
      if (errorEl) errorEl.textContent = '비밀번호는 8자 이상 입력해주세요.';
      return;
    }

    if (mode === 'register') {
      const confirmPwd = confirmEl ? confirmEl.value : '';
      if (password !== confirmPwd) {
        if (errorEl) errorEl.textContent = '비밀번호가 일치하지 않습니다.';
        return;
      }
    }

    if (errorEl) errorEl.textContent = '';

    try {
      if (mode === 'register') await register(email, password);
      else await login(email, password);

      await fetchMe();
      updateProfileButtonState();

      const hasToken = Boolean(getAccessToken());
      if (hasToken) {
        try {
          await loadSubscriptions({ force: true });
        } catch (e) {
          console.warn('Failed to refresh subscriptions after auth', e);
        }
      }

      closeAuthModal();
      showToast('로그인되었습니다', { type: 'success' });

      if (STATE.activeTab === 'my') await fetchAndRenderContent('my');
      else await fetchAndRenderContent(STATE.activeTab);
    } catch (e) {
      let message = '서버 오류가 발생했습니다.';

      if (e?.httpStatus === 401) {
        message = '이메일 또는 비밀번호가 올바르지 않습니다.';
      } else if (e?.code === 'EMAIL_ALREADY_EXISTS') {
        message = '이미 등록된 이메일입니다.';
      } else if (e?.code === 'PASSWORD_TOO_SHORT') {
        message = '비밀번호는 8자 이상이어야 합니다.';
      } else if (e?.code === 'JWT_SECRET_MISSING') {
        message = '서버 설정 오류로 로그인/회원가입을 진행할 수 없습니다. 관리자에게 문의해주세요.';
      } else if (e?.message) {
        message = e.message;
      }

      if (errorEl) errorEl.textContent = message;
      showToast(message, { type: 'error' });
    }
  };

  if (submitBtn) submitBtn.onclick = handleSubmit;
  [pwdEl, confirmEl].forEach((el) => {
    if (el)
      el.addEventListener('keydown', (evt) => {
        if (evt.key === 'Enter') handleSubmit();
      });
  });
}

window.openAuthModal = openAuthModal;
window.closeAuthModal = closeAuthModal;

/* =========================
   Navigation + Filters
   ========================= */

function renderBottomNav() {
  if (!UI.bottomNav) return;

  UI.bottomNav.innerHTML = '';
  const tabs = [
    { id: 'webtoon', label: 'Webtoon', icon: ICONS.webtoon },
    { id: 'novel', label: 'Web Novel', icon: ICONS.novel },
    { id: 'ott', label: 'OTT', icon: ICONS.ott },
    { id: 'series', label: 'Series', icon: ICONS.series },
    { id: 'my', label: 'My Sub', icon: ICONS.my },
  ];

  tabs.forEach((tab) => {
    const btn = document.createElement('button');
    const isActive = STATE.activeTab === tab.id;
    btn.className = `flex flex-col items-center justify-center w-full spring-bounce ${
      isActive ? 'text-[#4F46E5]' : 'text-[#525252]'
    }`;

    const iconClass = isActive ? 'scale-110 neon-drop-shadow' : 'scale-100';

    btn.innerHTML = `
      <div class="mb-1 transform transition-transform duration-200 ${iconClass}">
        ${tab.icon}
      </div>
      <span class="text-[10px] ${isActive ? 'font-bold' : 'font-medium'}">${tab.label}</span>
    `;
    btn.onclick = () => updateTab(tab.id);
    UI.bottomNav.appendChild(btn);
  });
}

async function updateTab(tabId) {
  STATE.activeTab = tabId;
  if (tabId !== 'my') STATE.lastBrowseTab = tabId;
  if (STATE.search.isOpen) closeInlineSearch();

  renderBottomNav();
  updateFilterVisibility(tabId);
  renderL1Filters(tabId);
  renderL2Filters(tabId);

  await fetchAndRenderContent(tabId);
  window.scrollTo({ top: 0 });
}

function updateFilterVisibility(tabId) {
  if (
    !UI.l1Filter ||
    !UI.l2Filter ||
    !UI.mySubToggle ||
    !UI.seriesSort ||
    !UI.seriesFooter
  )
    return;

  UI.l1Filter.classList.add('hidden');
  UI.l2Filter.classList.add('hidden');
  UI.mySubToggle.classList.add('hidden');
  UI.seriesSort.classList.add('hidden');
  UI.seriesFooter.classList.add('hidden');

  if (['webtoon', 'novel', 'ott'].includes(tabId)) {
    UI.l1Filter.classList.remove('hidden');
    UI.l2Filter.classList.remove('hidden');
  } else if (tabId === 'series') {
    UI.seriesSort.classList.remove('hidden');
    UI.seriesFooter.classList.remove('hidden');
  } else if (tabId === 'my') {
    UI.mySubToggle.classList.remove('hidden');
  }
}

function renderL1Filters(tabId) {
  if (!UI.l1Filter) return;

  UI.l1Filter.innerHTML = '';
  let items = [];

  if (tabId === 'webtoon') {
    items = [
      { id: 'all', label: '전체', color: '#A3A3A3' },
      { id: 'naver_webtoon', label: 'N', color: '#00D564' },
      { id: 'kakaowebtoon', label: 'K', color: '#F7E600' },
      { id: 'lezhin', label: 'L', color: '#E62E2E' },
      { id: 'laftel', label: 'R', color: '#6C5CE7' },
    ];
  } else if (tabId === 'novel') {
    items = [
      { id: 'all', label: 'All' },
      { id: 'naver_series', label: 'N', color: '#00D564' },
      { id: 'kakao_page', label: 'K', color: '#F7E600' },
      { id: 'ridi', label: 'R', color: '#0077D9' },
      { id: 'munpia', label: 'M' },
    ];
  } else if (tabId === 'ott') {
    items = [
      { id: 'all', label: 'All' },
      { id: 'netflix', label: 'N', color: 'red' },
      { id: 'disney', label: 'D', color: 'blue' },
      { id: 'tving', label: 'T' },
      { id: 'watcha', label: 'W' },
      { id: 'wavve', label: 'Wa' },
    ];
  } else {
    return;
  }

  items.forEach((item) => {
    const el = document.createElement('div');
    const isActive = STATE.filters?.[tabId]?.source === item.id;
    el.className = `l1-logo flex-shrink-0 cursor-pointer spring-bounce ${
      isActive ? 'active' : 'inactive'
    }`;
    el.textContent = item.label;

    if (item.color && isActive) el.style.borderColor = item.color;

    el.onclick = () => {
      STATE.filters[tabId].source = item.id;
      renderL1Filters(tabId);
      fetchAndRenderContent(tabId);
    };

    UI.l1Filter.appendChild(el);
  });
}

function renderL2Filters(tabId) {
  if (!UI.l2Filter) return;

  UI.l2Filter.innerHTML = '';
  let items = [];

  const days = [
    { id: 'all', label: 'ALL' },
    { id: 'mon', label: '월' },
    { id: 'tue', label: '화' },
    { id: 'wed', label: '수' },
    { id: 'thu', label: '목' },
    { id: 'fri', label: '금' },
    { id: 'sat', label: '토' },
    { id: 'sun', label: '일' },
    { id: 'daily', label: '매일' },
    { id: 'hiatus', label: '휴재' },
    { id: 'completed', label: '완결' },
  ];

  if (tabId === 'webtoon' || tabId === 'novel') {
    items = days;
  } else if (tabId === 'ott') {
    items = [
      { id: 'drama', label: '드라마' },
      { id: 'anime', label: '애니메이션' },
      { id: 'variety', label: '예능' },
      { id: 'docu', label: '다큐멘터리' },
    ];
  } else {
    return;
  }

  let activeKey = '';
  if (tabId === 'webtoon' || tabId === 'novel')
    activeKey = STATE.filters?.[tabId]?.day || 'all';
  if (tabId === 'ott') activeKey = STATE.filters?.[tabId]?.genre;

  items.forEach((item) => {
    const el = document.createElement('button');
    const isActive = activeKey === item.id;
    el.className = `l2-tab spring-bounce ${isActive ? 'active' : ''}`;
    el.textContent = item.label;

    el.onclick = () => {
      if (tabId === 'webtoon' || tabId === 'novel')
        STATE.filters[tabId].day = item.id;
      if (tabId === 'ott') STATE.filters[tabId].genre = item.id;

      renderL2Filters(tabId);
      fetchAndRenderContent(tabId);
    };

    UI.l2Filter.appendChild(el);
  });
}

function updateMySubTab(mode) {
  STATE.filters.my.viewMode = mode;

  if (UI.toggleIndicator) {
    UI.toggleIndicator.style.transform =
      mode === 'subscribing' ? 'translateX(0)' : 'translateX(100%)';
  }

  fetchAndRenderContent('my');
}

/* =========================
   Data fetching + rendering
   ========================= */

const appendCardsToGrid = (
  items,
  { tabId = 'webtoon', aspectClass = 'aspect-[3/4]', clearBeforeAppend = false } = {}
) => {
  if (!UI.contentGrid || !Array.isArray(items) || !items.length) return;
  if (clearBeforeAppend) UI.contentGrid.innerHTML = '';

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    fragment.appendChild(createCard(item, tabId, aspectClass));
  });

  UI.contentGrid.appendChild(fragment);
};

const renderNextBatch = () => {
  if (STATE.rendering.scheduled) return;

  STATE.rendering.scheduled = true;
  const expectedSeq = STATE.rendering.requestSeq;

  requestAnimationFrame(() => {
    STATE.rendering.scheduled = false;
    if (STATE.rendering.requestSeq !== expectedSeq || STATE.contentRequestSeq !== expectedSeq)
      return;

    const { list, index, batchSize, aspectClass, tabId } = STATE.rendering;
    if (!UI.contentGrid || !list || !list.length) return;

    const end = Math.min(index + batchSize, list.length);
    const fragment = document.createDocumentFragment();

    for (let i = index; i < end; i++) {
      fragment.appendChild(createCard(list[i], tabId, aspectClass));
    }

    UI.contentGrid.appendChild(fragment);
    STATE.rendering.index = end;
    updateCountIndicatorForBatched();

    if (end < list.length) {
      renderNextBatch();
    }
  });
};

const startBatchedRender = (items, tabId, aspectClass) => {
  const list = Array.isArray(items) ? items : [];
  STATE.rendering.list = list;
  STATE.rendering.index = 0;
  STATE.rendering.scheduled = false;
  STATE.rendering.requestSeq = STATE.contentRequestSeq;
  STATE.rendering.aspectClass = aspectClass;
  STATE.rendering.tabId = tabId;

  if (UI.contentGrid) UI.contentGrid.innerHTML = '';

  if (!list.length) {
    setCountIndicatorText('');
    return;
  }

  updateCountIndicatorForBatched();
  renderNextBatch();
};

async function loadNextPage(category) {
  const pg = STATE.pagination?.[category];
  if (!pg || pg.loading || pg.done) return;
  if (pg.requestSeq !== STATE.contentRequestSeq) return;

  pg.loading = true;
  updateLoadMoreUI(category);
  updateCountIndicator(category);

  const perPage = 300;
  const query = {
    type: pg.tabId,
    source: pg.source || 'all',
    per_page: perPage,
  };

  if (pg.cursor !== null && pg.cursor !== undefined) query.cursor = pg.cursor;
  else if (pg.legacyCursor) query.last_title = pg.legacyCursor;

  const url = buildUrl(`/api/contents/${category}`, query);

  try {
    const json = await apiRequest('GET', url);
    if (pg.requestSeq !== STATE.contentRequestSeq) return;

    const incoming = Array.isArray(json?.contents)
      ? json.contents.map((item) => ({ ...item, meta: normalizeMeta(item?.meta) }))
      : [];

    const next = json?.next_cursor ?? null;
    const legacyNext = !next ? json?.last_title ?? null : null;
    const parsedPageSize = Number(json?.page_size);
    const responsePageSize = Number.isFinite(parsedPageSize) ? parsedPageSize : perPage;

    const existingKeys = new Set(pg.items.map(contentKey));
    const toAppend = [];

    for (const c of incoming) {
      const key = contentKey(c);
      if (!key || existingKeys.has(key)) continue;
      existingKeys.add(key);
      pg.items.push(c);
      toAppend.push(c);
    }

    if (next) {
      pg.cursor = next;
      pg.legacyCursor = null;
    } else if (legacyNext) {
      pg.cursor = null;
      pg.legacyCursor = legacyNext;
    } else {
      pg.cursor = null;
      pg.legacyCursor = null;
    }

    const noNewItems = toAppend.length === 0;
    const hasPaginationToken = Boolean(next || legacyNext);
    const returnedCount = incoming.length;
    const missingCursor = !hasPaginationToken;
    const reachedEndByCount = returnedCount < responsePageSize;

    if (noNewItems) {
      console.warn('No new items returned; marking pagination as done to avoid stalls');
    }

    pg.done = noNewItems || (missingCursor && (reachedEndByCount || pg.items.length > 0));
    pg.totalLoaded = pg.items.length;

    appendCardsToGrid(toAppend, {
      tabId: pg.tabId,
      aspectClass: pg.aspectClass,
      clearBeforeAppend: pg.items.length === toAppend.length,
    });

    updateCountIndicator(category);
    updateLoadMoreUI(category);

    if (pg.done && getActivePaginationCategory() === category) {
      disconnectInfiniteObserver();
    }
  } catch (e) {
    showToast(e?.message || '콘텐츠를 불러오지 못했습니다.', { type: 'error' });
  } finally {
    pg.loading = false;
    updateLoadMoreUI(category);
  }
}

async function fetchAndRenderContent(tabId) {
  if (!UI.contentGrid) return;

  disconnectInfiniteObserver();
  setActivePaginationCategory(null);
  hideLoadMoreUI();
  setCountIndicatorText('');

  const requestSeq = ++STATE.contentRequestSeq;
  const isStale = () => STATE.contentRequestSeq !== requestSeq;

  UI.contentGrid.innerHTML = '';
  STATE.isLoading = true;

  let aspectClass = 'aspect-[3/4]';
  if (tabId === 'novel') aspectClass = 'aspect-[1/1.4]';
  if (tabId === 'ott') aspectClass = 'aspect-[2/3]';

  let skeletonShown = false;
  const showSkeleton = () => {
    if (isStale() || skeletonShown) return;
    skeletonShown = true;
    UI.contentGrid.innerHTML = '';
    for (let i = 0; i < 9; i++) {
      const skel = document.createElement('div');
      skel.className = `${aspectClass} rounded-lg skeleton`;
      UI.contentGrid.appendChild(skel);
    }
  };
  const skeletonTimer = setTimeout(showSkeleton, 120);

  let data = [];
  let emptyStateConfig = null;

  try {
    if (tabId === 'my') {
      const token = getAccessToken();
      if (!token) {
        if (!isStale()) {
          UI.contentGrid.innerHTML =
            '<div class="col-span-3 text-center text-gray-400 py-10 text-sm flex flex-col items-center gap-3"><p>로그인이 필요합니다.</p><button id="myTabLoginButton" class="px-4 py-2 rounded-lg bg-[#4f46e5] text-white text-xs font-bold">로그인하기</button></div>';

          const loginBtn = document.getElementById('myTabLoginButton');
          if (loginBtn) {
            loginBtn.onclick = () => openAuthModal({ reason: 'my-tab' });
          }
        }
        return;
      }

      let subs = [];
      try {
        subs = await loadSubscriptions();
      } catch (e) {
        if (!isStale()) {
          UI.contentGrid.innerHTML =
            '<div class="col-span-3 text-center text-gray-400 py-10 text-sm flex flex-col items-center gap-3"><p>구독 정보를 불러오지 못했습니다.</p><button id="mySubRetryButton" class="px-4 py-2 rounded-lg bg-[#4f46e5] text-white text-xs font-bold">다시 시도</button></div>';

          const retryBtn = document.getElementById('mySubRetryButton');
          if (retryBtn) {
            retryBtn.onclick = async () => {
              try {
                await loadSubscriptions({ force: true });
                fetchAndRenderContent('my');
              } catch (err) {
                showToast(err?.message || '오류가 발생했습니다.', { type: 'error' });
              }
            };
          }
        }
        return;
      }

      const mode = STATE.filters?.my?.viewMode || 'subscribing';

      data = (subs || []).filter((item) => {
        const fs = item?.final_state || {};
        const isScheduled = fs?.is_scheduled_completion === true;
        const isCompleted = fs?.final_status === '완결' && !isScheduled;

        if (mode === 'completed') return isCompleted;
        return !isCompleted;
      });

      if (!data.length) {
        if (mode === 'completed') {
          emptyStateConfig = {
            title: '완결된 구독 작품이 아직 없습니다',
            message: '구독 중인 작품이 완결되면 여기에 표시됩니다.',
            actions: [
              {
                label: '구독 목록 보기',
                variant: 'primary',
                onClick: () => {
                  STATE.filters.my.viewMode = 'subscribing';
                  updateTab('my');
                },
              },
              {
                label: '검색하기',
                variant: 'secondary',
                onClick: () => openSearchAndFocus(),
              },
            ],
          };
        } else {
          emptyStateConfig = {
            title: '구독한 작품이 없습니다',
            message: '작품을 검색한 뒤, 작품 화면에서 알림을 설정해보세요.',
            actions: [
              {
                label: '검색하기',
                variant: 'primary',
                onClick: () => openSearchAndFocus(),
              },
              {
                label: '웹툰 보기',
                variant: 'secondary',
                onClick: () => updateTab('webtoon'),
              },
            ],
          };
        }
      }
    } else {
      let url = '';

      if (tabId === 'webtoon' || tabId === 'novel') {
        const day = STATE.filters[tabId].day;
        const source = STATE.filters[tabId].source;
        const query = { type: tabId, source };

        if (day === 'completed' || day === 'hiatus') {
          resetPaginationState(day, { tabId, source, aspectClass, requestSeq });
          setActivePaginationCategory(day);
          updateLoadMoreUI(day);
          updateCountIndicator(day);
          setupInfiniteObserver(day);
          await loadNextPage(day);
          return;
        }

        if (day === 'completed') url = buildUrl('/api/contents/completed', query);
        else if (day === 'hiatus') url = buildUrl('/api/contents/hiatus', query);
        else url = buildUrl('/api/contents/ongoing', query);
      }

      if (url) {
        const json = await apiRequest('GET', url);

        if (tabId === 'webtoon' || tabId === 'novel') {
          const day = STATE.filters[tabId].day;

          if (day !== 'completed' && day !== 'hiatus' && day !== 'all') {
            data = Array.isArray(json?.[day]) ? json[day] : [];
          } else if (day === 'all') {
            data = [];
            Object.values(json || {}).forEach((arr) => {
              if (Array.isArray(arr)) data.push(...arr);
            });
          } else {
            data = Array.isArray(json?.contents) ? json.contents : [];
          }
        }
      } else {
        // minimal mocks for tabs without endpoints in current backend
        data = [
          { title: 'Mock Item 1', meta: { common: { thumbnail_url: null, authors: [] } } },
          { title: 'Mock Item 2', meta: { common: { thumbnail_url: null, authors: [] } } },
          { title: 'Mock Item 3', meta: { common: { thumbnail_url: null, authors: [] } } },
        ];
      }
    }

    data = Array.isArray(data)
      ? data.map((item) => ({ ...item, meta: normalizeMeta(item?.meta) }))
      : [];
  } catch (e) {
    console.error('Fetch error', e);
    showToast(e?.message || '오류가 발생했습니다.', { type: 'error' });
  } finally {
    clearTimeout(skeletonTimer);
    STATE.isLoading = false;
  }

  if (isStale()) return;

  UI.contentGrid.innerHTML = '';

  if (!data.length) {
    if (emptyStateConfig) renderEmptyState(UI.contentGrid, emptyStateConfig);
    else
      UI.contentGrid.innerHTML =
        '<div class="col-span-3 text-center text-gray-500 py-10 text-xs">콘텐츠가 없습니다.</div>';
    return;
  }

  if (tabId === 'webtoon' || tabId === 'novel') {
    startBatchedRender(data, tabId, aspectClass);
    return;
  }

  appendCardsToGrid(data, { tabId, aspectClass, clearBeforeAppend: true });
  setCountIndicatorText(`총 ${data.length}건`);
}

function createCard(content, tabId, aspectClass) {
  const el = document.createElement('div');
  el.className = 'relative group cursor-pointer fade-in';

  const meta = normalizeMeta(content?.meta);
  const thumb = meta?.common?.thumbnail_url || FALLBACK_THUMB;
  const authors = Array.isArray(meta?.common?.authors)
    ? meta.common.authors.join(', ')
    : '';

  const cardContainer = document.createElement('div');
  cardContainer.className = `${aspectClass} rounded-lg overflow-hidden bg-[#1E1E1E] relative mb-2`;

  const imgEl = document.createElement('img');
  imgEl.src = thumb;
  imgEl.loading = 'lazy';
  imgEl.decoding = 'async';
  imgEl.onerror = () => {
    if (imgEl.dataset.fallbackApplied === '1') return;
    imgEl.dataset.fallbackApplied = '1';
    imgEl.src = FALLBACK_THUMB;
  };
  imgEl.className =
    'w-full h-full object-cover group-hover:scale-105 transition-transform duration-300';
  cardContainer.appendChild(imgEl);

  // Badge logic
  if (tabId === 'my') {
    const fs = safeObj(content?.final_state);
    const isScheduled = fs?.is_scheduled_completion === true;
    const scheduledDate = safeString(fs?.scheduled_completed_at, '');
    const isCompleted = safeString(fs?.final_status, '') === '완결';
    const isHiatus =
      safeString(fs?.final_status, '') === '휴재' || content?.status === '휴재';

    const badgeEl = document.createElement('div');
    badgeEl.className =
      'absolute top-0 left-0 backdrop-blur-md px-2 py-1 rounded-br-lg z-10 flex items-center gap-1';

    if (isScheduled) {
      badgeEl.className += ' bg-yellow-500/80';
      const formatted = scheduledDate ? formatDateKST(scheduledDate) : '';
      badgeEl.innerHTML = `<span class="text-[10px] font-black text-black leading-none">완결 예정</span>${
        formatted
          ? `<span class="text-[10px] text-black leading-none">${formatted}</span>`
          : ''
      }`;
      cardContainer.appendChild(badgeEl);
    } else if (isCompleted) {
      badgeEl.className += ' bg-green-500/80';
      badgeEl.innerHTML = `<span class="text-[10px] font-black text-black leading-none">완결</span>`;
      cardContainer.appendChild(badgeEl);
    } else if (isHiatus) {
      badgeEl.className += ' bg-gray-600/80';
      badgeEl.innerHTML = `<span class="text-[10px] font-black text-white leading-none">휴재</span>`;
      cardContainer.appendChild(badgeEl);
    }
  } else if (content.status === '완결') {
    const badgeEl = document.createElement('div');
    badgeEl.className =
      'absolute top-0 left-0 bg-black/60 backdrop-blur-md px-2 py-1 rounded-br-lg z-10 flex items-center gap-0.5';
    badgeEl.innerHTML = `<span class="text-[10px] font-black text-white leading-none">EN</span><span class="text-[10px] text-yellow-400 leading-none">🔔</span>`;
    cardContainer.appendChild(badgeEl);
  }

  const gradient = document.createElement('div');
  gradient.className =
    'absolute inset-0 bg-gradient-to-t from-black/60 via-transparent to-transparent opacity-60';
  cardContainer.appendChild(gradient);

  const subscribed = isSubscribed(content);
  if (subscribed) {
    const badgeEl = document.createElement('div');
    badgeEl.className =
      'absolute top-2 right-2 z-10 flex items-center justify-center h-[26px] px-2 rounded-full bg-black/60 text-white text-xs font-semibold pointer-events-none select-none';
    badgeEl.setAttribute('aria-hidden', 'true');
    badgeEl.textContent = '★';
    cardContainer.appendChild(badgeEl);
  }

  el.appendChild(cardContainer);

  const textContainer = document.createElement('div');
  textContainer.className = 'px-0.5';

  const titleEl = document.createElement('h3');
  titleEl.className =
    'font-bold text-[13px] text-[#E5E5E5] leading-[1.4] truncate';
  titleEl.textContent = content.title || '';

  const authorEl = document.createElement('p');
  authorEl.className = 'text-[11px] text-[#A3A3A3] mt-0.5 truncate';
  authorEl.textContent = authors;

  textContainer.appendChild(titleEl);
  textContainer.appendChild(authorEl);
  el.appendChild(textContainer);

  el.onclick = () => openSubscribeModal(content);
  return el;
}

/* =========================
   Modal: subscription toggle
   ========================= */

const syncModalButton = () => {
  const btn = document.getElementById('subscribeButton');
  const content = STATE.currentModalContent;
  if (!btn || !content) return;
  const on = isSubscribed(content);
  btn.textContent = on ? '구독 해제' : '구독하기';
};

function getContentUrl(content) {
  const normalizeUrl = (raw) => {
    const trimmed = typeof raw === 'string' ? raw.trim() : '';
    if (!trimmed) return '';

    const NAVER_MOBILE_HOST = 'm.comic.naver.com';

    if (!/^https?:\/\//i.test(trimmed)) {
      if (trimmed.includes(NAVER_MOBILE_HOST)) {
        return trimmed.replace(NAVER_MOBILE_HOST, 'comic.naver.com');
      }
      return trimmed;
    }

    try {
      const urlObj = new URL(trimmed);
      if (urlObj.hostname === NAVER_MOBILE_HOST) {
        urlObj.hostname = 'comic.naver.com';
      }
      return urlObj.toString();
    } catch (e) {
      if (trimmed.includes(NAVER_MOBILE_HOST)) {
        return trimmed.replace(NAVER_MOBILE_HOST, 'comic.naver.com');
      }
      return trimmed;
    }
  };

  const candidates = [
    content?.meta?.common?.content_url,
    content?.meta?.common?.url,
    content?.content_url,
    content?.meta?.content_url,
  ];

  for (const u of candidates) {
    const normalized = normalizeUrl(u);
    if (normalized) return normalized;
  }

  const source = String(content?.source || '').toLowerCase();
  const contentId = String(
    content?.content_id ?? content?.contentId ?? content?.id ?? ''
  ).trim();
  const title = String(content?.title || '').trim();

  if (!contentId) return '';

  if (source.includes('naver')) {
    return `https://comic.naver.com/webtoon/list?titleId=${encodeURIComponent(
      contentId
    )}`;
  }

  if (source.includes('kakao')) {
    if (!title) return '';
    return `https://webtoon.kakao.com/content/${encodeURIComponent(
      title
    )}/${encodeURIComponent(contentId)}`;
  }

  return '';
}

function openSubscribeModal(content) {
  STATE.currentModalContent = content;
  const titleEl = document.getElementById('modalWebtoonTitle');
  const modalEl = document.getElementById('subscribeModal');
  const linkContainer = document.getElementById('modalWebtoonLinkContainer');
  closeProfileMenu();

  const titleText = String(content?.title || '').trim();
  const url = getContentUrl(content);

  if (!titleEl && window.ES_DEBUG) {
    console.error('[subscribeModal] title element missing');
  }

  if (window.ES_DEBUG) {
    console.debug('[subscribeModal] url=', url, 'title=', titleText);
  }

  if (titleEl) {
    while (titleEl.firstChild) titleEl.removeChild(titleEl.firstChild);

    if (url && titleText) {
      const anchor = document.createElement('a');
      anchor.textContent = titleText;
      anchor.href = url;
      anchor.target = '_blank';
      anchor.rel = 'noopener noreferrer';
      anchor.className =
        'inline-block text-blue-500 underline underline-offset-2 hover:text-blue-300 cursor-pointer focus-visible:underline focus-visible:outline-none pointer-events-auto';
      titleEl.appendChild(anchor);
    } else {
      titleEl.textContent = titleText;
    }
  } else if (modalEl) {
    const fallbackEl = modalEl.querySelector('p');
    if (fallbackEl) fallbackEl.textContent = titleText;
  }

  if (linkContainer) {
    while (linkContainer.firstChild) linkContainer.removeChild(linkContainer.firstChild);
    linkContainer.classList.add('hidden');
  }
  if (modalEl) {
    openModal(modalEl, {
      initialFocusEl: document.getElementById('subscribeButton') || modalEl,
    });
  }
  syncModalButton();
}

function closeSubscribeModal() {
  const modalEl = document.getElementById('subscribeModal');
  if (modalEl) closeModal(modalEl);
  STATE.currentModalContent = null;
}

window.toggleSubscriptionFromModal = async function () {
  const content = STATE.currentModalContent;
  if (!content) return;
  if (!requireAuthOrPrompt('subscription-toggle-modal')) return;

  const key = buildSubscriptionKey(content);
  const btn = document.getElementById('subscribeButton');
  if (key && STATE.pendingSubOps.has(key)) return;
  if (key) STATE.pendingSubOps.add(key);
  if (btn) {
    btn.disabled = true;
    btn.classList.add('opacity-80', 'cursor-not-allowed');
  }

  const currently = isSubscribed(content);
  try {
    if (currently) await unsubscribeContent(content);
    else await subscribeContent(content);

    syncModalButton();

    if (STATE.activeTab === 'my') {
      fetchAndRenderContent('my');
    }
  } catch (e) {
    // errors already toasted in subscribe/unsubscribe
  } finally {
    if (key) STATE.pendingSubOps.delete(key);
    if (btn) {
      btn.disabled = false;
      btn.classList.remove('opacity-80', 'cursor-not-allowed');
    }
  }
};

/* =========================
   Series sort (minimal, optional)
   ========================= */

function setupSeriesSortHandlers() {
  if (!UI.seriesSort) return;

  UI.seriesSort.addEventListener('click', (evt) => {
    const btn = evt.target?.closest?.('[data-sort]');
    if (!btn) return;

    const sort = btn.getAttribute('data-sort');
    if (!sort) return;

    STATE.filters.series.sort = sort;
    fetchAndRenderContent('series');
  });
}

/* =========================
   Expose required globals
   ========================= */

if (DEBUG_TOOLS) {
  window.__es = {
    state: STATE,
    loadSubscriptions: () => loadSubscriptions({ force: true }),
    setToken: (t) => localStorage.setItem('es_access_token', t),
    clearToken: () => localStorage.removeItem('es_access_token'),
  };
}

window.updateMySubTab = updateMySubTab;
window.closeSubscribeModal = closeSubscribeModal;

// Quick sanity test steps (manual):
// 1) localStorage.setItem('es_access_token', '<token>')
// 2) Reload and open the "My Sub" tab
// 3) Open a card to trigger the subscribe modal and use the modal button to watch POST/DELETE /api/me/subscriptions
