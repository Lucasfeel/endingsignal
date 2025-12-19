/* static/app.js
   CP2 integrated: apiRequest + normalizeMeta + conflict-free merge
   Notes:
   - No backend changes
   - Keeps existing UI behavior
*/

const DEBUG_API = false;
function debugLog(...args) {
  if (DEBUG_API) console.log(...args);
}

const ICONS = {
  webtoon: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M21 4H3C1.9 4 1 4.9 1 6v13c0 1.1.9 2 2 2h18c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zM3 19V6h8v13H3zm18 0h-8V6h8v13z"/></svg>`, // Open Book approximation
  novel: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M7.127 22.562l-7.127 1.438 1.438-7.128 5.689 5.69zm1.414-1.414l11.228-11.225-5.69-5.692-11.227 11.227 5.689 5.69zm9.768-21.148l-2.816 2.817 5.691 5.691 2.816-2.819-5.691-5.689z"/></svg>`, // Feather Pen approximation
  ott: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 14.5v-9l6 4.5-6 4.5z"/></svg>`, // Play Circle
  series: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M21 3H3c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h5v2h8v-2h5c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 14H3V5h18v12z"/></svg>`, // TV
  my: `<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 24 24"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>`, // Star
};

const STATE = {
  activeTab: 'webtoon',
  filters: {
    webtoon: { source: 'all', day: 'mon' },
    novel: { source: 'all', day: 'mon' },
    ott: { source: 'all', genre: 'drama' },
    series: { sort: 'latest' },
    my: { viewMode: 'subscribing' },
  },
  contents: {},
  isLoading: false,
  currentModalContent: null,
  subscriptionsSet: new Set(),
  mySubscriptions: [],
  subscriptionsLoadedAt: null,
};

const UI = {
  bottomNav: document.getElementById('bottomNav'),
  contentGrid: document.getElementById('contentGridContainer'),
  l1Filter: document.getElementById('l1FilterContainer'),
  l2Filter: document.getElementById('l2FilterContainer'),
  filtersWrapper: document.getElementById('filtersWrapper'),
  mySubToggle: document.getElementById('mySubToggleContainer'),
  seriesSort: document.getElementById('seriesSortOptions'),
  seriesFooter: document.getElementById('seriesFooterButton'),
  toggleIndicator: document.getElementById('toggleIndicator'),
  header: document.getElementById('mainHeader'),
};

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

const requireAuthOrPrompt = (_actionName) => {
  const token = getAccessToken();
  if (!token) {
    alert('로그인이 필요합니다.');
    return false;
  }
  return true;
};

/* =========================
   CP2: API Contract Baseline
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

    if (!handled) {
      try {
        const text = await response.clone().text();
        if (text) {
          message = text.slice(0, 300);
        }
      } catch {
        // ignore text fallback failures
      }
    }

    return { httpStatus: response.status, code, message };
  };

  if (!response.ok) {
    throw await buildError();
  }

  if (isJsonResponse(response)) {
    try {
      return await response.json();
    } catch {
      return null;
    }
  }

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
   Subscription helpers/state
   ========================= */

const buildSubscriptionKey = (content) => {
  if (!content) return '';
  const source = content.source || content?.meta?.source || '';
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

  try {
    const res = await apiRequest('GET', '/api/me/subscriptions', { token });
    if (!res || res.success !== true || !Array.isArray(res.data)) {
      throw new Error('구독 정보를 불러오지 못했습니다.');
    }

    const normalized = res.data.map((item) => {
      const finalState =
        item?.final_state && typeof item.final_state === 'object'
          ? item.final_state
          : {};
      return {
        ...item,
        meta: normalizeMeta(item?.meta),
        final_state: finalState,
      };
    });

    const nextSet = new Set();
    normalized.forEach((item) => {
      const key = buildSubscriptionKey(item);
      if (key) nextSet.add(key);
    });

    STATE.subscriptionsSet = nextSet;
    STATE.mySubscriptions = normalized;
    STATE.subscriptionsLoadedAt = Date.now();
    return normalized;
  } catch (e) {
    alert(e?.message || '구독 정보를 불러오지 못했습니다.');
    STATE.subscriptionsSet = new Set();
    STATE.mySubscriptions = [];
    STATE.subscriptionsLoadedAt = null;
    return [];
  }
}

async function subscribeContent(content) {
  const token = getAccessToken();
  if (!token) {
    alert('로그인이 필요합니다.');
    return;
  }

  const contentId = content?.content_id || content?.contentId || content?.id;
  const source = content?.source;
  const key = buildSubscriptionKey({ ...content, content_id: contentId, source });
  if (!contentId || !source) {
    alert('콘텐츠 정보가 없습니다.');
    return;
  }

  try {
    await apiRequest('POST', '/api/me/subscriptions', {
      body: { content_id: contentId, contentId, source },
      token,
    });
    if (key) STATE.subscriptionsSet.add(key);
    await loadSubscriptions({ force: true });
  } catch (e) {
    if (key) STATE.subscriptionsSet.delete(key);
    alert(e?.message || '구독에 실패했습니다.');
    throw e;
  }
}

async function unsubscribeContent(content) {
  const token = getAccessToken();
  if (!token) {
    alert('로그인이 필요합니다.');
    return;
  }

  const contentId = content?.content_id || content?.contentId || content?.id;
  const source = content?.source;
  const key = buildSubscriptionKey({ ...content, content_id: contentId, source });
  if (!contentId || !source) {
    alert('콘텐츠 정보가 없습니다.');
    return;
  }

  try {
    await apiRequest('DELETE', '/api/me/subscriptions', {
      body: { content_id: contentId, contentId, source },
      token,
    });
    if (key) STATE.subscriptionsSet.delete(key);
    await loadSubscriptions({ force: true });
  } catch (e) {
    if (key) STATE.subscriptionsSet.add(key);
    alert(e?.message || '구독 해제에 실패했습니다.');
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
  try {
    await loadSubscriptions();
  } catch (e) {
    console.warn('Failed to preload subscriptions', e);
  }

  renderBottomNav();
  updateTab('webtoon'); // Initial Load
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
      <span class="text-[10px] ${isActive ? 'font-bold' : 'font-medium'}">${
      tab.label
    }</span>
    `;
    btn.onclick = () => updateTab(tab.id);
    UI.bottomNav.appendChild(btn);
  });
}

async function updateTab(tabId) {
  STATE.activeTab = tabId;

  renderBottomNav();
  updateFilterVisibility(tabId);
  renderL1Filters(tabId);
  renderL2Filters(tabId);

  await fetchAndRenderContent(tabId);

  window.scrollTo({ top: 0 });
}

function updateFilterVisibility(tabId) {
  if (!UI.l1Filter || !UI.l2Filter || !UI.mySubToggle || !UI.seriesSort || !UI.seriesFooter) return;

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
      { id: 'lezhin', label: 'L', color: '#E62E2E' }, // may return empty from backend
      { id: 'laftel', label: 'R', color: '#6C5CE7' }, // may return empty from backend
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

  // Added 'hiatus' to match existing fetch branch (safe; backend supports /hiatus)
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
      if (tabId === 'webtoon' || tabId === 'novel') STATE.filters[tabId].day = item.id;
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

async function fetchAndRenderContent(tabId) {
  if (!UI.contentGrid) return;

  UI.contentGrid.innerHTML = '';
  STATE.isLoading = true;

  let aspectClass = 'aspect-[3/4]';
  if (tabId === 'novel') aspectClass = 'aspect-[1/1.4]';
  if (tabId === 'ott') aspectClass = 'aspect-[2/3]';

  // Skeletons (match aspect for nicer feel)
  for (let i = 0; i < 9; i++) {
    const skel = document.createElement('div');
    skel.className = `${aspectClass} rounded-lg skeleton`;
    UI.contentGrid.appendChild(skel);
  }

  let data = [];

  try {
    if (tabId === 'my') {
      const token = getAccessToken();
      if (!token) {
        STATE.isLoading = false;
        UI.contentGrid.innerHTML =
          '<div class="col-span-3 text-center text-gray-400 py-10 text-sm flex flex-col items-center gap-3"><p>로그인이 필요합니다.</p><button class="px-4 py-2 rounded-lg bg-[#4f46e5] text-white text-xs font-bold" onclick="alert(\'로그인이 필요합니다.\')">로그인하기</button></div>';
        return;
      }

      const subs = await loadSubscriptions();
      const mode = STATE.filters?.my?.viewMode || 'subscribing';

      data = (subs || []).filter((item) => {
        const finalState = item?.final_state || {};
        const isScheduled = finalState?.is_scheduled_completion === true;
        const isCompleted = finalState?.final_status === '완결' && !isScheduled;
        if (mode === 'completed') return isCompleted;
        return !isCompleted;
      });
    } else {
      let url = '';
      let query = {};

      if (tabId === 'webtoon' || tabId === 'novel') {
        const day = STATE.filters[tabId].day;
        const source = STATE.filters[tabId].source;
        query = { type: tabId, source };

        if (day === 'completed') {
          url = buildUrl('/api/contents/completed', query);
        } else if (day === 'hiatus') {
          url = buildUrl('/api/contents/hiatus', query);
        } else {
          url = buildUrl('/api/contents/ongoing', query);
        }
      }

      // Simulated delay for effect
      await new Promise((r) => setTimeout(r, 300));

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
            // completed/hiatus endpoints: { contents: [...], next_cursor: ... }
            data = Array.isArray(json?.contents) ? json.contents : [];
          }
        }
      } else {
        // Mock for Series / OTT as backend endpoints might be missing
        data = [
          { title: 'Mock Item 1', meta: { common: { thumbnail_url: null, authors: [] } } },
          { title: 'Mock Item 2', meta: { common: { thumbnail_url: null, authors: [] } } },
          { title: 'Mock Item 3', meta: { common: { thumbnail_url: null, authors: [] } } },
        ];
      }
    }

    // Normalize meta for safety
    data = Array.isArray(data)
      ? data.map((item) => ({ ...item, meta: normalizeMeta(item?.meta) }))
      : [];
  } catch (e) {
    console.error('Fetch error', e);
  }

  STATE.isLoading = false;
  UI.contentGrid.innerHTML = '';

  if (!data.length) {
    UI.contentGrid.innerHTML =
      '<div class="col-span-3 text-center text-gray-500 py-10 text-xs">콘텐츠가 없습니다.</div>';
    return;
  }

  data.forEach((item) => {
    const card = createCard(item, tabId, aspectClass);
    UI.contentGrid.appendChild(card);
  });
}

function createCard(content, tabId, aspectClass) {
  const el = document.createElement('div');
  el.className = 'relative group cursor-pointer fade-in';

  const meta = normalizeMeta(content?.meta);
  const thumb =
    meta?.common?.thumbnail_url ||
    'https://via.placeholder.com/300x400/333/999?text=No+Img';
  const authors = Array.isArray(meta?.common?.authors)
    ? meta.common.authors.join(', ')
    : '';

  const cardContainer = document.createElement('div');
  cardContainer.className = `${aspectClass} rounded-lg overflow-hidden bg-[#1E1E1E] relative mb-2`;

  const imgEl = document.createElement('img');
  imgEl.src = thumb;
  imgEl.className =
    'w-full h-full object-cover group-hover:scale-105 transition-transform duration-300';
  cardContainer.appendChild(imgEl);

  // Badge Logic
  if (tabId === 'my') {
    const finalState = content?.final_state || {};
    const isScheduled = finalState?.is_scheduled_completion === true;
    const isCompleted = finalState?.final_status === '완결';
    const isHiatus =
      finalState?.final_status === '휴재' || content?.status === '휴재';

    const badgeEl = document.createElement('div');
    badgeEl.className =
      'absolute top-0 left-0 backdrop-blur-md px-2 py-1 rounded-br-lg z-10 flex items-center gap-0.5';

    if (isScheduled) {
      badgeEl.className += ' bg-yellow-500/80';
      badgeEl.innerHTML = `<span class="text-[10px] font-black text-black leading-none">완결 예정</span><span class="text-[10px] text-black leading-none">${formatDateKST(finalState?.scheduled_completed_at)}</span>`;
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

  const starButton = document.createElement('button');
  starButton.type = 'button';
  starButton.className =
    'absolute top-[6px] right-[6px] h-[26px] px-2 rounded-[12px] backdrop-blur-[4px] flex items-center gap-1 z-20 text-xs font-bold transition-colors';

  const setStarVisual = (on) => {
    starButton.innerHTML = on
      ? '<span class="text-[12px]">★</span><span class="text-[10px]">구독중</span>'
      : '<span class="text-[12px]">☆</span><span class="text-[10px]">구독</span>';
    starButton.className =
      'absolute top-[6px] right-[6px] h-[26px] px-2 rounded-[12px] backdrop-blur-[4px] flex items-center gap-1 z-20 text-xs font-bold transition-colors ' +
      (on ? 'bg-black/60 text-[#4F46E5]' : 'bg-black/40 text-gray-400');
  };

  setStarVisual(isSubscribed(content));

  starButton.onclick = async (evt) => {
    evt.stopPropagation();
    if (!requireAuthOrPrompt()) return;

    const key = buildSubscriptionKey(content);
    if (!key) {
      alert('콘텐츠 정보가 없습니다.');
      return;
    }
    const currentlySubscribed = isSubscribed(content);
    const nextState = !currentlySubscribed;

    setStarVisual(nextState);
    if (nextState) STATE.subscriptionsSet.add(key);
    else STATE.subscriptionsSet.delete(key);

    try {
      if (nextState) {
        await subscribeContent(content);
      } else {
        await unsubscribeContent(content);
      }
      if (STATE.activeTab === 'my') {
        fetchAndRenderContent('my');
      }
    } catch (e) {
      if (key) {
        if (currentlySubscribed) STATE.subscriptionsSet.add(key);
        else STATE.subscriptionsSet.delete(key);
      }
      setStarVisual(currentlySubscribed);
    }
  };

  cardContainer.appendChild(starButton);

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

  el.onclick = () => openModal(content);
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

function openModal(content) {
  STATE.currentModalContent = content;
  const titleEl = document.getElementById('modalWebtoonTitle');
  const modalEl = document.getElementById('subscribeModal');

  if (titleEl) titleEl.textContent = content.title || '';
  if (modalEl) modalEl.classList.remove('hidden');
  syncModalButton();
}

function closeModal() {
  const modalEl = document.getElementById('subscribeModal');
  if (modalEl) modalEl.classList.add('hidden');
  STATE.currentModalContent = null;
}

window.toggleSubscriptionFromModal = async function () {
  const content = STATE.currentModalContent;
  if (!content) return;
  if (!requireAuthOrPrompt()) return;

  const currently = isSubscribed(content);
  try {
    if (currently) {
      await unsubscribeContent(content);
    } else {
      await subscribeContent(content);
    }
    syncModalButton();
    alert(currently ? '구독이 해제되었습니다.' : '구독이 설정되었습니다.');
    if (STATE.activeTab === 'my') {
      fetchAndRenderContent('my');
    }
  } catch (e) {
    // errors already handled in subscribe/unsubscribe
  }
};

/* =========================
   Series sort (minimal, optional)
   ========================= */

function setupSeriesSortHandlers() {
  // This is intentionally minimal; keep behavior stable.
  // If your HTML has buttons inside #seriesSortOptions with data-sort attributes,
  // this will toggle STATE.filters.series.sort and refresh.
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

window.updateMySubTab = updateMySubTab;
window.closeModal = closeModal;

// Quick sanity test steps (manual):
// 1) localStorage.setItem('es_access_token', '<token>')
// 2) Open the "My Sub" tab
// 3) Toggle the star on content cards to confirm subscription changes
