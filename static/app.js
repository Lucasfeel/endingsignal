/* static/app.js
   CP2 + CP2.1 + CP4 + CP4.1 integrated (conflict-free)
   - apiRequest + normalizeMeta
   - authenticated subscriptions via /api/me/subscriptions
   - My Sub uses real data + final_state (scheduled completion supported)
   - toast-based UX (no alert) + hardened schema parsing
   - no backend changes
*/

/* =========================
   Constants & UI tokens
   ========================= */

const DEBUG_API = false;
const DEBUG_TOOLS = false;
const DEBUG_RUNTIME = typeof window !== 'undefined' && Boolean(window.ES_DEBUG);
const USE_BROWSE_PAGINATION_V2 = true;
const PAGE_SIZE = 80;
const THEME_STORAGE_KEY = 'es_theme';
const DARK_MODE_MEDIA_QUERY = '(prefers-color-scheme: dark)';

function debugLog(...args) {
  if (DEBUG_API) console.log(...args);
}

let fatalBannerVisible = false;
function showFatalBanner(message) {
  if (fatalBannerVisible) return;
  fatalBannerVisible = true;
  const banner = document.createElement('div');
  banner.textContent =
    message || 'UI 초기화 중 오류가 발생했습니다. 새로고침 후에도 계속되면 관리자에게 문의하세요.';
  banner.style.position = 'fixed';
  banner.style.top = '0';
  banner.style.left = '0';
  banner.style.right = '0';
  banner.style.padding = '12px 16px';
  banner.style.background = '#b91c1c';
  banner.style.color = '#fff';
  banner.style.fontSize = '14px';
  banner.style.fontWeight = '700';
  banner.style.zIndex = '2000';
  banner.style.textAlign = 'center';
  (document.body || document.documentElement || document).appendChild(banner);
}

const ICONS = {
  home: `<svg class="w-6 h-6" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M10.9 2.8a1.6 1.6 0 0 1 2.2 0l8.1 7.3a1 1 0 0 1-.7 1.7h-.8v7.1a2 2 0 0 1-2 2h-3.7a1 1 0 0 1-1-1v-4.6h-2v4.6a1 1 0 0 1-1 1H6.3a2 2 0 0 1-2-2v-7.1h-.8a1 1 0 0 1-.7-1.7z"/></svg>`,
  webtoon: `<span aria-hidden="true" style="display:block;width:24px;height:24px;background-color:currentColor;-webkit-mask:url('/static/webtoon_bubble_24_currentColor.svg') center/contain no-repeat;mask:url('/static/webtoon_bubble_24_currentColor.svg') center/contain no-repeat;"></span>`,
  novel: `<span aria-hidden="true" style="display:block;width:24px;height:24px;background-color:currentColor;-webkit-mask:url('/static/webnovel_leaf_24_white.svg') center/contain no-repeat;mask:url('/static/webnovel_leaf_24_white.svg') center/contain no-repeat;"></span>`,
  ott: `<span aria-hidden="true" style="display:block;width:24px;height:24px;background-color:currentColor;-webkit-mask:url('/static/ott_youtube_like_filled.svg') center/145% auto no-repeat;mask:url('/static/ott_youtube_like_filled.svg') center/145% auto no-repeat;"></span>`,
  my: `<svg class="w-6 h-6" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 3.2a1.1 1.1 0 0 1 1.01.67l1.95 4.27 4.65.55a1.1 1.1 0 0 1 .62 1.92l-3.44 3.1.94 4.58a1.1 1.1 0 0 1-1.62 1.17L12 17.5l-4.1 2.35a1.1 1.1 0 0 1-1.63-1.16l.94-4.58-3.44-3.1a1.1 1.1 0 0 1 .62-1.92l4.66-.55 1.94-4.27A1.1 1.1 0 0 1 12 3.2z"/></svg>`,
  me: `<svg class="w-6 h-6" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 3.2a4.6 4.6 0 1 1 0 9.2 4.6 4.6 0 0 1 0-9.2m0 10.9c4.8 0 8.8 2.7 9.7 6.5a.9.9 0 0 1-.9 1.2H3.2a.9.9 0 0 1-.9-1.2c.9-3.8 4.9-6.5 9.7-6.5"/></svg>`,
};

// UI_CLASSES: Tailwind class tokens for reusable UI primitives.
// - Prefer tokens over ad-hoc class strings to keep styling consistent.
// - Keep layout/positioning (fixed/inset/z) in HTML; tokens cover visual styling only.
// How to extend:
// - Add a new token here with a concise, semantic name.
// - If used in static HTML, annotate the element with data-ui and add a map entry below.
// - For JS-only elements, add a DATA_UI_CLASS_MAP entry with a "dynamic-only" note.
// - Avoid mixing layout utilities into tokens; keep spacing/positioning close to markup.
// - Smoke test search page, modals, cards, and toasts after changes.
const UI_CLASSES = {
  // Buttons
  btnPrimary:
    'es-btn es-btn-primary h-10 px-4 rounded-xl text-sm font-semibold disabled:opacity-50 disabled:cursor-not-allowed',
  btnSecondary:
    'es-btn es-btn-secondary h-10 px-4 rounded-xl text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed',
  btnSolid:
    'es-btn es-btn-solid h-10 px-4 rounded-xl text-sm font-semibold disabled:opacity-50 disabled:cursor-not-allowed',
  btnDisabled: 'opacity-60 cursor-not-allowed',

  // Icon buttons
  iconBtn: 'es-icon-btn h-10 w-10 flex items-center justify-center rounded-xl',
  iconBtnSm: 'es-icon-btn h-8 w-8 flex items-center justify-center rounded-lg',
  headerSearchIcon:
    'es-icon-btn h-10 w-10 flex items-center justify-center rounded-xl transition-colors',
  headerProfileIcon:
    'es-icon-btn h-10 w-10 flex items-center justify-center rounded-xl transition-colors',

  // Chips & empty states
  chip: 'es-chip h-9 px-3 inline-flex items-center rounded-full text-sm',
  emptyWrap: 'py-12 px-4 flex flex-col items-center justify-center text-center',
  emptyTitle: 'text-lg font-semibold es-text',
  emptyMsg: 'mt-2 text-sm es-muted max-w-md',

  // Typography helpers
  sectionTitle: 'text-base font-semibold es-text',
  sectionSubtle: 'text-sm es-muted transition-colors',

  // Card overlays/badges
  starBadge:
    'es-star-badge absolute top-2 right-2 z-10 flex items-center justify-center h-[24px] px-2 rounded-full text-xs font-semibold pointer-events-none select-none',
  badgeBase: 'es-badge-base z-10 inline-flex px-2 py-1 rounded-lg items-center',
  affordOverlay:
    'absolute inset-0 z-[5] pointer-events-none opacity-0 transition-opacity duration-150 es-afford-overlay group-hover:opacity-100',
  affordHint:
    'absolute bottom-2 left-2 z-[6] pointer-events-none select-none opacity-0 transition-opacity duration-150 group-hover:opacity-100',
  pillHint: 'es-pill-hint text-[11px] rounded-full px-2 py-1',

  // Cards
  cardRoot:
    'es-card-root relative cursor-pointer fade-in focus-visible:outline-none',
  cardThumb: 'es-card-thumb overflow-hidden relative',
  cardBadgeRow: 'es-card-badge-row',
  cardImage: 'w-full h-full object-cover',
  cardGradient: 'absolute inset-0 es-card-gradient opacity-60',
  thumbStack: 'thumbStack kakaoStack',
  thumbBg: 'thumbBg',
  thumbChar: 'thumbChar',
  thumbTitle: 'thumbTitle',
  cardTextWrap: 'es-card-text',
  cardTitle: 'es-card-title font-semibold text-[13px] leading-[1.35]',
  cardMeta: 'es-card-meta text-[11px] mt-1',

  // Inputs
  inputBase:
    'es-input-base w-full h-10 rounded-xl px-4 pr-10 outline-none text-base',
  inputSm:
    'es-input-sm w-full px-3 py-2 rounded-xl text-sm focus:outline-none',
  searchTrigger:
    'es-input-base transition-all duration-200 rounded-xl px-3 py-2 text-sm focus:outline-none',
  inputLabel: 'block text-sm font-medium es-muted',

  // Modal
  modalWrap: 'flex items-center justify-center',
  modalCard:
    'es-modal-card relative z-10 p-6 rounded-2xl w-[90%] max-w-sm mx-auto shadow-2xl transform transition-all',
  modalTitle: 'text-xl font-semibold mb-1 es-text tracking-[-0.02em]',
  modalBodyText: 'es-muted text-sm',

  // Layout grids
  grid2to3: 'grid grid-cols-3 gap-2 items-start content-start',

  // Pages & overlays
  pageOverlayRoot: 'es-page-overlay-root',
  pageOverlayContainer: 'mx-auto h-full max-w-[520px] px-4',
  pageCard: 'es-page-card rounded-2xl p-4 shadow-sm',

  // Menus
  menuWrap: 'es-menu-wrap rounded-xl shadow-md overflow-hidden py-2',
  menuItem:
    'es-menu-item w-full text-left px-4 py-3 text-sm focus:outline-none',
  menuItemDanger:
    'es-menu-item es-menu-item-danger w-full text-left px-4 py-3 text-sm focus:outline-none',

  // Pagination controls
  loadMoreBtn:
    'es-load-more w-full h-[44px] rounded-xl text-[13px] font-semibold transition-colors',

  // Toasts
  toastWrap: 'pointer-events-none w-full text-center transition-all duration-300 opacity-0 -translate-y-2',
  toastInfo: 'es-toast inline-flex px-4 py-2 rounded-xl text-sm',
  toastSuccess: 'es-toast es-toast-success inline-flex px-4 py-2 rounded-xl text-sm',
  toastError: 'es-toast es-toast-error inline-flex px-4 py-2 rounded-xl text-sm',
};

const FALLBACK_THUMB = `data:image/svg+xml;utf8,${encodeURIComponent(
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 400" preserveAspectRatio="xMidYMid slice"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#f2f4f6"/><stop offset="100%" stop-color="#e5e8eb"/></linearGradient></defs><rect width="300" height="400" fill="url(#g)"/><rect x="32" y="48" width="236" height="304" rx="20" fill="rgba(255,255,255,0.9)" stroke="rgba(209,214,219,0.95)" stroke-width="2"/><path d="M90 270l42-54 38 42 32-28 44 58H90z" fill="rgba(139,149,161,0.35)"/><circle cx="124" cy="144" r="24" fill="rgba(139,149,161,0.24)"/><text x="150" y="330" text-anchor="middle" fill="#6b7684" font-family="system-ui, sans-serif" font-size="20" font-weight="600">No Poster</text></svg>'
)}`;

/* =========================
   UI state persistence (filters + scroll)
   ========================= */

// Storage helpers: JSON + schema versioning with defensive guards so blocked storage
// (e.g., private mode) does not break the UI.
const UI_STATE_KEYS = {
  filters: {
    sources: 'endingsignal.filters.sources', // localStorage: keep across reloads
    source: 'endingsignal.filters.source', // legacy single-source key
    status: 'endingsignal.filters.status', // localStorage: keep across reloads
    day: 'endingsignal.filters.day', // sessionStorage: reset on new session
    novelGenreGroup: 'endingsignal.filters.novel.genreGroup',
    novelIsCompleted: 'endingsignal.filters.novel.isCompleted',
  },
  scroll: {
    home: 'endingsignal.scroll.home',
    webtoon: 'endingsignal.scroll.webtoon',
    novel: 'endingsignal.scroll.novel',
    ott: 'endingsignal.scroll.ott',
    mysub: 'endingsignal.scroll.mysub',
    search: 'endingsignal.scroll.search',
  },
};

const UI_STATE_DEFAULTS = {
  filters: {
    sources: [],
    status: 'ongoing',
    day: 'all', // Day defaults to ALL on a fresh visit per product decision
  },
};

const DEFAULT_NOVEL_GENRE_GROUP = 'all';
const DEFAULT_NOVEL_IS_COMPLETED = false;
const NOVEL_GENRE_GROUP_OPTIONS = [
  { id: 'all', label: '\uC804\uCCB4' },
  { id: 'fantasy', label: '\uD310\uD0C0\uC9C0' },
  { id: 'romance', label: '\uB85C\uB9E8\uC2A4' },
  { id: 'romance_fantasy', label: '\uB85C\uD310' },
  { id: 'light_novel', label: '\uB77C\uC774\uD2B8\uB178\uBCA8' },
  { id: 'wuxia', label: '\uBB34\uD611' },
  { id: 'bl', label: 'BL' },
];
const NOVEL_GENRE_GROUP_IDS = NOVEL_GENRE_GROUP_OPTIONS.map((item) => item.id);

const SOURCE_ID_ALIASES = {
  disney: 'disney_plus',
  disneyplus: 'disney_plus',
  'disney+': 'disney_plus',
  disney_plus: 'disney_plus',
  coupang_play: 'coupangplay',
  kakao_webtoon: 'kakaowebtoon',
};

const normalizeSourceId = (sourceId) => {
  const safeSourceId = String(sourceId || '')
    .trim()
    .toLowerCase();
  if (!safeSourceId) return '';
  return SOURCE_ID_ALIASES[safeSourceId] || safeSourceId;
};

const toApiSourceId = (sourceId) => {
  const normalized = normalizeSourceId(sourceId);
  if (!normalized) return '';
  return normalized;
};

const SOURCE_OPTIONS = {
  webtoon: [
    { id: 'naver_webtoon', label: 'Naver' },
    { id: 'kakaowebtoon', label: 'Kakao' },
  ],
  novel: [
    { id: 'naver_series', label: 'Naver' },
    { id: 'kakao_page', label: 'KakaoPage' },
    { id: 'ridi', label: 'RIDI' },
  ],
  ott: [
    { id: 'netflix', label: 'Netflix' },
    { id: 'tving', label: 'TVING' },
    { id: 'wavve', label: 'wavve' },
    { id: 'coupangplay', label: 'Coupang Play' },
    { id: 'disney_plus', label: 'Disney+' },
    { id: 'laftel', label: 'Laftel' },
  ],
};

const SOURCE_BRAND_META = {
  naver_webtoon: {
    bg: '#00DC64',
    border: 'rgba(0,0,0,0.06)',
    logoColor: '#111111',
  },
  kakaowebtoon: {
    bg: '#FFD400',
    border: 'rgba(0,0,0,0.08)',
    logoColor: '#111111',
  },
  naver_series: {
    bg: '#03C75A',
    border: 'rgba(0,0,0,0.06)',
    logoColor: '#111111',
  },
  kakao_page: {
    bg: '#FEE102',
    border: 'rgba(0,0,0,0.08)',
    logoColor: '#111111',
  },
  munpia: {
    bg: '#2F80FF',
    border: 'rgba(255,255,255,0.25)',
    logoColor: '#FFFFFF',
  },
  ridi: {
    bg: '#1E9EFF',
    border: 'rgba(255,255,255,0.25)',
    logoColor: '#FFFFFF',
  },
  netflix: {
    bg: '#FFFFFF',
    border: '#E5E8EB',
  },
  tving: {
    bg: '#F6F6F6',
    border: '#E5E8EB',
  },
  wavve: {
    bg: 'linear-gradient(135deg, #5DD0FF 0%, #B4EFFF 100%)',
    border: 'rgba(255,255,255,0.25)',
    logoColor: '#FFFFFF',
  },
  coupangplay: {
    bg: '#FFFFFF',
    border: '#E5E8EB',
  },
  disney_plus: {
    bg: '#01147C',
    border: 'rgba(255,255,255,0.18)',
    logoColor: '#FFFFFF',
  },
  laftel: {
    bg: '#AFA3ED',
    border: 'rgba(255,255,255,0.20)',
    logoColor: '#111111',
  },
  watcha: {
    bg: '#FF0558',
    border: 'rgba(255,255,255,0.18)',
    logoColor: '#FFFFFF',
  },
};

const SOURCE_LOGO_ASSETS = {
  naver_webtoon: '/static/source_logos/naver_webtoon.png',
  kakaowebtoon: '/static/source_logos/kakaowebtoon.jpg',
  naver_series: '/static/source_logos/naver_series.png',
  kakao_page: '/static/source_logos/kakao_page.jpeg',
  ridi: '/static/source_logos/ridi.jpeg',
  netflix: '/static/source_logos/netflix.jpeg',
  tving: '/static/source_logos/tving.png',
  coupangplay: '/static/source_logos/coupangplay.png',
  disney_plus: '/static/source_logos/disney_plus.jpeg',
  laftel: '/static/source_logos/laftel.png',
};

const SOURCE_ICON_SVGS = {
  naver_webtoon:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#00c73c"/><path d="M9 20V8h2.3l4.4 7.1V8H18v12h-2.2l-4.5-7.2V20H9z" fill="#fff"/></svg>',
  kakaowebtoon:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#ffd400"/><path d="M8.7 20V8h2.8v4.7L15.9 8H19l-4.3 5.4L19.1 20h-3.2l-3.1-4.6-1.3 1.6V20H8.7z" fill="#111"/></svg>',
  naver_series:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#0ea85a"/><path d="M8.4 20V8h2.2l5 7V8h2.2v12h-2.2L10.6 13v7H8.4z" fill="#fff"/></svg>',
  kakao_page:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#f7e700"/><path d="M8.8 20V8h2.5v5.1L16 8h3.1l-4.8 5.4 5.1 6.6H16L12.4 15l-1.1 1.3V20H8.8z" fill="#111"/></svg>',
  ridi: '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#1d4ed8"/><path d="M9 20V8h5c2.6 0 4 1.4 4 3.6 0 1.8-.9 2.9-2.5 3.3l2.8 5.1h-2.7l-2.5-4.7h-1.7V20H9zm2.4-6.6h2.4c1.1 0 1.7-.6 1.7-1.6 0-1-.6-1.6-1.8-1.6h-2.3v3.2z" fill="#fff"/></svg>',
  munpia:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#6b7280"/><path d="M8 20V8h2.3l3.7 6.1L17.6 8H20v12h-2.2v-8l-3.5 5.7h-.6L10.2 12v8H8z" fill="#fff"/></svg>',
  netflix:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#111"/><path d="M9 20V8h2.6l4.2 7.2V8H18v12h-2.4l-4.4-7.5V20H9z" fill="#e50914"/></svg>',
  disney_plus:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#113b7a"/><path d="M8.2 14.5c1.9-3.4 8.4-3.3 10.5.2m-9.8 2.8h6.2c1.9 0 3-.9 3-2.5s-1.1-2.5-3-2.5h-1.3" stroke="#fff" stroke-width="1.7" stroke-linecap="round"/></svg>',
  disney:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#113b7a"/><path d="M8.2 14.5c1.9-3.4 8.4-3.3 10.5.2m-9.8 2.8h6.2c1.9 0 3-.9 3-2.5s-1.1-2.5-3-2.5h-1.3" stroke="#fff" stroke-width="1.7" stroke-linecap="round"/></svg>',
  tving:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#e11d48"/><path d="M8.4 10h11.2v2.2h-4.4V20h-2.4v-7.8H8.4V10z" fill="#fff"/></svg>',
  watcha:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#fb7185"/><path d="M8.3 8h2.6l2.5 8.1L15.9 8h2.5l2.4 12h-2.3l-1.2-7.3-2.2 7.3h-1.7l-2.2-7.3L10 20H7.7L8.3 8z" fill="#fff"/></svg>',
  wavve:
    '<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#0f172a"/><path d="M7.8 9h2.4l1.5 6 1.7-6h2.1l1.7 6 1.5-6h2.4l-3 11h-1.9l-1.8-6-1.8 6h-1.9L7.8 9z" fill="#93c5fd"/></svg>',
};

function getSourceTextFallbackMarkup(sourceId, fallbackLabel) {
  const normalizedSourceId = normalizeSourceId(sourceId);
  const known = SOURCE_ICON_SVGS[normalizedSourceId];
  if (known) return known;
  const safeInitial = String(fallbackLabel || normalizedSourceId || '?')
    .toUpperCase()
    .replace(/[^A-Z0-9+]/g, '')
    .slice(0, 2);
  const text = safeInitial || '?';
  return `<svg viewBox="0 0 28 28" fill="none"><rect x="2.5" y="2.5" width="23" height="23" rx="7" fill="#1e293b"/><text x="14" y="18" text-anchor="middle" fill="#dbeafe" font-family="system-ui,sans-serif" font-size="10" font-weight="700">${text}</text></svg>`;
}

function getSourceIconMarkup(sourceId, fallbackLabel) {
  const normalizedSourceId = normalizeSourceId(sourceId);
  const fallbackMarkup = getSourceTextFallbackMarkup(normalizedSourceId, fallbackLabel);
  const assetPath = SOURCE_LOGO_ASSETS[normalizedSourceId];
  if (!assetPath) return fallbackMarkup;
  return `<span class="es-source-logo-wrap" aria-hidden="true"><img class="es-source-logo-img" src="${assetPath}" alt="" aria-hidden="true" decoding="async" /><span class="es-source-logo-fallback" aria-hidden="true" hidden>${fallbackMarkup}</span></span>`;
}

function bindSourceLogoFallback(chipEl) {
  if (!(chipEl instanceof HTMLElement)) return;
  const logoImg = chipEl.querySelector('.es-source-logo-img');
  const fallback = chipEl.querySelector('.es-source-logo-fallback');
  if (!(logoImg instanceof HTMLImageElement) || !(fallback instanceof HTMLElement)) return;

  const showFallback = () => {
    logoImg.hidden = true;
    fallback.hidden = false;
  };

  if (logoImg.complete && logoImg.naturalWidth === 0) {
    showFallback();
    return;
  }
  logoImg.addEventListener('error', showFallback, { once: true });
}

const ALL_SOURCE_IDS = Object.values(SOURCE_OPTIONS).flatMap((group) =>
  group.map((item) => normalizeSourceId(item.id))
);

const getSourceItemsForTab = (tabId) => SOURCE_OPTIONS[tabId] || [];

const getAllowedSourcesForTab = (tabId) =>
  getSourceItemsForTab(tabId).map((item) => normalizeSourceId(item.id));

const sanitizeSourcesArray = (value, allowed = ALL_SOURCE_IDS) => {
  const list = Array.isArray(value)
    ? value
    : typeof value === 'string' && value
      ? [value]
      : [];
  const normalizedAllowed = Array.isArray(allowed)
    ? Array.from(
        new Set(
          allowed
            .map((entry) => normalizeSourceId(entry))
            .filter(Boolean)
        )
      )
    : [];
  if (!normalizedAllowed.length) return [];
  const allowedSet = new Set(normalizedAllowed);

  const deduped = [];
  const seen = new Set();
  list.forEach((entry) => {
    const safeEntry = normalizeSourceId(entry);
    if (!safeEntry || !allowedSet.has(safeEntry) || seen.has(safeEntry)) return;
    seen.add(safeEntry);
    deduped.push(safeEntry);
  });

  return deduped;
};

const areSourcesEqual = (left, right) => {
  const a = Array.isArray(left) ? left : [];
  const b = Array.isArray(right) ? right : [];
  if (a.length !== b.length) return false;
  const set = new Set(a);
  return b.every((item) => set.has(item));
};

const getSelectedSourcesForTab = (tabId) => {
  const allowed = getAllowedSourcesForTab(tabId);
  return sanitizeSourcesArray(STATE.filters?.[tabId]?.sources, allowed);
};

const getSourceRequestConfig = (tabId, { preferServerMulti = false } = {}) => {
  const selectedSources = getSelectedSourcesForTab(tabId);
  if (selectedSources.length === 1) {
    return {
      querySource: toApiSourceId(selectedSources[0]) || 'all',
      querySources: null,
      filterSources: [],
    };
  }
  if (selectedSources.length > 1) {
    const apiSources = selectedSources.map((sourceId) => toApiSourceId(sourceId)).filter(Boolean);
    if (preferServerMulti && apiSources.length) {
      return {
        querySource: 'all',
        querySources: apiSources.join(','),
        filterSources: [],
      };
    }
    return { querySource: 'all', querySources: null, filterSources: selectedSources };
  }
  return { querySource: 'all', querySources: null, filterSources: [] };
};

const applySourceQuery = (query, sourceConfig) => {
  const nextQuery = { ...(query || {}) };
  if (sourceConfig?.querySources) {
    nextQuery.sources = sourceConfig.querySources;
    delete nextQuery.source;
    return nextQuery;
  }
  nextQuery.source = sourceConfig?.querySource || 'all';
  return nextQuery;
};

const filterItemsBySources = (items, sources) => {
  if (!Array.isArray(items) || !Array.isArray(sources) || sources.length === 0) return items;
  const sourceSet = new Set(
    sources
      .map((sourceId) => normalizeSourceId(sourceId))
      .filter(Boolean)
  );
  return items.filter((item) => sourceSet.has(normalizeSourceId(item?.source)));
};

const safeLoadStorage = (storageObj, key) => {
  try {
    const raw = storageObj?.getItem?.(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.v !== 1) return null;
    return parsed.value;
  } catch (e) {
    console.warn('Failed to load storage key', key, e);
    return null;
  }
};

const safeSaveStorage = (storageObj, key, value) => {
  try {
    storageObj?.setItem?.(key, JSON.stringify({ v: 1, value }));
  } catch (e) {
    console.warn('Failed to save storage key', key, e);
  }
};

const sanitizeFilterValue = (value, allowed, fallback) => {
  const safeVal = typeof value === 'string' ? value : '';
  return allowed.includes(safeVal) ? safeVal : fallback;
};

const sanitizeNovelGenreGroup = (value, fallback = DEFAULT_NOVEL_GENRE_GROUP) => {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return NOVEL_GENRE_GROUP_IDS.includes(normalized) ? normalized : fallback;
};

const coerceBooleanFilter = (value, fallback = false) => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 't', 'yes', 'y', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'f', 'no', 'n', 'off'].includes(normalized)) return false;
  }
  return fallback;
};

const getFilterTargetTab = () => {
  const browseTab = STATE.lastBrowseTab || 'webtoon';
  if (STATE.isMyPageOpen) return browseTab;
  if (STATE.activeTab === 'my' || STATE.activeTab === 'home') return browseTab;
  return STATE.activeTab || browseTab;
};

const getScrollViewKeyForTab = (tabId) => {
  if (tabId === 'home') return 'home';
  if (tabId === 'my') return 'mysub';
  if (tabId === 'webtoon' || tabId === 'novel' || tabId === 'ott') return tabId;
  return 'webtoon';
};

const getCurrentScrollViewKey = () => {
  if (STATE.search.pageOpen) return 'search';
  return getScrollViewKeyForTab(STATE.activeTab);
};

const UIState = {
  load() {
    const savedSources = safeLoadStorage(localStorage, UI_STATE_KEYS.filters.sources);
    const savedSource = safeLoadStorage(localStorage, UI_STATE_KEYS.filters.source);
    const savedStatus = safeLoadStorage(localStorage, UI_STATE_KEYS.filters.status);
    const savedDay = safeLoadStorage(sessionStorage, UI_STATE_KEYS.filters.day);
    const savedNovelGenreGroup = safeLoadStorage(
      localStorage,
      UI_STATE_KEYS.filters.novelGenreGroup,
    );
    const savedNovelIsCompleted = safeLoadStorage(
      localStorage,
      UI_STATE_KEYS.filters.novelIsCompleted,
    );
    const migratedSources =
      savedSources !== null && savedSources !== undefined ? savedSources : savedSource;

    return {
      filters: {
        sources: sanitizeSourcesArray(migratedSources, ALL_SOURCE_IDS),
        status: sanitizeFilterValue(
          savedStatus,
          ['ongoing', 'completed'],
          UI_STATE_DEFAULTS.filters.status,
        ),
        day: sanitizeFilterValue(
          savedDay,
          ['all', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'daily', 'hiatus', 'completed'],
          UI_STATE_DEFAULTS.filters.day,
        ),
        novelGenreGroup: sanitizeNovelGenreGroup(
          savedNovelGenreGroup,
          DEFAULT_NOVEL_GENRE_GROUP,
        ),
        novelIsCompleted: coerceBooleanFilter(
          savedNovelIsCompleted,
          DEFAULT_NOVEL_IS_COMPLETED,
        ),
      },
    };
  },

  get() {
    const tabId = getFilterTargetTab();
    const fallbackFilters = UI_STATE_DEFAULTS.filters;
    const tabFilters = STATE.filters?.[tabId] || {};
    const allowedSources = getAllowedSourcesForTab(tabId);
    const snapshot = {
      filters: {
        sources: sanitizeSourcesArray(tabFilters.sources, allowedSources),
        novelGenreGroup: sanitizeNovelGenreGroup(
          STATE.filters?.novel?.genreGroup,
          DEFAULT_NOVEL_GENRE_GROUP,
        ),
        novelIsCompleted: coerceBooleanFilter(
          STATE.filters?.novel?.isCompleted,
          DEFAULT_NOVEL_IS_COMPLETED,
        ),
      },
    };

    if (tabId === 'novel') {
      return snapshot;
    }

    snapshot.filters.status = sanitizeFilterValue(
      tabFilters.status,
      ['ongoing', 'completed'],
      fallbackFilters.status,
    );
    snapshot.filters.day = sanitizeFilterValue(
      tabFilters.day,
      ['all', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'daily', 'hiatus', 'completed'],
      fallbackFilters.day,
    );

    return snapshot;
  },

  apply(nextState, { rerender = true, fetch = false } = {}) {
    const tabId = getFilterTargetTab();
    if (!STATE.filters?.[tabId]) return;

    const incoming = nextState?.filters || {};
    const allowedSources = getAllowedSourcesForTab(tabId);
    const nextSources = sanitizeSourcesArray(incoming.sources ?? incoming.source, allowedSources);

    const current = STATE.filters[tabId];
    let changed = !areSourcesEqual(current.sources, nextSources);
    STATE.filters[tabId].sources = nextSources;

    if (tabId === 'novel') {
      const nextGenreGroup = sanitizeNovelGenreGroup(
        incoming.novelGenreGroup ?? incoming.genreGroup,
        DEFAULT_NOVEL_GENRE_GROUP,
      );
      const nextIsCompleted = coerceBooleanFilter(
        incoming.novelIsCompleted ?? incoming.isCompleted,
        DEFAULT_NOVEL_IS_COMPLETED,
      );

      changed =
        changed ||
        current.genreGroup !== nextGenreGroup ||
        current.isCompleted !== nextIsCompleted;

      STATE.filters[tabId].genreGroup = nextGenreGroup;
      STATE.filters[tabId].isCompleted = nextIsCompleted;
    } else {
      const nextStatus = sanitizeFilterValue(
        incoming.status,
        ['ongoing', 'completed'],
        UI_STATE_DEFAULTS.filters.status,
      );
      const nextDay = sanitizeFilterValue(
        incoming.day,
        ['all', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'daily', 'hiatus', 'completed'],
        UI_STATE_DEFAULTS.filters.day,
      );

      changed = changed || current.status !== nextStatus || current.day !== nextDay;
      STATE.filters[tabId].status = nextStatus;
      STATE.filters[tabId].day = nextDay;
    }

    if (STATE.filters?.novel) {
      STATE.filters.novel.genreGroup = sanitizeNovelGenreGroup(
        incoming.novelGenreGroup ?? incoming.genreGroup ?? STATE.filters.novel.genreGroup,
        DEFAULT_NOVEL_GENRE_GROUP,
      );
      STATE.filters.novel.isCompleted = coerceBooleanFilter(
        incoming.novelIsCompleted ?? incoming.isCompleted ?? STATE.filters.novel.isCompleted,
        DEFAULT_NOVEL_IS_COMPLETED,
      );
    }

    if (rerender) {
      renderL1Filters(tabId);
      renderL2Filters(tabId);
    }

    if (fetch && changed) {
      fetchAndRenderContent(tabId);
    }
  },

  save() {
    const snapshot = this.get();
    // Persist long-lived filters in localStorage; day lives in sessionStorage so it resets on
    // a fresh session per product requirement.
    safeSaveStorage(localStorage, UI_STATE_KEYS.filters.sources, snapshot.filters.sources);
    try {
      localStorage?.removeItem?.(UI_STATE_KEYS.filters.source);
    } catch (e) {
      console.warn('Failed to remove legacy source filter key', e);
    }
    if (typeof snapshot.filters.status === 'string') {
      safeSaveStorage(localStorage, UI_STATE_KEYS.filters.status, snapshot.filters.status);
    }
    if (typeof snapshot.filters.day === 'string') {
      safeSaveStorage(sessionStorage, UI_STATE_KEYS.filters.day, snapshot.filters.day);
    }
    safeSaveStorage(
      localStorage,
      UI_STATE_KEYS.filters.novelGenreGroup,
      sanitizeNovelGenreGroup(STATE.filters?.novel?.genreGroup, DEFAULT_NOVEL_GENRE_GROUP),
    );
    safeSaveStorage(
      localStorage,
      UI_STATE_KEYS.filters.novelIsCompleted,
      coerceBooleanFilter(STATE.filters?.novel?.isCompleted, DEFAULT_NOVEL_IS_COMPLETED),
    );
  },
};

const saveScroll = (viewKey) => {
  const storageKey = UI_STATE_KEYS.scroll[viewKey];
  if (!storageKey) return;
  try {
    safeSaveStorage(sessionStorage, storageKey, Math.max(0, Math.round(window.scrollY || 0)));
  } catch (e) {
    console.warn('Failed to save scroll', viewKey, e);
  }
};

const callAfterRender = (fn, { container = null, requireChildren = false, timeoutMs = 320 } = {}) => {
  let settled = false;

  const run = () => {
    if (settled) return;
    settled = true;
    requestAnimationFrame(() => requestAnimationFrame(fn));
  };

  if (!container || !requireChildren) {
    run();
    return;
  }

  if (container.children.length > 0) {
    run();
    return;
  }

  const observer = new MutationObserver(() => {
    if (container.children.length > 0) {
      observer.disconnect();
      run();
    }
  });

  observer.observe(container, { childList: true });
  setTimeout(() => {
    observer.disconnect();
    run();
  }, timeoutMs);
};

const restoreScroll = (viewKey, { container = null, requireChildren = false } = {}) => {
  const storageKey = UI_STATE_KEYS.scroll[viewKey];
  if (!storageKey) return;

  let scrollY = null;
  try {
    scrollY = safeLoadStorage(sessionStorage, storageKey);
  } catch (e) {
    console.warn('Failed to load scroll', viewKey, e);
    return;
  }

  const targetY = Number.isFinite(scrollY) ? scrollY : 0;
  const performRestore = () => window.scrollTo({ top: targetY });
  callAfterRender(performRestore, { container, requireChildren });
};

/* =========================
   Application state
   ========================= */

const STATE = {
  activeTab: 'home',
  lastBrowseTab: 'webtoon',
  renderToken: 0,
  filters: {
    webtoon: { sources: [], day: 'all' },
    novel: {
      sources: [],
      genreGroup: DEFAULT_NOVEL_GENRE_GROUP,
      isCompleted: DEFAULT_NOVEL_IS_COMPLETED,
    },
    ott: { sources: [], genre: 'all' },
    my: { viewMode: 'completion' },
  },
  search: {
    pageOpen: false,
    query: '',
    results: [],
    isLoading: false,
    uiMode: 'idle',
    debounceTimer: null,
    requestSeq: 0,
    activeIndex: -1,
    recentlyOpened: [],
  },
  isMyPageOpen: false,
  overlayStack: [],
  contents: {},
  isLoading: false,
  contentRequestSeq: 0,
  currentModalContent: null,
  subscribeModalOpen: false,
  subscribeToggleInFlight: false,
  subscribeModalState: { isLoading: false, loadFailed: false },

  auth: {
    isAuthenticated: false,
    user: null,
    isChecking: false,
    uiMode: 'login',
    avatarImageFailed: false,
    lastAvatarUrl: null,
  },

  // subscriptions
  subscriptionsSet: new Set(),
  publicationSubscriptionsSet: new Set(),
  pendingSubOps: new Set(),
  mySubscriptions: [],
  subscriptionsLoadedAt: null,
  subscriptionsLoadPromise: null,
  subscriptionsRequestSeq: 0,
  subscriptionsAbortController: null,
  subscriptionsSoftRefreshTimer: null,
  subscriptionsSoftRefreshLastAt: 0,
  subscriptionsSoftRefreshIdleHandle: null,
  subscriptionsSoftRefreshPendingReason: null,
  subscriptionsNeedFreshHintAt: 0,

  pagination: {
    ongoing: {
      cursor: null,
      legacyCursor: null,
      done: false,
      loading: false,
      items: [],
      totalLoaded: 0,
      requestSeq: 0,
      tabId: null,
      source: 'all',
      filterSources: [],
      aspectClass: 'aspect-[3/4]',
      endpointPath: '',
      baseQuery: {},
    },
    novels: {
      cursor: null,
      legacyCursor: null,
      done: false,
      loading: false,
      items: [],
      totalLoaded: 0,
      requestSeq: 0,
      tabId: null,
      source: 'all',
      filterSources: [],
      aspectClass: 'aspect-[3/4]',
      endpointPath: '',
      baseQuery: {},
    },
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
      filterSources: [],
      aspectClass: 'aspect-[3/4]',
      endpointPath: '',
      baseQuery: {},
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
      filterSources: [],
      aspectClass: 'aspect-[3/4]',
      endpointPath: '',
      baseQuery: {},
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
  renderAbortController: null,
  gridRenderAbort: null,
  searchRenderAbort: null,
  searchAbortController: null,
  tabAbortController: null,
  isMySubOpen: false,
  hasBootstrapped: false,
};

const getOverlayStackTop = () => STATE.overlayStack[STATE.overlayStack.length - 1] || null;

const pushOverlayState = (overlay, payload = {}) => {
  const top = getOverlayStackTop();
  if (top?.overlay === overlay) return top;

  const entry = {
    overlay,
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    ...payload,
  };

  STATE.overlayStack.push(entry);
  try {
    history.pushState({ overlay, id: entry.id }, '');
  } catch (err) {
    console.warn('Failed to push overlay history', err);
  }
  return entry;
};

const popOverlayState = (overlay, overlayId = null) => {
  const top = getOverlayStackTop();
  if (top && top.overlay === overlay && (!overlayId || top.id === overlayId)) {
    STATE.overlayStack.pop();
    return true;
  }
  return false;
};

const closeOverlayByType = (overlay, { fromPopstate = false, overlayId = null } = {}) => {
  if (overlay === 'modal') {
    closeSubscribeModal({ fromPopstate: true, overlayId });
    return true;
  }
  if (overlay === 'myPage') {
    closeMyPage({ fromPopstate: true, overlayId });
    return true;
  }
  if (overlay === 'search') {
    closeSearchPage({ fromPopstate: true, overlayId });
    return true;
  }
  return false;
};

const requestCloseOverlay = (overlay) => {
  const top = getOverlayStackTop();
  if (top?.overlay === overlay) {
    history.back();
    return true;
  }
  return closeOverlayByType(overlay);
};

const handleOverlayPopstate = (event) => {
  const targetOverlay = event?.state?.overlay || null;
  const targetId = event?.state?.id || null;
  const maxSteps = 20;
  let safety = 0;

  // Reconcile from the current stack top so browser back always unwinds overlays in LIFO order.
  while (safety < maxSteps) {
    const top = getOverlayStackTop();
    if (!top) break;

    if (targetOverlay) {
      const reachedTarget =
        top.overlay === targetOverlay && (!targetId || top.id === targetId);
      if (reachedTarget) break;
    }

    const closed = closeOverlayByType(top.overlay, { fromPopstate: true, overlayId: top.id });
    if (!closed) {
      popOverlayState(top.overlay, top.id);
    }
    safety += 1;
  }

  if (safety === maxSteps) {
    console.warn('Overlay popstate reconciliation reached safety limit', {
      targetOverlay,
      targetId,
      stackDepth: STATE.overlayStack.length,
    });
  }

  ensureScrollLockConsistency();
};

function runDevSelfCheck() {
  if (!DEBUG_RUNTIME) return;
  try {
    const scrollKeys = Object.values(UI_STATE_KEYS?.scroll || {});
    const duplicates = scrollKeys.filter((key, idx) => scrollKeys.indexOf(key) !== idx);
    if (duplicates.length) console.warn('[self-check] duplicate scroll keys', duplicates);

    ['updateTab', 'fetchAndRenderContent', 'restoreScroll'].forEach((fnName) => {
      if (typeof window?.[fnName] !== 'function' && typeof globalThis?.[fnName] !== 'function') {
        console.warn(`[self-check] missing function: ${fnName}`);
      }
    });
  } catch (err) {
    console.warn('[self-check] failed', err);
  }
}

/* =========================
   DOM cache
   ========================= */

const UI = {
  bottomNav: document.getElementById('bottomNav'),
  contentGrid: document.getElementById('contentGridContainer'),
  contentCountIndicator: document.getElementById('contentCountIndicator'),
  contentLoadMoreBtn: document.getElementById('contentLoadMoreBtn'),
  contentGridSentinel: document.getElementById('contentGridSentinel'),
  l1Filter: document.getElementById('l1FilterContainer'),
  l2Filter: document.getElementById('l2FilterContainer'),
  filtersWrapper: document.getElementById('filtersWrapper'),
  subscribeModal: document.getElementById('subscribeModal'),
  subscribePublicationButton: document.getElementById('subscribePublicationButton'),
  subscribeCompletionButton: document.getElementById('subscribeCompletionButton'),
  subscribeStateLine: document.getElementById('subscribeStateLine'),
  subscribeStateDot: document.getElementById('subscribeStateDot'),
  subscribeStateText: document.getElementById('subscribeStateText'),
  subscribeInlineError: document.getElementById('subscribeInlineError'),
  mySubToggle: document.getElementById('mySubToggleContainer'),
  seriesSort: document.getElementById('seriesSortOptions'),
  seriesFooter: document.getElementById('seriesFooterButton'),
  toggleIndicator: document.getElementById('toggleIndicator'),
  header: document.getElementById('mainHeader'),
  homeButton: document.getElementById('homeButton'),
  profileButton: document.getElementById('profileButton'),
  profileButtonText: document.getElementById('profileButtonText'),
  profileMenu: document.getElementById('profileMenu'),
  profileMenuMy: document.getElementById('profileMenuMy'),
  profileMenuThemeToggle: document.getElementById('profileMenuThemeToggle'),
  profileMenuAdmin: document.getElementById('profileMenuAdmin'),
  profileMenuLogout: document.getElementById('profileMenuLogout'),
  headerSearchWrap: document.getElementById('headerSearchWrap'),
  searchButton: document.getElementById('searchButton'),
  aitSearchTrigger: document.getElementById('aitSearchTrigger'),
  searchInput: document.getElementById('searchInput'),
  searchPage: document.getElementById('searchPage'),
  searchPageInput: document.getElementById('searchPageInput'),
  searchBackButton: document.getElementById('searchBackButton'),
  searchClearButton: document.getElementById('searchClearButton'),
  searchIdle: document.getElementById('searchIdle'),
  searchResultsView: document.getElementById('searchResultsView'),
  searchResultsMeta: document.getElementById('searchResultsMeta'),
  searchPageResults: document.getElementById('searchPageResults'),
  searchPageEmpty: document.getElementById('searchPageEmpty'),
  searchPageLoading: document.getElementById('searchPageLoading'),
  searchEmptyTitle: document.getElementById('searchEmptyTitle'),
  searchEmptySubtitle: document.getElementById('searchEmptySubtitle'),
  searchEmptyActions: document.getElementById('searchEmptyActions'),
  searchRecentChips: document.getElementById('searchRecentChips'),
  searchRecentClearAll: document.getElementById('searchRecentClearAll'),
  searchPopularGrid: document.getElementById('searchPopularGrid'),
  searchPopularTitle: document.getElementById('searchPopularTitle'),
  searchPopularSubtitle: document.getElementById('searchPopularSubtitle'),
  searchResultCount: document.getElementById('searchResultCount'),
  myPage: document.getElementById('myPage'),
  myPageBackBtn: document.getElementById('myPageBackBtn'),
  myPageEmailValue: document.getElementById('myPageEmailValue'),
  myPageCreatedAtRow: document.getElementById('myPageCreatedAtRow'),
  myPageCreatedAtValue: document.getElementById('myPageCreatedAtValue'),
  myPagePwCurrent: document.getElementById('myPagePwCurrent'),
  myPagePwNew: document.getElementById('myPagePwNew'),
  myPagePwConfirm: document.getElementById('myPagePwConfirm'),
  myPagePwSubmit: document.getElementById('myPagePwSubmit'),
  myPagePwError: document.getElementById('myPagePwError'),
  myPageGoMySubBtn: document.getElementById('myPageGoMySubBtn'),
  myPageLogoutBtn: document.getElementById('myPageLogoutBtn'),
  myPageEntryButton: document.getElementById('myPageEntryButton'),
  profileMenuMyPage: document.getElementById('profileMenuMyPage'),
};

// DATA_UI_CLASS_MAP: maps data-ui keys in static HTML to UI_CLASSES tokens.
// - Keys mirror data-ui="..." attributes in templates/index.html.
// - Some entries are dynamic-only for nodes created in JS (not always present in templates).
const DATA_UI_CLASS_MAP = {
  // Static HTML (templates/index.html)
  'search-back': UI_CLASSES.iconBtn,
  'search-clear': cx(UI_CLASSES.iconBtnSm, 'hidden'),
  'search-input': UI_CLASSES.inputBase,
  'search-trigger': UI_CLASSES.searchTrigger,
  'search-recent-clear': UI_CLASSES.sectionSubtle,
  'search-popular-title': UI_CLASSES.sectionTitle,
  'search-popular-subtitle': UI_CLASSES.sectionSubtle,
  'search-result-label': UI_CLASSES.sectionSubtle,
  'search-empty-title': UI_CLASSES.emptyTitle,
  'search-empty-msg': UI_CLASSES.emptyMsg,
  'search-empty-button': cx(UI_CLASSES.btnSecondary, 'mt-6'),
  'header-search-icon': UI_CLASSES.headerSearchIcon,
  'header-profile-icon': UI_CLASSES.headerProfileIcon,
  'grid-2to3': UI_CLASSES.grid2to3,
  'modal-wrap': UI_CLASSES.modalWrap,
  'modal-card': UI_CLASSES.modalCard,
  'modal-title': UI_CLASSES.modalTitle,
  'modal-body': UI_CLASSES.modalBodyText,
  'modal-primary': cx(UI_CLASSES.btnPrimary, 'spring-bounce'),
  'modal-secondary': cx(UI_CLASSES.btnSecondary, 'spring-bounce'),
  'input-sm': UI_CLASSES.inputSm,
  'input-label': UI_CLASSES.inputLabel,
  'btn-primary': UI_CLASSES.btnPrimary,
  'btn-solid': UI_CLASSES.btnSolid,
  'menu-wrap': UI_CLASSES.menuWrap,
  'menu-item': UI_CLASSES.menuItem,
  'menu-item-danger': UI_CLASSES.menuItemDanger,
  'load-more': UI_CLASSES.loadMoreBtn,
  'page-container': 'mx-auto h-full max-w-[520px] px-4',
  'section-title': UI_CLASSES.sectionTitle,
  'section-subtle': UI_CLASSES.sectionSubtle,
  'page-overlay-root': UI_CLASSES.pageOverlayRoot,
  'page-overlay-container': UI_CLASSES.pageOverlayContainer,
  'page-card': UI_CLASSES.pageCard,

  // Dynamic-only (JS-generated nodes)
  'pill-hint': UI_CLASSES.pillHint, // dynamic-only: card affordance hint
  'btn-secondary': UI_CLASSES.btnSecondary, // dynamic-only: secondary CTAs injected by JS
};

// applyDataUiClasses: applies token classes to nodes annotated with data-ui.
// - Idempotent: per-element guard prevents duplicate class application on re-runs.
// - Safe to call after dynamically inserting any data-ui elements into the DOM.
function applyDataUiClasses(root = document) {
  const elements = root?.querySelectorAll?.('[data-ui]');
  if (!elements) return;

  elements.forEach((el) => {
    if (el.dataset.uiApplied === '1') return; // guard against repeat application
    const key = el.getAttribute('data-ui');
    const tokenClass = DATA_UI_CLASS_MAP[key];
    if (!tokenClass) return;
    const classParts = tokenClass.split(/\s+/).filter(Boolean);
    if (classParts.length) el.classList.add(...classParts);
    el.dataset.uiApplied = '1';
  });
}

function setClasses(el, classStr) {
  if (!el) return el;
  el.className = classStr;
  return el;
}

function cx(...parts) {
  return parts.filter(Boolean).join(' ');
}

async function renderInBatches({
  items,
  container,
  renderItem,
  batchSize = 24,
  yieldMs = 8,
  signal,
}) {
  if (!container) return;
  container.innerHTML = '';

  const safeItems = Array.isArray(items) ? items : [];
  let i = 0;

  while (i < safeItems.length) {
    if (signal?.aborted) return;

    const end = Math.min(i + batchSize, safeItems.length);
    const frag = document.createDocumentFragment();
    for (; i < end; i += 1) {
      frag.appendChild(renderItem(safeItems[i], i));
    }
    container.appendChild(frag);

    await new Promise((r) => requestAnimationFrame(r));
    const start = performance.now();
    if (yieldMs > 0) {
      while (performance.now() - start < yieldMs) {
        // allow paint + microtasks
        await Promise.resolve();
        break;
      }
    }
  }

  if (!signal?.aborted) {
    syncAllRenderedStarBadges();
  }
}

function createConcurrencyLimiter(limit = 3) {
  const queue = [];
  let activeCount = 0;

  const next = () => {
    if (!queue.length || activeCount >= limit) return;
    const { fn, resolve, reject } = queue.shift();
    activeCount += 1;
    Promise.resolve()
      .then(fn)
      .then((value) => {
        activeCount -= 1;
        resolve(value);
        next();
      })
      .catch((err) => {
        activeCount -= 1;
        reject(err);
        next();
      });
  };

  return (fn) =>
    new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
}

/* =========================
   Generic helpers
   ========================= */

function renderEmptyState(containerEl, { title = '', message = '', actions = [] } = {}) {
  if (!containerEl) return;
  containerEl.innerHTML = '';

  const wrapper = setClasses(
    document.createElement('div'),
    cx('w-full col-span-full', UI_CLASSES.emptyWrap),
  );

  if (title) {
    const titleEl = setClasses(document.createElement('h3'), UI_CLASSES.emptyTitle);
    titleEl.textContent = title;
    wrapper.appendChild(titleEl);
  }

  if (message) {
    const msgEl = setClasses(document.createElement('p'), UI_CLASSES.emptyMsg);
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
      const variantClass =
        action.variant === 'secondary' ? UI_CLASSES.btnSecondary : UI_CLASSES.btnPrimary;
      setClasses(btn, `${variantClass} spring-bounce`);
      btn.textContent = action.label;
      btn.onclick = action.onClick;
      actionsWrap.appendChild(btn);
    });

    wrapper.appendChild(actionsWrap);
  }

  containerEl.appendChild(wrapper);
}

let contentGridObserver = null;

/* =========================
   Modals
   ========================= */
const modalStack = [];
const modalMeta = new Map();
let bodyOverflowBackup = '';
let htmlOverflowBackup = '';
let scrollLockCount = 0;
const restoreScrollOverflowStyles = () => {
  document.body.style.overflow = bodyOverflowBackup || '';
  document.documentElement.style.overflow = htmlOverflowBackup || '';
};

const lockBodyScroll = () => {
  if (scrollLockCount === 0) {
    bodyOverflowBackup = document.body.style.overflow;
    htmlOverflowBackup = document.documentElement.style.overflow;
    document.body.style.overflow = 'hidden';
    document.documentElement.style.overflow = 'hidden';
  }
  scrollLockCount += 1;
};

const unlockBodyScroll = () => {
  scrollLockCount = Math.max(0, scrollLockCount - 1);
  if (scrollLockCount === 0) {
    restoreScrollOverflowStyles();
  }
};

const isAnyModalOpen = () => modalStack.length > 0;
const getTopModal = () => modalStack[modalStack.length - 1] || null;
const ensureScrollLockConsistency = () => {
  const shouldLock =
    isAnyModalOpen() || Boolean(STATE.search?.pageOpen) || Boolean(STATE.isMyPageOpen);
  if (!shouldLock) {
    if (scrollLockCount > 0) scrollLockCount = 0;
    if (
      document.body.style.overflow === 'hidden' ||
      document.documentElement.style.overflow === 'hidden'
    ) {
      restoreScrollOverflowStyles();
    }
  }
};

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
      if (modalEl.id === 'subscribeModal') requestCloseOverlay('modal');
      else closeModal(modalEl);
    }
  });
};

const isFocusableInDocument = (el) =>
  Boolean(el) && document.contains(el) && typeof el.focus === 'function';

const focusSearchInput = () => {
  const input = UI.searchPageInput;
  if (
    STATE?.search?.pageOpen &&
    input &&
    document.contains(input) &&
    typeof input.focus === 'function'
  ) {
    input.focus();
    return true;
  }
  return false;
};

function openModal(modalEl, { initialFocusEl, returnFocusEl } = {}) {
  if (!modalEl) return;
  setupModalRoot(modalEl);
  if (modalStack.includes(modalEl)) return;

  const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const focusReturnEl = returnFocusEl instanceof HTMLElement ? returnFocusEl : null;

  lockBodyScroll();

  modalStack.push(modalEl);
  modalMeta.set(modalEl, { opener, returnFocusEl: focusReturnEl });

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

  if (modalEl.id === 'subscribeModal') {
    STATE.subscribeModalOpen = false;
    STATE.subscribeToggleInFlight = false;
    STATE.currentModalContent = null;
    if (UI.subscribeInlineError) UI.subscribeInlineError.textContent = '';
  }

  unlockBodyScroll();
  ensureScrollLockConsistency();

  const hadReturnEl = Boolean(meta.returnFocusEl);
  const focusTarget =
    (isFocusableInDocument(meta.returnFocusEl) ? meta.returnFocusEl : null) ||
    (isFocusableInDocument(meta.opener) ? meta.opener : null);

  if (focusTarget) {
    if (focusTarget.dataset?.searchIndex !== undefined) {
      const idx = Number(focusTarget.dataset.searchIndex);
      if (!Number.isNaN(idx)) setActiveSearchIndex(idx);
      focusTarget.setAttribute('aria-selected', 'true');
    }
    focusTarget.focus();
    return;
  }

  if (hadReturnEl && focusSearchInput()) return;
  if (focusSearchInput()) return;

  if (UI.profileButton && typeof UI.profileButton.focus === 'function') {
    UI.profileButton.focus();
  }
}

function createStarBadgeEl() {
  const badgeEl = document.createElement('div');
  setClasses(badgeEl, UI_CLASSES.starBadge);
  badgeEl.setAttribute('aria-hidden', 'true');
  badgeEl.setAttribute('data-star-badge', 'true');
  badgeEl.textContent = '★';
  return badgeEl;
}

function syncStarBadgeForCard(cardEl, subscribedOverride = null) {
  if (!cardEl) return;

  const thumb = cardEl.querySelector('[data-card-thumb="true"]');
  if (!thumb) return;

  const contentId = cardEl.getAttribute('data-content-id');
  const source = cardEl.getAttribute('data-source');
  const contentType = cardEl.getAttribute('data-content-type');
  const content = {
    content_id: contentId,
    source,
    content_type: contentType,
  };
  const shouldShow =
    typeof subscribedOverride === 'boolean' ? subscribedOverride : isAnySubscribedForCard(content);
  const existing = thumb.querySelector('[data-star-badge="true"]');

  if (shouldShow && !existing) {
    thumb.appendChild(createStarBadgeEl());
  } else if (!shouldShow && existing) {
    existing.remove();
  }
}

function syncAllRenderedStarBadges() {
  document.querySelectorAll('[data-content-id][data-source]').forEach((cardEl) => {
    const contentId = cardEl.getAttribute('data-content-id');
    const source = cardEl.getAttribute('data-source');
    const contentType = cardEl.getAttribute('data-content-type');
    const content = { content_id: contentId, source, content_type: contentType };
    syncStarBadgeForCard(cardEl, isAnySubscribedForCard(content));
  });
}

document.addEventListener(
  'keydown',
  (evt) => {
    if (!isAnyModalOpen()) return;
    const topModal = getTopModal();
    if (!topModal) return;

    if (evt.key === 'Escape') {
      evt.preventDefault();
      if (topModal.id === 'subscribeModal') {
        requestCloseOverlay('modal');
        return;
      }
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
  return `${src}:${cid}`;
};

const resetPaginationState = (
  category,
  { tabId, source, filterSources, aspectClass, requestSeq, endpointPath, baseQuery }
) => {
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
  target.filterSources = Array.isArray(filterSources) ? filterSources : [];
  target.aspectClass = aspectClass;
  target.requestSeq = requestSeq;
  target.endpointPath = typeof endpointPath === 'string' ? endpointPath : '';
  target.baseQuery =
    baseQuery && typeof baseQuery === 'object' && !Array.isArray(baseQuery) ? { ...baseQuery } : {};
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
  setClasses(toast, UI_CLASSES.toastWrap);
  toast.setAttribute('role', type === 'error' ? 'alert' : 'status');
  toast.setAttribute('aria-atomic', 'true');

  const inner = document.createElement('div');
  const toastTone =
    type === 'success'
      ? UI_CLASSES.toastSuccess
      : type === 'error'
      ? UI_CLASSES.toastError
      : UI_CLASSES.toastInfo;
  setClasses(inner, toastTone);
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

  if (STATE.subscriptionsAbortController) {
    STATE.subscriptionsAbortController.abort();
    STATE.subscriptionsAbortController = null;
  }
  if (STATE.subscriptionsSoftRefreshTimer) {
    clearTimeout(STATE.subscriptionsSoftRefreshTimer);
    STATE.subscriptionsSoftRefreshTimer = null;
  }
  cancelIdle(STATE.subscriptionsSoftRefreshIdleHandle);
  STATE.subscriptionsSoftRefreshIdleHandle = null;
  STATE.subscriptionsSoftRefreshPendingReason = null;
  STATE.subscriptionsSoftRefreshLastAt = 0;
  STATE.subscriptionsNeedFreshHintAt = 0;

  STATE.subscriptionsSet = new Set();
  STATE.publicationSubscriptionsSet = new Set();
  STATE.mySubscriptions = [];
  STATE.subscriptionsLoadedAt = null;

  if (!silent) showToast('로그아웃되었습니다', { type: 'info' });
  updateProfileButtonState();
  fetchAndRenderContent(STATE.activeTab);
}

let authRevalidateTimer = null;
const scheduleAuthRevalidate = () => {
  const token = getAccessToken();
  if (!token || STATE.auth.isChecking) return;
  if (authRevalidateTimer) clearTimeout(authRevalidateTimer);
  authRevalidateTimer = setTimeout(async () => {
    authRevalidateTimer = null;
    if (!getAccessToken()) return;
    await fetchMe();
    preloadSubscriptionsOnce({ force: true }).catch((e) => {
      console.warn('Failed to refresh subscriptions after revalidate', e);
    });
  }, 200);
};

function setupAuthReturnListeners() {
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      scheduleAuthRevalidate();
    }
  });

  window.addEventListener('focus', () => {
    scheduleAuthRevalidate();
  });

  window.addEventListener('storage', (event) => {
    if (event.key !== 'es_access_token') return;
    if (!event.newValue) {
      STATE.auth.isAuthenticated = false;
      STATE.auth.user = null;
      updateProfileButtonState();
      return;
    }
    scheduleAuthRevalidate();
  });
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

async function apiRequest(method, path, { query, body, token, signal } = {}) {
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

  const response = await fetch(url, { method, headers, body: serializedBody, signal });
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

    if (response.status === 401) {
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
const isAbortError = (err) => err && (err.name === 'AbortError' || err.code === 20);
const SUBS_SOFT_REFRESH_DEBOUNCE_MS = 1500;
const SUBS_SOFT_REFRESH_MIN_INTERVAL_MS = 10000;
const SUBS_MY_TAB_EXPEDITE_MS = 500;
const SUBS_IDLE_TIMEOUT_MS = 2000;

function normalizeSearchText(s) {
  return (s || '')
    .toString()
    .trim()
    .normalize('NFKC')
    .replace(/\s+/g, '')
    .toLowerCase();
}

const normalizeSubscriptionItem = (item) => {
  if (!item || typeof item !== 'object') return null;

  const contentId = item.content_id || item.contentId || item.id;
  const source = safeString(item.source, '');
  if (!contentId || !source) return null;

  const subscriptionRaw = safeObj(item.subscription);
  const wantsCompletion =
    'wants_completion' in subscriptionRaw
      ? safeBool(subscriptionRaw.wants_completion, false)
      : true;
  const wantsPublication =
    'wants_publication' in subscriptionRaw
      ? safeBool(subscriptionRaw.wants_publication, false)
      : false;

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
    subscription: {
      wants_completion: wantsCompletion,
      wants_publication: wantsPublication,
    },
    final_state: finalState,
  };
};

/* =========================
   Subscriptions helpers/state (CP4 + CP4.1)
   ========================= */

const subKey = (content) => {
  if (!content) return '';
  const source = String(content.source || '').trim();
  const cidRaw = content.content_id ?? content.contentId ?? content.id;
  const contentId = cidRaw === undefined || cidRaw === null ? '' : String(cidRaw).trim();
  if (!source || !contentId) return '';
  return `${source}:${contentId}`;
};

const buildSubscriptionKey = (content) => subKey(content);

const getContentType = (content) => {
  const rawType = content?.content_type || content?.contentType || content?.type;
  if (rawType) return String(rawType).toLowerCase();
  if (['webtoon', 'novel', 'ott', 'series'].includes(STATE.activeTab)) return STATE.activeTab;
  if (['webtoon', 'novel', 'ott', 'series'].includes(STATE.lastBrowseTab))
    return STATE.lastBrowseTab;
  return '';
};

const supportsPublicationUI = (content) => {
  const ct = getContentType(content);
  return ct === 'ott' || ct === 'series';
};

const isCompletionSubscribed = (content) => {
  const key = subKey(content);
  return key ? STATE.subscriptionsSet.has(key) : false;
};

const isPublicationSubscribed = (content) => {
  const key = subKey(content);
  return key ? STATE.publicationSubscriptionsSet.has(key) : false;
};

const isAnySubscribedForCard = (content) => {
  const ct = getContentType(content);
  if (ct === 'ott' || ct === 'series') {
    return isCompletionSubscribed(content) || isPublicationSubscribed(content);
  }
  return isCompletionSubscribed(content);
};

const setsEqual = (a, b) => {
  if (a === b) return true;
  if (!a || !b || a.size !== b.size) return false;
  for (const val of a) {
    if (!b.has(val)) return false;
  }
  return true;
};

function scheduleIdle(fn, timeoutMs = SUBS_IDLE_TIMEOUT_MS) {
  if (typeof requestIdleCallback === 'function') {
    return requestIdleCallback(fn, { timeout: timeoutMs });
  }
  return setTimeout(() => fn({ didTimeout: true, timeRemaining: () => 0 }), timeoutMs);
}

function cancelIdle(handle) {
  if (!handle) return;
  if (typeof cancelIdleCallback === 'function') cancelIdleCallback(handle);
  else clearTimeout(handle);
}

async function loadSubscriptions(opts = {}) {
  const force = Boolean(opts.force);
  const silent = Boolean(opts.silent);
  const token = getAccessToken();
  if (!token) {
    if (STATE.subscriptionsAbortController) {
      STATE.subscriptionsAbortController.abort();
      STATE.subscriptionsAbortController = null;
    }
    STATE.subscriptionsSet = new Set();
    STATE.publicationSubscriptionsSet = new Set();
    STATE.mySubscriptions = [];
    STATE.subscriptionsLoadedAt = null;
    STATE.subscriptionsLoadPromise = null;
    syncAllRenderedStarBadges();
    syncMySubListInPlace();
    if (STATE.currentModalContent) syncSubscribeModalUI(STATE.currentModalContent);
    return [];
  }

  if (!force && STATE.subscriptionsLoadPromise) {
    return STATE.subscriptionsLoadPromise;
  }

  if (!force && STATE.subscriptionsLoadedAt) {
    return STATE.mySubscriptions;
  }

  const seq = (STATE.subscriptionsRequestSeq || 0) + 1;
  STATE.subscriptionsRequestSeq = seq;
  const tokenSnapshot = token;

  if (STATE.subscriptionsAbortController) {
    STATE.subscriptionsAbortController.abort();
  }
  const ac = new AbortController();
  STATE.subscriptionsAbortController = ac;

  const loadPromise = (async () => {
    try {
      const res = await apiRequest('GET', '/api/me/subscriptions', {
        token,
        signal: ac.signal,
      });
      if (!res || res.success !== true || !Array.isArray(res.data)) {
        throw new Error('구독 정보를 불러오지 못했습니다.');
      }

      const normalized = res.data
        .map((x) => normalizeSubscriptionItem(x))
        .filter(Boolean);

      if (seq !== STATE.subscriptionsRequestSeq) return normalized;
      if (getAccessToken() !== tokenSnapshot) return normalized;

      const completionSet = new Set();
      const publicationSet = new Set();
      normalized.forEach((item) => {
        const key = buildSubscriptionKey(item);
        if (!key) return;
        if (item.subscription?.wants_completion) completionSet.add(key);
        if (item.subscription?.wants_publication) publicationSet.add(key);
      });

      const completionChanged = !setsEqual(STATE.subscriptionsSet, completionSet);
      const publicationChanged = !setsEqual(
        STATE.publicationSubscriptionsSet,
        publicationSet
      );
      const shouldSync = completionChanged || publicationChanged;

      STATE.subscriptionsSet = completionSet;
      STATE.publicationSubscriptionsSet = publicationSet;
      STATE.mySubscriptions = normalized;
      STATE.subscriptionsLoadedAt = Date.now();
      if (shouldSync) {
        syncAllRenderedStarBadges();
        syncMySubListInPlace();
        if (STATE.currentModalContent) syncSubscribeModalUI(STATE.currentModalContent);
      }

      return normalized;
    } catch (err) {
      if (isAbortError(err)) return STATE.mySubscriptions || [];
      if (silent) return STATE.mySubscriptions || [];
      throw err;
    }
  })();

  STATE.subscriptionsLoadPromise = loadPromise;

  try {
    return await loadPromise;
  } finally {
    if (STATE.subscriptionsLoadPromise === loadPromise) {
      STATE.subscriptionsLoadPromise = null;
    }
    if (STATE.subscriptionsAbortController === ac) {
      STATE.subscriptionsAbortController = null;
    }
  }
}

function preloadSubscriptionsOnce({ force = false } = {}) {
  const token = getAccessToken();
  if (!token) return Promise.resolve([]);
  return loadSubscriptions({ force }).catch((e) => {
    console.warn('Failed to preload subscriptions', e);
    throw e;
  });
}

async function retryModalSubscriptionLoad(content) {
  STATE.subscribeModalState = { ...STATE.subscribeModalState, isLoading: true, loadFailed: false };
  syncSubscribeModalUI(content);

  try {
    await loadSubscriptions({ force: true });
    STATE.subscribeModalState.isLoading = false;
    STATE.subscribeModalState.loadFailed = false;
    syncSubscribeModalUI(content);
  } catch (e) {
    if (isAbortError(e)) {
      STATE.subscribeModalState.isLoading = false;
      STATE.subscribeModalState.loadFailed = false;
      syncSubscribeModalUI(content);
      return;
    }
    STATE.subscribeModalState.isLoading = false;
    STATE.subscribeModalState.loadFailed = true;
    showToast('구독 상태를 불러오지 못했습니다. 다시 시도해 주세요.', { type: 'error' });
    syncSubscribeModalUI(content);
  }
}

async function subscribeContent(content, alertType = 'completion') {
  const token = getAccessToken();
  if (!token) throw { httpStatus: 401, message: '로그인이 필요합니다.' };

  const contentId = content?.content_id || content?.contentId || content?.id;
  const source = content?.source;

  if (!contentId || !source) throw new Error('콘텐츠 정보가 없습니다.');

  try {
    const res = await apiRequest('POST', '/api/me/subscriptions', {
      body: { content_id: contentId, contentId, source, alert_type: alertType },
      token,
    });

    STATE.subscriptionsLoadedAt = null;
    applyServerSubscriptionFlags(content, res?.subscription ?? null);
    return res;

  } catch (e) {
    throw e;
  }
}

async function unsubscribeContent(content, alertType = 'completion') {
  const token = getAccessToken();
  if (!token) throw { httpStatus: 401, message: '로그인이 필요합니다.' };

  const contentId = content?.content_id || content?.contentId || content?.id;
  const source = content?.source;

  if (!contentId || !source) throw new Error('콘텐츠 정보가 없습니다.');

  try {
    const res = await apiRequest('DELETE', '/api/me/subscriptions', {
      body: { content_id: contentId, contentId, source, alert_type: alertType },
      token,
    });

    STATE.subscriptionsLoadedAt = null;
    applyServerSubscriptionFlags(content, res?.subscription ?? null);
    return res;
  } catch (e) {
    throw e;
  }
}

function scheduleSubscriptionsSoftRefresh(reason = 'toggle', opts = {}) {
  const now = Date.now();
  const expedite = Boolean(opts.expedite);

  STATE.subscriptionsSoftRefreshPendingReason = reason;

  const debounceMs = expedite ? SUBS_MY_TAB_EXPEDITE_MS : SUBS_SOFT_REFRESH_DEBOUNCE_MS;

  if (
    !expedite &&
    now - (STATE.subscriptionsSoftRefreshLastAt || 0) <
      SUBS_SOFT_REFRESH_MIN_INTERVAL_MS
  ) {
    if (STATE.subscriptionsSoftRefreshTimer) return;
  }

  if (STATE.subscriptionsSoftRefreshTimer) {
    clearTimeout(STATE.subscriptionsSoftRefreshTimer);
  }

  STATE.subscriptionsSoftRefreshTimer = setTimeout(() => {
    STATE.subscriptionsSoftRefreshTimer = null;

    const now2 = Date.now();
    if (
      !expedite &&
      now2 - (STATE.subscriptionsSoftRefreshLastAt || 0) <
        SUBS_SOFT_REFRESH_MIN_INTERVAL_MS
    )
      return;

    cancelIdle(STATE.subscriptionsSoftRefreshIdleHandle);
    STATE.subscriptionsSoftRefreshIdleHandle = scheduleIdle(async () => {
      STATE.subscriptionsSoftRefreshIdleHandle = null;

      const token = getAccessToken();
      if (!token) return;

      const now3 = Date.now();
      if (
        !expedite &&
        now3 - (STATE.subscriptionsSoftRefreshLastAt || 0) <
          SUBS_SOFT_REFRESH_MIN_INTERVAL_MS
      )
        return;

      STATE.subscriptionsSoftRefreshLastAt = now3;
      await loadSubscriptions({ force: true, silent: true });
    });
  }, debounceMs);
}

function applyServerSubscriptionFlags(content, flags) {
  const key = subKey(content);
  if (!key) return;

  if (!flags) {
    STATE.subscriptionsSet.delete(key);
    STATE.publicationSubscriptionsSet.delete(key);
  } else {
    const wantsCompletion = Boolean(flags.wants_completion);
    const wantsPublication = Boolean(flags.wants_publication);
    if (wantsCompletion) STATE.subscriptionsSet.add(key);
    else STATE.subscriptionsSet.delete(key);
    if (wantsPublication) STATE.publicationSubscriptionsSet.add(key);
    else STATE.publicationSubscriptionsSet.delete(key);
  }

  syncSubscribeModalUI(content);
  syncAllRenderedStarBadges();
  syncMySubListInPlace();
}

function syncSubscribeModalUI(content) {
  const modalKey = subKey(STATE.currentModalContent);
  const incomingKey = subKey(content);

  if (!STATE.subscribeModalOpen) return;
  if (!modalKey || !incomingKey || modalKey !== incomingKey) return;

  const modalState = STATE.subscribeModalState || { isLoading: false, loadFailed: false };
  const publicationSupported = supportsPublicationUI(content);
  const completionSubscribed =
    !modalState.isLoading && !modalState.loadFailed ? isCompletionSubscribed(content) : null;
  const publicationSubscribed =
    publicationSupported && !modalState.isLoading && !modalState.loadFailed
      ? isPublicationSubscribed(content)
      : null;
  const showLoadingState = modalState.isLoading;
  const showSubscribedState = completionSubscribed === true || publicationSubscribed === true;
  const shouldShowStateLine = showLoadingState || !modalState.loadFailed;

  if (UI.subscribeStateLine) {
    UI.subscribeStateLine.classList.toggle('hidden', !shouldShowStateLine);
  }

  if (UI.subscribeStateText) {
    if (showLoadingState) {
      UI.subscribeStateText.textContent = '불러오는 중';
    } else if (completionSubscribed && publicationSubscribed) {
      UI.subscribeStateText.textContent = '공개/완결 알림 구독 중';
    } else if (publicationSubscribed) {
      UI.subscribeStateText.textContent = '공개 알림 구독 중';
    } else if (completionSubscribed) {
      UI.subscribeStateText.textContent = '완결 알림 구독 중';
    } else {
      UI.subscribeStateText.textContent = '알림 구독 없음';
    }
  }
  if (UI.subscribeStateDot) {
    UI.subscribeStateDot.classList.remove('is-loading', 'is-active');
    if (showSubscribedState) UI.subscribeStateDot.classList.add('is-active');
    else if (showLoadingState) UI.subscribeStateDot.classList.add('is-loading');
  }

  const disabledClasses = UI_CLASSES.btnDisabled.split(' ');
  const shouldDisable = modalState.isLoading || STATE.subscribeToggleInFlight;
  const setButtonState = (btn, { label, isSubscribed }) => {
    if (!btn) return;
    if (shouldDisable) btn.classList.add(...disabledClasses);
    else btn.classList.remove(...disabledClasses);
    btn.disabled = shouldDisable;
    if (modalState.isLoading) {
      btn.innerHTML = `<span class="btn-spinner" aria-hidden="true"></span><span>${label}</span>`;
    } else {
      btn.textContent = label;
    }
    btn.dataset.subscribed = isSubscribed === null ? '' : isSubscribed ? '1' : '0';
    btn.classList.toggle('is-active', isSubscribed === true);
  };

  if (UI.subscribePublicationButton) {
    UI.subscribePublicationButton.classList.toggle('hidden', !publicationSupported);
    const publicationLabel = modalState.isLoading
      ? '불러오는 중'
      : modalState.loadFailed
        ? '다시 시도'
        : publicationSubscribed
          ? '공개 해제'
          : '공개 구독';
    setButtonState(UI.subscribePublicationButton, {
      label: publicationLabel,
      isSubscribed: publicationSubscribed,
    });
  }

  if (UI.subscribeCompletionButton) {
    UI.subscribeCompletionButton.classList.toggle('w-full', !publicationSupported);
    const completionLabel = modalState.isLoading
      ? '불러오는 중'
      : modalState.loadFailed
        ? '다시 시도'
        : completionSubscribed
          ? '완결 해제'
          : '완결 구독';
    setButtonState(UI.subscribeCompletionButton, {
      label: completionLabel,
      isSubscribed: completionSubscribed,
    });
  }
}

function syncMySubListInPlace() {
  if (!STATE.isMySubOpen) return;
  const root = document.getElementById('mySubscriptionsList') || UI.contentGrid;
  if (!root) return;

  root.querySelectorAll('[data-content-id][data-source]').forEach((cardEl) => {
    const contentId = cardEl.getAttribute('data-content-id');
    const source = cardEl.getAttribute('data-source');
    const key = source && contentId ? `${source}:${contentId}` : null;
    if (key && !STATE.subscriptionsSet.has(key)) {
      cardEl.remove();
    }
  });
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

const parseKstNaiveToDate = (publicAtStr) => {
  const raw = safeString(publicAtStr, '').trim();
  if (!raw) return null;
  const normalized = raw.replace('T', ' ').trim();
  const match =
    /^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/.exec(
      normalized
    );
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4] || 0);
  const minute = Number(match[5] || 0);
  const second = Number(match[6] || 0);
  const date = new Date(year, month - 1, day, hour, minute, second);
  if (Number.isNaN(date.getTime())) return null;
  return date;
};

const formatPublicAtShort = (publicAtStr, { includeTime = true } = {}) => {
  const date = parseKstNaiveToDate(publicAtStr);
  if (!date) return '';
  const pad = (val) => String(val).padStart(2, '0');
  const y = date.getFullYear();
  const m = pad(date.getMonth() + 1);
  const d = pad(date.getDate());
  if (!includeTime) return `${y}-${m}-${d}`;
  const hh = pad(date.getHours());
  const mm = pad(date.getMinutes());
  return `${y}-${m}-${d} ${hh}:${mm}`;
};

const getPublicationPublicAt = (item) =>
  safeString(
    item?.publication?.public_at ||
      item?.publication_at ||
      item?.publication?.publicAt ||
      item?.publicationAt,
    ''
  );

const buildPublicationStatusText = (item) => {
  const publicAt = getPublicationPublicAt(item);
  const date = parseKstNaiveToDate(publicAt);
  if (!date) return '공개일 미정';
  const now = new Date();
  if (date.getTime() > now.getTime()) {
    const formatted = formatPublicAtShort(publicAt, { includeTime: true });
    return formatted ? `공개 예정 · ${formatted}` : '공개일 미정';
  }
  const formatted = formatPublicAtShort(publicAt, { includeTime: false });
  return formatted ? `공개됨 · ${formatted}` : '공개일 미정';
};

const sortPublicationItems = (items) => {
  const list = Array.isArray(items) ? [...items] : [];
  list.sort((a, b) => {
    const aDate = parseKstNaiveToDate(getPublicationPublicAt(a));
    const bDate = parseKstNaiveToDate(getPublicationPublicAt(b));
    const aHas = Boolean(aDate);
    const bHas = Boolean(bDate);

    if (aHas && bHas) {
      const diff = aDate.getTime() - bDate.getTime();
      if (diff !== 0) return diff;
    } else if (aHas !== bHas) {
      return aHas ? -1 : 1;
    }

    const aTitle = safeString(a?.title, '');
    const bTitle = safeString(b?.title, '');
    const titleDiff = aTitle.localeCompare(bTitle, 'ko-KR');
    if (titleDiff !== 0) return titleDiff;

    const aId = String(a?.content_id ?? a?.contentId ?? a?.id ?? '');
    const bId = String(b?.content_id ?? b?.contentId ?? b?.id ?? '');
    return aId.localeCompare(bId, 'ko-KR');
  });
  return list;
};

function ensureEnhancedThemeAssets() {
  const homeButton = document.getElementById('homeButton');
  if (homeButton) {
    const logoImg = homeButton.querySelector('img');
    if (logoImg) {
      logoImg.alt = '콘텐츠 완결 알리미';
    }
  }

  if (UI.searchButton) UI.searchButton.setAttribute('aria-label', '검색');
  if (UI.aitSearchTrigger) UI.aitSearchTrigger.setAttribute('aria-label', '검색');
  if (UI.profileButton) UI.profileButton.setAttribute('aria-label', '프로필');
  if (UI.profileMenu) UI.profileMenu.setAttribute('aria-label', '프로필 메뉴');
  if (UI.searchBackButton) UI.searchBackButton.setAttribute('aria-label', '뒤로');
  if (UI.searchClearButton) UI.searchClearButton.setAttribute('aria-label', '검색어 지우기');
  if (UI.myPageBackBtn) UI.myPageBackBtn.setAttribute('aria-label', '뒤로');
  if (UI.myPageEntryButton) UI.myPageEntryButton.setAttribute('aria-label', '마이페이지');
}
const getThemePreference = () => {
  try {
    const saved = localStorage.getItem(THEME_STORAGE_KEY);
    return saved === 'dark' || saved === 'light' ? saved : null;
  } catch (e) {
    console.warn('Failed to read theme preference', e);
    return null;
  }
};

const applyThemePreference = (pref) => {
  const root = document.documentElement;
  if (!root) return;
  if (pref === 'dark' || pref === 'light') {
    root.dataset.theme = pref;
    return;
  }
  delete root.dataset.theme;
};

const isDarkEffective = () => {
  const explicit = document.documentElement?.dataset?.theme;
  if (explicit === 'dark') return true;
  if (explicit === 'light') return false;
  if (!window.matchMedia) return false;
  return window.matchMedia(DARK_MODE_MEDIA_QUERY).matches;
};

const updateThemeToggleLabel = () => {
  const toggle = UI.profileMenuThemeToggle;
  if (!toggle) return;
  const dark = isDarkEffective();
  toggle.textContent = dark ? '다크 모드: 켜짐' : '다크 모드: 꺼짐';
  toggle.setAttribute('aria-checked', dark ? 'true' : 'false');
};

const toggleTheme = () => {
  const nextPreference = isDarkEffective() ? 'light' : 'dark';
  try {
    localStorage.setItem(THEME_STORAGE_KEY, nextPreference);
  } catch (e) {
    console.warn('Failed to save theme preference', e);
  }
  applyThemePreference(nextPreference);
  updateThemeToggleLabel();
};

function setupThemePreferenceListeners() {
  if (window.matchMedia) {
    const media = window.matchMedia(DARK_MODE_MEDIA_QUERY);
    const handleSystemThemeChange = () => {
      if (getThemePreference()) return;
      updateThemeToggleLabel();
    };
    if (typeof media.addEventListener === 'function') {
      media.addEventListener('change', handleSystemThemeChange);
    } else if (typeof media.addListener === 'function') {
      media.addListener(handleSystemThemeChange);
    }
  }

  window.addEventListener('storage', (event) => {
    if (event.key !== THEME_STORAGE_KEY) return;
    applyThemePreference(getThemePreference());
    updateThemeToggleLabel();
  });
}

/* =========================
   App lifecycle
   ========================= */

async function initApp() {
  applyThemePreference(getThemePreference());
  ensureEnhancedThemeAssets();
  applyDataUiClasses();
  updateThemeToggleLabel();
  ensureKakaoThumbStyles();
  setupThemePreferenceListeners();
  setupAuthModalListeners();
  setupProfileButton();
  setupHomeButton();
  updateProfileButtonState();
  setupAuthReturnListeners();
  setupSearchHandlers();
  setupMyPageHandlers();
  setupMyPagePasswordChange();

  window.addEventListener('popstate', handleOverlayPopstate);

  if (UI.contentLoadMoreBtn) {
    UI.contentLoadMoreBtn.addEventListener('click', () => {
      const active = getActivePaginationCategory();
      if (active) loadNextPage(active);
    });
  }

  try {
    // preload subscriptions so stars render correctly (if token exists)
    await fetchMe();
    preloadSubscriptionsOnce();
  } catch (e) {
    console.warn('Failed to preload subscriptions', e);
  }

  const initialUIState = UIState.load();
  UIState.apply(initialUIState, { rerender: false, fetch: false });

  renderBottomNav();
  updateTab('home');
  setupScrollEffect();
  runDevSelfCheck();
}

document.addEventListener('DOMContentLoaded', () => {
  (function boot() {
    try {
      const maybePromise = initApp();
      if (maybePromise && typeof maybePromise.catch === 'function') {
        maybePromise.catch((err) => {
          console.error('[BOOT ERROR]', err);
          showFatalBanner();
        });
      }
    } catch (e) {
      console.error('[BOOT ERROR]', e);
      showFatalBanner();
    }
  })();
});

function setupScrollEffect() {
  if (!UI.filtersWrapper) return;

  const handleScroll = () => {
    const scrolled = window.scrollY > 10;
    UI.filtersWrapper.classList.toggle('is-scrolled', scrolled);
  };

  handleScroll();
  window.addEventListener('scroll', handleScroll);
}

/* =========================
   Search page
   ========================= */

const RECENT_SEARCH_KEY = 'es_recent_searches';
const MAX_RECENT_SEARCHES = 8;
const RECENTLY_OPENED_KEY = 'es_recently_opened';
const MAX_RECENTLY_OPENED = 12;
const RECENTLY_SEARCHED_CONTENTS_KEY = 'es_recently_searched_contents';
const MAX_RECENTLY_SEARCHED_CONTENTS = 12;
const HOME_RECOMMENDATIONS_LIMIT = 12;
const POPULAR_GRID_LIMIT = 9;
const KAKAO_THUMB_STYLE_ID = 'kakao-thumb-styles';
const GRID_LAYOUT_CLASS_TOKENS = ['grid', 'grid-cols-3', 'gap-2'];
const HOME_LAYOUT_CLASS_TOKENS = ['flex', 'flex-col', 'gap-6'];

function ensureKakaoThumbStyles() {
  if (document.getElementById(KAKAO_THUMB_STYLE_ID)) return;
  const style = document.createElement('style');
  style.id = KAKAO_THUMB_STYLE_ID;
  style.textContent = `
.kakaoStack {
  position: relative;
  overflow: hidden;
  width: 100%;
  height: 100%;
}
.kakaoStack::after {
  content: '';
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  height: 45%;
  background: linear-gradient(to top, var(--es-kakao-thumb-grad-from), var(--es-kakao-thumb-grad-to));
  pointer-events: none;
  z-index: 1;
}
.kakaoStack .thumbBg {
  width: 100%;
  height: 100%;
  object-fit: cover;
  object-position: center top;
  display: block;
}
.kakaoStack .thumbChar {
  position: absolute;
  left: 0;
  top: 0;
  width: 100%;
  height: 100%;
  object-fit: contain;
  object-position: center bottom;
  pointer-events: none;
  z-index: 2;
}
.kakaoStack .thumbTitle {
  position: absolute;
  left: 0;
  bottom: 0;
  width: 100%;
  height: 36%;
  object-fit: contain;
  object-position: center bottom;
  pointer-events: none;
  z-index: 3;
  padding: 0 6px 6px 6px;
  box-sizing: border-box;
}
.kakaoTitleImg {
  width: 100%;
  max-width: 167px;
  height: auto;
  display: block;
  filter: drop-shadow(0 2px 6px rgba(0,0,0,0.85));
}
@media (min-width: 1024px) {
  .kakaoTitleImg {
    max-width: 218px;
  }
}
`;
  document.head.appendChild(style);
}

const getSearchType = () =>
  STATE.activeTab === 'my' || STATE.activeTab === 'home'
    ? STATE.lastBrowseTab || 'webtoon'
    : STATE.activeTab || 'webtoon';

const getSearchSource = (type) => {
  if (['webtoon', 'novel', 'ott'].includes(type)) {
    const sources = getSelectedSourcesForTab(type);
    return sources.length === 1 ? sources[0] : 'all';
  }
  return 'all';
};

const getAspectByType = (type) => {
  if (type === 'novel') return 'aspect-[1/1.4]';
  if (type === 'ott') return 'aspect-[2/3]';
  return 'aspect-[3/4]';
};

let searchViewportCleanup = null;
let myPageViewportCleanup = null;

const applySearchPageViewportHeight = () => {
  if (!UI.searchPage) return;
  const viewportHeight = window.visualViewport?.height;
  if (viewportHeight) {
    const unit = viewportHeight * 0.01;
    UI.searchPage.style.setProperty('--vvh', `${unit}px`);
    UI.searchPage.style.height = 'calc(var(--vvh, 1vh) * 100)';
  } else {
    UI.searchPage.style.removeProperty('--vvh');
    UI.searchPage.style.height = '100dvh';
  }
};

const startSearchViewportSync = () => {
  applySearchPageViewportHeight();
  if (!window.visualViewport) {
    searchViewportCleanup = null;
    return;
  }

  const handler = () => applySearchPageViewportHeight();
  window.visualViewport.addEventListener('resize', handler);
  window.visualViewport.addEventListener('scroll', handler);
  searchViewportCleanup = () => {
    window.visualViewport.removeEventListener('resize', handler);
    window.visualViewport.removeEventListener('scroll', handler);
  };
};

const stopSearchViewportSync = () => {
  if (searchViewportCleanup) searchViewportCleanup();
  searchViewportCleanup = null;
  if (UI.searchPage) {
    UI.searchPage.style.removeProperty('--vvh');
    UI.searchPage.style.removeProperty('height');
  }
};

const applyMyPageViewportHeight = () => {
  if (!UI.myPage) return;
  const viewportHeight = window.visualViewport?.height;
  if (viewportHeight) {
    const unit = viewportHeight * 0.01;
    UI.myPage.style.setProperty('--vvh', `${unit}px`);
    UI.myPage.style.height = 'calc(var(--vvh, 1vh) * 100)';
  } else {
    UI.myPage.style.removeProperty('--vvh');
    UI.myPage.style.height = '100dvh';
  }
};

const startMyPageViewportSync = () => {
  applyMyPageViewportHeight();
  if (myPageViewportCleanup) myPageViewportCleanup();

  if (!window.visualViewport) {
    myPageViewportCleanup = null;
    return;
  }

  const handler = () => applyMyPageViewportHeight();
  window.visualViewport.addEventListener('resize', handler);
  window.visualViewport.addEventListener('scroll', handler);
  myPageViewportCleanup = () => {
    window.visualViewport.removeEventListener('resize', handler);
    window.visualViewport.removeEventListener('scroll', handler);
  };
};

const stopMyPageViewportSync = () => {
  if (myPageViewportCleanup) myPageViewportCleanup();
  myPageViewportCleanup = null;
  if (UI.myPage) {
    UI.myPage.style.removeProperty('--vvh');
    UI.myPage.style.removeProperty('height');
  }
};

const loadRecentSearches = () => {
  try {
    const raw = localStorage.getItem(RECENT_SEARCH_KEY);
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed.filter((v) => typeof v === 'string');
  } catch (e) {
    console.warn('Failed to load recent searches', e);
  }
  return [];
};

const normalizeSearchTerm = (term) =>
  (term || '')
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();

const saveRecentSearches = (list) => {
  try {
    localStorage.setItem(RECENT_SEARCH_KEY, JSON.stringify(list.slice(0, MAX_RECENT_SEARCHES)));
  } catch (e) {
    console.warn('Failed to save recent searches', e);
  }
};

const loadRecentlyOpened = () => {
  try {
    const raw = localStorage.getItem(RECENTLY_OPENED_KEY);
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed
        .map((entry) => ({
          key: safeString(entry?.key, ''),
          content: entry?.content,
          openedAt: Number(entry?.openedAt) || 0,
        }))
        .filter((entry) => entry.key && entry.content);
    }
  } catch (e) {
    console.warn('Failed to load recently opened', e);
  }
  return [];
};

const saveRecentlyOpened = (list) => {
  try {
    localStorage.setItem(RECENTLY_OPENED_KEY, JSON.stringify(list.slice(0, MAX_RECENTLY_OPENED)));
  } catch (e) {
    console.warn('Failed to save recently opened', e);
  }
};

const buildRecentContentSnapshot = (content) => {
  const normalizedMeta = normalizeMeta(content?.meta);
  const thumb =
    normalizedMeta?.common?.thumbnail_url ||
    content?.normalized_thumbnail ||
    content?.thumbnail_url ||
    content?.thumbnail ||
    '';
  const authors = Array.isArray(normalizedMeta?.common?.authors)
    ? normalizedMeta.common.authors
    : [];

  return {
    title: safeString(content?.title, ''),
    status: safeString(content?.status, ''),
    source: content?.source || '',
    content_id: content?.content_id || content?.contentId || content?.id,
    id: content?.id || content?.content_id || content?.contentId,
    content_type:
      safeString(content?.content_type || content?.contentType || content?.type, '') ||
      getContentType(content),
    meta: {
      common: {
        ...safeObj(normalizedMeta?.common),
        thumbnail_url: thumb,
        authors,
      },
    },
  };
};

const recordRecentlyOpened = (content) => {
  const key = buildSubscriptionKey(content) || contentKey(content);
  if (!key) return;

  const snapshot = buildRecentContentSnapshot(content);
  const existing = loadRecentlyOpened();
  const next = [
    { key, content: snapshot, openedAt: Date.now() },
    ...existing.filter((entry) => entry.key !== key),
  ].slice(0, MAX_RECENTLY_OPENED);

  STATE.search.recentlyOpened = next;
  saveRecentlyOpened(next);
  renderPopularGrid();
};

const coerceRecentlySearchedEntry = (entry) => {
  const rawContent = entry?.content || entry;
  const snapshot = buildRecentContentSnapshot(rawContent);
  const key = safeString(entry?.key, '') || buildSubscriptionKey(snapshot) || contentKey(snapshot);
  const openedAt = Number(entry?.openedAt) || 0;
  if (!key || !snapshot?.content_id || !snapshot?.source) return null;
  return { key, content: snapshot, openedAt };
};

const loadRecentlySearchedContents = () => {
  try {
    const raw = localStorage.getItem(RECENTLY_SEARCHED_CONTENTS_KEY);
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.map((entry) => coerceRecentlySearchedEntry(entry)).filter(Boolean);
  } catch (e) {
    console.warn('Failed to load recently searched contents', e);
    return [];
  }
};

const saveRecentlySearchedContents = (list) => {
  try {
    const safeList = Array.isArray(list) ? list.slice(0, MAX_RECENTLY_SEARCHED_CONTENTS) : [];
    localStorage.setItem(RECENTLY_SEARCHED_CONTENTS_KEY, JSON.stringify(safeList));
  } catch (e) {
    console.warn('Failed to save recently searched contents', e);
  }
};

const recordRecentlySearchedContent = (content) => {
  const snapshot = buildRecentContentSnapshot(content);
  const key = buildSubscriptionKey(snapshot) || contentKey(snapshot);
  if (!key || !snapshot?.content_id || !snapshot?.source) return;

  const existing = loadRecentlySearchedContents();
  const next = [
    { key, content: snapshot, openedAt: Date.now() },
    ...existing.filter((entry) => entry.key !== key),
  ].slice(0, MAX_RECENTLY_SEARCHED_CONTENTS);

  saveRecentlySearchedContents(next);
};

const renderRecentSearches = () => {
  const chips = UI.searchRecentChips;
  if (!chips) return;
  const list = loadRecentSearches();
  chips.innerHTML = '';

  if (!list.length) {
    const empty = document.createElement('div');
    setClasses(empty, UI_CLASSES.sectionSubtle);
    empty.textContent = '최근 검색어가 없습니다';
    chips.appendChild(empty);
    return;
  }

  list.slice(0, MAX_RECENT_SEARCHES).forEach((query) => {
    const wrapper = setClasses(document.createElement('div'), UI_CLASSES.chip);

    const labelBtn = document.createElement('button');
    labelBtn.type = 'button';
    labelBtn.className = 'truncate text-left';
    labelBtn.textContent = query;
    labelBtn.onclick = () => {
      if (UI.searchPageInput) {
        UI.searchPageInput.value = query;
        performSearch(query);
        UI.searchPageInput.focus();
      }
    };

    const deleteBtn = document.createElement('button');
    deleteBtn.type = 'button';
    deleteBtn.setAttribute('aria-label', '최근 검색어 삭제');
    deleteBtn.className = 'es-icon-btn es-icon-btn-xs';
    deleteBtn.textContent = '×';
    deleteBtn.onclick = (evt) => {
      evt.stopPropagation();
      removeRecentSearch(query);
    };

    wrapper.appendChild(labelBtn);
    wrapper.appendChild(deleteBtn);
    chips.appendChild(wrapper);
  });
};

const addRecentSearch = (query) => {
  const q = (query || '').trim();
  if (!q) return;
  const list = loadRecentSearches();
  const normalized = normalizeSearchTerm(q);
  const filtered = list.filter((item) => normalizeSearchTerm(item) !== normalized);
  filtered.unshift(q);
  const next = filtered.slice(0, MAX_RECENT_SEARCHES);
  saveRecentSearches(next);
  renderRecentSearches();
};

const clearRecentSearches = () => {
  saveRecentSearches([]);
  renderRecentSearches();
};

const removeRecentSearch = (query) => {
  const list = loadRecentSearches();
  const filtered = list.filter((item) => item.toLowerCase() !== query.toLowerCase());
  saveRecentSearches(filtered);
  renderRecentSearches();
};

const normalizeContentForGrid = (content, fallbackSource) => {
  const normalizedMeta = normalizeMeta(content?.meta);
  const normalizedType = safeString(content?.content_type || content?.contentType || content?.type, '');
  const normalized = {
    ...content,
    meta: normalizedMeta,
    title: safeString(content?.title, ''),
    status: safeString(content?.status, ''),
    content_id: content?.content_id || content?.contentId || content?.id,
    id: content?.id || content?.content_id || content?.contentId,
    source: content?.source || fallbackSource || '',
    content_type: normalizedType || undefined,
  };
  normalized.__search_title_n = normalizeSearchText(normalized.title);
  normalized.__search_alt_n = normalizeSearchText(
    safeString(
      content?.alt_title ||
        content?.subtitle ||
        normalizedMeta?.common?.alt_title ||
        normalizedMeta?.common?.title_alias,
      ''
    )
  );
  return normalized;
};

const matchesSearchQuery = (content, normalizedQuery) => {
  if (!normalizedQuery) return false;
  const titleN = content?.__search_title_n ?? normalizeSearchText(content?.title);
  const altN =
    content?.__search_alt_n ??
    normalizeSearchText(
      safeString(
        content?.alt_title ||
          content?.subtitle ||
          content?.meta?.common?.alt_title ||
          content?.meta?.common?.title_alias,
        ''
      )
    );

  return Boolean((titleN && titleN.includes(normalizedQuery)) || (altN && altN.includes(normalizedQuery)));
};

const shuffleArray = (arr) => {
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
};

const renderPopularGrid = () => {
  const grid = UI.searchPopularGrid;
  if (!grid) return;

  grid.innerHTML = '';
  const tabId = getSearchType();
  const aspectClass = getAspectByType(tabId);
  const recent = (STATE.search.recentlyOpened.length
    ? STATE.search.recentlyOpened
    : loadRecentlyOpened()
  ).sort((a, b) => (b.openedAt || 0) - (a.openedAt || 0));

  if (!STATE.search.recentlyOpened.length) {
    STATE.search.recentlyOpened = recent;
  }

  const useRecent = recent.length > 0;

  if (UI.searchPopularTitle) {
    UI.searchPopularTitle.textContent = useRecent ? '최근 본 작품' : '추천 작품';
  }
  if (UI.searchPopularSubtitle) {
    UI.searchPopularSubtitle.textContent = useRecent
      ? '최근에 열어본 작품이 여기에 표시됩니다.'
      : '최근 열어본 작품이 없어서 추천 작품을 보여드려요.';
    UI.searchPopularSubtitle.classList.toggle('hidden', false);
  }

  const items = useRecent
    ? recent
        .map((entry) => normalizeContentForGrid(entry?.content, getSearchSource(tabId)))
        .filter((item) => item?.content_id && item?.source)
        .slice(0, POPULAR_GRID_LIMIT)
    : (() => {
        const pool = Array.isArray(STATE.rendering?.list) ? [...STATE.rendering.list] : [];
        if (!pool.length) return [];
        return shuffleArray(pool)
          .slice(0, 30)
          .slice(0, POPULAR_GRID_LIMIT)
          .map((item) => normalizeContentForGrid(item, getSearchSource(tabId)));
      })();

  if (!items.length) {
    const placeholder = document.createElement('div');
    placeholder.className = 'text-sm es-muted col-span-full text-center py-8';
    placeholder.textContent = '추천 작품을 불러오지 못했습니다.';
    grid.appendChild(placeholder);
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item, idx) => {
    const card = createCard(item, tabId, aspectClass);
    card.dataset.searchIndex = String(idx);
    fragment.appendChild(card);
  });

  grid.appendChild(fragment);
};

function setSearchUiMode(mode) {
  STATE.search.uiMode = mode;
  const showIdle = mode === 'idle' || mode === 'no_results';
  if (UI.searchIdle) UI.searchIdle.classList.toggle('hidden', !showIdle);
  if (UI.searchResultsView) UI.searchResultsView.classList.toggle('hidden', mode === 'idle');
  if (UI.searchPageLoading) UI.searchPageLoading.classList.toggle('hidden', mode !== 'loading');
  if (UI.searchPageResults) UI.searchPageResults.classList.toggle('hidden', mode !== 'results');
  if (UI.searchPageEmpty) UI.searchPageEmpty.classList.toggle('hidden', mode !== 'no_results');
  if (UI.searchResultsMeta) UI.searchResultsMeta.classList.toggle('hidden', mode !== 'results');
}

const showSearchIdle = () => {
  STATE.search.activeIndex = -1;
  setActiveSearchIndex(-1);
  STATE.search.results = [];
  if (STATE.searchRenderAbort) STATE.searchRenderAbort.abort();
  setSearchUiMode('idle');
  if (UI.searchPageResults) UI.searchPageResults.innerHTML = '';
  if (UI.searchPageEmpty) UI.searchPageEmpty.classList.add('hidden');
  if (UI.searchResultCount) UI.searchResultCount.textContent = '0';
  renderRecentSearches();
  renderPopularGrid();
};

function showSearchEmpty(title, { message = '', actions = [] } = {}) {
  STATE.search.activeIndex = -1;
  setActiveSearchIndex(-1);
  STATE.search.results = [];
  if (STATE.searchRenderAbort) STATE.searchRenderAbort.abort();
  setSearchUiMode('no_results');
  if (UI.searchPageResults) {
    UI.searchPageResults.classList.add('hidden');
    UI.searchPageResults.innerHTML = '';
  }
  if (UI.searchPageLoading) UI.searchPageLoading.classList.add('hidden');
  if (UI.searchResultCount) UI.searchResultCount.textContent = '0';

  if (UI.searchEmptyTitle) UI.searchEmptyTitle.textContent = title || '검색 결과가 없어요';
  if (UI.searchEmptySubtitle) UI.searchEmptySubtitle.textContent = message || '다른 키워드로 검색해보세요.';

  if (UI.searchEmptyActions) {
    UI.searchEmptyActions.innerHTML = '';
    const hasActions = Array.isArray(actions) && actions.length;
    UI.searchEmptyActions.classList.toggle('hidden', !hasActions);
    if (hasActions) {
      const fragment = document.createDocumentFragment();
      actions.forEach((action) => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = action?.label || '';
        btn.dataset.ui = action?.variant === 'primary' ? 'btn-primary' : 'btn-secondary';
        btn.onclick = () => {
          if (typeof action?.onClick === 'function') action.onClick();
        };
        fragment.appendChild(btn);
      });
      UI.searchEmptyActions.appendChild(fragment);
      applyDataUiClasses(UI.searchEmptyActions);
    }
  }

  renderRecentSearches();
  renderPopularGrid();
}

function buildSearchEmptyActions() {
  const clearAction = {
    label: '검색어 지우기',
    variant: 'secondary',
    onClick: () => {
      if (UI.searchPageInput) {
        UI.searchPageInput.value = '';
      }
      STATE.search.query = '';
      updateSearchClearButton();
      performSearch('');
      if (UI.searchPageInput) UI.searchPageInput.focus();
    },
  };

  const recommendAction = {
    label: '추천 작품 보기',
    variant: 'primary',
    onClick: () => {
      if (UI.searchPageInput) {
        UI.searchPageInput.value = '';
      }
      STATE.search.query = '';
      updateSearchClearButton();
      performSearch('');
      requestAnimationFrame(() => {
        if (UI.searchPopularGrid) {
          UI.searchPopularGrid.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      });
    },
  };

  return [clearAction, recommendAction];
}

const SEARCH_ACTIVE_CLASSES = ['shadow-sm'];

const getSearchResultElements = () => {
  if (!UI.searchPageResults) return [];
  return Array.from(UI.searchPageResults.querySelectorAll('[data-search-index]'));
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

const getActiveSearchOptionEl = () => {
  const elements = getSearchResultElements();
  const idx = STATE.search.activeIndex;
  if (!elements.length || idx < 0 || idx >= elements.length) return null;
  return elements[idx];
};

function openActiveSearchResult() {
  const el = getActiveSearchOptionEl();
  if (!el) return;
  const content = el.__content;
  if (!content) return;
  openSubscribeModal(content, { returnFocusEl: el });
}

function renderSearchLoading(type) {
  setSearchUiMode('loading');
  if (UI.searchPageResults) {
    UI.searchPageResults.classList.add('hidden');
    UI.searchPageResults.innerHTML = '';
  }

  const container = UI.searchPageLoading;
  if (!container) return;
  container.classList.remove('hidden');
  container.innerHTML = '';
  void type;
  for (let i = 0; i < 6; i += 1) {
    const item = document.createElement('div');
    item.className = 'h-[92px] rounded-2xl skeleton';
    container.appendChild(item);
  }
}

function updateSearchClearButton() {
  if (!UI.searchClearButton || !UI.searchPageInput) return;
  UI.searchClearButton.classList.toggle('hidden', !UI.searchPageInput.value.trim());
}

async function renderSearchResults(items, effectiveType) {
  const grid = UI.searchPageResults;
  if (!grid) return;

  setSearchUiMode('results');

  STATE.search.activeIndex = -1;
  grid.innerHTML = '';
  grid.classList.remove('hidden');
  grid.setAttribute('role', 'listbox');
  grid.setAttribute('aria-label', '검색 결과');
  const aspectClass = getAspectByType(effectiveType);

  if (STATE.searchRenderAbort) STATE.searchRenderAbort.abort();
  const renderController = new AbortController();
  STATE.searchRenderAbort = renderController;

  const normalizedItems = Array.isArray(items)
    ? items.map((raw) => normalizeContentForGrid(raw, getSearchSource(effectiveType)))
    : [];
  STATE.search.results = normalizedItems;
  if (UI.searchResultCount) UI.searchResultCount.textContent = String(normalizedItems.length || 0);

  if (!normalizedItems.length) {
    const queryText = (STATE.search.query || '').trim();
    const subtitle = queryText
      ? `"${queryText}"에 대한 결과를 찾지 못했어요. 띄어쓰기를 바꿔 보거나, 더 짧은 키워드로 검색해 보세요.`
      : '다른 키워드로 검색해보세요.';
    showSearchEmpty('검색 결과가 없어요', {
      message: subtitle,
      actions: buildSearchEmptyActions(),
    });
    return;
  }

  await renderInBatches({
    items: normalizedItems,
    container: grid,
    signal: renderController.signal,
    renderItem: (normalized, idx) => {
      const card = createCard(normalized, effectiveType, aspectClass);
      card.dataset.searchIndex = String(idx);
      card.setAttribute('role', 'option');
      card.setAttribute('aria-selected', 'false');
      card.__content = normalized;
      card.addEventListener('mouseenter', () => setActiveSearchIndex(idx));
      card.onclick = () => {
        setActiveSearchIndex(idx);
        openSubscribeModal(normalized, { returnFocusEl: card });
      };
      return card;
    },
  });

  if (!renderController.signal.aborted) {
    setActiveSearchIndex(normalizedItems.length ? 0 : -1);
  }
}

async function performSearch(q) {
  const query = (q || '').trim();
  const normalizedQuery = normalizeSearchText(query);
  const hasWhitespace = /\s/.test(query);
  const effectiveType = 'all';
  const source = 'all';

  STATE.search.query = query;
  STATE.search.activeIndex = -1;
  setActiveSearchIndex(-1);
  updateSearchClearButton();

  if (STATE.searchRenderAbort) STATE.searchRenderAbort.abort();
  if (STATE.searchAbortController) STATE.searchAbortController.abort();
  STATE.searchAbortController = null;

  if (!query) {
    STATE.search.requestSeq += 1;
    if (UI.searchPageLoading) UI.searchPageLoading.classList.add('hidden');
    showSearchIdle();
    return;
  }

  const seq = ++STATE.search.requestSeq;
  STATE.search.isLoading = true;

  const controller = new AbortController();
  STATE.searchAbortController = controller;

  renderSearchLoading(effectiveType);

  try {
    const res = await apiRequest('GET', '/api/contents/search', {
      query: { q: query, type: effectiveType, source },
      signal: controller.signal,
    });

    if (seq !== STATE.search.requestSeq) return;
    const items = Array.isArray(res?.data) ? res.data : Array.isArray(res) ? res : [];
    let normalizedItems = items;

    if ((!normalizedItems.length && hasWhitespace) || (!items.length && hasWhitespace)) {
      const pool = Array.isArray(STATE.rendering?.list) ? STATE.rendering.list : [];
      const fallbackResults = pool.filter((item) => matchesSearchQuery(item, normalizedQuery));
      if (fallbackResults.length) {
        await renderSearchResults(fallbackResults, effectiveType);
        addRecentSearch(query);
        return;
      }
    }

    await renderSearchResults(normalizedItems, effectiveType);
    addRecentSearch(query);
  } catch (e) {
    if (controller.signal.aborted || seq !== STATE.search.requestSeq) return;
    showToast(e?.message || '검색에 실패했습니다.', { type: 'error' });
    showSearchEmpty('검색 결과가 없어요', {
      message: '다른 키워드로 검색해보세요.',
      actions: buildSearchEmptyActions(),
    });
  } finally {
    if (seq !== STATE.search.requestSeq) return;
    STATE.search.isLoading = false;
    if (UI.searchPageLoading) UI.searchPageLoading.classList.add('hidden');
  }
}

function debouncedSearch(q) {
  if (STATE.search.debounceTimer) clearTimeout(STATE.search.debounceTimer);
  STATE.search.debounceTimer = setTimeout(() => performSearch(q), 300);
}

function openSearchPage({ focus = true } = {}) {
  if (!UI.searchPage) return;

  saveScroll(getCurrentScrollViewKey());
  UIState.save();

  const wasOpen = STATE.search.pageOpen;
  if (!wasOpen) {
    STATE.search.pageOpen = true;
    lockBodyScroll();
    startSearchViewportSync();
    pushOverlayState('search');
  } else {
    STATE.search.pageOpen = true;
  }
  UI.searchPage.classList.remove('hidden');

  if (UI.searchPageInput) {
    UI.searchPageInput.value = STATE.search.query || '';
    updateSearchClearButton();
  }

  if (STATE.search.query) {
    if (STATE.search.results.length) renderSearchResults(STATE.search.results, 'all');
    else performSearch(STATE.search.query);
  } else {
    showSearchIdle();
  }

  restoreScroll('search', { container: UI.searchPageResults, requireChildren: false });

  if (focus && UI.searchPageInput) {
    requestAnimationFrame(() => UI.searchPageInput.focus());
  }
}

function closeSearchPage({ fromPopstate = false, overlayId = null } = {}) {
  if (!STATE.search.pageOpen) {
    popOverlayState('search', overlayId);
    ensureScrollLockConsistency();
    return;
  }
  if (!fromPopstate) {
    const top = getOverlayStackTop();
    if (top?.overlay === 'search') {
      history.back();
      return;
    }
  }

  saveScroll('search');
  STATE.search.pageOpen = false;
  STATE.search.activeIndex = -1;
  setActiveSearchIndex(-1);
  if (UI.searchPage) UI.searchPage.classList.add('hidden');
  stopSearchViewportSync();
  unlockBodyScroll();

  UIState.apply(UIState.load(), { rerender: true, fetch: false });
  restoreScroll(getScrollViewKeyForTab(STATE.activeTab), {
    container: UI.contentGrid,
    requireChildren: true,
  });
  popOverlayState('search', overlayId);
  ensureScrollLockConsistency();
}

function openSearchAndFocus() {
  openSearchPage({ focus: true });
}

function setupSearchHandlers() {
  renderRecentSearches();

  if (UI.searchButton) UI.searchButton.onclick = () => openSearchPage({ focus: true });
  if (UI.aitSearchTrigger) UI.aitSearchTrigger.onclick = () => openSearchPage({ focus: true });

  if (UI.searchInput) {
    UI.searchInput.addEventListener('focus', () => openSearchPage({ focus: true }));
    UI.searchInput.addEventListener('click', () => openSearchPage({ focus: true }));
    UI.searchInput.addEventListener('keydown', (evt) => {
      evt.preventDefault();
      openSearchPage({ focus: true });
    });
  }

  if (UI.searchBackButton) UI.searchBackButton.onclick = () => closeSearchPage();

  if (UI.searchClearButton)
    UI.searchClearButton.onclick = () => {
      if (UI.searchPageInput) {
        UI.searchPageInput.value = '';
        STATE.search.query = '';
        performSearch('');
        UI.searchPageInput.focus();
        updateSearchClearButton();
      }
    };

  if (UI.searchRecentClearAll) UI.searchRecentClearAll.onclick = () => clearRecentSearches();

  if (UI.searchPageInput) {
    UI.searchPageInput.addEventListener('input', (evt) => {
      updateSearchClearButton();
      STATE.search.activeIndex = -1;
      setActiveSearchIndex(-1);
      debouncedSearch(evt.target.value);
    });

    UI.searchPageInput.addEventListener('keydown', (evt) => {
      if (!STATE.search.pageOpen) return;
      const elements = getSearchResultElements();
      const hasResults = elements.length > 0;

      if (evt.key === 'ArrowDown') {
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
      } else if (evt.key === 'Escape') {
        closeSearchPage();
      }
    });
  }

  document.addEventListener('keydown', (evt) => {
    if (isAnyModalOpen()) {
      if ((evt.ctrlKey || evt.metaKey) && evt.key.toLowerCase() === 'k') evt.preventDefault();
      return;
    }
    if (evt.key === 'Escape') {
      if (STATE.search.pageOpen) requestCloseOverlay('search');
      else if (STATE.isMyPageOpen) requestCloseOverlay('myPage');
    } else if ((evt.ctrlKey || evt.metaKey) && evt.key.toLowerCase() === 'k') {
      evt.preventDefault();
      openSearchPage({ focus: true });
    }
  });
}

/* =========================
   My page
   ========================= */

function formatKstDateTime(dt) {
  if (!dt) return '';
  try {
    const date = new Date(dt);
    if (Number.isNaN(date.getTime())) return '';

    const parts = new Intl.DateTimeFormat('ko-KR', {
      timeZone: 'Asia/Seoul',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    })
      .formatToParts(date)
      .reduce((acc, part) => {
        if (part.type !== 'literal') acc[part.type] = part.value;
        return acc;
      }, {});

    const year = parts.year || '';
    const month = parts.month || '';
    const day = parts.day || '';
    const hour = parts.hour || '';
    const minute = parts.minute || '';
    if (!year || !month || !day || !hour || !minute) return '';

    return `${year}-${month}-${day} ${hour}:${minute}`;
  } catch (e) {
    console.warn('Failed to format datetime', dt, e);
    return '';
  }
}

function renderMyPageEmail(user = {}) {
  if (UI.myPageEmailValue) UI.myPageEmailValue.textContent = safeString(user?.email, '-') || '-';

  if (UI.myPageCreatedAtRow && UI.myPageCreatedAtValue) {
    const formatted = formatKstDateTime(user?.created_at);
    if (formatted) {
      UI.myPageCreatedAtValue.textContent = formatted;
      UI.myPageCreatedAtRow.classList.remove('hidden');
    } else {
      UI.myPageCreatedAtValue.textContent = '';
      UI.myPageCreatedAtRow.classList.add('hidden');
    }
  }
}

async function fetchMyPageUser() {
  const token = getAccessToken();

  if (!token) {
    handleMyPageUnauthorized();
    return;
  }

  try {
    const res = await apiRequest('GET', '/api/auth/me', { token });
    const user = res?.data?.user || res?.user || null;

    STATE.auth.user = user;
    STATE.auth.isAuthenticated = Boolean(user || token);
    renderMyPageEmail(user || {});
    updateProfileButtonState();
  } catch (e) {
    if (e?.httpStatus === 401 || e?.httpStatus === 403) {
      STATE.auth.isAuthenticated = false;
      STATE.auth.user = null;
      updateProfileButtonState();
      handleMyPageUnauthorized();
    } else {
      console.warn('Failed to load my page info', e);
      showToast('계정 정보를 불러오지 못했습니다.', { type: 'error' });
    }
  }
}

function handleMyPageUnauthorized() {
  closeMyPage({ fromPopstate: true });
  openAuthModal({ reason: 'my-page' });
}

function setMyPagePwError(message = '') {
  if (UI.myPagePwError) UI.myPagePwError.textContent = message || '';
}

function resetMyPagePasswordForm() {
  if (UI.myPagePwCurrent) UI.myPagePwCurrent.value = '';
  if (UI.myPagePwNew) UI.myPagePwNew.value = '';
  if (UI.myPagePwConfirm) UI.myPagePwConfirm.value = '';
  setMyPagePwError('');
}

function setMyPagePwSubmitting(isSubmitting) {
  if (!UI.myPagePwSubmit) return;
  UI.myPagePwSubmit.disabled = isSubmitting;
  UI.myPagePwSubmit.textContent = isSubmitting ? '변경 중...' : '변경하기';
}

async function handleMyPageChangePassword() {
  const currentPassword = (UI.myPagePwCurrent?.value || '').trim();
  const newPassword = (UI.myPagePwNew?.value || '').trim();
  const confirmPassword = (UI.myPagePwConfirm?.value || '').trim();

  setMyPagePwError('');

  if (!currentPassword) {
    setMyPagePwError('현재 비밀번호를 입력해주세요.');
    return;
  }

  if (!newPassword) {
    setMyPagePwError('새 비밀번호를 입력해주세요.');
    return;
  }

  if (newPassword.length < 8) {
    setMyPagePwError('비밀번호는 8자 이상이어야 합니다.');
    return;
  }

  if (newPassword !== confirmPassword) {
    setMyPagePwError('새 비밀번호가 일치하지 않습니다.');
    return;
  }

  const token = getAccessToken();
  if (!token) {
    handleMyPageUnauthorized();
    return;
  }

  setMyPagePwSubmitting(true);

  try {
    await apiRequest('POST', '/api/auth/change-password', {
      body: { current_password: currentPassword, new_password: newPassword },
      token,
    });

    showToast('비밀번호가 변경되었습니다.', { type: 'success' });
    resetMyPagePasswordForm();
  } catch (e) {
    if (e?.httpStatus === 401) {
      handleMyPageUnauthorized();
      return;
    }

    let message = '비밀번호 변경에 실패했습니다.';
    if (e?.code === 'INVALID_PASSWORD') {
      message = '현재 비밀번호가 올바르지 않습니다.';
    } else if (e?.code === 'WEAK_PASSWORD' || e?.code === 'PASSWORD_TOO_SHORT') {
      message = '비밀번호는 8자 이상이어야 합니다.';
    } else if (e?.code === 'INVALID_INPUT') {
      message = '비밀번호를 다시 확인해주세요.';
    }

    setMyPagePwError(message);
    showToast(message, { type: 'error' });
  } finally {
    setMyPagePwSubmitting(false);
  }
}

function openMyPage() {
  if (!UI.myPage) return;

  if (!STATE.auth.isAuthenticated) {
    openAuthModal({ reason: 'my-page' });
    return;
  }

  const wasOpen = STATE.isMyPageOpen;
  if (!wasOpen) {
    STATE.isMyPageOpen = true;
    lockBodyScroll();
    startMyPageViewportSync();
    pushOverlayState('myPage');
  } else {
    STATE.isMyPageOpen = true;
  }

  UI.myPage.classList.remove('hidden');
  renderBottomNav();

  if (STATE.auth.user) renderMyPageEmail(STATE.auth.user);
  fetchMyPageUser();
}

function closeMyPage({ fromPopstate = false, overlayId = null } = {}) {
  if (!STATE.isMyPageOpen) {
    popOverlayState('myPage', overlayId);
    ensureScrollLockConsistency();
    return;
  }

  if (!fromPopstate) {
    const top = getOverlayStackTop();
    if (top?.overlay === 'myPage') {
      history.back();
      return;
    }
  }

  STATE.isMyPageOpen = false;
  if (UI.myPage) UI.myPage.classList.add('hidden');
  stopMyPageViewportSync();
  unlockBodyScroll();
  if (UI.profileButton) UI.profileButton.focus();
  else if (UI.myPageEntryButton) UI.myPageEntryButton.focus();
  popOverlayState('myPage', overlayId);
  ensureScrollLockConsistency();
  renderBottomNav();
}

function setupMyPageHandlers() {
  if (UI.myPageBackBtn) UI.myPageBackBtn.onclick = () => closeMyPage();

  if (UI.profileMenuMyPage) {
    UI.profileMenuMyPage.onclick = () => {
      closeProfileMenu();
      openMyPage();
    };
  }

  if (UI.myPageEntryButton) {
    UI.myPageEntryButton.onclick = () => {
      openMyPage();
    };
  }

  if (UI.myPageGoMySubBtn) {
    UI.myPageGoMySubBtn.onclick = () => {
      closeMyPage({ fromPopstate: true });
      updateTab('my');
    };
  }

  if (UI.myPageLogoutBtn) {
    UI.myPageLogoutBtn.onclick = () => {
      closeMyPage({ fromPopstate: true });
      logout();
    };
  }
}

function setupMyPagePasswordChange() {
  if (UI.myPagePwSubmit) {
    UI.myPagePwSubmit.addEventListener('click', (evt) => {
      evt.preventDefault();
      handleMyPageChangePassword();
    });
  }

  [UI.myPagePwCurrent, UI.myPagePwNew, UI.myPagePwConfirm].forEach((input) => {
    if (!input) return;
    input.addEventListener('keydown', (evt) => {
      if (evt.key === 'Enter') {
        evt.preventDefault();
        handleMyPageChangePassword();
      }
    });
  });
}

/* =========================
   Auth modal + profile
   ========================= */

const PROFILE_OUTLINE_ICON = `
  <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
    stroke-linecap="round" stroke-linejoin="round" class="es-icon-stroke" aria-hidden="true">
    <circle cx="12" cy="8" r="4"></circle>
    <path d="M4 20c1.8-4 5.2-6 8-6s6.2 2 8 6"></path>
  </svg>
`;

const AVATAR_BG_COLORS = ['#F4C7C3', '#F9D6A5', '#C7E9F6', '#D6C7F6', '#CFE6D4', '#F6C7DE'];

const getUserDisplayName = (user) => {
  const name = safeString(user?.name, '');
  const fullName = safeString(user?.full_name, '');
  const email = safeString(user?.email, '');
  const username = safeString(user?.username, '');
  return name || fullName || email || username || '';
};

const getUserInitial = (user) => {
  const displayName = getUserDisplayName(user);
  const normalized = displayName.trim().normalize('NFKC');
  const match = normalized.match(/[A-Za-z0-9]/);
  return match ? match[0].toUpperCase() : '?';
};

const getAvatarUrl = (user) => {
  const candidates = [
    user?.profile_image_url,
    user?.avatar_url,
    user?.profileImageUrl,
    user?.avatarUrl,
    user?.image_url,
    user?.imageUrl,
  ]
    .map((value) => safeString(value, '').trim())
    .filter(Boolean);
  return candidates[0] || '';
};

const getAvatarBgColor = (user) => {
  const key = safeString(user?.id, '') || getUserDisplayName(user);
  if (!key) return AVATAR_BG_COLORS[0];
  let hash = 0;
  for (let i = 0; i < key.length; i += 1) {
    hash = (hash * 31 + key.charCodeAt(i)) % 1024;
  }
  return AVATAR_BG_COLORS[hash % AVATAR_BG_COLORS.length];
};

const isProfileMenuOpen = () => UI.profileMenu && !UI.profileMenu.classList.contains('hidden');

function closeProfileMenu() {
  if (UI.profileMenu) UI.profileMenu.classList.add('hidden');
  if (UI.profileButton) UI.profileButton.setAttribute('aria-expanded', 'false');
}

function openProfileMenu() {
  if (!UI.profileMenu || !UI.profileButton) return;
  UI.profileMenu.classList.remove('hidden');
  UI.profileButton.setAttribute('aria-expanded', 'true');
  updateThemeToggleLabel();
  const firstItem = UI.profileMenu.querySelector('[role="menuitem"], [role="menuitemcheckbox"]');
  if (firstItem) firstItem.focus();
}

function toggleProfileMenu() {
  if (isProfileMenuOpen()) closeProfileMenu();
  else openProfileMenu();
}

function updateAdminEntryVisibility() {
  const adminEntry = UI.profileMenuAdmin;
  if (!adminEntry) return;

  const isAdmin = STATE.auth.user?.role === 'admin';
  if (isAdmin) {
    adminEntry.classList.remove('hidden');
  } else {
    adminEntry.classList.add('hidden');
  }
}

function updateProfileButtonState() {
  const btn = UI.profileButton;
  const textEl = UI.profileButtonText;
  if (!btn || !textEl) return;

  btn.setAttribute('aria-expanded', isProfileMenuOpen() ? 'true' : 'false');

  const isAuth = STATE.auth.isAuthenticated;
  const hasToken = Boolean(getAccessToken());
  const isLoggedIn = isAuth || hasToken;
  const user = STATE.auth.user;

  btn.className = UI_CLASSES.headerProfileIcon;

  if (!isLoggedIn) {
    textEl.innerHTML = PROFILE_OUTLINE_ICON;
    btn.setAttribute('title', '로그인');
    btn.setAttribute('aria-label', '로그인');
    closeProfileMenu();
    updateAdminEntryVisibility();
    return;
  }

  const displayName = getUserDisplayName(user);
  const avatarUrl = getAvatarUrl(user);
  if (STATE.auth.lastAvatarUrl !== avatarUrl) {
    STATE.auth.lastAvatarUrl = avatarUrl;
    STATE.auth.avatarImageFailed = false;
  }
  const shouldShowImage = Boolean(avatarUrl) && !STATE.auth.avatarImageFailed;

  textEl.innerHTML = '';
  const avatarWrap = document.createElement('span');
  avatarWrap.className = 'inline-flex h-9 w-9 items-center justify-center rounded-full text-sm font-semibold';
  avatarWrap.style.background = getAvatarBgColor(user);
  avatarWrap.style.color = '#111';

  if (shouldShowImage) {
    const img = document.createElement('img');
    img.src = avatarUrl;
    img.alt = displayName ? `${displayName} 아바타` : '사용자 아바타';
    img.className = 'h-9 w-9 rounded-full object-cover';
    img.addEventListener('error', () => {
      if (STATE.auth.avatarImageFailed) return;
      STATE.auth.avatarImageFailed = true;
      updateProfileButtonState();
    });
    avatarWrap.appendChild(img);
  } else {
    avatarWrap.textContent = getUserInitial(user);
  }

  textEl.appendChild(avatarWrap);
  btn.setAttribute('title', displayName || '프로필');
  btn.setAttribute('aria-label', displayName ? `프로필 ${displayName}` : '프로필');
  updateAdminEntryVisibility();
}

function setupHomeButton() {
  const btn = UI.homeButton;
  if (!btn) return;

  btn.onclick = (evt) => {
    evt.preventDefault();
    closeProfileMenu();
    updateTab('home');
  };
}

function setupProfileButton() {
  const btn = UI.profileButton;
  if (!btn) return;

  btn.onclick = () => {
    const isAuth = STATE.auth.isAuthenticated;
    const hasToken = Boolean(getAccessToken());
    if (isAuth || hasToken) {
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

  if (UI.profileMenuAdmin) {
    UI.profileMenuAdmin.onclick = () => {
      closeProfileMenu();
      window.location.href = '/admin';
    };
  }

  if (UI.profileMenuThemeToggle) {
    UI.profileMenuThemeToggle.onclick = (evt) => {
      evt.preventDefault();
      toggleTheme();
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
        preloadSubscriptionsOnce({ force: true }).catch((e) => {
          console.warn('Failed to refresh subscriptions after auth', e);
        });
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
  const activeId = STATE.activeTab || 'home';
  const tabs = [
    { id: 'home', label: '홈', icon: ICONS.home },
    { id: 'webtoon', label: '웹툰', icon: ICONS.webtoon },
    { id: 'novel', label: '웹소설', icon: ICONS.novel },
    { id: 'ott', label: 'OTT', icon: ICONS.ott },
    { id: 'my', label: '내 구독', icon: ICONS.my },
  ];

  tabs.forEach((tab) => {
    const btn = document.createElement('button');
    const isActive = activeId === tab.id;
    btn.type = 'button';
    btn.className = 'bottom-nav-item flex flex-col items-center justify-center w-full spring-bounce';
    if (isActive) btn.classList.add('is-active');
    btn.setAttribute('data-tab-id', tab.id);
    btn.setAttribute('aria-label', tab.label);
    if (isActive) btn.setAttribute('aria-current', 'page');
    else btn.removeAttribute('aria-current');

    const iconClass = isActive ? 'scale-105' : 'scale-100 opacity-90';

    btn.innerHTML = `
      <div class="h-6 w-6 mb-0.5 flex items-center justify-center transform transition-transform duration-200 ${iconClass}">
        ${tab.icon}
      </div>
      <span class="text-[10px] leading-[1.15] ${isActive ? 'font-semibold' : 'font-medium'}">${tab.label}</span>
    `;
    btn.onclick = () => {
      updateTab(tab.id);
    };
    UI.bottomNav.appendChild(btn);
  });
}

async function updateTab(tabId, { preserveScroll = true } = {}) {
  const prevTab = STATE.activeTab || 'home';
  const prevViewKey = getScrollViewKeyForTab(prevTab);
  const nextViewKey = getScrollViewKeyForTab(tabId);

  STATE.renderToken = (STATE.renderToken || 0) + 1;
  const renderToken = STATE.renderToken;

  if (preserveScroll && STATE.hasBootstrapped) saveScroll(prevViewKey);
  UIState.save();

  if (STATE.search.pageOpen) closeSearchPage({ fromPopstate: true });
  STATE.activeTab = tabId;
  if (['webtoon', 'novel', 'ott'].includes(tabId)) STATE.lastBrowseTab = tabId;
  STATE.isMySubOpen = tabId === 'my';

  renderBottomNav();
  updateFilterVisibility(tabId);
  renderL1Filters(tabId);
  renderL2Filters(tabId);

  const renderResult = await fetchAndRenderContent(tabId, { renderToken });

  const shouldRestore = preserveScroll && STATE.hasBootstrapped && renderToken === STATE.renderToken;
  if (shouldRestore) {
    const container = UI.contentGrid;
    const requireChildren = Boolean(renderResult?.itemCount);
    restoreScroll(nextViewKey, { container, requireChildren });
  } else {
    window.scrollTo({ top: 0 });
  }

  STATE.hasBootstrapped = true;
}

function updateFilterVisibility(tabId) {
  if (!UI.l1Filter || !UI.l2Filter || !UI.mySubToggle) return;

  UI.l1Filter.classList.add('hidden');
  UI.l2Filter.classList.add('hidden');
  UI.mySubToggle.classList.add('hidden');
  if (UI.seriesSort) UI.seriesSort.classList.add('hidden');
  if (UI.seriesFooter) UI.seriesFooter.classList.add('hidden');

  if (['webtoon', 'novel', 'ott'].includes(tabId)) {
    UI.l1Filter.classList.remove('hidden');
    UI.l2Filter.classList.remove('hidden');
  } else if (tabId === 'my') {
    UI.mySubToggle.classList.remove('hidden');
    syncMySubToggleUI();
    STATE.subscriptionsNeedFreshHintAt = Date.now();
    scheduleSubscriptionsSoftRefresh('enter_my', { expedite: true });
  }
}

function renderL1Filters(tabId) {
  if (!UI.l1Filter) return;

  UI.l1Filter.innerHTML = '';
  const items = getSourceItemsForTab(tabId);
  if (!items.length) return;

  const selectedSources = getSelectedSourcesForTab(tabId);
  const selectedSet = new Set(selectedSources);

  items.forEach((item) => {
    const sourceId = normalizeSourceId(item.id);
    const keepCurrentLogoFit = sourceId === 'tving' || sourceId === 'laftel';
    const el = document.createElement('div');
    const isActive = selectedSet.has(sourceId);
    const brightnessClass = selectedSet.size > 0 && isActive ? 'is-bright' : 'is-dim';
    el.className = `l1-logo flex-shrink-0 cursor-pointer spring-bounce ${
      isActive ? 'active' : 'inactive'
    } ${brightnessClass}`;
    el.dataset.sourceId = sourceId;

    const brandMeta = SOURCE_BRAND_META[sourceId] || {};
    if (brandMeta.logoColor) el.style.setProperty('--chip-fg', brandMeta.logoColor);
    else el.style.removeProperty('--chip-fg');
    el.style.setProperty('--logo-size', keepCurrentLogoFit ? '30px' : '40px');
    el.style.setProperty('--logo-fit', keepCurrentLogoFit ? 'contain' : 'cover');

    el.setAttribute('role', 'button');
    el.setAttribute('tabindex', '0');
    el.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    el.setAttribute('aria-label', `${item.label} source filter`);
    el.innerHTML = `
      <span class="l1-icon" aria-hidden="true">${getSourceIconMarkup(sourceId, item.label)}</span>
    `;
    bindSourceLogoFallback(el);

    el.onclick = () => {
      const nextSelected = new Set(getSelectedSourcesForTab(tabId));
      if (nextSelected.has(sourceId)) nextSelected.delete(sourceId);
      else nextSelected.add(sourceId);
      STATE.filters[tabId].sources = Array.from(nextSelected);
      renderL1Filters(tabId);
      fetchAndRenderContent(tabId);
      UIState.save();
    };
    el.onkeydown = (evt) => {
      if (evt.key === 'Enter' || evt.key === ' ') {
        evt.preventDefault();
        el.click();
      }
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

  if (tabId === 'webtoon') {
    items = days;
  } else if (tabId === 'novel') {
    items = NOVEL_GENRE_GROUP_OPTIONS;
  } else if (tabId === 'ott') {
    items = [
      { id: 'all', label: 'ALL' },
      { id: 'drama', label: '드라마' },
      { id: 'anime', label: '애니메이션' },
      { id: 'variety', label: '예능' },
      { id: 'docu', label: '다큐멘터리' },
    ];
  } else {
    return;
  }

  let activeKey = '';
  if (tabId === 'webtoon')
    activeKey = STATE.filters?.[tabId]?.day || 'all';
  if (tabId === 'novel')
    activeKey = sanitizeNovelGenreGroup(
      STATE.filters?.novel?.genreGroup,
      DEFAULT_NOVEL_GENRE_GROUP,
    );
  if (tabId === 'ott') activeKey = STATE.filters?.[tabId]?.genre || 'all';

  items.forEach((item) => {
    const el = document.createElement('button');
    const isActive = activeKey === item.id;
    el.className = `l2-tab spring-bounce ${isActive ? 'active' : ''}`;
    el.textContent = item.label;

    el.onclick = () => {
      if (tabId === 'webtoon') STATE.filters[tabId].day = item.id;
      if (tabId === 'novel') STATE.filters[tabId].genreGroup = item.id;
      if (tabId === 'ott') STATE.filters[tabId].genre = item.id;

      renderL2Filters(tabId);
      fetchAndRenderContent(tabId);
      UIState.save();
    };

    UI.l2Filter.appendChild(el);
  });

  if (tabId === 'novel') {
    const wrap = document.createElement('label');
    wrap.className = 'l2-checkbox ml-2 inline-flex items-center gap-2 select-none';

    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'l2-checkbox-input';
    checkbox.checked = coerceBooleanFilter(
      STATE.filters?.novel?.isCompleted,
      DEFAULT_NOVEL_IS_COMPLETED,
    );
    checkbox.setAttribute('aria-label', '\uC644\uACB0');
    checkbox.onchange = () => {
      STATE.filters.novel.isCompleted = checkbox.checked;
      fetchAndRenderContent(tabId);
      UIState.save();
    };

    const label = document.createElement('span');
    label.className = 'l2-checkbox-label';
    label.textContent = '\uC644\uACB0';

    wrap.appendChild(checkbox);
    wrap.appendChild(label);
    UI.l2Filter.appendChild(wrap);
  }
}

function syncMySubToggleUI() {
  const mode = STATE.filters?.my?.viewMode || 'completion';

  if (UI.toggleIndicator) {
    const x =
      mode === 'publication'
        ? 'translateX(0%)'
        : mode === 'completion'
          ? 'translateX(100%)'
          : 'translateX(200%)';
    UI.toggleIndicator.style.transform = x;
  }

  const btns = document.querySelectorAll(
    '#mySubToggleContainer button[data-mode]'
  );
  btns.forEach((btn) => {
    const active = btn.dataset.mode === mode;
    btn.classList.toggle('is-active', active);
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function updateMySubTab(mode) {
  STATE.filters.my.viewMode = mode;
  syncMySubToggleUI();
  fetchAndRenderContent('my');
}

/* =========================
   Data fetching + rendering
   ========================= */

const setContentGridLayout = (mode = 'grid') => {
  if (!UI.contentGrid) return;
  UI.contentGrid.classList.remove(...GRID_LAYOUT_CLASS_TOKENS, ...HOME_LAYOUT_CLASS_TOKENS);
  if (mode === 'home') {
    UI.contentGrid.classList.add(...HOME_LAYOUT_CLASS_TOKENS);
    return;
  }
  UI.contentGrid.classList.add(...GRID_LAYOUT_CLASS_TOKENS);
};

const resolveCardTabId = (content, fallback = 'webtoon') => {
  const raw = safeString(content?.content_type || content?.contentType || content?.type, '').toLowerCase();
  if (['webtoon', 'novel', 'ott', 'series', 'my'].includes(raw)) return raw;
  if (['webtoon', 'novel', 'ott', 'series', 'my'].includes(fallback)) return fallback;
  return 'webtoon';
};

const extractOttGenreTokens = (content) => {
  const meta = normalizeMeta(content?.meta);
  const attrs = safeObj(meta?.attributes);
  const common = safeObj(meta?.common);
  const candidates = [
    attrs?.genres,
    attrs?.genre,
    attrs?.category,
    common?.genres,
    common?.genre,
    content?.genres,
    content?.genre,
  ];

  const parseCandidate = (value) => {
    if (Array.isArray(value)) {
      return value
        .map((entry) => safeString(entry, '').trim().toLowerCase())
        .filter(Boolean);
    }
    const asString = safeString(value, '').trim();
    if (!asString) return [];

    if ((asString.startsWith('[') && asString.endsWith(']')) || (asString.startsWith('{') && asString.endsWith('}'))) {
      try {
        return parseCandidate(JSON.parse(asString));
      } catch {
        return [];
      }
    }

    return asString
      .split(/[,\|\/]/)
      .map((entry) => entry.trim().toLowerCase())
      .filter(Boolean);
  };

  const tokenSet = new Set();
  candidates.forEach((candidate) => {
    parseCandidate(candidate).forEach((token) => tokenSet.add(token));
  });
  return Array.from(tokenSet);
};

const filterOttItemsByGenre = (items, genreFilter) => {
  if (!Array.isArray(items)) return [];
  const target = safeString(genreFilter || 'all', 'all').trim().toLowerCase();
  if (!target || target === 'all') return items;

  let sawGenreMetadata = false;
  const filtered = items.filter((item) => {
    const genres = extractOttGenreTokens(item);
    if (!genres.length) return true;
    sawGenreMetadata = true;
    return genres.some((genre) => genre.includes(target) || target.includes(genre));
  });

  return sawGenreMetadata ? filtered : items;
};

async function fetchHomeRecommendations({ limit = HOME_RECOMMENDATIONS_LIMIT, signal } = {}) {
  const response = await apiRequest('GET', '/api/contents/recommendations', {
    query: { limit },
    signal,
  });
  const list = Array.isArray(response?.data)
    ? response.data
    : Array.isArray(response)
      ? response
      : [];
  return list
    .map((item) => normalizeContentForGrid(item, item?.source))
    .filter((item) => item?.content_id && item?.source)
    .slice(0, HOME_RECOMMENDATIONS_LIMIT);
}

const renderHomeSection = ({
  title,
  items,
  emptyTitle,
  emptyMessage,
  actions = [],
}) => {
  const section = document.createElement('section');
  section.className = 'space-y-3';

  const heading = document.createElement('h2');
  setClasses(heading, UI_CLASSES.sectionTitle);
  heading.textContent = title;
  section.appendChild(heading);

  if (Array.isArray(items) && items.length) {
    const grid = document.createElement('div');
    grid.className = 'grid grid-cols-3 gap-2';
    const fragment = document.createDocumentFragment();
    items.forEach((item) => {
      const cardTabId = resolveCardTabId(item, STATE.lastBrowseTab || 'webtoon');
      fragment.appendChild(createCard(item, cardTabId, getAspectByType(cardTabId)));
    });
    grid.appendChild(fragment);
    section.appendChild(grid);
    return section;
  }

  const empty = document.createElement('div');
  empty.className = 'es-page-card rounded-2xl p-4 text-center';

  const emptyTitleEl = document.createElement('p');
  setClasses(emptyTitleEl, UI_CLASSES.emptyTitle);
  emptyTitleEl.textContent = emptyTitle || '';
  empty.appendChild(emptyTitleEl);

  if (emptyMessage) {
    const emptyMsgEl = document.createElement('p');
    setClasses(emptyMsgEl, cx(UI_CLASSES.emptyMsg, 'mx-auto mt-2'));
    emptyMsgEl.textContent = emptyMessage;
    empty.appendChild(emptyMsgEl);
  }

  if (Array.isArray(actions) && actions.length) {
    const actionsWrap = document.createElement('div');
    actionsWrap.className = 'mt-4 flex flex-wrap justify-center gap-2';
    actions.forEach((action) => {
      if (!action?.label || typeof action?.onClick !== 'function') return;
      const btn = document.createElement('button');
      btn.type = 'button';
      const variantClass = action.variant === 'secondary' ? UI_CLASSES.btnSecondary : UI_CLASSES.btnPrimary;
      setClasses(btn, `${variantClass} spring-bounce`);
      btn.textContent = action.label;
      btn.onclick = action.onClick;
      actionsWrap.appendChild(btn);
    });
    empty.appendChild(actionsWrap);
  }

  section.appendChild(empty);
  return section;
};

async function renderHomeFeed({ signal } = {}) {
  if (!UI.contentGrid) return { itemCount: 0 };

  let recommendations = [];
  try {
    recommendations = await fetchHomeRecommendations({ signal });
  } catch (err) {
    if (!signal?.aborted) {
      console.warn('Failed to load home recommendations', err);
    }
  }
  if (signal?.aborted) return { itemCount: 0 };

  const historyItems = loadRecentlySearchedContents()
    .sort((a, b) => (b?.openedAt || 0) - (a?.openedAt || 0))
    .map((entry) => normalizeContentForGrid(entry?.content, entry?.content?.source))
    .filter((item) => item?.content_id && item?.source)
    .slice(0, MAX_RECENTLY_SEARCHED_CONTENTS);

  UI.contentGrid.innerHTML = '';
  const fragment = document.createDocumentFragment();
  fragment.appendChild(
    renderHomeSection({
      title: '추천작',
      items: recommendations,
      emptyTitle: '추천작이 아직 준비되지 않았어요',
      emptyMessage: '웹툰 탭에서 최신 작품을 먼저 살펴보세요.',
      actions: [
        {
          label: '웹툰 보기',
          variant: 'primary',
          onClick: () => updateTab('webtoon'),
        },
      ],
    }),
  );
  fragment.appendChild(
    renderHomeSection({
      title: '검색했던 작품',
      items: historyItems,
      emptyTitle: '검색해서 열어본 작품이 없어요',
      emptyMessage: '검색에서 작품을 열어보면 여기에 저장됩니다.',
      actions: [
        {
          label: '검색 열기',
          variant: 'primary',
          onClick: () => openSearchPage({ focus: true }),
        },
      ],
    }),
  );

  UI.contentGrid.appendChild(fragment);
  syncAllRenderedStarBadges();

  const totalCount = recommendations.length + historyItems.length;
  setCountIndicatorText(totalCount ? `총 ${totalCount}건` : '');
  hideLoadMoreUI();
  return { itemCount: totalCount };
}

const appendCardsToGrid = (
  items,
  { tabId = 'webtoon', aspectClass = 'aspect-[3/4]', clearBeforeAppend = false } = {}
) => {
  if (!UI.contentGrid || !Array.isArray(items) || !items.length) return;
  if (clearBeforeAppend) UI.contentGrid.innerHTML = '';

  const fragment = document.createDocumentFragment();
  items.forEach((item) => {
    const cardTabId = resolveCardTabId(item, tabId);
    fragment.appendChild(createCard(item, cardTabId, aspectClass));
  });

  UI.contentGrid.appendChild(fragment);
  syncAllRenderedStarBadges();
};

async function loadNextPage(category, { signal } = {}) {
  const effectiveSignal = signal || STATE.tabAbortController?.signal;
  const pg = STATE.pagination?.[category];
  if (!pg || pg.loading || pg.done) return;
  if (pg.requestSeq !== STATE.contentRequestSeq) return;
  if (effectiveSignal?.aborted) return;

  pg.loading = true;
  updateLoadMoreUI(category);
  updateCountIndicator(category);

  const perPage = PAGE_SIZE;
  const baseQuery =
    pg.baseQuery && typeof pg.baseQuery === 'object' && !Array.isArray(pg.baseQuery)
      ? { ...pg.baseQuery }
      : {};
  const query = { ...baseQuery, per_page: perPage };
  const useLegacyDefaults = !pg.endpointPath;
  if (useLegacyDefaults && !Object.prototype.hasOwnProperty.call(query, 'type') && pg.tabId) {
    query.type = pg.tabId;
  }
  if (
    useLegacyDefaults &&
    !Object.prototype.hasOwnProperty.call(query, 'source') &&
    !Object.prototype.hasOwnProperty.call(query, 'sources')
  ) {
    query.source = pg.source || 'all';
  }

  if (pg.cursor !== null && pg.cursor !== undefined) query.cursor = pg.cursor;
  else if (pg.legacyCursor) query.last_title = pg.legacyCursor;

  const endpointPath = pg.endpointPath || `/api/contents/${category}`;
  const url = buildUrl(endpointPath, query);

  try {
    const json = await apiRequest('GET', url, { signal: effectiveSignal });
    if (pg.requestSeq !== STATE.contentRequestSeq) return;
    if (effectiveSignal?.aborted) return;

    const incoming = Array.isArray(json?.contents)
      ? json.contents.map((item) => ({ ...item, meta: normalizeMeta(item?.meta) }))
      : [];
    let filteredIncoming = filterItemsBySources(incoming, pg.filterSources);
    if (pg.tabId === 'ott') {
      filteredIncoming = filterOttItemsByGenre(filteredIncoming, STATE.filters?.ott?.genre || 'all');
    }

    const next = json?.next_cursor ?? null;
    const legacyNext = !next ? json?.last_title ?? null : null;
    const parsedPageSize = Number(json?.page_size);
    const responsePageSize = Number.isFinite(parsedPageSize) ? parsedPageSize : perPage;

    const existingKeys = new Set(pg.items.map(contentKey));
    const toAppend = [];

    for (const c of filteredIncoming) {
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

    const hasOttGenreFilter =
      pg.tabId === 'ott' && safeString(STATE.filters?.ott?.genre || 'all', 'all') !== 'all';
    const allowEmptyPageStop = !pg.filterSources?.length && !hasOttGenreFilter;
    if (noNewItems && allowEmptyPageStop) {
      console.warn('No new items returned; marking pagination as done to avoid stalls');
    }
    pg.done =
      (allowEmptyPageStop && noNewItems) ||
      (missingCursor && (reachedEndByCount || pg.items.length > 0));
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
    if (effectiveSignal?.aborted) return;
    showToast(e?.message || '콘텐츠를 불러오지 못했습니다.', { type: 'error' });
  } finally {
    pg.loading = false;
    updateCountIndicator(category);
    updateLoadMoreUI(category);
  }
}

async function fetchAndRenderContent(tabId, { renderToken } = {}) {
  if (!UI.contentGrid) return;

  if (STATE.tabAbortController) STATE.tabAbortController.abort();
  if (STATE.gridRenderAbort) STATE.gridRenderAbort.abort();

  const tabAbortController = new AbortController();
  STATE.tabAbortController = tabAbortController;
  STATE.renderAbortController = tabAbortController;
  STATE.gridRenderAbort = null;

  const { signal } = tabAbortController;

  disconnectInfiniteObserver();
  setActivePaginationCategory(null);
  hideLoadMoreUI();
  setCountIndicatorText('');

  const requestSeq = ++STATE.contentRequestSeq;
  const isRenderStale = () => renderToken && renderToken !== STATE.renderToken;
  const isStale = () => STATE.contentRequestSeq !== requestSeq || signal.aborted || isRenderStale();

  UI.contentGrid.innerHTML = '';
  STATE.isLoading = true;
  setContentGridLayout(tabId === 'home' ? 'home' : 'grid');

  let aspectClass = 'aspect-[3/4]';
  if (tabId === 'novel') aspectClass = 'aspect-[1/1.4]';
  if (tabId === 'ott') aspectClass = 'aspect-[2/3]';

  if (tabId === 'home') {
    try {
      const homeResult = await renderHomeFeed({ signal });
      if (isStale()) return { stale: true };
      return { itemCount: homeResult?.itemCount || 0, aspectClass };
    } catch (e) {
      if (signal.aborted) return { stale: true };
      console.error('Home feed error', e);
      renderEmptyState(UI.contentGrid, {
        title: '홈 피드를 불러오지 못했습니다.',
        actions: [
          {
            label: '웹툰 보기',
            variant: 'primary',
            onClick: () => updateTab('webtoon'),
          },
        ],
      });
      return { itemCount: 0, aspectClass };
    } finally {
      STATE.isLoading = false;
    }
  }

  let skeletonShown = false;
  const showSkeleton = () => {
    if (isStale() || skeletonShown) return;
    skeletonShown = true;
    UI.contentGrid.innerHTML = '';
    for (let i = 0; i < 8; i++) {
      const skel = document.createElement('div');
      skel.className = 'h-[92px] rounded-2xl skeleton';
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
          renderEmptyState(UI.contentGrid, {
            title: '로그인이 필요해요',
            message: '내 구독은 로그인 후 확인할 수 있어요.',
            actions: [
              {
                label: '로그인하기',
                variant: 'primary',
                onClick: () => openAuthModal({ reason: 'my-tab' }),
              },
            ],
          });
        }
        return { itemCount: 0, aspectClass };
      }

      let subs = [];
      try {
        subs = await loadSubscriptions();
        if (isStale()) return { stale: true };
      } catch (e) {
        if (!isStale()) {
          renderEmptyState(UI.contentGrid, {
            title: '구독 정보를 불러오지 못했어요',
            message: '잠시 후 다시 시도해 주세요.',
            actions: [
              {
                label: '다시 시도',
                variant: 'primary',
                onClick: async () => {
                  try {
                    await loadSubscriptions({ force: true });
                    fetchAndRenderContent('my');
                  } catch (err) {
                    showToast(err?.message || '오류가 발생했습니다.', { type: 'error' });
                  }
                },
              },
            ],
          });
        }
        return { stale: true };
      }

      const mode = STATE.filters?.my?.viewMode || 'completion';

      data = (subs || []).filter((item) => {
        const sub = item?.subscription || {};
        const fs = item?.final_state || {};
        const isScheduled = fs?.is_scheduled_completion === true;
        const isCompleted = fs?.final_status === '완결' && !isScheduled;

        if (mode === 'publication') {
          if (sub.wants_publication !== true) return false;
          if (!supportsPublicationUI(item)) return false;
          return true;
        }

        if (sub.wants_completion !== true) return false;

        if (mode === 'completed') return isCompleted;
        return !isCompleted;
      });

      if (mode === 'publication') {
        data = sortPublicationItems(data);
      }

      if (!data.length) {
        if (mode === 'publication') {
          emptyStateConfig = {
            title: '공개 알림을 구독한 작품이 없습니다',
            message: '작품 화면에서 공개 알림을 설정해보세요.',
            actions: [
              {
                label: '검색하기',
                variant: 'primary',
                onClick: () => openSearchAndFocus(),
              },
              {
                label: 'OTT 보기',
                variant: 'secondary',
                onClick: () => updateTab('ott'),
              },
            ],
          };
        } else if (mode === 'completed') {
          emptyStateConfig = {
            title: '완결된 구독 작품이 아직 없습니다',
            message: '구독 중인 작품이 완결되면 여기에 표시됩니다.',
            actions: [
              {
                label: '구독 목록 보기',
                variant: 'primary',
                onClick: () => {
                  STATE.filters.my.viewMode = 'completion';
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
            title: '완결 알림을 구독한 작품이 없습니다',
            message: '작품 화면에서 완결 알림을 설정해보세요.',
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
      let sourceFilter = [];
      let responsePayload = null;

      const runPaginatedBrowse = async ({ category, sourceConfig, endpointPath, baseQuery }) => {
        resetPaginationState(category, {
          tabId,
          source: sourceConfig?.querySource || 'all',
          filterSources: sourceConfig?.filterSources || [],
          aspectClass,
          requestSeq,
          endpointPath,
          baseQuery,
        });
        setActivePaginationCategory(category);
        updateLoadMoreUI(category);
        updateCountIndicator(category);
        setupInfiniteObserver(category);
        await loadNextPage(category, { signal });
        if (isStale()) return { stale: true };

        const pg = STATE.pagination?.[category];
        if (pg && pg.items.length === 0 && pg.done) {
          UI.contentGrid.innerHTML = '';
          renderEmptyState(UI.contentGrid, {
            title: '콘텐츠가 없습니다.',
            message: '조건에 맞는 콘텐츠가 없습니다.',
          });
          setCountIndicatorText('');
          hideLoadMoreUI();
          disconnectInfiniteObserver();
          return { itemCount: 0 };
        }

        return { itemCount: pg?.items?.length || 0 };
      };

      if (tabId === 'webtoon' || tabId === 'ott') {
        let statusKey = 'ongoing';
        const webtoonDay = safeString(STATE.filters?.webtoon?.day || 'all', 'all').toLowerCase();
        if (tabId === 'webtoon') {
          statusKey = webtoonDay === 'completed' || webtoonDay === 'hiatus' ? webtoonDay : 'ongoing';
        } else if (tabId === 'ott') {
          const ottStatusRaw = safeString(
            STATE.filters?.ott?.status || STATE.filters?.ott?.day || 'ongoing',
            'ongoing',
          ).toLowerCase();
          statusKey = ['ongoing', 'completed', 'hiatus'].includes(ottStatusRaw)
            ? ottStatusRaw
            : 'ongoing';
        }

        const shouldUsePaginatedStatus = statusKey === 'completed' || statusKey === 'hiatus';
        const shouldUsePaginatedOngoing = USE_BROWSE_PAGINATION_V2 && statusKey === 'ongoing';
        const sourceConfig = getSourceRequestConfig(tabId, {
          preferServerMulti: shouldUsePaginatedStatus || shouldUsePaginatedOngoing,
        });
        sourceFilter = sourceConfig.filterSources;

        if (shouldUsePaginatedStatus) {
          const pagedResult = await runPaginatedBrowse({
            category: statusKey,
            sourceConfig,
            endpointPath: `/api/contents/${statusKey}`,
            baseQuery: applySourceQuery({ type: tabId }, sourceConfig),
          });
          if (pagedResult?.stale) return { stale: true };
          return { itemCount: pagedResult?.itemCount || 0, aspectClass };
        }

        if (shouldUsePaginatedOngoing) {
          const ongoingBaseQuery =
            tabId === 'webtoon' ? { type: 'webtoon', day: webtoonDay } : { type: 'ott' };
          const pagedResult = await runPaginatedBrowse({
            category: 'ongoing',
            sourceConfig,
            endpointPath: '/api/contents/ongoing_v2',
            baseQuery: applySourceQuery(ongoingBaseQuery, sourceConfig),
          });
          if (pagedResult?.stale) return { stale: true };
          return { itemCount: pagedResult?.itemCount || 0, aspectClass };
        }

        const query = applySourceQuery({ type: tabId }, sourceConfig);
        url = buildUrl('/api/contents/ongoing', query);
      } else if (tabId === 'novel') {
        const sourceConfig = getSourceRequestConfig(tabId, {
          preferServerMulti: USE_BROWSE_PAGINATION_V2,
        });
        sourceFilter = sourceConfig.filterSources;
        const query = {
          genre_group: sanitizeNovelGenreGroup(
            STATE.filters?.novel?.genreGroup,
            DEFAULT_NOVEL_GENRE_GROUP,
          ),
        };
        if (coerceBooleanFilter(STATE.filters?.novel?.isCompleted, DEFAULT_NOVEL_IS_COMPLETED)) {
          query.is_completed = 'true';
        }

        if (USE_BROWSE_PAGINATION_V2) {
          const pagedResult = await runPaginatedBrowse({
            category: 'novels',
            sourceConfig,
            endpointPath: '/api/contents/novels_v2',
            baseQuery: applySourceQuery(query, sourceConfig),
          });
          if (pagedResult?.stale) return { stale: true };
          return { itemCount: pagedResult?.itemCount || 0, aspectClass };
        }

        url = buildUrl('/api/contents/novels', applySourceQuery(query, sourceConfig));
      }

      if (url) {
        responsePayload = await apiRequest('GET', url, { signal });

        if (tabId === 'webtoon') {
          const day = STATE.filters?.[tabId]?.day || 'all';

          if (day !== 'completed' && day !== 'hiatus' && day !== 'all') {
            data = Array.isArray(responsePayload?.[day]) ? responsePayload[day] : [];
          } else if (day === 'all') {
            data = [];
            Object.values(responsePayload || {}).forEach((arr) => {
              if (Array.isArray(arr)) data.push(...arr);
            });
          } else {
            data = Array.isArray(responsePayload?.contents) ? responsePayload.contents : [];
          }
        } else if (tabId === 'novel') {
          data = Array.isArray(responsePayload?.contents)
            ? responsePayload.contents
            : Array.isArray(responsePayload)
              ? responsePayload
              : [];
        } else if (tabId === 'ott') {
          data = Array.isArray(responsePayload?.contents)
            ? responsePayload.contents
            : Array.isArray(responsePayload)
              ? responsePayload
              : [];
        } else {
          data = Array.isArray(responsePayload?.contents) ? responsePayload.contents : [];
        }
      }

      if (sourceFilter.length) {
        data = filterItemsBySources(data, sourceFilter);
      }
      if (tabId === 'ott') {
        data = filterOttItemsByGenre(data, STATE.filters?.ott?.genre || 'all');
      }
    }

    data = Array.isArray(data)
      ? data.map((item) => normalizeContentForGrid(item, getSearchSource(tabId)))
      : [];
    if (isStale()) return { stale: true };
  } catch (e) {
    if (signal.aborted) return { stale: true };
    console.error('Fetch error', e);
    showToast(e?.message || '오류가 발생했습니다.', { type: 'error' });
  } finally {
    clearTimeout(skeletonTimer);
    STATE.isLoading = false;
  }

  if (isStale()) return { stale: true };

  UI.contentGrid.innerHTML = '';

  if (!data.length) {
    if (emptyStateConfig) renderEmptyState(UI.contentGrid, emptyStateConfig);
    else renderEmptyState(UI.contentGrid, { title: '콘텐츠가 없습니다.' });
    return { itemCount: 0, aspectClass };
  }

  const renderController = new AbortController();
  STATE.gridRenderAbort = renderController;
  STATE.renderAbortController = renderController;
  STATE.rendering.list = data;
  STATE.rendering.aspectClass = aspectClass;
  STATE.rendering.tabId = tabId;
  setCountIndicatorText(`총 ${data.length}건`);

  await renderInBatches({
    items: data,
    container: UI.contentGrid,
    signal: renderController.signal,
    renderItem: (item) => createCard(item, tabId, aspectClass),
  });

  if (isStale()) return { stale: true };

  return { itemCount: data.length, aspectClass };
}

/* =========================
   Cards
   ========================= */

const applyNoReferrer = (img) => {
  img.referrerPolicy = 'no-referrer';
  img.setAttribute('referrerpolicy', 'no-referrer');
};

const hexToRgb = (hex) => {
  if (!hex || typeof hex !== 'string') return null;
  let normalized = hex.trim();
  if (!normalized) return null;
  if (normalized.startsWith('#')) normalized = normalized.slice(1);
  if (normalized.length === 3) {
    normalized = normalized
      .split('')
      .map((ch) => ch + ch)
      .join('');
  }
  if (!/^[0-9a-fA-F]{6}$/.test(normalized)) return null;
  const intVal = parseInt(normalized, 16);
  return {
    r: (intVal >> 16) & 255,
    g: (intVal >> 8) & 255,
    b: intVal & 255,
  };
};

const pickKakaoTitleSource = (kakaoAssets) => {
  if (!kakaoAssets) return null;
  const titleA = kakaoAssets?.title_a || null;
  const titleB = kakaoAssets?.title_b || null;
  if (titleA && !titleB) return titleA;
  if (titleB && !titleA) return titleB;
  if (!titleA && !titleB) return null;

  const bgRgb = hexToRgb(kakaoAssets?.bg_color || '');
  if (!bgRgb) return titleB || titleA;
  const luminance = (0.2126 * bgRgb.r + 0.7152 * bgRgb.g + 0.0722 * bgRgb.b) / 255;
  const bgIsDark = luminance < 0.55;
  return bgIsDark ? titleA || titleB : titleB || titleA;
};

const buildPicture = ({
  webp,
  fallbackUrl,
  fallbackType,
  imgClass,
  imgStyle,
  wrapperClass,
  noReferrer,
  altText,
}) => {
  const picture = document.createElement('picture');
  if (wrapperClass) setClasses(picture, wrapperClass);
  picture.style.pointerEvents = 'none';

  const webpSource = webp ? document.createElement('source') : null;
  if (webpSource) {
    webpSource.type = 'image/webp';
    webpSource.srcset = webp;
    picture.appendChild(webpSource);
  }

  if (fallbackUrl) {
    const fallbackSource = document.createElement('source');
    fallbackSource.type = fallbackType;
    fallbackSource.srcset = fallbackUrl;
    picture.appendChild(fallbackSource);
  }

  const img = document.createElement('img');
  if (noReferrer) applyNoReferrer(img);
  img.loading = 'lazy';
  img.decoding = 'async';
  img.fetchPriority = 'low';
  img.alt = altText || '';
  img.src = fallbackUrl || webp || FALLBACK_THUMB;
  if (imgClass) setClasses(img, imgClass);
  if (imgStyle) Object.assign(img.style, imgStyle);
  img.onerror = () => {
    if (img.dataset.fallbackStage === 'fallback') {
      img.src = FALLBACK_THUMB;
      return;
    }
    if (fallbackUrl && img.src !== fallbackUrl) {
      if (webpSource) webpSource.remove();
      img.dataset.fallbackStage = 'fallback';
      img.src = fallbackUrl;
      return;
    }
    img.src = FALLBACK_THUMB;
  };
  picture.appendChild(img);
  return picture;
};

function createCard(content, tabId, aspectClass) {
  void aspectClass;
  const el = document.createElement('div');
  setClasses(el, UI_CLASSES.cardRoot);
  const subscriptionKey = buildSubscriptionKey(content);
  const contentId = content?.content_id ?? content?.contentId ?? content?.id;
  const source = content?.source;
  if (subscriptionKey) {
    el.setAttribute('data-sub-key', subscriptionKey);
  }
  if (contentId !== undefined && contentId !== null) {
    el.setAttribute('data-content-id', String(contentId));
  }
  if (source) {
    el.setAttribute('data-source', String(source));
  }
  const contentType = getContentType({ ...content, type: tabId });
  if (contentType) {
    el.setAttribute('data-content-type', contentType);
  }

  el.setAttribute('role', 'button');
  el.setAttribute('tabindex', '0');
  el.setAttribute('aria-label', `${content?.title || '콘텐츠'} 열기`);

  const meta = normalizeMeta(content?.meta);
  const rawAuthors = meta?.common?.authors;
  const authors = Array.isArray(rawAuthors)
    ? rawAuthors
        .map((author) => safeString(author, ''))
        .filter(Boolean)
        .join(', ')
    : safeString(rawAuthors, '');

  const cardContainer = document.createElement('div');
  setClasses(cardContainer, UI_CLASSES.cardThumb);
  cardContainer.setAttribute('data-card-thumb', 'true');
  const badgeRow = document.createElement('div');
  setClasses(badgeRow, UI_CLASSES.cardBadgeRow);
  badgeRow.setAttribute('data-card-badge-row', 'true');
  cardContainer.appendChild(badgeRow);

  // Badge logic
  if (tabId === 'my') {
    const myViewMode = STATE.filters?.my?.viewMode || 'completion';
    const fs = safeObj(content?.final_state);
    const isScheduled = fs?.is_scheduled_completion === true;
    const scheduledDate = safeString(fs?.scheduled_completed_at, '');
    const isCompleted = safeString(fs?.final_status, '') === '완결';
    const isHiatus =
      safeString(fs?.final_status, '') === '휴재' || content?.status === '휴재';

    const badgeEl = document.createElement('div');

    if (myViewMode !== 'publication') {
      if (isScheduled) {
        setClasses(badgeEl, cx(UI_CLASSES.badgeBase, 'gap-1 es-badge-warning'));
        const formatted = scheduledDate ? formatDateKST(scheduledDate) : '';
        badgeEl.innerHTML = `<span class="text-[10px] font-black text-black leading-none">완결 예정</span>${
          formatted
            ? `<span class="text-[10px] text-black leading-none">${formatted}</span>`
            : ''
        }`;
        badgeRow.appendChild(badgeEl);
      } else if (isCompleted) {
        setClasses(badgeEl, cx(UI_CLASSES.badgeBase, 'gap-1 es-badge-success'));
        badgeEl.innerHTML = `<span class="text-[10px] font-black text-black leading-none">완결</span>`;
        badgeRow.appendChild(badgeEl);
      } else if (isHiatus) {
        setClasses(badgeEl, cx(UI_CLASSES.badgeBase, 'gap-1 es-badge-neutral'));
        badgeEl.innerHTML = `<span class="text-[10px] font-black leading-none">휴재</span>`;
        badgeRow.appendChild(badgeEl);
      }
    }
  }

  const textContainer = document.createElement('div');
  setClasses(textContainer, UI_CLASSES.cardTextWrap);

  const titleEl = document.createElement('h3');
  setClasses(titleEl, UI_CLASSES.cardTitle);
  titleEl.textContent = content.title || '';

  const authorEl = document.createElement('p');
  setClasses(authorEl, UI_CLASSES.cardMeta);
  authorEl.textContent = authors || '작가 정보 없음';

  textContainer.appendChild(titleEl);
  textContainer.appendChild(authorEl);

  if (tabId === 'my') {
    const myViewMode = STATE.filters?.my?.viewMode || 'completion';
    if (myViewMode === 'publication') {
      const publicationText = buildPublicationStatusText(content);
      const publicationEl = document.createElement('div');
      setClasses(publicationEl, 'mt-1 text-xs es-muted');
      publicationEl.textContent = publicationText;
      textContainer.appendChild(publicationEl);
    }
  }
  cardContainer.appendChild(textContainer);
  el.appendChild(cardContainer);

  el.addEventListener('keydown', (evt) => {
    if (evt.key === 'Enter' || evt.key === ' ') {
      evt.preventDefault();
      if (isAnyModalOpen()) return;
      openSubscribeModal(content, { returnFocusEl: el });
    }
  });

  el.onclick = () => openSubscribeModal(content, { returnFocusEl: el });

  syncStarBadgeForCard(el, isAnySubscribedForCard(content));
  return el;
}

/* =========================
   Modal: subscription toggle
   ========================= */

const syncModalButton = () => {
  if (!STATE.currentModalContent) return;
  syncSubscribeModalUI(STATE.currentModalContent);
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

function openSubscribeModal(content, opts = {}) {
  const viewKey = getCurrentScrollViewKey();
  saveScroll(viewKey);
  UIState.save();

  const contentId = content?.content_id ?? content?.contentId ?? content?.id;
  if (!STATE.subscribeModalOpen) {
    pushOverlayState('modal', { contentId });
  }

  STATE.currentModalContent = content;
  STATE.subscribeModalOpen = true;
  STATE.subscribeToggleInFlight = false;
  STATE.subscribeModalState = {
    isLoading: Boolean(getAccessToken()) && !STATE.subscriptionsLoadedAt,
    loadFailed: false,
  };
  const titleEl = document.getElementById('modalWebtoonTitle');
  const modalEl = document.getElementById('subscribeModal');
  const linkContainer = document.getElementById('modalWebtoonLinkContainer');
  if (UI.subscribeInlineError) UI.subscribeInlineError.textContent = '';
  const returnFocusEl = opts?.returnFocusEl instanceof HTMLElement ? opts.returnFocusEl : null;
  closeProfileMenu();

  recordRecentlyOpened(content);
  if (STATE.search.pageOpen) {
    recordRecentlySearchedContent(content);
  }

  const titleText = String(content?.title || '').trim();
  const url = getContentUrl(content);
  const meta = normalizeMeta(content?.meta);
  const rawAuthors = meta?.common?.authors;
  const authorText = (() => {
    if (Array.isArray(rawAuthors)) {
      return rawAuthors
        .map((author) => String(author || '').trim())
        .filter(Boolean)
        .join(', ');
    }
    if (typeof rawAuthors === 'string') {
      return rawAuthors.trim();
    }
    return '';
  })();
  const displayText = authorText ? `${titleText}(${authorText})` : titleText;

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
      anchor.textContent = displayText;
      anchor.href = url;
      anchor.target = '_blank';
      anchor.rel = 'noopener noreferrer';
      anchor.className = 'es-link inline-block cursor-pointer pointer-events-auto';
      titleEl.appendChild(anchor);
    } else {
      titleEl.textContent = displayText;
    }
  } else if (modalEl) {
    const fallbackEl = modalEl.querySelector('p');
    if (fallbackEl) fallbackEl.textContent = displayText;
  }

  if (linkContainer) {
    while (linkContainer.firstChild) linkContainer.removeChild(linkContainer.firstChild);
    linkContainer.classList.add('hidden');
  }
  if (modalEl) {
    const initialFocusEl = supportsPublicationUI(content)
      ? UI.subscribePublicationButton || UI.subscribeCompletionButton
      : UI.subscribeCompletionButton;
    openModal(modalEl, {
      initialFocusEl: initialFocusEl || modalEl,
      returnFocusEl,
    });
  }
  syncSubscribeModalUI(content);

  preloadSubscriptionsOnce()
    .then(() => {
      if (!STATE.subscribeModalOpen) return;
      STATE.subscribeModalState.isLoading = false;
      STATE.subscribeModalState.loadFailed = false;
      syncSubscribeModalUI(content);
    })
    .catch(() => {
      if (!STATE.subscribeModalOpen) return;
      STATE.subscribeModalState.isLoading = false;
      STATE.subscribeModalState.loadFailed = true;
      showToast('구독 상태를 불러오지 못했습니다. 다시 시도해 주세요.', {
        type: 'error',
      });
      syncSubscribeModalUI(content);
    });
}

function performCloseSubscribeModal() {
  const modalEl = document.getElementById('subscribeModal');
  if (modalEl) closeModal(modalEl);
  STATE.currentModalContent = null;
  STATE.subscribeModalOpen = false;
  restoreScroll(getCurrentScrollViewKey(), { container: UI.contentGrid, requireChildren: true });
  ensureScrollLockConsistency();
}

function closeSubscribeModal({ fromPopstate = false, overlayId = null, skipHistory = false } = {}) {
  if (!STATE.subscribeModalOpen && !STATE.currentModalContent) {
    popOverlayState('modal', overlayId);
    ensureScrollLockConsistency();
    return;
  }

  if (!fromPopstate && !skipHistory) {
    const top = getOverlayStackTop();
    if (top?.overlay === 'modal') {
      history.back();
      return;
    }
  }

  performCloseSubscribeModal();
  popOverlayState('modal', overlayId);
  ensureScrollLockConsistency();
}

window.toggleSubscriptionFromModal = async function (alertType = 'completion') {
  const content = STATE.currentModalContent;
  if (!content) return;
  const modalState = STATE.subscribeModalState || {};
  if (modalState.isLoading) return;
  if (STATE.subscribeToggleInFlight) return;

  if (modalState.loadFailed) {
    retryModalSubscriptionLoad(content);
    return;
  }

  if (!requireAuthOrPrompt('subscription-toggle-modal')) return;
  const normalizedType =
    String(alertType || '').toLowerCase() === 'publication' ? 'publication' : 'completion';
  if (normalizedType === 'publication' && !supportsPublicationUI(content)) return;

  const btn =
    normalizedType === 'publication'
      ? UI.subscribePublicationButton
      : UI.subscribeCompletionButton;
  const disabledClasses = UI_CLASSES.btnDisabled.split(' ');
  const currently =
    normalizedType === 'publication'
      ? isPublicationSubscribed(content)
      : isCompletionSubscribed(content);
  if (UI.subscribeInlineError) UI.subscribeInlineError.textContent = '';

  STATE.subscribeToggleInFlight = true;
  const activeLabel = currently ? '해제하는 중…' : '구독하는 중…';
  if (UI.subscribePublicationButton) {
    UI.subscribePublicationButton.disabled = true;
    UI.subscribePublicationButton.classList.add(...disabledClasses);
    if (btn === UI.subscribePublicationButton) {
      UI.subscribePublicationButton.textContent = activeLabel;
    }
  }
  if (UI.subscribeCompletionButton) {
    UI.subscribeCompletionButton.disabled = true;
    UI.subscribeCompletionButton.classList.add(...disabledClasses);
    if (btn === UI.subscribeCompletionButton) {
      UI.subscribeCompletionButton.textContent = activeLabel;
    }
  }

  try {
    const res = currently
      ? await unsubscribeContent(content, normalizedType)
      : await subscribeContent(content, normalizedType);
    const flags = res?.subscription ?? null;
    const isOn =
      normalizedType === 'publication'
        ? Boolean(flags?.wants_publication)
        : Boolean(flags?.wants_completion);
    const label = normalizedType === 'publication' ? '공개 알림' : '완결 알림';
    showToast(isOn ? `${label}을 구독했습니다.` : `${label}을 해제했습니다.`, {
      type: 'success',
    });
    if (res && Object.prototype.hasOwnProperty.call(res, 'subscription')) {
      scheduleSubscriptionsSoftRefresh('toggle');
    } else {
      loadSubscriptions({ force: true, silent: true }).catch((err) => {
        if (isAbortError(err)) return;
        console.warn('Failed to refresh subscriptions after toggle', err);
      });
    }
  } catch (e) {
    if (e?.httpStatus === 401) {
      showToast('로그인이 필요합니다.', { type: 'error' });
      openAuthModal({ reason: 'subscription-auth' });
    } else {
      if (UI.subscribeInlineError) {
        UI.subscribeInlineError.textContent =
          e?.message || '잠시 후 다시 시도해 주세요.';
      }
      showToast(e?.message || '잠시 후 다시 시도해 주세요.', { type: 'error' });
    }
  } finally {
    STATE.subscribeToggleInFlight = false;
    if (UI.subscribePublicationButton) {
      UI.subscribePublicationButton.disabled = false;
      UI.subscribePublicationButton.classList.remove(...disabledClasses);
    }
    if (UI.subscribeCompletionButton) {
      UI.subscribeCompletionButton.disabled = false;
      UI.subscribeCompletionButton.classList.remove(...disabledClasses);
    }
    if (content) syncSubscribeModalUI(content);
  }
};

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
