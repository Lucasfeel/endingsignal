export const ICONS = {
  home: `<svg class="w-6 h-6" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M10.9 2.8a1.6 1.6 0 0 1 2.2 0l8.1 7.3a1 1 0 0 1-.7 1.7h-.8v7.1a2 2 0 0 1-2 2h-3.7a1 1 0 0 1-1-1v-4.6h-2v4.6a1 1 0 0 1-1 1H6.3a2 2 0 0 1-2-2v-7.1h-.8a1 1 0 0 1-.7-1.7z"/></svg>`,
  webtoon: `<span aria-hidden="true" style="display:block;width:24px;height:24px;background-color:currentColor;-webkit-mask:url('/static/webtoon_bubble_24_currentColor.svg') center/contain no-repeat;mask:url('/static/webtoon_bubble_24_currentColor.svg') center/contain no-repeat;"></span>`,
  novel: `<span aria-hidden="true" style="display:block;width:24px;height:24px;background-color:currentColor;-webkit-mask:url('/static/webnovel_leaf_24_white.svg') center/contain no-repeat;mask:url('/static/webnovel_leaf_24_white.svg') center/contain no-repeat;"></span>`,
  ott: `<span aria-hidden="true" style="display:block;width:24px;height:24px;background-color:currentColor;-webkit-mask:url('/static/ott_youtube_like_filled.svg') center/145% auto no-repeat;mask:url('/static/ott_youtube_like_filled.svg') center/145% auto no-repeat;"></span>`,
  my: `<svg class="w-full h-full" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 3.2a1.1 1.1 0 0 1 1.01.67l1.95 4.27 4.65.55a1.1 1.1 0 0 1 .62 1.92l-3.44 3.1.94 4.58a1.1 1.1 0 0 1-1.62 1.17L12 17.5l-4.1 2.35a1.1 1.1 0 0 1-1.63-1.16l.94-4.58-3.44-3.1a1.1 1.1 0 0 1 .62-1.92l4.66-.55 1.94-4.27A1.1 1.1 0 0 1 12 3.2z"/></svg>`,
  me: `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="es-icon-stroke" aria-hidden="true"><circle cx="12" cy="8" r="4"></circle><path d="M4 20c1.8-4 5.2-6 8-6s6.2 2 8 6"></path></svg>`,
} as const;

export const UI_CLASSES = {
  btnPrimary:
    "es-btn es-btn-primary h-10 px-4 rounded-xl text-sm font-semibold disabled:opacity-50 disabled:cursor-not-allowed",
  btnSecondary:
    "es-btn es-btn-secondary h-10 px-4 rounded-xl text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed",
  btnSolid:
    "es-btn es-btn-solid h-10 px-4 rounded-xl text-sm font-semibold disabled:opacity-50 disabled:cursor-not-allowed",
  iconBtn: "es-icon-btn h-10 w-10 flex items-center justify-center rounded-xl",
  iconBtnSm: "es-icon-btn h-8 w-8 flex items-center justify-center rounded-lg",
  headerSearchIcon: "es-icon-btn h-10 w-10 flex items-center justify-center rounded-xl transition-colors",
  headerProfileIcon: "es-icon-btn h-10 w-10 flex items-center justify-center rounded-xl transition-colors",
  emptyWrap: "w-full col-span-full py-12 px-4 flex flex-col items-center justify-center text-center",
  emptyTitle: "text-lg font-semibold es-text",
  emptyMsg: "mt-2 text-sm es-muted max-w-md",
  sectionTitle: "text-base font-semibold es-text",
  sectionSubtle: "text-sm es-muted transition-colors",
  inputBase: "es-input-base w-full h-10 rounded-xl px-4 pr-10 outline-none text-base",
  inputSm: "es-input-sm w-full px-3 py-2 rounded-xl text-sm focus:outline-none",
  searchTrigger: "es-input-base transition-all duration-200 rounded-xl px-3 py-2 text-sm focus:outline-none",
  inputLabel: "block text-sm font-medium es-muted",
  modalWrap: "flex items-center justify-center",
  modalCard: "es-modal-card relative z-10 p-6 rounded-2xl w-[90%] max-w-sm mx-auto shadow-2xl transform transition-all",
  modalTitle: "text-xl font-semibold mb-1 es-text tracking-[-0.02em]",
  modalBodyText: "es-muted text-sm",
  grid2to3: "grid grid-cols-3 gap-2 items-start content-start",
  pageOverlayRoot: "es-page-overlay-root",
  pageOverlayContainer: "mx-auto h-full max-w-[520px] px-4",
  pageCard: "es-page-card rounded-2xl p-4 shadow-sm",
  menuWrap: "es-menu-wrap rounded-xl shadow-md overflow-hidden py-2",
  menuItem: "es-menu-item w-full text-left px-4 py-3 text-sm focus:outline-none",
  menuItemDanger: "es-menu-item es-menu-item-danger w-full text-left px-4 py-3 text-sm focus:outline-none",
  loadMoreBtn: "es-load-more w-full h-[44px] rounded-xl text-[13px] font-semibold transition-colors",
} as const;
