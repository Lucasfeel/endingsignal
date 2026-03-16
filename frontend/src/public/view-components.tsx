import type { CSSProperties, FormEvent, ReactNode, RefObject } from "react";

import { buildContentKey, extractContentUrl, extractDisplayMeta } from "../shared/content";
import type { ContentCard, SubscriptionItem } from "../shared/types";
import { ICONS, UI_CLASSES } from "./ui-tokens";

export type PublicTab = "home" | "webtoon" | "novel" | "ott" | "my";

export type ActionButton = {
  label: string;
  onClick: () => void;
  variant?: "primary" | "secondary";
};

export type SourceChipItem = {
  active: boolean;
  asset?: string;
  hasSelection: boolean;
  label: string;
  normalizedId: string;
  onClick: () => void;
  style?: CSSProperties;
};

export type FilterItem = {
  active: boolean;
  id: string;
  label: string;
  onClick: () => void;
};

const CARD_TITLE_CLAMP_STYLE: CSSProperties = {
  display: "-webkit-box",
  maxHeight: "calc(1.35em * 2)",
  overflow: "hidden",
  textOverflow: "ellipsis",
  WebkitBoxOrient: "vertical",
  WebkitLineClamp: 2,
};

const CARD_META_ELLIPSIS_STYLE: CSSProperties = {
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
};

export function InlineIcon({ markup }: { markup: string }) {
  return <span dangerouslySetInnerHTML={{ __html: markup }} />;
}

export function SourceLogoChip({
  active,
  hasSelection,
  label,
  normalizedId,
  asset,
  onClick,
  style,
}: {
  active: boolean;
  hasSelection: boolean;
  label: string;
  normalizedId: string;
  asset?: string;
  onClick: () => void;
  style?: CSSProperties;
}) {
  const brightnessClass = hasSelection ? (active ? "is-bright" : "is-dim") : "is-neutral";

  return (
    <button
      aria-label={`${label} source filter`}
      aria-pressed={active}
      className={`l1-logo flex-shrink-0 cursor-pointer spring-bounce ${active ? "active" : "inactive"} ${brightnessClass}`}
      data-source-id={normalizedId}
      onClick={onClick}
      style={style}
      type="button"
    >
      <span className="l1-icon" aria-hidden="true">
        {asset ? <img alt="" className="es-source-logo-img" decoding="async" src={asset} /> : label.slice(0, 2)}
      </span>
    </button>
  );
}

export function HeaderShell({
  isProfileMenuOpen,
  onHome,
  onOpenSearch,
  onToggleProfileMenu,
  profileAvatar,
  profileButtonRef,
  profileMenu,
}: {
  isProfileMenuOpen: boolean;
  onHome: () => void;
  onOpenSearch: () => void;
  onToggleProfileMenu: () => void;
  profileAvatar: ReactNode;
  profileButtonRef: RefObject<HTMLButtonElement | null>;
  profileMenu?: ReactNode;
}) {
  return (
    <header
      className="sticky top-0 z-50 h-[56px] px-4 flex items-center justify-between transition-all duration-200 relative"
      id="mainHeader"
    >
      <button
        aria-label="홈으로 이동"
        className="flex items-center cursor-pointer spring-bounce bg-transparent border-0 p-0"
        id="homeButton"
        onClick={onHome}
        type="button"
      >
        <img alt="콘텐츠 완결 알리미" className="block h-[44px] w-auto object-contain" src="/static/brand_logo.svg" />
      </button>
      <div className="flex items-center gap-4">
        <div className="relative flex items-center" id="headerSearchWrap">
          <button
            aria-label="검색"
            className={UI_CLASSES.headerSearchIcon}
            data-ui="header-search-icon"
            id="searchButton"
            onClick={onOpenSearch}
            type="button"
          >
            <svg fill="none" height="22" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" width="22">
              <circle cx="11" cy="11" r="7"></circle>
              <line x1="16.65" x2="21" y1="16.65" y2="21"></line>
            </svg>
          </button>
        </div>
        <div className="relative">
          <button
            aria-expanded={isProfileMenuOpen}
            aria-haspopup="menu"
            className={UI_CLASSES.headerProfileIcon}
            data-ui="header-profile-icon"
            id="profileButton"
            onClick={onToggleProfileMenu}
            ref={profileButtonRef}
            type="button"
          >
            <span className="sr-only">프로필</span>
            <span className="inline-flex items-center justify-center" id="profileButtonText">
              {profileAvatar}
            </span>
          </button>
          {profileMenu}
        </div>
      </div>
    </header>
  );
}

export function FilterBar({
  activeTab,
  categoryFilters,
  myViewMode,
  onSetMyViewMode,
  sourceChips,
}: {
  activeTab: PublicTab;
  categoryFilters: FilterItem[];
  myViewMode: "completion" | "completed";
  onSetMyViewMode: (mode: "completion" | "completed") => void;
  sourceChips: SourceChipItem[];
}) {
  return (
    <div className="sticky top-[56px] z-40" id="filtersWrapper">
      <div className={`${activeTab === "my" ? "" : "hidden "}px-4 py-3 h-[68px] items-center`} id="mySubToggleContainer">
        <div className="es-toggle-shell w-full h-[44px] rounded-lg p-1 flex relative">
          <div
            className={`absolute top-1 bottom-1 w-1/2 rounded-md transition-transform duration-300 ${myViewMode === "completed" ? "translate-x-full" : "translate-x-0"}`}
            id="toggleIndicator"
          ></div>
          <button
            className={`flex-1 z-10 text-[13px] font-semibold text-center leading-[36px] ${myViewMode === "completion" ? "is-active" : ""}`}
            data-mode="completion"
            id="mySubToggleCompletion"
            onClick={() => onSetMyViewMode("completion")}
            type="button"
          >
            완결구독
          </button>
          <button
            className={`flex-1 z-10 text-[13px] font-semibold text-center leading-[36px] ${myViewMode === "completed" ? "is-active" : ""}`}
            data-mode="completed"
            id="mySubToggleCompleted"
            onClick={() => onSetMyViewMode("completed")}
            type="button"
          >
            완결됨
          </button>
        </div>
      </div>

      <div
        className={`${activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott" ? "" : "hidden "}h-[60px] flex items-center px-4 overflow-x-auto hide-scrollbar gap-3`}
        id="l1FilterContainer"
      >
        {sourceChips.map((item) => (
          <SourceLogoChip key={`${item.normalizedId}:${item.label}`} {...item} />
        ))}
      </div>

      <div
        className={`${activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott" ? "" : "hidden "}h-[40px] flex items-center px-4 overflow-x-auto hide-scrollbar border-b`}
        id="l2FilterContainer"
      >
        {categoryFilters.map((item) => (
          <button
            className={`l2-tab spring-bounce ${item.active ? "active" : ""}`}
            key={item.id}
            onClick={item.onClick}
            type="button"
          >
            {item.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export function ParityCard({
  content,
  onOpen,
  tabId,
}: {
  content: ContentCard | SubscriptionItem;
  onOpen: () => void;
  tabId: PublicTab;
}) {
  const meta = extractDisplayMeta(content);
  const authors = meta.authors?.join(", ") || (tabId === "ott" ? "출연진 정보 없음" : "작가 정보 없음");
  const finalLabel =
    tabId === "my" && "final_state" in content ? content.final_state?.label || content.status : null;

  return (
    <button
      aria-label={`${content.title || "콘텐츠"} 열기`}
      className="es-card-root relative cursor-pointer fade-in focus-visible:outline-none"
      data-content-id={String(content.content_id)}
      data-content-type={String(content.content_type || tabId || "")}
      data-source={content.source}
      data-sub-key={buildContentKey(content)}
      onClick={onOpen}
      type="button"
    >
      <div className="px-2 py-2.5 min-h-[72px] flex flex-col justify-start gap-0.5 text-left">
        {finalLabel ? (
          <div className="es-card-badge-row !p-0" data-card-badge-row="true">
            <div className="es-badge-base z-10 inline-flex px-2 py-1 rounded-lg items-center gap-1 es-badge-success">
              <span className="text-[10px] font-black text-black leading-none">{finalLabel}</span>
            </div>
          </div>
        ) : null}
        <div className="es-card-text !h-auto !p-0 !gap-1 items-start w-full min-w-0">
          <h3 className="es-card-title w-full min-w-0 font-semibold text-[13px] leading-[1.35]" style={CARD_TITLE_CLAMP_STYLE}>
            {content.title || ""}
          </h3>
          <p className="es-card-meta w-full min-w-0 text-[11px] mt-1" style={CARD_META_ELLIPSIS_STYLE}>
            {authors}
          </p>
        </div>
      </div>
    </button>
  );
}

export function HomeSection({
  actions,
  emptyMessage,
  emptyTitle,
  items,
  onOpen,
  resolveTabId,
  title,
}: {
  actions?: ActionButton[];
  emptyMessage: string;
  emptyTitle: string;
  items: Array<ContentCard | SubscriptionItem>;
  onOpen: (content: ContentCard | SubscriptionItem) => void;
  resolveTabId: (content: ContentCard | SubscriptionItem) => PublicTab;
  title: string;
}) {
  return (
    <section className="w-full space-y-3">
      <h2 className="text-base font-semibold es-text">{title}</h2>
      {items.length ? (
        <div className="grid grid-cols-3 gap-2">
          {items.map((item) => (
            <ParityCard
              content={item}
              key={`${item.source}:${item.content_id}:${title}`}
              onOpen={() => onOpen(item)}
              tabId={resolveTabId(item)}
            />
          ))}
        </div>
      ) : (
        <div className="w-full es-page-card rounded-2xl p-4 text-center">
          <p className="text-lg font-semibold es-text">{emptyTitle}</p>
          <p className="mt-2 text-sm es-muted max-w-md mx-auto">{emptyMessage}</p>
          {actions?.length ? (
            <div className="mt-4 flex flex-wrap justify-center gap-2">
              {actions.map((action) => (
                <button
                  className={`${action.variant === "secondary" ? UI_CLASSES.btnSecondary : UI_CLASSES.btnPrimary} spring-bounce`}
                  key={action.label}
                  onClick={action.onClick}
                  type="button"
                >
                  {action.label}
                </button>
              ))}
            </div>
          ) : null}
        </div>
      )}
    </section>
  );
}

export function SearchOverlay({
  onClearRecentSearches,
  onClose,
  onOpenContent,
  onSearchInputChange,
  onSubmitSearch,
  recentSearches,
  resolveTabId,
  searchInput,
  searchIsLoading,
  searchOpen,
  searchPopularItems,
  searchPopularSubtitle,
  searchResults,
  usesRecentSearchPopular,
}: {
  onClearRecentSearches: () => void;
  onClose: () => void;
  onOpenContent: (content: ContentCard | SubscriptionItem) => void;
  onSearchInputChange: (value: string) => void;
  onSubmitSearch: (query: string) => void;
  recentSearches: string[];
  resolveTabId: (content: ContentCard | SubscriptionItem) => PublicTab;
  searchInput: string;
  searchIsLoading: boolean;
  searchOpen: boolean;
  searchPopularItems: Array<ContentCard | SubscriptionItem>;
  searchPopularSubtitle: string;
  searchResults: ContentCard[];
  usesRecentSearchPopular: boolean;
}) {
  const hasQuery = searchInput.trim().length > 0;

  return (
    <div
      className={`fixed inset-0 z-[90] ${searchOpen ? "" : "hidden "}h-[100dvh] overflow-hidden ${UI_CLASSES.pageOverlayRoot}`}
      data-ui="page-overlay-root"
      id="searchPage"
      style={{ height: "calc(var(--vvh, 1vh) * 100)" }}
    >
      <div className="relative z-10 h-full">
        <div className={`h-full ${UI_CLASSES.pageOverlayContainer}`} data-ui="page-overlay-container" id="searchPageContainer">
          <div className="flex h-full flex-col min-h-0">
            <div className="es-overlay-header flex items-center gap-2 pb-3">
              <button aria-label="뒤로" className={UI_CLASSES.iconBtn} data-ui="search-back" id="searchBackButton" onClick={onClose} type="button">
                ←
              </button>
              <div className="flex-1 relative">
                <input
                  autoComplete="off"
                  className={UI_CLASSES.inputBase}
                  data-ui="search-input"
                  id="searchPageInput"
                  inputMode="search"
                  onChange={(event) => onSearchInputChange(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") onSubmitSearch(searchInput);
                  }}
                  placeholder="작품을 검색해 보세요"
                  type="text"
                  value={searchInput}
                />
                <button
                  aria-label="검색어 지우기"
                  className={`absolute right-2 top-1/2 -translate-y-1/2 ${UI_CLASSES.iconBtnSm} hidden`}
                  data-ui="search-clear"
                  id="searchClearButton"
                  onClick={() => onSearchInputChange("")}
                  type="button"
                >
                  ×
                </button>
              </div>
            </div>

            <div className="flex-1 min-h-0 overflow-y-auto pb-[calc(96px+env(safe-area-inset-bottom))] pt-2 space-y-6" id="searchPageBody">
              {hasQuery ? (
                <div className="space-y-4" id="searchResultsView">
                  <div className="flex items-center justify-between" id="searchResultsMeta">
                    <div className={UI_CLASSES.sectionSubtle} data-ui="search-result-label" id="searchResultLabel">
                      현재 표시 중인 작품 <span id="searchResultCount">{searchResults.length}</span>개
                    </div>
                  </div>
                  <div className={`${UI_CLASSES.grid2to3} mt-2 pb-4 overflow-visible`} data-ui="grid-2to3" id="searchPageResults">
                    {searchResults.map((content) => (
                      <ParityCard
                        content={content}
                        key={`${content.source}:${content.content_id}:search`}
                        onOpen={() => onOpenContent(content)}
                        tabId="home"
                      />
                    ))}
                  </div>
                  {!searchIsLoading && searchResults.length === 0 ? (
                    <div className="hidden flex flex-col items-center gap-3 rounded-xl border px-4 py-10 text-center" id="searchPageEmpty" style={{ display: "flex" }}>
                      <div className={UI_CLASSES.emptyTitle} data-ui="search-empty-title" id="searchEmptyTitle">
                        검색 결과가 없어요
                      </div>
                      <div className={`${UI_CLASSES.emptyMsg} max-w-[320px] text-center`} data-ui="search-empty-msg" id="searchEmptySubtitle">
                        다른 키워드로 검색해보세요.
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : (
                <div className="space-y-6" id="searchIdle">
                  <div className="flex items-center justify-between">
                    <h2 className="text-base font-semibold">최근 검색어</h2>
                    <button
                      className={UI_CLASSES.sectionSubtle}
                      data-ui="search-recent-clear"
                      id="searchRecentClearAll"
                      onClick={onClearRecentSearches}
                      type="button"
                    >
                      전체삭제
                    </button>
                  </div>
                  <div className="flex flex-wrap gap-2" id="searchRecentChips">
                    {recentSearches.length
                      ? recentSearches.map((query) => (
                          <button className="es-chip h-9 px-3 inline-flex items-center rounded-full text-sm" key={query} onClick={() => onSubmitSearch(query)} type="button">
                            {query}
                          </button>
                        ))
                      : (
                          <div className="text-sm es-muted">최근 검색어가 없습니다</div>
                        )}
                  </div>
                  <div className="flex items-center justify-between">
                    <div className="space-y-0.5">
                      <h2 className={UI_CLASSES.sectionTitle} data-ui="search-popular-title" id="searchPopularTitle">
                        {usesRecentSearchPopular ? "최근 본 작품" : "추천 작품"}
                      </h2>
                      <p className={UI_CLASSES.sectionSubtle} data-ui="search-popular-subtitle" id="searchPopularSubtitle">
                        {searchPopularSubtitle}
                      </p>
                    </div>
                  </div>
                  <div className={UI_CLASSES.grid2to3} data-ui="grid-2to3" id="searchPopularGrid">
                    {searchPopularItems.map((content) => (
                      <ParityCard
                        content={content}
                        key={`${content.source}:${content.content_id}:popular`}
                        onOpen={() => onOpenContent(content)}
                        tabId={resolveTabId(content)}
                      />
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export function GridEmptyState({
  actions,
  message,
  title,
}: {
  actions?: ActionButton[];
  message: string;
  title: string;
}) {
  return (
    <div className={`col-span-3 ${UI_CLASSES.emptyWrap}`}>
      <h2 className={UI_CLASSES.emptyTitle}>{title}</h2>
      <p className={UI_CLASSES.emptyMsg}>{message}</p>
      {actions?.length ? (
        <div className="mt-4 flex flex-wrap items-center justify-center gap-3">
          {actions.map((action) => (
            <button
              className={`${action.variant === "secondary" ? UI_CLASSES.btnSecondary : UI_CLASSES.btnPrimary} spring-bounce`}
              key={action.label}
              onClick={action.onClick}
              type="button"
            >
              {action.label}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function ProfileAvatar({ loggedIn, label }: { loggedIn: boolean; label: string }) {
  if (!loggedIn) {
    return <InlineIcon markup={ICONS.me} />;
  }

  return (
    <span
      className="inline-flex h-9 w-9 items-center justify-center rounded-full text-sm font-semibold"
      style={{
        background: "#cfe6d4",
        color: "#111111",
      }}
    >
      {label}
    </span>
  );
}

export function ProfileMenu({
  isAdmin,
  isOpen,
  onAdmin,
  onLogout,
  onMyPage,
  onSubscriptions,
  onToggleTheme,
  themeToggleLabel,
}: {
  isAdmin: boolean;
  isOpen: boolean;
  onAdmin: () => void;
  onLogout: () => void;
  onMyPage: () => void;
  onSubscriptions: () => void;
  onToggleTheme: () => void;
  themeToggleLabel: string;
}) {
  return (
    <div
      aria-label="프로필 메뉴"
      className={`${isOpen ? "" : "hidden "}${UI_CLASSES.menuWrap} absolute right-0 top-full mt-2 z-[1000] w-40`}
      data-ui="menu-wrap"
      id="profileMenu"
      role="menu"
    >
      <button className={UI_CLASSES.menuItem} data-ui="menu-item" id="profileMenuMy" onClick={onSubscriptions} role="menuitem" type="button">
        내 구독
      </button>
      <button className={UI_CLASSES.menuItem} data-ui="menu-item" id="profileMenuMyPage" onClick={onMyPage} role="menuitem" type="button">
        마이페이지
      </button>
      <button
        aria-checked={themeToggleLabel.includes("켜짐")}
        className={UI_CLASSES.menuItem}
        data-ui="menu-item"
        id="profileMenuThemeToggle"
        onClick={onToggleTheme}
        role="menuitemcheckbox"
        type="button"
      >
        {themeToggleLabel}
      </button>
      {isAdmin ? (
        <button className={UI_CLASSES.menuItem} data-ui="menu-item" id="profileMenuAdmin" onClick={onAdmin} role="menuitem" type="button">
          Admin
        </button>
      ) : null}
      <button className={UI_CLASSES.menuItemDanger} data-ui="menu-item-danger" id="profileMenuLogout" onClick={onLogout} role="menuitem" type="button">
        로그아웃
      </button>
    </div>
  );
}

export function BottomNav({
  activeTab,
  onSelect,
}: {
  activeTab: PublicTab;
  onSelect: (tab: PublicTab) => void;
}) {
  const tabs: Array<{ id: PublicTab; label: string }> = [
    { id: "home", label: "홈" },
    { id: "webtoon", label: "웹툰" },
    { id: "novel", label: "웹소설" },
    { id: "ott", label: "OTT" },
    { id: "my", label: "내 구독" },
  ];

  return (
    <nav className="fixed bottom-0 max-w-[520px] w-full mx-auto z-50 pb-safe h-[64px] grid grid-cols-5" id="bottomNav">
      {tabs.map((tab) => {
        const isActive = activeTab === tab.id;
        return (
          <button
            aria-current={isActive ? "page" : undefined}
            aria-label={tab.label}
            className={`bottom-nav-item flex flex-col items-center justify-center w-full spring-bounce ${isActive ? "is-active" : ""}`}
            data-tab-id={tab.id}
            key={tab.id}
            onClick={() => onSelect(tab.id)}
            type="button"
          >
            <div
              className={`${tab.id === "my" ? "h-7 w-7" : "h-6 w-6"} mb-0.5 flex items-center justify-center transform transition-transform duration-200 ${isActive ? "scale-105" : "scale-100 opacity-90"}`}
            >
              <InlineIcon markup={ICONS[tab.id]} />
            </div>
            <span className={`text-[10px] leading-[1.15] ${isActive ? "font-semibold" : "font-medium"}`}>{tab.label}</span>
          </button>
        );
      })}
    </nav>
  );
}

export function ContentAreaShell({
  activeTab,
  browseHasNextPage,
  browseIsLoading,
  contentCountIndicator,
  contentGridRef,
  contentGridSentinelRef,
  gridItems,
  homeRecommendations,
  onLoginRequired,
  onGoSearch,
  onGoWebtoon,
  onLoadMore,
  onOpenContent,
  recentHistoryItems,
  resolveTabId,
  showMyEmptyState,
  showMyLoggedOutState,
  subscriptionsIsLoading,
}: {
  activeTab: PublicTab;
  browseHasNextPage: boolean;
  browseIsLoading: boolean;
  contentCountIndicator: string;
  contentGridRef: RefObject<HTMLDivElement | null>;
  contentGridSentinelRef: RefObject<HTMLDivElement | null>;
  gridItems: Array<ContentCard | SubscriptionItem>;
  homeRecommendations: ContentCard[];
  onLoginRequired: () => void;
  onGoSearch: () => void;
  onGoWebtoon: () => void;
  onLoadMore: () => void;
  onOpenContent: (content: ContentCard | SubscriptionItem) => void;
  recentHistoryItems: ContentCard[];
  resolveTabId: (content: ContentCard | SubscriptionItem) => PublicTab;
  showMyEmptyState: boolean;
  showMyLoggedOutState: boolean;
  subscriptionsIsLoading: boolean;
}) {
  return (
    <>
      <div
        className={`px-4 py-3 fade-in mt-1 min-h-[50vh] ${activeTab === "home" ? "flex flex-col gap-5" : "grid grid-cols-3 gap-1.5 items-start content-start"}`}
        id="contentGridContainer"
        ref={contentGridRef}
      >
        {activeTab === "home" ? (
          <>
            <HomeSection
              actions={[{ label: "웹툰 보기", onClick: onGoWebtoon, variant: "primary" }]}
              emptyMessage="웹툰 탭에서 최신 작품을 먼저 살펴보세요."
              emptyTitle="추천작이 아직 준비되지 않았어요"
              items={homeRecommendations}
              onOpen={onOpenContent}
              resolveTabId={resolveTabId}
              title="추천작"
            />
            <HomeSection
              actions={[{ label: "검색 열기", onClick: onGoSearch, variant: "primary" }]}
              emptyMessage="검색에서 작품을 열어보면 여기에 저장됩니다."
              emptyTitle="검색해서 열어본 작품이 없어요"
              items={recentHistoryItems}
              onOpen={onOpenContent}
              resolveTabId={resolveTabId}
              title="검색했던 작품"
            />
          </>
        ) : showMyLoggedOutState ? (
          <GridEmptyState
            actions={[{ label: "로그인하기", onClick: onLoginRequired, variant: "primary" }]}
            message="내 구독은 로그인 후 확인할 수 있어요."
            title="로그인이 필요해요"
          />
        ) : showMyEmptyState ? (
          <GridEmptyState
            actions={[
              { label: "검색하기", onClick: onGoSearch, variant: "primary" },
              { label: "웹툰 보기", onClick: onGoWebtoon, variant: "secondary" },
            ]}
            message="작품 화면에서 완결 알림을 설정해보세요."
            title="완결 알림을 구독한 작품이 없습니다"
          />
        ) : (
          gridItems.map((content) => (
            <ParityCard
              content={content}
              key={`${content.source}:${content.content_id}`}
              onOpen={() => onOpenContent(content)}
              tabId={activeTab}
            />
          ))
        )}
      </div>

      <div className="px-4 pb-4 space-y-2" id="contentGridFooter">
        <div className="text-center text-[11px]" id="contentCountIndicator">
          {contentCountIndicator}
        </div>
        {(activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott") && browseHasNextPage ? (
          <button
            className="hidden es-load-more w-full h-[44px] rounded-xl text-[13px] font-semibold transition-colors"
            data-ui="load-more"
            id="contentLoadMoreBtn"
            onClick={onLoadMore}
            type="button"
          >
            더 불러오기
          </button>
        ) : (
          <button className="hidden" data-ui="load-more" id="contentLoadMoreBtn" type="button">
            더 불러오기
          </button>
        )}
        <div className={`${browseHasNextPage ? "h-8" : "h-2"} w-full`} id="contentGridSentinel" ref={contentGridSentinelRef}></div>
      </div>

      <div className={`${activeTab === "ott" ? "" : "hidden "}px-4 pb-6 w-full`} id="seriesFooterButton">
        <button className="w-full h-[52px] border border-dashed rounded-xl flex justify-center items-center cursor-pointer transition-all group" type="button">
          <span className="text-[13px]">(+)&nbsp;당신의 시리즈를 추가해주세요</span>
        </button>
      </div>

      <div className="text-center py-20" id="statusIndicator">
        {activeTab !== "home" && (browseIsLoading || (activeTab === "my" && subscriptionsIsLoading))
          ? "불러오는 중"
          : ""}
      </div>
    </>
  );
}

export function SubscribeModal({
  content,
  isAuthenticated,
  isOpen,
  isSubscribed,
  onClose,
  onRequireAuth,
  onTrackSubscriptionClick,
  onToggleSubscription,
}: {
  content: (ContentCard & { meta?: Record<string, unknown> | null }) | null;
  isAuthenticated: boolean;
  isOpen: boolean;
  isSubscribed: boolean;
  onClose: () => void;
  onRequireAuth: () => void;
  onTrackSubscriptionClick: () => void;
  onToggleSubscription: () => void;
}) {
  return (
    <div
      aria-labelledby="subscribeModalTitle"
      aria-modal="true"
      className={`fixed inset-0 z-[100] ${isOpen ? "" : "hidden "}${UI_CLASSES.modalWrap}`}
      data-ui="modal-wrap"
      id="subscribeModal"
      role="dialog"
      tabIndex={-1}
    >
      <div className="absolute inset-0 es-modal-overlay" data-modal-overlay="true" onClick={onClose}></div>
      <div className={UI_CLASSES.modalCard} data-ui="modal-card" id="modalContent">
        <button
          aria-label="닫기"
          className="absolute right-3 top-3 z-10 inline-flex h-8 w-8 items-center justify-center rounded-full es-icon-btn spring-bounce"
          id="subscribeModalCloseButton"
          onClick={onClose}
          type="button"
        >
          ×
        </button>
        <h2 className={`pr-10 ${UI_CLASSES.modalTitle}`} data-ui="modal-title" id="subscribeModalTitle">
          완결 알림 받기
        </h2>
        <p className={`mb-4 ${UI_CLASSES.modalBodyText}`} data-ui="modal-body" id="modalWebtoonTitle">
          {content
            ? `${content.title}${extractDisplayMeta(content).authors?.length ? `(${extractDisplayMeta(content).authors?.join(", ")})` : ""}`
            : ""}
        </p>
        <div className="flex items-center gap-2 text-sm mb-2" id="subscribeStateLine">
          <span className="h-2 w-2 rounded-full" id="subscribeStateDot"></span>
          <span id="subscribeStateText">{content ? (isSubscribed ? "완결 알림 구독 중" : "아직 구독하지 않았어요") : ""}</span>
        </div>
        <p className="text-xs mt-2" id="subscribeInlineError"></p>
        <div className="mt-4 flex flex-col gap-2 w-full">
          {content && extractContentUrl(content) ? (
            <a
              className={`w-full ${UI_CLASSES.btnSecondary} spring-bounce`}
              data-ui="modal-secondary"
              href={extractContentUrl(content)}
              id="subscribeVisitButton"
              rel="noreferrer"
              target="_blank"
            >
              작품 보러 가기
            </a>
          ) : null}
          <button
            className={`w-full ${UI_CLASSES.btnPrimary} spring-bounce`}
            data-ui="modal-primary"
            id="subscribeCompletionButton"
            onClick={() => {
              onTrackSubscriptionClick();
              if (!isAuthenticated) {
                onRequireAuth();
                return;
              }
              onToggleSubscription();
            }}
            type="button"
          >
            {isSubscribed ? "구독 해제" : "완결 구독"}
          </button>
        </div>
      </div>
    </div>
  );
}

export function AuthModal({
  authDescription,
  authEmail,
  authError,
  authMode,
  authPassword,
  authPasswordConfirm,
  isOpen,
  onClose,
  onSetAuthEmail,
  onSetAuthMode,
  onSetAuthPassword,
  onSetAuthPasswordConfirm,
  onSubmit,
}: {
  authDescription: string;
  authEmail: string;
  authError: string;
  authMode: "login" | "register";
  authPassword: string;
  authPasswordConfirm: string;
  isOpen: boolean;
  onClose: () => void;
  onSetAuthEmail: (value: string) => void;
  onSetAuthMode: (mode: "login" | "register") => void;
  onSetAuthPassword: (value: string) => void;
  onSetAuthPasswordConfirm: (value: string) => void;
  onSubmit: () => void;
}) {
  return (
    <div
      aria-labelledby="authTitle"
      aria-modal="true"
      className={`fixed inset-0 z-[120] ${isOpen ? "" : "hidden "}${UI_CLASSES.modalWrap}`}
      data-ui="modal-wrap"
      id="authModal"
      role="dialog"
      tabIndex={-1}
    >
      <div className="absolute inset-0 es-modal-overlay" data-modal-overlay="true" onClick={onClose}></div>
      <div className={UI_CLASSES.modalCard} data-ui="modal-card">
        <h2 className={UI_CLASSES.modalTitle} data-ui="modal-title" id="authTitle">
          {authMode === "login" ? "로그인" : "회원가입"}
        </h2>
        <p className={`mb-4 ${UI_CLASSES.modalBodyText}`} data-ui="modal-body" id="authDescription">
          {authDescription}
        </p>
        <form
          className="space-y-3"
          onSubmit={(event) => {
            event.preventDefault();
            onSubmit();
          }}
        >
          <input
            autoComplete="email"
            className={UI_CLASSES.inputSm}
            data-ui="input-sm"
            id="authEmail"
            name="username"
            onChange={(event) => onSetAuthEmail(event.target.value)}
            placeholder="email"
            type="email"
            value={authEmail}
          />
          <input
            autoComplete={authMode === "login" ? "current-password" : "new-password"}
            className={UI_CLASSES.inputSm}
            data-ui="input-sm"
            id="authPassword"
            name="password"
            onChange={(event) => onSetAuthPassword(event.target.value)}
            placeholder="password"
            type="password"
            value={authPassword}
          />
          <div className={`space-y-2 ${authMode === "register" ? "" : "hidden "}`} id="authPasswordConfirmRow">
            <label className={UI_CLASSES.inputLabel} data-ui="input-label">
              비밀번호 확인
            </label>
            <input
              autoComplete="new-password"
              className={UI_CLASSES.inputSm}
              data-ui="input-sm"
              id="authPasswordConfirm"
              name="new-password-confirm"
              onChange={(event) => onSetAuthPasswordConfirm(event.target.value)}
              placeholder="비밀번호를 다시 입력"
              type="password"
              value={authPasswordConfirm}
            />
          </div>
          <p className="text-xs min-h-[16px]" id="authError">
            {authError}
          </p>
          <div className="flex justify-end gap-2 mt-4">
            <button className={`${UI_CLASSES.btnSecondary} spring-bounce`} data-ui="modal-secondary" id="authCloseBtn" onClick={onClose} type="button">
              닫기
            </button>
            <button className={`${UI_CLASSES.btnPrimary} spring-bounce`} data-ui="modal-primary" id="authSubmitBtn" type="submit">
              {authMode === "login" ? "로그인" : "회원가입"}
            </button>
          </div>
        </form>
        <div className="mt-4 text-center text-xs es-muted">
          <span id="authModeHintText">{authMode === "login" ? "계정이 없으신가요?" : "이미 계정이 있나요?"}</span>
          <button
            className="ml-1 font-bold hover:underline"
            id="authToggleModeBtn"
            onClick={() => onSetAuthMode(authMode === "login" ? "register" : "login")}
            type="button"
          >
            {authMode === "login" ? "회원가입" : "로그인"}
          </button>
        </div>
      </div>
    </div>
  );
}

export function MyPageOverlay({
  email,
  isOpen,
  myPageSummaryStatus,
  onBack,
  onChangePassword,
  onGoSubscriptions,
  onLogout,
  subscriptionItems,
}: {
  email: string;
  isOpen: boolean;
  myPageSummaryStatus: string;
  onBack: () => void;
  onChangePassword: (event: FormEvent<HTMLFormElement>) => void;
  onGoSubscriptions: () => void;
  onLogout: () => void;
  subscriptionItems: SubscriptionItem[];
}) {
  return (
    <div
      className={`fixed inset-0 z-[85] ${isOpen ? "" : "hidden "}h-[100dvh] overflow-hidden ${UI_CLASSES.pageOverlayRoot}`}
      data-ui="page-overlay-root"
      id="myPage"
      style={{ height: "calc(var(--vvh, 1vh) * 100)" }}
    >
      <div className="relative z-10 h-full">
        <div className={`h-full ${UI_CLASSES.pageOverlayContainer}`} data-ui="page-overlay-container" id="myPageContainer">
          <div className="flex h-full flex-col min-h-0">
            <div className="es-overlay-header flex items-center gap-2 pb-3">
              <button aria-label="뒤로" className={UI_CLASSES.iconBtn} data-ui="search-back" id="myPageBackBtn" onClick={onBack} type="button">
                ←
              </button>
              <h2 className="text-base font-semibold">마이페이지</h2>
            </div>

            <div className="flex-1 min-h-0 overflow-y-auto pb-[calc(96px+env(safe-area-inset-bottom))]">
              <div className="space-y-3">
                <div className={`${UI_CLASSES.pageCard} space-y-3`} data-ui="page-card" id="myPageSummaryCard">
                  <div className="flex items-center justify-between">
                    <h3 className={UI_CLASSES.sectionTitle} data-ui="section-title">구독 요약</h3>
                    <span className="text-xs es-muted" id="myPageSummaryStatus">
                      {myPageSummaryStatus}
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="rounded-2xl border px-4 py-3 text-center">
                      <div className="text-xs es-muted">구독 중</div>
                      <div className="mt-2 text-2xl font-semibold" id="myPageSummaryActiveCount">
                        {subscriptionItems.filter((item) => item.final_state?.label !== "완결").length}
                      </div>
                    </div>
                    <div className="rounded-2xl border px-4 py-3 text-center">
                      <div className="text-xs es-muted">완결됨</div>
                      <div className="mt-2 text-2xl font-semibold" id="myPageSummaryCompletedCount">
                        {subscriptionItems.filter((item) => item.final_state?.label === "완결").length}
                      </div>
                    </div>
                  </div>
                </div>

                <div className={`${UI_CLASSES.pageCard} space-y-3`} data-ui="page-card">
                  <h3 className={UI_CLASSES.sectionTitle} data-ui="section-title">내 정보</h3>
                  <div className="space-y-2 text-sm">
                    <div className="flex items-center justify-between">
                      <span className={UI_CLASSES.sectionSubtle} data-ui="section-subtle">이메일</span>
                      <span className="font-medium" id="myPageEmailValue">{email}</span>
                    </div>
                  </div>
                </div>

                <div className={`${UI_CLASSES.pageCard} space-y-3`} data-ui="page-card">
                  <h3 className={UI_CLASSES.sectionTitle} data-ui="section-title">구독</h3>
                  <p className="text-xs es-muted">구독한 작품을 한 번에 확인하세요.</p>
                  <button className={`${UI_CLASSES.btnSolid} w-full h-11`} data-ui="btn-solid" id="myPageGoMySubBtn" onClick={onGoSubscriptions} type="button">
                    내 구독으로 이동
                  </button>
                </div>

                <form className={`${UI_CLASSES.pageCard} space-y-3`} data-ui="page-card" onSubmit={onChangePassword}>
                  <h3 className={UI_CLASSES.sectionTitle} data-ui="section-title">보안</h3>
                  <div className="space-y-2">
                    <label className={UI_CLASSES.inputLabel} data-ui="input-label" htmlFor="myPagePwCurrent">현재 비밀번호</label>
                    <input autoComplete="current-password" className={UI_CLASSES.inputSm} data-ui="input-sm" id="myPagePwCurrent" name="myPagePwCurrent" placeholder="현재 비밀번호" type="password" />
                  </div>
                  <div className="space-y-2">
                    <label className={UI_CLASSES.inputLabel} data-ui="input-label" htmlFor="myPagePwNew">새 비밀번호</label>
                    <input autoComplete="new-password" className={UI_CLASSES.inputSm} data-ui="input-sm" id="myPagePwNew" name="myPagePwNew" placeholder="새 비밀번호" type="password" />
                    <p className="text-xs es-muted">비밀번호는 8자 이상 입력해주세요.</p>
                  </div>
                  <div className="space-y-2">
                    <label className={UI_CLASSES.inputLabel} data-ui="input-label" htmlFor="myPagePwConfirm">새 비밀번호 확인</label>
                    <input autoComplete="new-password" className={UI_CLASSES.inputSm} data-ui="input-sm" id="myPagePwConfirm" name="myPagePwConfirm" placeholder="새 비밀번호 확인" type="password" />
                  </div>
                  <p className="text-xs min-h-[16px]" id="myPagePwError"></p>
                  <button className={`${UI_CLASSES.btnSolid} w-full h-11`} data-ui="btn-solid" id="myPagePwSubmit" type="submit">
                    변경하기
                  </button>
                </form>

                <div className={`${UI_CLASSES.pageCard} space-y-3`} data-ui="page-card">
                  <h3 className={UI_CLASSES.sectionTitle} data-ui="section-title">로그아웃</h3>
                  <p className="text-xs es-muted">계정에서 안전하게 로그아웃합니다.</p>
                  <button className={`${UI_CLASSES.btnSolid} w-full h-11`} data-ui="btn-solid" id="myPageLogoutBtn" onClick={onLogout} type="button">
                    로그아웃
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
