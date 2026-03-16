import {
  QueryClientProvider,
  useQueryClient,
} from "@tanstack/react-query";
import { Fragment, useRef } from "react";
import { AuthProvider, useAuth } from "../shared/hooks/use-auth";
import { PerfBridge } from "../shared/perf-bridge";
import { queryClient } from "../shared/query";
import { useTheme } from "../shared/theme";
import type { ContentCard, SubscriptionItem } from "../shared/types";
import {
  getBasePathForTab,
  type NavTab,
  resolveCardTabId,
} from "./config";
import {
  AuthModal,
  BottomNav,
  ContentAreaShell,
  FilterBar,
  HeaderShell,
  MyPageOverlay,
  ProfileMenu,
  ProfileAvatar,
  SearchOverlay,
  SubscribeModal,
} from "./view-components";
import { useFilterBarModel } from "./hooks/use-filter-bar-model";
import { useBrowsePaginationController } from "./hooks/use-browse-pagination-controller";
import { useGridModel } from "./hooks/use-grid-model";
import { usePublicHeaderModel } from "./hooks/use-public-header-model";
import { useModalScrollLock } from "./hooks/use-modal-scroll-lock";
import { usePublicDerivedState } from "./hooks/use-public-derived-state";
import { useProfileMenuState } from "./hooks/use-profile-menu-state";
import { usePublicActions } from "./hooks/use-public-actions";
import { usePublicContentModel } from "./hooks/use-public-content-model";
import { usePublicData } from "./hooks/use-public-data";
import { usePublicNavigation } from "./hooks/use-public-navigation";
import { usePublicModalModels } from "./hooks/use-public-modal-models";
import { usePublicTelemetry } from "./hooks/use-public-telemetry";
import { usePublicUiState } from "./hooks/use-public-ui-state";
import { recordRecentContent, useRecentActivity } from "./hooks/use-recent-activity";
import { useSearchViewModel } from "./hooks/use-search-view-model";
import { useSearchOverlayModel } from "./hooks/use-search-overlay-model";

function PublicShell() {
  const { theme, toggleTheme } = useTheme();
  const queryClient = useQueryClient();
  const auth = useAuth();
  const trackPublicEvent = usePublicTelemetry();
  const {
    activeTab,
    authEmail,
    authError,
    authMode,
    authPassword,
    authPasswordConfirm,
    myViewMode,
    novelFilter,
    ottFilter,
    resetAuthForm,
    searchInput,
    selectedSources,
    setActiveTab,
    setAuthEmail,
    setAuthError,
    setAuthMode,
    setAuthPassword,
    setAuthPasswordConfirm,
    setMyViewMode,
    setNovelFilter,
    setOttFilter,
    setSearchInput,
    setSelectedSources,
    setWebtoonFilter,
    webtoonFilter,
  } = usePublicUiState();
  const contentGridRef = useRef<HTMLDivElement | null>(null);
  const contentGridSentinelRef = useRef<HTMLDivElement | null>(null);
  const modalScrollYRef = useRef<number | null>(null);
  const {
    closeProfileMenu,
    isProfileMenuOpen,
    profileButtonRef,
    profileMenuRef,
    toggleProfileMenu,
  } = useProfileMenuState(trackPublicEvent);

  const {
    closeOverlay,
    location,
    myPageOpen,
    navigate,
    navigateTab,
    openContentModal,
    openMyPageOverlay,
    openSearchOverlay,
    routeContent,
    searchOpen,
  } = usePublicNavigation({
    activeTab,
    closeProfileMenu,
    recordRecentContent,
    scrollYRef: modalScrollYRef,
    setActiveTab,
    setSearchInput,
    trackPublicEvent,
  });
  useModalScrollLock({ routeContent, scrollYRef: modalScrollYRef });

  const {
    browseQuery,
    detailQuery,
    loginMeQuery,
    recommendationsQuery,
    searchQuery,
    subscriptionsQuery,
  } = usePublicData({
    activeTab,
    authToken: auth.token,
    novelFilter,
    ottFilter,
    routeContent,
    searchInput,
    searchOpen,
    selectedSources,
    webtoonFilter,
  });

  const {
    clearRecentSearches,
    recentHistoryItems,
    recentSearches,
    rememberSearch,
  } = useRecentActivity([location.pathname, routeContent?.contentId]);
  const subscriptionItems = subscriptionsQuery.data?.items || [];
  const { contentCountIndicator, gridItems, myPageSummaryStatus } = useGridModel({
    activeTab,
    browsePages: browseQuery.data?.pages,
    isSubscriptionsLoading: subscriptionsQuery.isLoading,
    myViewMode,
    recommendations: recommendationsQuery.data?.contents || [],
    subscriptionItems,
  });
  const {
    searchPopularItems,
    searchPopularSubtitle,
    searchResults,
    usesRecentSearchPopular,
  } = useSearchOverlayModel({
    activeTab,
    gridItems,
    recentHistoryItems,
    recommendations: recommendationsQuery.data?.contents || [],
    searchInput,
    searchQueryData: searchQuery.data,
  });

  const {
    modalContent,
    modalSubscribed,
    showAuthModal,
  } = usePublicDerivedState({
    auth,
    detailContent: detailQuery.data || null,
    loginEmail: loginMeQuery.data?.user?.email,
    myPageOpen,
    subscriptionItems,
    theme,
  });
  const headerModel = usePublicHeaderModel({
    auth,
    navigate,
    onOpenMyPage: openMyPageOverlay,
    theme,
    toggleTheme,
    trackPublicEvent,
  });

  const { requestNextBrowsePage } = useBrowsePaginationController({
    activeTab,
    browseQuery,
    sentinelRef: contentGridSentinelRef,
    trackPublicEvent,
  });

  const publicActions = usePublicActions({
    activeTab,
    auth,
    authEmail,
    authMode,
    authPassword,
    authPasswordConfirm,
    modalContent,
    modalSubscribed,
    myPageOpen,
    navigate,
    onAuthError: setAuthError,
    onAuthSuccess: resetAuthForm,
    onRememberSearch: rememberSearch,
    onSearchInputChange: setSearchInput,
    queryClient,
    trackPublicEvent,
  });

  function trackSubscriptionClick() {
    if (!modalContent) {
      return;
    }

    trackPublicEvent("subscription_cta_clicked", {
      action: modalSubscribed ? "unsubscribe" : "subscribe",
      contentType: modalContent.content_type || activeTab,
      fromTab: activeTab,
      requiresAuth: !auth.isAuthenticated,
      source: modalContent.source,
    });
  }

  const { categoryFilters, sourceChips } = useFilterBarModel({
    activeTab,
    myViewMode,
    novelFilter,
    ottFilter,
    selectedSources,
    setMyViewMode,
    setNovelFilter,
    setOttFilter,
    setSelectedSources,
    setWebtoonFilter,
    webtoonFilter,
  });
  const contentModel = usePublicContentModel({
    activeTab,
    authIsAuthenticated: auth.isAuthenticated,
    browseHasNextPage: browseQuery.hasNextPage,
    browseIsLoading: browseQuery.isLoading,
    contentCountIndicator,
    gridItems,
    homeRecommendations: recommendationsQuery.data?.contents || [],
    myPageSummaryStatus,
    navigate,
    onOpenContent: openContentModal,
    recentHistoryItems,
    requestNextBrowsePage,
    resolveTabId: (content) => resolveCardTabId(content, "webtoon"),
    setAuthMode,
    subscriptionsIsLoading: subscriptionsQuery.isLoading,
  });
  const searchViewModel = useSearchViewModel({
    clearRecentSearches,
    onClose: () => closeOverlay({ showAuthModal }),
    onOpenContent: openContentModal,
    onSearchInputChange: setSearchInput,
    onSubmitSearch: publicActions.submitSearch,
    recentSearches,
    resolveTabId: (content) => resolveCardTabId(content, "webtoon"),
    searchInput,
    searchIsLoading: searchQuery.isLoading,
    searchOpen,
    searchPopularItems,
    searchPopularSubtitle,
    searchResults,
    usesRecentSearchPopular,
  });
  const modalModels = usePublicModalModels({
    auth,
    authDescription: "이메일과 비밀번호를 입력해 주세요.",
    authEmail,
    authError,
    authMode,
    authPassword,
    authPasswordConfirm,
    email: loginMeQuery.data?.user?.email || auth.user?.email || "-",
    modalContent,
    modalSubscribed,
    myPageOpen,
    myPageSummaryStatus,
    onAuthClose: () => closeOverlay({ showAuthModal }),
    onAuthSubmit: publicActions.submitAuth,
    onBack: () => closeOverlay({ showAuthModal }),
    onChangePassword: publicActions.handleChangePassword,
    onGoSubscriptions: () => navigate("/subscriptions"),
    onLogout: () => auth.logout(),
    onRequireAuth: () => navigate("/mypage"),
    onSetAuthEmail: setAuthEmail,
    onSetAuthMode: setAuthMode,
    onSetAuthPassword: setAuthPassword,
    onSetAuthPasswordConfirm: setAuthPasswordConfirm,
    onTrackSubscriptionClick: trackSubscriptionClick,
    onToggleSubscription: () => publicActions.subscriptionMutation.mutate(),
    showAuthModal,
    subscriptionItems,
  });

  return (
    <Fragment>
      <div id="esPerfRoot">
        <PerfBridge />
      </div>
      <HeaderShell
        isProfileMenuOpen={isProfileMenuOpen}
        onHome={headerModel.onHome}
        onOpenSearch={openSearchOverlay}
        onToggleProfileMenu={toggleProfileMenu}
        profileAvatar={<ProfileAvatar label={headerModel.profileInitial} loggedIn={auth.isAuthenticated} />}
        profileButtonRef={profileButtonRef}
        profileMenu={
          auth.isAuthenticated ? (
            <div ref={profileMenuRef}>
              <ProfileMenu
                isAdmin={auth.user?.role === "admin"}
                isOpen={isProfileMenuOpen}
                onAdmin={headerModel.onAdmin}
                onLogout={headerModel.onLogout}
                onMyPage={headerModel.onMyPage}
                onSubscriptions={headerModel.onSubscriptions}
                onToggleTheme={headerModel.onToggleTheme}
                themeToggleLabel={headerModel.themeToggleLabel}
              />
            </div>
          ) : null
        }
      />

      <div
        className="fixed top-4 left-1/2 -translate-x-1/2 z-[110] space-y-2 w-[calc(100%-32px)] max-w-[520px] pointer-events-none"
        id="toastContainer"
        role="status"
        aria-atomic="true"
        aria-live="polite"
      ></div>

      <main className="min-h-screen pb-24 relative">
        <FilterBar
          activeTab={activeTab}
          categoryFilters={categoryFilters}
          myViewMode={myViewMode}
          onSetMyViewMode={setMyViewMode}
          sourceChips={sourceChips}
        />

        <ContentAreaShell
          activeTab={contentModel.activeTab}
          browseHasNextPage={contentModel.browseHasNextPage}
          browseIsLoading={contentModel.browseIsLoading}
          contentCountIndicator={contentModel.contentCountIndicator}
          contentGridRef={contentGridRef}
          contentGridSentinelRef={contentGridSentinelRef}
          gridItems={contentModel.gridItems}
          homeRecommendations={contentModel.homeRecommendations}
          onGoSearch={contentModel.onGoSearch}
          onGoWebtoon={contentModel.onGoWebtoon}
          onLoadMore={contentModel.onLoadMore}
          onLoginRequired={contentModel.onLoginRequired}
          onOpenContent={contentModel.onOpenContent}
          recentHistoryItems={contentModel.recentHistoryItems}
          resolveTabId={contentModel.resolveTabId}
          showMyEmptyState={contentModel.showMyEmptyState}
          showMyLoggedOutState={contentModel.showMyLoggedOutState}
          subscriptionsIsLoading={contentModel.subscriptionsIsLoading}
        />
      </main>

      <BottomNav activeTab={activeTab} onSelect={navigateTab} />

      <SubscribeModal {...modalModels.subscribeModalProps} />

      <SearchOverlay {...searchViewModel} />

      <MyPageOverlay {...modalModels.myPageOverlayProps} />

      <AuthModal {...modalModels.authModalProps} />
    </Fragment>
  );
}

export function PublicApp() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <PublicShell />
      </AuthProvider>
    </QueryClientProvider>
  );
}



