import { useMemo } from "react";
import type { NavigateFunction } from "react-router-dom";

import type { ContentCard, SubscriptionItem } from "../../shared/types";
import type { NavTab } from "../config";

export function usePublicContentModel({
  activeTab,
  authIsAuthenticated,
  browseHasNextPage,
  browseIsLoading,
  contentCountIndicator,
  gridItems,
  homeRecommendations,
  myPageSummaryStatus,
  navigate,
  onOpenContent,
  recentHistoryItems,
  requestNextBrowsePage,
  resolveTabId,
  setAuthMode,
  subscriptionsIsLoading,
}: {
  activeTab: NavTab;
  authIsAuthenticated: boolean;
  browseHasNextPage: boolean | undefined;
  browseIsLoading: boolean;
  contentCountIndicator: string;
  gridItems: Array<ContentCard | SubscriptionItem>;
  homeRecommendations: ContentCard[];
  myPageSummaryStatus: string;
  navigate: NavigateFunction;
  onOpenContent: (content: ContentCard | SubscriptionItem) => void;
  recentHistoryItems: ContentCard[];
  requestNextBrowsePage: (trigger: "auto" | "manual") => void;
  resolveTabId: (content: ContentCard | SubscriptionItem) => NavTab;
  setAuthMode: (mode: "login" | "register") => void;
  subscriptionsIsLoading: boolean;
}) {
  return useMemo(
    () => ({
      activeTab,
      browseHasNextPage,
      browseIsLoading,
      contentCountIndicator,
      gridItems,
      homeRecommendations,
      myPageSummaryStatus,
      onGoSearch: () => navigate("/search"),
      onGoWebtoon: () => navigate("/browse/webtoon"),
      onLoadMore: () => requestNextBrowsePage("manual"),
      onLoginRequired: () => {
        setAuthMode("login");
        navigate("/mypage");
      },
      onOpenContent,
      recentHistoryItems,
      resolveTabId,
      showMyEmptyState: activeTab === "my" && authIsAuthenticated && gridItems.length === 0,
      showMyLoggedOutState: activeTab === "my" && !authIsAuthenticated,
      subscriptionsIsLoading,
    }),
    [
      activeTab,
      authIsAuthenticated,
      browseHasNextPage,
      browseIsLoading,
      contentCountIndicator,
      gridItems,
      homeRecommendations,
      myPageSummaryStatus,
      navigate,
      onOpenContent,
      recentHistoryItems,
      requestNextBrowsePage,
      resolveTabId,
      setAuthMode,
      subscriptionsIsLoading,
    ],
  );
}
