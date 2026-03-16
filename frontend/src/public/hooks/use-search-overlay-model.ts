import { useMemo } from "react";

import type { ContentCard, SubscriptionItem } from "../../shared/types";
import type { NavTab } from "../config";

const SEARCH_IDLE_POPULAR_LIMIT = 9;

export function useSearchOverlayModel({
  activeTab,
  gridItems,
  recentHistoryItems,
  recommendations,
  searchInput,
  searchQueryData,
}: {
  activeTab: NavTab;
  gridItems: Array<ContentCard | SubscriptionItem>;
  recentHistoryItems: ContentCard[];
  recommendations: ContentCard[];
  searchInput: string;
  searchQueryData?: ContentCard[];
}) {
  const searchResults = searchInput.trim() ? searchQueryData || [] : [];
  const usesRecentSearchPopular = recentHistoryItems.length > 0;

  const searchPopularItems = useMemo(() => {
    if (usesRecentSearchPopular) {
      return recentHistoryItems.slice(0, SEARCH_IDLE_POPULAR_LIMIT);
    }
    const visibleItems = gridItems.length ? gridItems : recommendations;
    return visibleItems.slice(0, SEARCH_IDLE_POPULAR_LIMIT);
  }, [gridItems, recommendations, recentHistoryItems, usesRecentSearchPopular]);

  const searchPopularSubtitle = useMemo(() => {
    if (usesRecentSearchPopular) return "최근에 열어본 작품이 여기에 표시됩니다.";
    if (activeTab === "novel") return "웹소설 페이지에서 살펴본 작품을 보여드려요.";
    if (activeTab === "webtoon") return "웹툰 페이지에서 살펴본 작품을 보여드려요.";
    if (activeTab === "ott") return "OTT 페이지에서 살펴본 작품을 보여드려요.";
    return "최근에 많이 살펴본 작품을 보여드려요.";
  }, [activeTab, usesRecentSearchPopular]);

  return {
    searchPopularItems,
    searchPopularSubtitle,
    searchResults,
    usesRecentSearchPopular,
  };
}
