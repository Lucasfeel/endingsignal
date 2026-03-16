import { useMemo } from "react";

import type { ContentCard, SubscriptionItem } from "../../shared/types";
import type { NavTab } from "../config";

export function useSearchViewModel({
  clearRecentSearches,
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
  clearRecentSearches: () => void;
  onClose: () => void;
  onOpenContent: (content: ContentCard | SubscriptionItem) => void;
  onSearchInputChange: (value: string) => void;
  onSubmitSearch: (query: string, trigger?: string) => void;
  recentSearches: string[];
  resolveTabId: (content: ContentCard | SubscriptionItem) => NavTab;
  searchInput: string;
  searchIsLoading: boolean;
  searchOpen: boolean;
  searchPopularItems: Array<ContentCard | SubscriptionItem>;
  searchPopularSubtitle: string;
  searchResults: ContentCard[];
  usesRecentSearchPopular: boolean;
}) {
  return useMemo(
    () => ({
      onClearRecentSearches: clearRecentSearches,
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
    }),
    [
      clearRecentSearches,
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
    ],
  );
}
