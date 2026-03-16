import { useCallback, type RefObject } from "react";

import type { InfiniteData, InfiniteQueryObserverResult } from "@tanstack/react-query";

import { useAutoPagination } from "./use-auto-pagination";
import type { NavTab } from "../config";

type BrowsePage = {
  next_cursor: string | null;
};

export function useBrowsePaginationController({
  activeTab,
  browseQuery,
  sentinelRef,
  trackPublicEvent,
}: {
  activeTab: NavTab;
  browseQuery: Pick<
    InfiniteQueryObserverResult<InfiniteData<BrowsePage | unknown, unknown>, Error>,
    "fetchNextPage" | "hasNextPage" | "isFetchingNextPage" | "isLoading"
  >;
  sentinelRef: RefObject<HTMLDivElement | null>;
  trackPublicEvent: (name: string, payload?: Record<string, unknown>) => void;
}) {
  const requestNextBrowsePage = useCallback(
    (trigger: "auto" | "manual") => {
      if (!(activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott")) return;
      if (!browseQuery.hasNextPage || browseQuery.isFetchingNextPage || browseQuery.isLoading) return;
      trackPublicEvent("load_more_requested", { tab: activeTab, trigger });
      browseQuery.fetchNextPage();
    },
    [activeTab, browseQuery, trackPublicEvent],
  );

  useAutoPagination({
    activeTab,
    hasNextPage: browseQuery.hasNextPage,
    isFetchingNextPage: browseQuery.isFetchingNextPage,
    isLoading: browseQuery.isLoading,
    onLoadNextPage: () => requestNextBrowsePage("auto"),
    sentinelRef,
  });

  return {
    requestNextBrowsePage,
  };
}
