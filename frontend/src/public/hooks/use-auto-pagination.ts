import { useEffect, type RefObject } from "react";

import type { NavTab } from "../config";

export function useAutoPagination({
  activeTab,
  hasNextPage,
  isFetchingNextPage,
  isLoading,
  onLoadNextPage,
  sentinelRef,
}: {
  activeTab: NavTab;
  hasNextPage: boolean | undefined;
  isFetchingNextPage: boolean;
  isLoading: boolean;
  onLoadNextPage: () => void;
  sentinelRef: RefObject<HTMLDivElement | null>;
}) {
  useEffect(() => {
    if (!(activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott")) {
      return;
    }

    const sentinel = sentinelRef.current;
    if (!sentinel || !hasNextPage || isFetchingNextPage || isLoading || !("IntersectionObserver" in window)) {
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          onLoadNextPage();
        }
      },
      { root: null, rootMargin: "200px 0px" },
    );

    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [activeTab, hasNextPage, isFetchingNextPage, isLoading, onLoadNextPage, sentinelRef]);
}
