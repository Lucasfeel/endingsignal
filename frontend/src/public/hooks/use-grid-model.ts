import { useMemo } from "react";

import { getGridCountLabel, type NavTab } from "../config";
import type { ContentCard, SubscriptionItem } from "../../shared/types";

export function useGridModel({
  activeTab,
  browsePages,
  isSubscriptionsLoading,
  myViewMode,
  recommendations,
  subscriptionItems,
}: {
  activeTab: NavTab;
  browsePages?: Array<{ contents: ContentCard[] }>;
  isSubscriptionsLoading: boolean;
  myViewMode: "completion" | "completed";
  recommendations: ContentCard[];
  subscriptionItems: SubscriptionItem[];
}) {
  const gridItems = useMemo(() => {
    if (activeTab === "home") return recommendations;
    if (activeTab === "my") {
      if (myViewMode === "completed") {
        return subscriptionItems.filter((item) => item.final_state?.label === "완결");
      }
      return subscriptionItems.filter((item) => item.final_state?.label !== "완결");
    }
    return browsePages?.flatMap((page) => page.contents) || [];
  }, [activeTab, browsePages, myViewMode, recommendations, subscriptionItems]);

  const contentCountIndicator = getGridCountLabel(activeTab, gridItems.length);
  const myPageSummaryStatus = isSubscriptionsLoading
    ? "불러오는 중"
    : !subscriptionItems.length
      ? "구독 중인 작품이 아직 없어요"
      : `현재 기준 ${subscriptionItems.length}개 작품`;

  return {
    contentCountIndicator,
    gridItems,
    myPageSummaryStatus,
  };
}
