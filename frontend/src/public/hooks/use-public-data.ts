import {
  useInfiniteQuery,
  useQuery,
} from "@tanstack/react-query";

import { apiRequest, toQueryString } from "../../shared/api";
import type { ContentCard, SubscriptionItem } from "../../shared/types";
import type { NavTab } from "../config";

type RecommendationsResponse = {
  contents: ContentCard[];
  returned: number;
  limit: number;
};

type BrowseResponse = {
  contents: ContentCard[];
  next_cursor: string | null;
  returned: number;
  filters: Record<string, unknown>;
};

type SearchResponse = ContentCard[];
type DetailResponse = ContentCard & { meta?: Record<string, unknown> | null };
type SubscriptionListResponse = { items?: SubscriptionItem[] };
type AuthMeResponse = { success?: boolean; user?: { email?: string; role?: string } };

const HOME_RECOMMENDATIONS_LIMIT = 12;

export function usePublicData({
  activeTab,
  authToken,
  novelFilter,
  ottFilter,
  routeContent,
  searchInput,
  searchOpen,
  selectedSources,
  webtoonFilter,
}: {
  activeTab: NavTab;
  authToken: string | null;
  novelFilter: string;
  ottFilter: string;
  routeContent: { source: string; contentId: string } | null;
  searchInput: string;
  searchOpen: boolean;
  selectedSources: Record<NavTab, string[]>;
  webtoonFilter: string;
}) {
  const recommendationsQuery = useQuery({
    queryKey: ["legacy-home-recommendations"],
    queryFn: () =>
      apiRequest<RecommendationsResponse>(
        `/api/contents/recommendations_v2?limit=${HOME_RECOMMENDATIONS_LIMIT}`,
      ),
  });

  const browseQuery = useInfiniteQuery({
    queryKey: ["legacy-browse", activeTab, selectedSources[activeTab], webtoonFilter, novelFilter, ottFilter],
    enabled: activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott",
    initialPageParam: null as string | null,
    queryFn: ({ pageParam }) => {
      if (activeTab === "webtoon") {
        const status =
          webtoonFilter === "completed" ? "completed" : webtoonFilter === "hiatus" ? "hiatus" : "ongoing";
        const day =
          webtoonFilter !== "completed" && webtoonFilter !== "hiatus" && webtoonFilter !== "all"
            ? webtoonFilter
            : undefined;
        return apiRequest<BrowseResponse>(
          `/api/contents/browse_v3${toQueryString({
            type: "webtoon",
            status,
            day,
            sources: selectedSources.webtoon.length ? selectedSources.webtoon.join(",") : undefined,
            per_page: 80,
            cursor: pageParam,
          })}`,
        );
      }

      if (activeTab === "novel") {
        const isCompleted = novelFilter === "completed";
        return apiRequest<BrowseResponse>(
          `/api/contents/browse_v3${toQueryString({
            type: "novel",
            status: isCompleted ? "completed" : "ongoing",
            genre_group: !isCompleted && novelFilter !== "all" ? novelFilter : undefined,
            sources: selectedSources.novel.length ? selectedSources.novel.join(",") : undefined,
            per_page: 80,
            cursor: pageParam,
          })}`,
        );
      }

      return apiRequest<BrowseResponse>(
        `/api/contents/browse_v3${toQueryString({
          type: "ott",
          status: ottFilter === "completed" ? "completed" : "ongoing",
          sources: selectedSources.ott.length ? selectedSources.ott.join(",") : undefined,
          per_page: 80,
          cursor: pageParam,
        })}`,
      );
    },
    getNextPageParam: (page) => page.next_cursor || undefined,
  });

  const subscriptionsQuery = useQuery({
    queryKey: ["legacy-subscriptions", authToken],
    enabled: Boolean(authToken),
    queryFn: () => apiRequest<SubscriptionListResponse>("/api/me/subscriptions", { token: authToken }),
  });

  const searchQuery = useQuery({
    queryKey: ["legacy-search", searchInput],
    enabled: searchOpen && searchInput.trim().length > 0,
    queryFn: () => apiRequest<SearchResponse>(`/api/contents/search${toQueryString({ q: searchInput.trim() })}`),
  });

  const detailQuery = useQuery({
    queryKey: ["legacy-detail", routeContent?.source, routeContent?.contentId],
    enabled: Boolean(routeContent),
    queryFn: () =>
      apiRequest<DetailResponse>(
        `/api/contents/detail${toQueryString({
          content_id: routeContent?.contentId,
          source: routeContent?.source,
        })}`,
      ),
  });

  const loginMeQuery = useQuery({
    queryKey: ["legacy-auth-me", authToken],
    enabled: Boolean(authToken),
    queryFn: () => apiRequest<AuthMeResponse>("/api/auth/me", { token: authToken }),
  });

  return {
    browseQuery,
    detailQuery,
    loginMeQuery,
    recommendationsQuery,
    searchQuery,
    subscriptionsQuery,
  };
}
