import { useMemo } from "react";

import { buildContentKey } from "../../shared/content";
import type { AuthContextValue } from "../../shared/hooks/use-auth";
import type { ContentCard, SubscriptionItem } from "../../shared/types";

export function usePublicDerivedState({
  auth,
  detailContent,
  loginEmail,
  myPageOpen,
  subscriptionItems,
  theme,
}: {
  auth: AuthContextValue;
  detailContent: (ContentCard & { meta?: Record<string, unknown> | null }) | null;
  loginEmail?: string;
  myPageOpen: boolean;
  subscriptionItems: SubscriptionItem[];
  theme: "light" | "dark";
}) {
  return useMemo(() => {
    const authGateOpen =
      (!auth.hasToken && myPageOpen) ||
      (auth.hasToken && !auth.isLoading && !auth.isAuthenticated && myPageOpen);
    const showAuthModal = authGateOpen;
    const modalContent = detailContent || null;
    const modalSubscribed =
      Boolean(modalContent) &&
      subscriptionItems.some((item) => item.contentKey === buildContentKey(modalContent));
    const profileInitial = (loginEmail || auth.user?.email || "나").slice(0, 1).toUpperCase();
    const themeToggleLabel = theme === "dark" ? "다크 모드: 켜짐" : "다크 모드: 꺼짐";

    return {
      authGateOpen,
      modalContent,
      modalSubscribed,
      profileInitial,
      showAuthModal,
      themeToggleLabel,
    };
  }, [auth.hasToken, auth.isAuthenticated, auth.isLoading, auth.user?.email, detailContent, loginEmail, myPageOpen, subscriptionItems, theme]);
}
