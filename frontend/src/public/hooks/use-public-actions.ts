import { useMutation, useQueryClient } from "@tanstack/react-query";
import type React from "react";
import type { NavigateFunction } from "react-router-dom";

import { apiRequest, ApiError } from "../../shared/api";
import { buildContentKey } from "../../shared/content";
import type { AuthContextValue } from "../../shared/hooks/use-auth";
import type { ContentCard } from "../../shared/types";
import { getBasePathForTab, type NavTab } from "../config";

export function usePublicActions({
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
  onAuthError,
  onAuthSuccess,
  onRememberSearch,
  onSearchInputChange,
  queryClient,
  trackPublicEvent,
}: {
  activeTab: NavTab;
  auth: AuthContextValue;
  authEmail: string;
  authMode: "login" | "register";
  authPassword: string;
  authPasswordConfirm: string;
  modalContent: (ContentCard & { meta?: Record<string, unknown> | null }) | null;
  modalSubscribed: boolean;
  myPageOpen: boolean;
  navigate: NavigateFunction;
  onAuthError: (message: string) => void;
  onAuthSuccess: () => void;
  onRememberSearch: (query: string) => void;
  onSearchInputChange: (query: string) => void;
  queryClient: ReturnType<typeof useQueryClient>;
  trackPublicEvent: (name: string, payload?: Record<string, unknown>) => void;
}) {
  const subscriptionMutation = useMutation({
    mutationFn: async () => {
      if (!modalContent) return;
      if (!auth.token) throw new ApiError("로그인이 필요합니다.", 401, null);
      trackPublicEvent("subscription_toggle_requested", {
        action: modalSubscribed ? "unsubscribe" : "subscribe",
        contentId: modalContent.content_id,
        contentType: modalContent.content_type || activeTab,
        source: modalContent.source,
      });
      if (modalSubscribed) {
        await apiRequest("/api/me/subscriptions", {
          body: { contentKey: buildContentKey(modalContent) },
          method: "DELETE",
          token: auth.token,
        });
      } else {
        await apiRequest("/api/me/subscriptions", {
          body: { contentKey: buildContentKey(modalContent) },
          method: "POST",
          token: auth.token,
        });
      }
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["legacy-subscriptions"] });
    },
  });

  async function submitAuth() {
    onAuthError("");
    try {
      trackPublicEvent("auth_submitted", { mode: authMode });
      if (authMode === "register") {
        if (authPassword !== authPasswordConfirm) {
          onAuthError("비밀번호가 일치하지 않습니다.");
          return;
        }
        await auth.register({ email: authEmail, password: authPassword });
      } else {
        await auth.login({ email: authEmail, password: authPassword });
      }
      onAuthSuccess();
      trackPublicEvent("auth_succeeded", { mode: authMode });
      navigate(myPageOpen ? "/mypage" : getBasePathForTab(activeTab));
    } catch (error) {
      trackPublicEvent("auth_failed", {
        mode: authMode,
        message: error instanceof ApiError ? error.message : "request_failed",
      });
      onAuthError(error instanceof ApiError ? error.message : "인증 요청이 실패했습니다.");
    }
  }

  async function handleChangePassword(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const currentPassword = (event.currentTarget.elements.namedItem("myPagePwCurrent") as HTMLInputElement)?.value || "";
    const newPassword = (event.currentTarget.elements.namedItem("myPagePwNew") as HTMLInputElement)?.value || "";
    const confirmPassword = (event.currentTarget.elements.namedItem("myPagePwConfirm") as HTMLInputElement)?.value || "";
    if (newPassword !== confirmPassword) return;
    try {
      trackPublicEvent("password_change_requested");
      await auth.changePassword({ current_password: currentPassword, new_password: newPassword });
    } catch (_error) {
      // Keep shell identical first; richer feedback can come next.
    }
  }

  function submitSearch(query: string) {
    const normalized = query.trim();
    onSearchInputChange(normalized);
    if (!normalized) return;
    trackPublicEvent("search_submitted", { queryLength: normalized.length, fromTab: activeTab });
    onRememberSearch(normalized);
  }

  return {
    handleChangePassword,
    submitAuth,
    submitSearch,
    subscriptionMutation,
  };
}
