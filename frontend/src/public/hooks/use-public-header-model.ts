import { useMemo } from "react";
import type { NavigateFunction } from "react-router-dom";

import type { AuthContextValue } from "../../shared/hooks/use-auth";

export function usePublicHeaderModel({
  auth,
  navigate,
  onOpenMyPage,
  theme,
  toggleTheme,
  trackPublicEvent,
}: {
  auth: AuthContextValue;
  navigate: NavigateFunction;
  onOpenMyPage: () => void;
  theme: "light" | "dark";
  toggleTheme: () => void;
  trackPublicEvent: (name: string, payload?: Record<string, unknown>) => void;
}) {
  const profileInitial = (auth.user?.email || "나").slice(0, 1).toUpperCase();
  const themeToggleLabel = theme === "dark" ? "다크 모드: 켜짐" : "다크 모드: 꺼짐";

  return useMemo(
    () => ({
      onAdmin: () => {
        trackPublicEvent("profile_menu_item_clicked", { item: "admin" });
        navigate("/admin");
      },
      onHome: () => {
        trackPublicEvent("nav_home_clicked");
        navigate("/");
      },
      onLogout: () => {
        trackPublicEvent("auth_logout_requested", { from: "profile_menu" });
        auth.logout();
      },
      onMyPage: () => {
        trackPublicEvent("profile_menu_item_clicked", { item: "mypage" });
        onOpenMyPage();
      },
      onSubscriptions: () => {
        trackPublicEvent("profile_menu_item_clicked", { item: "subscriptions" });
        navigate("/subscriptions");
      },
      onToggleTheme: () => {
        trackPublicEvent("theme_toggled", { to: theme === "dark" ? "light" : "dark" });
        toggleTheme();
      },
      profileInitial,
      themeToggleLabel,
    }),
    [auth, navigate, onOpenMyPage, profileInitial, theme, themeToggleLabel, toggleTheme, trackPublicEvent],
  );
}
