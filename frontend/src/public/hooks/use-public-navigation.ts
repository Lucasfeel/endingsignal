import { useEffect, type MutableRefObject } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import type { ContentCard, SubscriptionItem } from "../../shared/types";
import {
  getBasePathForTab,
  isMyPagePath,
  isSearchPath,
  parseContentPath,
  type NavTab,
} from "../config";
import { rememberModalScrollPosition } from "./use-modal-scroll-lock";

export function usePublicNavigation({
  activeTab,
  closeProfileMenu,
  recordRecentContent,
  scrollYRef,
  setActiveTab,
  setSearchInput,
  trackPublicEvent,
}: {
  activeTab: NavTab;
  closeProfileMenu: (reason?: string) => void;
  recordRecentContent: (content: ContentCard | SubscriptionItem) => void;
  scrollYRef: MutableRefObject<number | null>;
  setActiveTab: (tab: NavTab) => void;
  setSearchInput: (query: string) => void;
  trackPublicEvent: (name: string, payload?: Record<string, unknown>) => void;
}) {
  const location = useLocation();
  const navigate = useNavigate();

  const searchOpen = isSearchPath(location.pathname);
  const myPageOpen = isMyPagePath(location.pathname);
  const routeContent = parseContentPath(location.pathname);

  useEffect(() => {
    if (location.pathname === "/") {
      setActiveTab("home");
      return;
    }
    if (location.pathname.startsWith("/browse/webtoon")) {
      setActiveTab("webtoon");
      return;
    }
    if (location.pathname.startsWith("/browse/novel")) {
      setActiveTab("novel");
      return;
    }
    if (location.pathname.startsWith("/browse/ott")) {
      setActiveTab("ott");
      return;
    }
    if (location.pathname === "/subscriptions") {
      setActiveTab("my");
    }
  }, [location.pathname, setActiveTab]);

  useEffect(() => {
    if (!searchOpen) {
      setSearchInput("");
    }
  }, [searchOpen, setSearchInput]);

  function navigateTab(nextTab: NavTab) {
    closeProfileMenu("navigate_tab");
    trackPublicEvent("nav_tab_selected", { from: activeTab, to: nextTab });
    navigate(getBasePathForTab(nextTab));
  }

  function openSearchOverlay() {
    trackPublicEvent("overlay_opened", { overlay: "search", fromTab: activeTab });
    navigate("/search");
  }

  function closeOverlay({ showAuthModal }: { showAuthModal: boolean }) {
    const overlay = routeContent ? "content" : searchOpen ? "search" : myPageOpen ? "mypage" : showAuthModal ? "auth" : "unknown";
    trackPublicEvent("overlay_closed", { overlay, returnTo: activeTab });
    navigate(getBasePathForTab(activeTab), { preventScrollReset: true });
  }

  function openMyPageOverlay() {
    closeProfileMenu("open_mypage");
    trackPublicEvent("overlay_opened", { overlay: "mypage", fromTab: activeTab });
    navigate("/mypage");
  }

  function openContentModal(content: ContentCard | SubscriptionItem) {
    const currentScrollY = window.scrollY;
    scrollYRef.current = currentScrollY;
    rememberModalScrollPosition(currentScrollY);
    recordRecentContent(content);
    trackPublicEvent("content_opened", {
      contentId: content.content_id,
      contentType: content.content_type || activeTab,
      source: content.source,
      fromTab: activeTab,
    });
    navigate(`/content/${encodeURIComponent(content.source)}/${encodeURIComponent(content.content_id)}`, {
      preventScrollReset: true,
    });
  }

  return {
    closeOverlay,
    location,
    myPageOpen,
    navigate,
    navigateTab,
    openContentModal,
    openMyPageOverlay,
    openSearchOverlay,
    routeContent,
    searchOpen,
  };
}
