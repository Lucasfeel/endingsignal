import { useMemo, useState } from "react";

import { buildContentKey, extractContentUrl, extractDisplayMeta } from "../../shared/content";
import type { ContentCard, SubscriptionItem } from "../../shared/types";

const RECENT_SEARCHES_KEY = "endingsignal.recent-searches";
const RECENTLY_SEARCHED_CONTENTS_KEY = "endingsignal.recently-searched-contents";
const MAX_RECENTLY_SEARCHED_CONTENTS = 12;

type RecentContentEntry = {
  key: string;
  content: ContentCard;
  openedAt: number;
};

function readRecentSearches() {
  try {
    const raw = window.localStorage.getItem(RECENT_SEARCHES_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((item) => typeof item === "string") : [];
  } catch (_error) {
    return [];
  }
}

function writeRecentSearches(values: string[]) {
  try {
    window.localStorage.setItem(RECENT_SEARCHES_KEY, JSON.stringify(values.slice(0, 10)));
  } catch (_error) {
    // Ignore storage failures.
  }
}

function readRecentlySearchedContents() {
  try {
    const raw = window.localStorage.getItem(RECENTLY_SEARCHED_CONTENTS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as RecentContentEntry[]) : [];
  } catch (_error) {
    return [];
  }
}

function writeRecentlySearchedContents(entries: RecentContentEntry[]) {
  try {
    window.localStorage.setItem(
      RECENTLY_SEARCHED_CONTENTS_KEY,
      JSON.stringify(entries.slice(0, MAX_RECENTLY_SEARCHED_CONTENTS)),
    );
  } catch (_error) {
    // Ignore storage failures.
  }
}

function buildRecentContentSnapshot(content: ContentCard | SubscriptionItem): ContentCard {
  return {
    content_id: content.content_id,
    content_type: content.content_type,
    content_url: extractContentUrl(content) || undefined,
    cursor: "cursor" in content ? content.cursor : undefined,
    display_meta: extractDisplayMeta(content),
    final_state_badge: "final_state_badge" in content ? content.final_state_badge : null,
    meta: content.meta,
    source: content.source,
    status: content.status,
    thumbnail_url: null,
    title: content.title,
  };
}

export function recordRecentContent(content: ContentCard | SubscriptionItem) {
  const snapshot = buildRecentContentSnapshot(content);
  const key = buildContentKey(snapshot);
  const existing = readRecentlySearchedContents();
  const next = [{ key, content: snapshot, openedAt: Date.now() }, ...existing.filter((entry) => entry.key !== key)];
  writeRecentlySearchedContents(next);
}

export function useRecentActivity(deps: Array<unknown>) {
  const [recentSearches, setRecentSearches] = useState<string[]>(() => readRecentSearches());

  const recentHistoryItems = useMemo(
    () =>
      readRecentlySearchedContents()
        .sort((a, b) => (b?.openedAt || 0) - (a?.openedAt || 0))
        .map((entry) => entry.content)
        .filter((item) => item?.content_id && item?.source)
        .slice(0, MAX_RECENTLY_SEARCHED_CONTENTS),
    deps,
  );

  function clearRecentSearches() {
    setRecentSearches([]);
    writeRecentSearches([]);
  }

  function rememberSearch(query: string) {
    const normalized = query.trim();
    if (!normalized) return "";
    const next = [normalized, ...recentSearches.filter((item) => item !== normalized)].slice(0, 10);
    setRecentSearches(next);
    writeRecentSearches(next);
    return normalized;
  }

  return {
    clearRecentSearches,
    recentHistoryItems,
    recentSearches,
    rememberSearch,
    setRecentSearches,
  };
}
