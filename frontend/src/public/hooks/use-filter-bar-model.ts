import { useMemo } from "react";

import {
  type NavTab,
  NOVEL_FILTER_OPTIONS,
  normalizeSourceId,
  OTT_FILTER_OPTIONS,
  SOURCE_BRAND_META,
  SOURCE_LOGO_ASSETS,
  SOURCE_OPTIONS,
  WEBTOON_DAY_FILTER_OPTIONS,
} from "../config";

type SetStringState = (value: string) => void;

export function useFilterBarModel({
  activeTab,
  myViewMode,
  novelFilter,
  ottFilter,
  selectedSources,
  trackPublicEvent,
  setMyViewMode,
  setNovelFilter,
  setOttFilter,
  setSelectedSources,
  setWebtoonFilter,
  webtoonFilter,
}: {
  activeTab: NavTab;
  myViewMode: "completion" | "completed";
  novelFilter: string;
  ottFilter: string;
  selectedSources: Record<NavTab, string[]>;
  trackPublicEvent: (name: string, payload?: Record<string, unknown>) => void;
  setMyViewMode: (mode: "completion" | "completed") => void;
  setNovelFilter: SetStringState;
  setOttFilter: SetStringState;
  setSelectedSources: React.Dispatch<React.SetStateAction<Record<NavTab, string[]>>>;
  setWebtoonFilter: SetStringState;
  webtoonFilter: string;
}) {
  const showBrowseFilters = activeTab === "webtoon" || activeTab === "novel" || activeTab === "ott";

  const sourceChips = useMemo(() => {
    if (!showBrowseFilters) return [];

    return SOURCE_OPTIONS[activeTab].map((item) => {
      const normalizedId = normalizeSourceId(item.id);
      const current = selectedSources[activeTab];
      const active = current.includes(normalizedId);
      const brand = SOURCE_BRAND_META[normalizedId];
      const isWideLogo = normalizedId === "tving" || normalizedId === "laftel";
      const isWavveLogo = normalizedId === "wavve";
      const logoStyle: Record<string, string> = {
        ["--chip-fg"]: brand?.logoColor || "#111111",
        ["--logo-size"]: isWideLogo ? "30px" : isWavveLogo ? "44px" : "40px",
        ["--logo-fit"]: isWideLogo ? "contain" : "cover",
        ["--logo-icon-radius"]: isWideLogo ? "0px" : "999px",
      };

      if (isWideLogo) {
        logoStyle["--logo-width"] = "33px";
        logoStyle["--logo-height"] = "15px";
        if (brand?.bg) {
          logoStyle.background = brand.bg;
        }
      }

      if (isWavveLogo) {
        logoStyle.overflow = "hidden";
      }

      return {
        active,
        asset: SOURCE_LOGO_ASSETS[normalizedId],
        hasSelection: current.length > 0,
        label: item.label,
        normalizedId,
        onClick: () => {
          const nextSelected = !active;
          trackPublicEvent("source_chip_toggled", {
            action: nextSelected ? "selected" : "deselected",
            sourceId: normalizedId,
            sourceLabel: item.label,
          });
          setSelectedSources((prev) => {
            const currentSet = new Set(prev[activeTab]);
            if (currentSet.has(normalizedId)) currentSet.delete(normalizedId);
            else currentSet.add(normalizedId);
            return { ...prev, [activeTab]: Array.from(currentSet) };
          });
        },
        style: logoStyle,
      };
    });
  }, [activeTab, selectedSources, setSelectedSources, showBrowseFilters, trackPublicEvent]);

  const categoryFilters = useMemo(() => {
    if (!showBrowseFilters) return [];

    const source =
      activeTab === "webtoon"
        ? WEBTOON_DAY_FILTER_OPTIONS
        : activeTab === "novel"
          ? NOVEL_FILTER_OPTIONS
          : OTT_FILTER_OPTIONS;

    return source.map((item) => ({
      active:
        activeTab === "webtoon"
          ? webtoonFilter === item.id
          : activeTab === "novel"
            ? novelFilter === item.id
            : ottFilter === item.id,
      id: item.id,
      label: item.label,
      onClick: () => {
        const previousValue =
          activeTab === "webtoon"
            ? webtoonFilter
            : activeTab === "novel"
              ? novelFilter
              : ottFilter;
        trackPublicEvent("category_filter_changed", {
          filterGroup: activeTab === "webtoon" ? "weekday" : activeTab === "novel" ? "novel_genre_group" : "ott_genre_group",
          nextValue: item.id,
          previousValue,
        });
        if (activeTab === "webtoon") setWebtoonFilter(item.id);
        if (activeTab === "novel") setNovelFilter(item.id);
        if (activeTab === "ott") setOttFilter(item.id);
      },
    }));
  }, [activeTab, novelFilter, ottFilter, setNovelFilter, setOttFilter, setWebtoonFilter, showBrowseFilters, trackPublicEvent, webtoonFilter]);

  return {
    categoryFilters,
    myViewMode,
    setMyViewMode,
    showBrowseFilters,
    sourceChips,
  };
}
