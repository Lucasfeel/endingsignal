import posthog from "posthog-js";

import { getRuntime, type UiTelemetryEvent } from "../shared/runtime";

const EVENT_NAME_MAP = {
  auth_modal_opened: "public_auth_modal_opened",
  category_filter_changed: "public_category_filter_changed",
  content_opened: "public_content_opened",
  load_more_requested: "public_load_more_requested",
  my_view_mode_changed: "public_my_view_mode_changed",
  nav_home_clicked: "public_home_clicked",
  nav_tab_selected: "public_tab_selected",
  overlay_closed: "public_overlay_closed",
  overlay_opened: "public_overlay_opened",
  profile_menu_closed: "public_profile_menu_closed",
  profile_menu_item_clicked: "public_profile_menu_item_clicked",
  profile_menu_opened: "public_profile_menu_opened",
  recent_search_clicked: "public_recent_search_clicked",
  recent_searches_cleared: "public_recent_searches_cleared",
  search_submitted: "public_search_submitted",
  source_chip_toggled: "public_source_chip_toggled",
  subscription_cta_clicked: "public_subscription_clicked",
} as const;

type SupportedUiEventName = keyof typeof EVENT_NAME_MAP;

let telemetryInstalled = false;

function isSupportedUiEventName(name: string): name is SupportedUiEventName {
  return Object.prototype.hasOwnProperty.call(EVENT_NAME_MAP, name);
}

function asString(value: unknown) {
  return typeof value === "string" ? value : String(value || "");
}

function asNumber(value: unknown) {
  return typeof value === "number" ? value : Number(value || 0);
}

function asBoolean(value: unknown) {
  return Boolean(value);
}

function asStringArray(value: unknown) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((entry) => String(entry)).filter(Boolean);
}

function deriveEventFamily(event: UiTelemetryEvent) {
  switch (event.name) {
    case "category_filter_changed":
    case "load_more_requested":
    case "my_view_mode_changed":
    case "source_chip_toggled":
      return "discovery";
    case "nav_home_clicked":
    case "nav_tab_selected":
      return "navigation";
    case "auth_modal_opened":
    case "profile_menu_closed":
    case "profile_menu_item_clicked":
    case "profile_menu_opened":
      return "account";
    case "content_opened":
      return "content";
    case "recent_search_clicked":
    case "recent_searches_cleared":
    case "search_submitted":
      return "search";
    case "subscription_cta_clicked":
      return "subscription";
    case "overlay_opened":
    case "overlay_closed":
      return "overlay";
    default:
      return "other";
  }
}

function deriveJourneyStage(event: UiTelemetryEvent) {
  switch (event.name) {
    case "category_filter_changed":
    case "load_more_requested":
    case "my_view_mode_changed":
    case "nav_home_clicked":
    case "nav_tab_selected":
    case "source_chip_toggled":
      return "browse";
    case "auth_modal_opened":
    case "profile_menu_closed":
    case "profile_menu_item_clicked":
    case "profile_menu_opened":
      return "account";
    case "recent_search_clicked":
    case "recent_searches_cleared":
    case "search_submitted":
      return "search";
    case "content_opened":
      return "evaluate";
    case "subscription_cta_clicked":
      return "convert";
    case "overlay_opened":
    case "overlay_closed": {
      const overlay = asString(event.payload?.overlay);
      if (overlay === "search") return "search";
      if (overlay === "content") return "evaluate";
      if (overlay === "mypage" || overlay === "auth") return "account";
      return "browse";
    }
    default:
      return "other";
  }
}

function deriveAuthState(event: UiTelemetryEvent) {
  return asBoolean(event.payload?.isAuthenticated) ? "authenticated" : "anonymous";
}

function deriveContentDomain(event: UiTelemetryEvent) {
  const explicit = asString(event.payload?.contentType).toLowerCase();
  if (explicit === "webtoon" || explicit === "novel" || explicit === "ott") {
    return explicit;
  }

  const entryTab = asString(event.payload?.activeTab).toLowerCase();
  if (entryTab === "webtoon" || entryTab === "novel" || entryTab === "ott") {
    return entryTab;
  }

  return "none";
}

function buildCommonPublicProperties(event: UiTelemetryEvent) {
  return {
    auth_provider: asString(event.payload?.authProvider),
    auth_state: deriveAuthState(event),
    content_domain: deriveContentDomain(event),
    entry_filter: asString(event.payload?.activeFilter),
    entry_source_count: asNumber(event.payload?.selectedSourceCount),
    entry_sources: asStringArray(event.payload?.selectedSources),
    entry_tab: asString(event.payload?.activeTab),
    event_family: deriveEventFamily(event),
    is_authenticated: asBoolean(event.payload?.isAuthenticated),
    journey_stage: deriveJourneyStage(event),
    my_view_mode: asString(event.payload?.myViewMode),
    novel_filter: asString(event.payload?.novelFilter),
    ott_filter: asString(event.payload?.ottFilter),
    path: event.path,
    route_kind: asString(event.payload?.routeKind),
    search_input_length: asNumber(event.payload?.searchInputLength),
    surface: "public_web",
    ui_event_name: event.name,
    user_role: asString(event.payload?.userRole),
    webtoon_filter: asString(event.payload?.webtoonFilter),
  };
}

export function mapPublicUiEventToPosthog(name: string) {
  return isSupportedUiEventName(name) ? EVENT_NAME_MAP[name] : null;
}

export function buildPublicPosthogProperties(event: UiTelemetryEvent) {
  if (event.app !== "public") {
    return null;
  }

  switch (event.name) {
    case "auth_modal_opened":
      return {
        ...buildCommonPublicProperties(event),
        auth_mode: asString(event.payload?.authMode),
        entrypoint: asString(event.payload?.entrypoint),
        has_modal_content: asBoolean(event.payload?.hasModalContent),
      };
    case "category_filter_changed":
      return {
        ...buildCommonPublicProperties(event),
        filter_group: asString(event.payload?.filterGroup),
        next_value: asString(event.payload?.nextValue),
        previous_value: asString(event.payload?.previousValue),
      };
    case "nav_tab_selected":
      return {
        ...buildCommonPublicProperties(event),
        tab_from: String(event.payload?.from || ""),
        tab_to: String(event.payload?.to || ""),
      };
    case "load_more_requested":
      return {
        ...buildCommonPublicProperties(event),
        load_trigger: asString(event.payload?.trigger),
        tab: asString(event.payload?.tab),
      };
    case "my_view_mode_changed":
      return {
        ...buildCommonPublicProperties(event),
        next_value: asString(event.payload?.nextValue),
        previous_value: asString(event.payload?.previousValue),
      };
    case "nav_home_clicked":
      return {
        ...buildCommonPublicProperties(event),
      };
    case "content_opened":
      return {
        ...buildCommonPublicProperties(event),
        authors_count: asNumber(event.payload?.authorsCount),
        content_status: asString(event.payload?.contentStatus),
        content_source: String(event.payload?.source || ""),
        content_type: String(event.payload?.contentType || ""),
        from_tab: String(event.payload?.fromTab || ""),
        genre_count: asNumber(event.payload?.genreCount),
        has_content_url: asBoolean(event.payload?.hasContentUrl),
        has_thumbnail: asBoolean(event.payload?.hasThumbnail),
        is_upcoming: asBoolean(event.payload?.isUpcoming),
        platform_count: asNumber(event.payload?.platformCount),
        release_end_status: asString(event.payload?.releaseEndStatus),
        trigger: asString(event.payload?.trigger),
        weekday_count: asNumber(event.payload?.weekdayCount),
      };
    case "profile_menu_closed":
      return {
        ...buildCommonPublicProperties(event),
        close_reason: asString(event.payload?.reason),
      };
    case "profile_menu_item_clicked":
      return {
        ...buildCommonPublicProperties(event),
        menu_item: asString(event.payload?.item),
      };
    case "profile_menu_opened":
      return {
        ...buildCommonPublicProperties(event),
        open_reason: asString(event.payload?.reason),
      };
    case "recent_search_clicked":
      return {
        ...buildCommonPublicProperties(event),
        query_length: asNumber(event.payload?.queryLength),
      };
    case "recent_searches_cleared":
      return {
        ...buildCommonPublicProperties(event),
        cleared_count: asNumber(event.payload?.clearedCount),
      };
    case "search_submitted":
      return {
        ...buildCommonPublicProperties(event),
        from_tab: String(event.payload?.fromTab || ""),
        query_length: Number(event.payload?.queryLength || 0),
        query_word_count: asNumber(event.payload?.queryWordCount),
        search_trigger: asString(event.payload?.trigger),
        used_recent_search: asBoolean(event.payload?.usedRecentSearch),
      };
    case "source_chip_toggled":
      return {
        action: asString(event.payload?.action),
        ...buildCommonPublicProperties(event),
        source_id: asString(event.payload?.sourceId),
        source_label: asString(event.payload?.sourceLabel),
      };
    case "subscription_cta_clicked":
      return {
        action: String(event.payload?.action || ""),
        ...buildCommonPublicProperties(event),
        authors_count: asNumber(event.payload?.authorsCount),
        content_status: asString(event.payload?.contentStatus),
        content_source: String(event.payload?.source || ""),
        content_type: String(event.payload?.contentType || ""),
        from_tab: String(event.payload?.fromTab || ""),
        genre_count: asNumber(event.payload?.genreCount),
        has_content_url: asBoolean(event.payload?.hasContentUrl),
        is_subscribed_before_click: asBoolean(event.payload?.isSubscribedBeforeClick),
        is_upcoming: asBoolean(event.payload?.isUpcoming),
        release_end_status: asString(event.payload?.releaseEndStatus),
        requires_auth: Boolean(event.payload?.requiresAuth),
        trigger: asString(event.payload?.trigger),
      };
    case "overlay_opened":
      return {
        ...buildCommonPublicProperties(event),
        entrypoint: asString(event.payload?.entrypoint),
        from_tab: String(event.payload?.fromTab || ""),
        overlay: String(event.payload?.overlay || ""),
      };
    case "overlay_closed":
      return {
        ...buildCommonPublicProperties(event),
        close_reason: asString(event.payload?.closeReason),
        overlay: String(event.payload?.overlay || ""),
        return_to: String(event.payload?.returnTo || ""),
      };
    default:
      return null;
  }
}

export function installPublicTelemetry() {
  if (telemetryInstalled) {
    return false;
  }

  telemetryInstalled = true;

  const previousSink = window.__esTrackUiEvent;
  const posthogConfig = getRuntime().posthog;
  const projectApiKey = String(posthogConfig?.projectApiKey || "").trim();
  const enabled = Boolean(posthogConfig?.enabled && projectApiKey);

  if (enabled) {
    posthog.init(projectApiKey, {
      api_host: String(posthogConfig?.apiHost || "https://us.i.posthog.com").trim(),
      autocapture: false,
      capture_pageleave: false,
      capture_pageview: false,
      disable_session_recording: true,
      disable_surveys: true,
      person_profiles: "identified_only",
      persistence: "localStorage",
    });
  }

  window.__esTrackUiEvent = (event) => {
    previousSink?.(event);

    if (!enabled || event.app !== "public") {
      return;
    }

    const posthogEventName = mapPublicUiEventToPosthog(event.name);
    const properties = buildPublicPosthogProperties(event);
    if (!posthogEventName || !properties) {
      return;
    }

    posthog.capture(posthogEventName, properties);
  };

  return enabled;
}
