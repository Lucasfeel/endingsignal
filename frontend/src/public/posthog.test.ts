import { beforeEach, describe, expect, it, vi } from "vitest";

const captureMock = vi.fn();
const initMock = vi.fn();
const getRuntimeMock = vi.fn();

vi.mock("posthog-js", () => ({
  default: {
    capture: captureMock,
    init: initMock,
  },
}));

vi.mock("../shared/runtime", async () => {
  const actual = await vi.importActual<typeof import("../shared/runtime")>("../shared/runtime");
  return {
    ...actual,
    getRuntime: getRuntimeMock,
  };
});

describe("public posthog telemetry", () => {
  beforeEach(() => {
    captureMock.mockReset();
    getRuntimeMock.mockReset();
    initMock.mockReset();
    delete window.__esTrackUiEvent;
    vi.resetModules();
  });

  it("maps the minimal public ui event set to PostHog event names", async () => {
    const { mapPublicUiEventToPosthog } = await import("./posthog");

    expect(mapPublicUiEventToPosthog("nav_tab_selected")).toBe("public_tab_selected");
    expect(mapPublicUiEventToPosthog("content_opened")).toBe("public_content_opened");
    expect(mapPublicUiEventToPosthog("search_submitted")).toBe("public_search_submitted");
    expect(mapPublicUiEventToPosthog("subscription_cta_clicked")).toBe("public_subscription_clicked");
    expect(mapPublicUiEventToPosthog("overlay_opened")).toBe("public_overlay_opened");
    expect(mapPublicUiEventToPosthog("overlay_closed")).toBe("public_overlay_closed");
    expect(mapPublicUiEventToPosthog("auth_submitted")).toBeNull();
  });

  it("keeps public posthog payloads low-cardinality", async () => {
    const { buildPublicPosthogProperties } = await import("./posthog");

    const properties = buildPublicPosthogProperties({
      app: "public",
      name: "content_opened",
      path: "/browse/ott",
      payload: {
        activeFilter: "anime",
        activeTab: "ott",
        authorsCount: 4,
        contentId: "high-cardinality-id",
        contentStatus: "ongoing",
        contentType: "ott",
        fromTab: "ott",
        genreCount: 2,
        hasContentUrl: true,
        hasThumbnail: false,
        isAuthenticated: false,
        platformCount: 1,
        routeKind: "browse",
        searchInputLength: 0,
        selectedSourceCount: 2,
        selectedSources: ["netflix", "laftel"],
        source: "netflix",
        trigger: "content_card",
      },
      timestamp: "2026-03-16T00:00:00.000Z",
    });

    expect(properties).toEqual({
      auth_provider: "",
      entry_filter: "anime",
      entry_source_count: 2,
      entry_sources: ["netflix", "laftel"],
      entry_tab: "ott",
      authors_count: 4,
      content_status: "ongoing",
      content_source: "netflix",
      content_type: "ott",
      from_tab: "ott",
      genre_count: 2,
      has_content_url: true,
      has_thumbnail: false,
      is_authenticated: false,
      is_upcoming: false,
      my_view_mode: "",
      novel_filter: "",
      ott_filter: "",
      path: "/browse/ott",
      platform_count: 1,
      release_end_status: "",
      route_kind: "browse",
      search_input_length: 0,
      surface: "public_web",
      trigger: "content_card",
      ui_event_name: "content_opened",
      user_role: "",
      webtoon_filter: "",
      weekday_count: 0,
    });
  });

  it("installs a public-only sink and captures only whitelisted events", async () => {
    getRuntimeMock.mockReturnValue({
      posthog: {
        apiHost: "https://us.i.posthog.com",
        enabled: true,
        projectApiKey: "phc_test_public_key",
      },
    });

    const { installPublicTelemetry } = await import("./posthog");
    installPublicTelemetry();

    expect(initMock).toHaveBeenCalledWith(
      "phc_test_public_key",
      expect.objectContaining({
        api_host: "https://us.i.posthog.com",
        autocapture: false,
        capture_pageleave: false,
        capture_pageview: false,
        disable_session_recording: true,
        disable_surveys: true,
        person_profiles: "identified_only",
      }),
    );

    window.__esTrackUiEvent?.({
      app: "public",
      name: "search_submitted",
      path: "/search",
      payload: {
        activeFilter: "all",
        activeTab: "webtoon",
        fromTab: "webtoon",
        isAuthenticated: false,
        queryLength: 6,
        queryWordCount: 1,
        routeKind: "search",
        searchInputLength: 6,
        selectedSourceCount: 0,
        selectedSources: [],
        trigger: "keyboard_enter",
        usedRecentSearch: false,
      },
      timestamp: "2026-03-16T00:00:00.000Z",
    });

    window.__esTrackUiEvent?.({
      app: "admin",
      name: "search_submitted",
      path: "/admin",
      payload: {
        fromTab: "admin",
        queryLength: 6,
      },
      timestamp: "2026-03-16T00:00:00.000Z",
    });

    window.__esTrackUiEvent?.({
      app: "public",
      name: "auth_submitted",
      path: "/",
      payload: {
        mode: "login",
      },
      timestamp: "2026-03-16T00:00:00.000Z",
    });

    expect(captureMock).toHaveBeenCalledTimes(1);
    expect(captureMock).toHaveBeenCalledWith(
      "public_search_submitted",
      expect.objectContaining({
        entry_filter: "all",
        entry_source_count: 0,
        entry_sources: [],
        entry_tab: "webtoon",
        from_tab: "webtoon",
        path: "/search",
        query_length: 6,
        query_word_count: 1,
        route_kind: "search",
        search_trigger: "keyboard_enter",
        used_recent_search: false,
      }),
    );
  });
});
