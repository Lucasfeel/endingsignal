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
        contentId: "high-cardinality-id",
        contentType: "ott",
        fromTab: "ott",
        source: "netflix",
      },
      timestamp: "2026-03-16T00:00:00.000Z",
    });

    expect(properties).toEqual({
      content_source: "netflix",
      content_type: "ott",
      from_tab: "ott",
      path: "/browse/ott",
      surface: "public_web",
      ui_event_name: "content_opened",
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
        fromTab: "webtoon",
        queryLength: 6,
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
        from_tab: "webtoon",
        path: "/search",
        query_length: 6,
      }),
    );
  });
});
