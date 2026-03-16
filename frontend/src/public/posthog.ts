import posthog from "posthog-js";

import { getRuntime, type UiTelemetryEvent } from "../shared/runtime";

const EVENT_NAME_MAP = {
  content_opened: "public_content_opened",
  nav_tab_selected: "public_tab_selected",
  overlay_closed: "public_overlay_closed",
  overlay_opened: "public_overlay_opened",
  search_submitted: "public_search_submitted",
  subscription_cta_clicked: "public_subscription_clicked",
} as const;

type SupportedUiEventName = keyof typeof EVENT_NAME_MAP;

let telemetryInstalled = false;

function isSupportedUiEventName(name: string): name is SupportedUiEventName {
  return Object.prototype.hasOwnProperty.call(EVENT_NAME_MAP, name);
}

export function mapPublicUiEventToPosthog(name: string) {
  return isSupportedUiEventName(name) ? EVENT_NAME_MAP[name] : null;
}

export function buildPublicPosthogProperties(event: UiTelemetryEvent) {
  if (event.app !== "public") {
    return null;
  }

  switch (event.name) {
    case "nav_tab_selected":
      return {
        path: event.path,
        surface: "public_web",
        tab_from: String(event.payload?.from || ""),
        tab_to: String(event.payload?.to || ""),
        ui_event_name: event.name,
      };
    case "content_opened":
      return {
        content_source: String(event.payload?.source || ""),
        content_type: String(event.payload?.contentType || ""),
        from_tab: String(event.payload?.fromTab || ""),
        path: event.path,
        surface: "public_web",
        ui_event_name: event.name,
      };
    case "search_submitted":
      return {
        from_tab: String(event.payload?.fromTab || ""),
        path: event.path,
        query_length: Number(event.payload?.queryLength || 0),
        surface: "public_web",
        ui_event_name: event.name,
      };
    case "subscription_cta_clicked":
      return {
        action: String(event.payload?.action || ""),
        content_source: String(event.payload?.source || ""),
        content_type: String(event.payload?.contentType || ""),
        from_tab: String(event.payload?.fromTab || ""),
        path: event.path,
        requires_auth: Boolean(event.payload?.requiresAuth),
        surface: "public_web",
        ui_event_name: event.name,
      };
    case "overlay_opened":
      return {
        from_tab: String(event.payload?.fromTab || ""),
        overlay: String(event.payload?.overlay || ""),
        path: event.path,
        surface: "public_web",
        ui_event_name: event.name,
      };
    case "overlay_closed":
      return {
        overlay: String(event.payload?.overlay || ""),
        path: event.path,
        return_to: String(event.payload?.returnTo || ""),
        surface: "public_web",
        ui_event_name: event.name,
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
