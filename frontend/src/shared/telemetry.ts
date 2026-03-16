import type { UiTelemetryEvent } from "./runtime";

export const UI_EVENT_CHANNEL = "es:ui-event";

export function trackUiEvent(
  app: "public" | "admin" | string,
  name: string,
  payload?: Record<string, unknown>,
) {
  const event: UiTelemetryEvent = {
    app,
    name,
    path: window.location.pathname,
    timestamp: new Date().toISOString(),
    payload,
  };

  try {
    window.dispatchEvent(new CustomEvent<UiTelemetryEvent>(UI_EVENT_CHANNEL, { detail: event }));
  } catch (_error) {
    // Ignore browser environments that block custom events.
  }

  try {
    if (typeof window.__esTrackUiEvent === "function") {
      window.__esTrackUiEvent(event);
    }
  } catch (_error) {
    // Ignore tracking sink failures; UI actions should remain resilient.
  }

  return event;
}
