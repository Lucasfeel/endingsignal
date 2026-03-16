import { trackUiEvent, UI_EVENT_CHANNEL } from "./telemetry";

describe("trackUiEvent", () => {
  it("dispatches a browser event and returns the payload", () => {
    const events: Array<Record<string, unknown>> = [];
    const handler = (event: Event) => {
      events.push((event as CustomEvent).detail as Record<string, unknown>);
    };

    window.addEventListener(UI_EVENT_CHANNEL, handler as EventListener);

    try {
      const payload = trackUiEvent("public", "search_opened", { fromTab: "home" });
      expect(payload.app).toBe("public");
      expect(payload.name).toBe("search_opened");
      expect(payload.payload).toEqual({ fromTab: "home" });
      expect(events).toHaveLength(1);
      expect(events[0].name).toBe("search_opened");
    } finally {
      window.removeEventListener(UI_EVENT_CHANNEL, handler as EventListener);
    }
  });
});
