import { useCallback } from "react";

import { trackUiEvent } from "../../shared/telemetry";

export function usePublicTelemetry() {
  return useCallback((name: string, payload?: Record<string, unknown>) => {
    trackUiEvent("public", name, payload);
  }, []);
}
