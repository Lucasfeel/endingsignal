export type RuntimePayload = {
  apiBaseUrl?: string;
  sentry?: {
    enabled?: boolean;
    environment?: string;
    repo?: string;
    surface?: string;
  };
  theme?: {
    initialTheme?: "light" | "dark" | "system" | string;
  };
  app?: "public" | "admin" | string;
};

export type UiTelemetryEvent = {
  app: "public" | "admin" | string;
  name: string;
  path: string;
  timestamp: string;
  payload?: Record<string, unknown>;
};

declare global {
  interface Window {
    __ES_RUNTIME__?: RuntimePayload;
    esReportException?: (error: unknown, context?: Record<string, unknown>) => void;
    __esTrackUiEvent?: (event: UiTelemetryEvent) => void;
  }
}

let cachedRuntime: RuntimePayload | null = null;

export function getRuntime(): RuntimePayload {
  if (cachedRuntime) {
    return cachedRuntime;
  }

  cachedRuntime = window.__ES_RUNTIME__ || {};
  return cachedRuntime;
}

export function getApiBaseUrl(): string {
  const value = getRuntime().apiBaseUrl || "";
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

export function reportException(error: unknown, context?: Record<string, unknown>) {
  if (typeof window.esReportException === "function") {
    window.esReportException(error, context);
  }
}
