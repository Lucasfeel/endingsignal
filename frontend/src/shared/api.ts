import { getApiBaseUrl, reportException } from "./runtime";

export class ApiError extends Error {
  status: number;
  payload: unknown;

  constructor(message: string, status: number, payload: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

type RequestOptions = {
  method?: string;
  token?: string | null;
  body?: unknown;
  signal?: AbortSignal;
};

function buildUrl(path: string) {
  const baseUrl = getApiBaseUrl();
  if (!baseUrl) {
    return path;
  }
  return `${baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
}

export async function apiRequest<T>(path: string, options: RequestOptions = {}) {
  const response = await fetch(buildUrl(path), {
    method: options.method || "GET",
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.token ? { Authorization: `Bearer ${options.token}` } : {}),
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
    signal: options.signal,
  }).catch((error) => {
    reportException(error, { tags: { area: "frontend-api", path, kind: "network" } });
    throw error;
  });

  const contentType = response.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");
  const payload = isJson ? await response.json().catch(() => null) : await response.text().catch(() => null);

  if (!response.ok) {
    const payloadObject = payload as Record<string, unknown> | null;
    const payloadError =
      payloadObject && payloadObject.error && typeof payloadObject.error === "object"
        ? (payloadObject.error as Record<string, unknown>)
        : null;
    const message =
      (typeof payloadError?.message === "string" ? payloadError.message : null) ||
      (typeof payloadObject?.message === "string" ? payloadObject.message : null) ||
      response.statusText ||
      "Request failed";
    throw new ApiError(String(message), response.status, payload);
  }

  return payload as T;
}

export function toQueryString(params: Record<string, string | number | boolean | null | undefined | string[]>) {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }

    if (Array.isArray(value)) {
      value.forEach((entry) => {
        if (entry !== "") {
          searchParams.append(key, entry);
        }
      });
      return;
    }

    searchParams.set(key, String(value));
  });

  const query = searchParams.toString();
  return query ? `?${query}` : "";
}
