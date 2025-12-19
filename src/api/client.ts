export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';

export type RequestOptions = {
  query?: Record<string, string | number | boolean | null | undefined>;
  body?: unknown;
  auth?: { token: string } | string;
};

export class ApiError extends Error {
  httpStatus: number;
  code?: string;

  constructor(message: string, httpStatus: number, code?: string) {
    super(message);
    this.name = 'ApiError';
    this.httpStatus = httpStatus;
    this.code = code;
  }
}

const isJsonResponse = (response: Response) => {
  const contentType = response.headers.get('content-type');
  return contentType ? contentType.includes('application/json') : false;
};

const buildUrl = (path: string, query?: RequestOptions['query']) => {
  if (!query || Object.keys(query).length === 0) return path;
  const params = new URLSearchParams();
  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    params.append(key, String(value));
  });
  const queryString = params.toString();
  return queryString ? `${path}?${queryString}` : path;
};

const buildApiError = async (response: Response): Promise<ApiError> => {
  const status = response.status;

  if (isJsonResponse(response)) {
    try {
      const data = await response.clone().json();
      const wrappedError = data?.error;
      if (data?.success === false && wrappedError) {
        return new ApiError(
          wrappedError.message || response.statusText,
          status,
          wrappedError.code,
        );
      }
      if (typeof data?.message === 'string') {
        return new ApiError(data.message, status);
      }
    } catch {
      // fallthrough to generic error below
    }
  }

  let fallbackMessage = response.statusText || 'Request failed';
  try {
    const text = await response.text();
    if (text) fallbackMessage = text;
  } catch {
    // ignore
  }

  return new ApiError(fallbackMessage, status);
};

export async function request<T>(
  method: HttpMethod,
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const { query, body, auth } = options;
  const url = buildUrl(path, query);

  const headers: Record<string, string> = {
    Accept: 'application/json',
  };

  if (auth) {
    const token = typeof auth === 'string' ? auth : auth.token;
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }
  }

  let serializedBody: BodyInit | undefined;
  if (body !== undefined) {
    if (body instanceof FormData || body instanceof Blob) {
      serializedBody = body as BodyInit;
    } else {
      headers['Content-Type'] = 'application/json';
      serializedBody = JSON.stringify(body);
    }
  }

  const response = await fetch(url, {
    method,
    headers,
    body: serializedBody,
  });

  if (!response.ok) {
    throw await buildApiError(response);
  }

  if (isJsonResponse(response)) {
    return response.json() as Promise<T>;
  }

  // If the response has no body or is not JSON, return null to prevent crashes.
  return null as unknown as T;
}
