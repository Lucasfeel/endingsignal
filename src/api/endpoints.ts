import { ApiError, request } from './client';
import { attachNormalizedMeta, ContentLike } from './normalizers';

type AuthInfo = { token: string };

type LoginResponse = {
  access_token: string;
  token_type: string;
  expires_in: number;
  user: unknown;
};

type MeResponse = { success: true; user: unknown };
type SubscriptionsResponse = { success: true; data: unknown[] };

type ContentsList = Array<ContentLike>;
type ContentsWithCursor = { contents: ContentsList; next_cursor: string | null };

type OngoingGrouped = Record<string, ContentsList>;

const withContentType = <T extends ContentLike>(items: T[], contentType?: string): T[] =>
  items.map((item) => ({ ...item, content_type: item.content_type ?? contentType }));

export async function authLogin(email: string, password: string): Promise<LoginResponse> {
  const data = await request<LoginResponse>('POST', '/api/auth/login', {
    body: { email, password },
  });
  return data;
}

export async function authMe(auth: AuthInfo): Promise<unknown> {
  const data = await request<MeResponse>('GET', '/api/auth/me', {
    auth,
  });
  if (!data?.success) {
    throw new ApiError('Malformed auth response', 200);
  }
  return data.user;
}

export async function listSubscriptions(auth: AuthInfo): Promise<unknown[]> {
  const data = await request<SubscriptionsResponse>('GET', '/api/me/subscriptions', {
    auth,
  });
  if (!data?.success) {
    throw new ApiError('Malformed subscriptions response', 200);
  }
  return data.data;
}

type ContentsQuery = { type?: string; source?: string };
type SearchQuery = ContentsQuery & { q: string };

export async function searchContents(query: SearchQuery): Promise<ContentsList> {
  const { type, source, q } = query;
  const payload = await request<ContentsList>('GET', '/api/contents/search', {
    query: { q, type, source },
  });
  const normalized = attachNormalizedMeta(payload, type);
  return withContentType(normalized as ContentsList, type);
}

export async function getOngoing(query: ContentsQuery): Promise<OngoingGrouped | ContentsList> {
  const { type, source } = query;
  const payload = await request<OngoingGrouped | ContentsList>('GET', '/api/contents/ongoing', {
    query: { type, source },
  });

  const normalized = attachNormalizedMeta(payload, type);

  if (Array.isArray(normalized)) {
    return withContentType(normalized as ContentsList, type);
  }

  const grouped: OngoingGrouped = {};
  Object.entries(normalized as Record<string, unknown>).forEach(([day, list]) => {
    if (Array.isArray(list)) {
      grouped[day] = withContentType(
        attachNormalizedMeta(list, type) as ContentsList,
        type,
      );
    }
  });
  return grouped;
}

type HiatusCompletedQuery = ContentsQuery & { last_title?: string };

const withCursorNormalization = async (
  path: string,
  query: HiatusCompletedQuery,
): Promise<ContentsWithCursor> => {
  const payload = await request<ContentsWithCursor>('GET', path, {
    query,
  });
  const normalized = attachNormalizedMeta(payload, query.type);
  const contents = Array.isArray((normalized as ContentsWithCursor).contents)
    ? ((normalized as ContentsWithCursor).contents as ContentsList)
    : [];
  return {
    contents: withContentType(contents, query.type),
    next_cursor: (normalized as ContentsWithCursor).next_cursor ?? null,
  };
};

export function getHiatus(query: HiatusCompletedQuery): Promise<ContentsWithCursor> {
  return withCursorNormalization('/api/contents/hiatus', query);
}

export function getCompleted(query: HiatusCompletedQuery): Promise<ContentsWithCursor> {
  return withCursorNormalization('/api/contents/completed', query);
}
