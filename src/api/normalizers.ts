export type ContentLike = {
  content_id: string;
  title: string;
  status?: string;
  meta?: unknown;
  source: string;
  content_type?: string;
  [key: string]: unknown;
};

export const normalizeMeta = (input: unknown): Record<string, unknown> => {
  if (input === null || input === undefined) return {};

  if (typeof input === 'string') {
    try {
      const parsed = JSON.parse(input);
      return typeof parsed === 'object' && parsed !== null ? parsed : {};
    } catch {
      return {};
    }
  }

  if (typeof input === 'object' && !Array.isArray(input)) {
    return input as Record<string, unknown>;
  }

  return {};
};

const normalizeContentEntry = <T extends ContentLike>(item: T, fallbackType?: string): T => ({
  ...item,
  meta: normalizeMeta(item.meta),
  content_type: item.content_type ?? fallbackType,
});

export function attachNormalizedMeta<T>(payload: T, fallbackType?: string): T {
  if (Array.isArray(payload)) {
    return payload.map((item) =>
      typeof item === 'object' && item !== null
        ? normalizeContentEntry(item as ContentLike, fallbackType)
        : item,
    ) as unknown as T;
  }

  if (payload && typeof payload === 'object') {
    const cloned: Record<string, unknown> = { ...(payload as Record<string, unknown>) };

    if ('meta' in cloned) {
      cloned.meta = normalizeMeta(cloned.meta);
      if (fallbackType && !cloned.content_type) {
        cloned.content_type = fallbackType;
      }
    }

    Object.entries(cloned).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        cloned[key] = value.map((item) =>
          typeof item === 'object' && item !== null
            ? normalizeContentEntry(item as ContentLike, fallbackType)
            : item,
        );
      }
    });

    return cloned as T;
  }

  return payload;
}
