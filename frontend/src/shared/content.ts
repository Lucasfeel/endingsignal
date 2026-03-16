import type { BaseContent, ContentCard, DisplayMeta } from "./types";

function safeRecord(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function safeStringArray(value: unknown) {
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry)).filter(Boolean);
  }
  if (typeof value === "string" && value.trim()) {
    return [value.trim()];
  }
  return [];
}

export function buildContentHref(content: Pick<BaseContent, "source" | "content_id">) {
  return `/content/${encodeURIComponent(content.source)}/${encodeURIComponent(content.content_id)}`;
}

export function buildContentKey(content: Pick<BaseContent, "source" | "content_id">) {
  return encodeURIComponent(`${content.source}:${content.content_id}`);
}

export function extractDisplayMeta(content: Partial<ContentCard> & { meta?: unknown }): DisplayMeta {
  if (content.display_meta) {
    return content.display_meta;
  }

  const meta = safeRecord(content.meta);
  const common = safeRecord(meta.common);
  const attributes = safeRecord(meta.attributes);
  const ott = safeRecord(meta.ott);

  return {
    authors: safeStringArray(common.authors || ott.cast),
    content_url: (common.content_url as string) || (common.url as string) || "",
    url: (common.url as string) || "",
    thumbnail_url: (common.thumbnail_url as string) || "",
    alt_title: (common.alt_title as string) || "",
    title_alias: (common.title_alias as string) || "",
    weekdays: safeStringArray(attributes.weekdays),
    genres: safeStringArray(
      attributes.genres ||
        attributes.genre ||
        common.genres ||
        common.genre ||
        meta.genres ||
        meta.genre ||
        ott.genres ||
        ott.genre,
    ),
    platforms: safeStringArray(ott.platforms),
    cast: safeStringArray(ott.cast),
    upcoming: Boolean(ott.upcoming),
    release_start_at: (ott.release_start_at as string) || null,
    release_end_at: (ott.release_end_at as string) || null,
    release_end_status: (ott.release_end_status as string) || "",
    needs_end_date_verification: Boolean(ott.needs_end_date_verification),
  };
}

export function extractThumbnail(content: Partial<ContentCard> & { meta?: unknown }) {
  return content.thumbnail_url || extractDisplayMeta(content).thumbnail_url || "";
}

export function extractContentUrl(content: Partial<ContentCard> & { meta?: unknown }) {
  return (
    content.content_url ||
    extractDisplayMeta(content).content_url ||
    extractDisplayMeta(content).url ||
    ""
  );
}
