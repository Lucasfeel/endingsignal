export type DisplayMeta = {
  authors?: string[];
  content_url?: string;
  url?: string;
  thumbnail_url?: string;
  alt_title?: string;
  title_alias?: string;
  weekdays?: string[];
  genres?: string[];
  platforms?: string[];
  cast?: string[];
  upcoming?: boolean;
  release_start_at?: string | null;
  release_end_at?: string | null;
  release_end_status?: string;
  needs_end_date_verification?: boolean;
};

export type BaseContent = {
  content_id: string;
  title: string;
  status?: string | null;
  source: string;
  content_type?: string | null;
  meta?: Record<string, unknown> | null;
};

export type ContentCard = BaseContent & {
  thumbnail_url?: string | null;
  content_url?: string | null;
  display_meta?: DisplayMeta;
  final_state_badge?: string | null;
  cursor?: string | null;
};

export type SubscriptionItem = BaseContent & {
  contentKey: string;
  publication?: {
    public_at?: string | null;
    is_scheduled_publication?: boolean;
    is_published?: boolean;
  };
  subscription?: {
    wants_completion?: boolean;
    wants_publication?: boolean;
  };
  final_state?: {
    label?: string | null;
    state?: string | null;
  };
};

export type AuthUser = {
  id?: number;
  email?: string;
  role?: string;
  user_key?: string | null;
  display_name?: string | null;
  auth_provider?: string | null;
};
