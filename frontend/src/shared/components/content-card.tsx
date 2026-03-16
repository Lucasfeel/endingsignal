import { Link } from "react-router-dom";

import { buildContentHref, extractDisplayMeta, extractThumbnail } from "../content";
import type { BaseContent, ContentCard } from "../types";

type ContentCardProps = {
  content: Partial<ContentCard> & BaseContent;
  compact?: boolean;
};

export function ContentCardView({ content, compact = false }: ContentCardProps) {
  const displayMeta = extractDisplayMeta(content);
  const thumbnail = extractThumbnail(content);

  return (
    <article className={`es-card ${compact ? "is-compact" : ""}`}>
      <Link className="es-card-thumb" to={buildContentHref(content)}>
        {thumbnail ? <img alt="" loading="lazy" src={thumbnail} /> : <span>NO IMAGE</span>}
      </Link>
      <div className="es-card-body">
        <div className="es-card-topline">
          <span className="es-badge">{content.source}</span>
          {content.final_state_badge ? <span className="es-badge">{content.final_state_badge}</span> : null}
        </div>
        <Link className="es-card-title" to={buildContentHref(content)}>
          {content.title}
        </Link>
        <p className="es-card-meta">
          {(displayMeta.authors || []).slice(0, 3).join(" · ") || "작가 정보 없음"}
        </p>
        {displayMeta.genres?.length ? (
          <p className="es-card-tags">{displayMeta.genres.slice(0, 3).join(" · ")}</p>
        ) : null}
      </div>
    </article>
  );
}
