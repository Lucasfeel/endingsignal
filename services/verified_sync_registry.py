from __future__ import annotations

from typing import Dict, List, Sequence

from run_all_crawlers import ALL_CRAWLERS


def build_source_lookup() -> Dict[str, type]:
    lookup: Dict[str, type] = {}
    for crawler_class in ALL_CRAWLERS:
        instance = crawler_class()
        source_name = str(getattr(instance, "source_name", "")).strip()
        display_name = str(getattr(crawler_class, "DISPLAY_NAME", crawler_class.__name__)).strip()
        for token in {
            source_name,
            crawler_class.__name__,
            crawler_class.__name__.lower(),
            display_name,
            display_name.lower().replace(" ", "_"),
        }:
            if token:
                lookup[token] = crawler_class
    return lookup


def resolve_crawler_class(source_name: str) -> type:
    crawler_class = build_source_lookup().get(str(source_name or "").strip())
    if crawler_class is None:
        raise ValueError(f"Unknown source: {source_name}")
    return crawler_class


def resolve_crawler_classes(requested_sources: Sequence[str]) -> List[type]:
    if not requested_sources:
        return list(ALL_CRAWLERS)

    lookup = build_source_lookup()
    resolved: List[type] = []
    seen = set()
    for token in requested_sources:
        crawler_class = lookup.get(str(token or "").strip())
        if crawler_class is None:
            raise ValueError(f"Unknown source: {token}")
        if crawler_class not in seen:
            resolved.append(crawler_class)
            seen.add(crawler_class)
    return resolved
