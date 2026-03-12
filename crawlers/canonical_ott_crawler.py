from __future__ import annotations

from typing import Any, Dict

from services.ott_content_service import load_ott_source_snapshot, upsert_ott_source_entries

from .base_crawler import ContentCrawler


class CanonicalOttCrawler(ContentCrawler):
    CONTENT_TYPE = "ott"
    STATUS_ONGOING = "연재중"
    STATUS_COMPLETED = "완결"

    def build_prefetch_context_from_snapshot(self, snapshot):
        context = super().build_prefetch_context_from_snapshot(snapshot)
        safe_snapshot = snapshot if isinstance(snapshot, dict) else {}
        context["snapshot_existing_rows"] = safe_snapshot.get("existing_rows") or []
        context["platform_links"] = safe_snapshot.get("platform_links") or []
        context["watchlist_rows"] = safe_snapshot.get("watchlist_rows") or []
        return context

    def load_remote_snapshot(self, conn) -> Dict[str, Any]:
        return load_ott_source_snapshot(conn, self.source_name)

    def _load_snapshot_state(self, conn, cursor):
        snapshot = self.load_remote_snapshot(conn)
        return self._build_snapshot_state(
            existing_rows=snapshot.get("existing_rows") or [],
            override_rows=[],
            prefetch_context=self.build_prefetch_context_from_snapshot(snapshot),
        )

    def apply_remote_daily_check_payload(self, conn, payload):
        payload = dict(payload or {})
        if str(payload.get("source_name") or self.source_name) != self.source_name:
            raise ValueError("remote apply payload source does not match crawler")
        return self._apply_write_phase(
            conn,
            pending_cdc_records=[],
            skip_database_sync=bool(payload.get("skip_database_sync")),
            all_content_today=payload.get("all_content_today") or {},
            ongoing_today=payload.get("ongoing_today") or {},
            hiatus_today=payload.get("hiatus_today") or {},
            finished_today=payload.get("finished_today") or {},
        )

    def _apply_write_phase(
        self,
        conn,
        *,
        pending_cdc_records,
        skip_database_sync,
        all_content_today,
        ongoing_today,
        hiatus_today,
        finished_today,
    ):
        sync_stats = {
            "inserted_count": 0,
            "updated_count": 0,
            "unchanged_count": 0,
            "write_skipped_count": 0,
        }
        added = 0
        if not skip_database_sync:
            sync_stats = self.normalize_sync_result(
                self.synchronize_database(
                    conn,
                    all_content_today,
                    ongoing_today,
                    hiatus_today,
                    finished_today,
                )
            )
            added = sync_stats["inserted_count"]

        conn.commit()
        return {
            "added": added,
            "cdc_events_inserted_count": 0,
            "cdc_events_inserted_items": [],
            "default_publication_seeded_count": 0,
            **sync_stats,
        }

    def synchronize_database(
        self,
        conn,
        all_content_today,
        ongoing_today,
        hiatus_today,
        finished_today,
    ):
        return upsert_ott_source_entries(
            conn,
            platform_source=self.source_name,
            all_content_today=all_content_today,
        )
