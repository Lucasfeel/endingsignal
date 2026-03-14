#crawlers/base_crawler.py
import inspect
import os
from abc import ABC, abstractmethod

from database import get_cursor
from services.cdc_event_service import record_content_completed_event
from services.final_state_resolver import resolve_final_state
from utils.content_indexing import canonicalize_json
from utils.time import parse_iso_naive_kst
import config


class ContentCrawler(ABC):
    """
    모든 콘텐츠 크롤러를 위한 추상 기본 클래스입니다.
    각 크롤러는 이 클래스를 상속받아 특정 콘텐츠 소스에 대한
    데이터 수집, 동기화, 점검 로직을 구현해야 합니다.
    """

    def __init__(self, source_name):
        self.source_name = source_name
        self._prefetch_context = {}

    def build_prefetch_context(self, conn, cursor, db_status_map, override_map, db_state_before_sync):
        """Optional hook for crawler-specific DB snapshot context before fetch."""
        return {}

    def get_prefetch_context(self):
        return self._prefetch_context

    def _verification_candidate_limit(self):
        source_token = str(self.source_name or "").strip().upper().replace("-", "_")
        env_keys = [f"VERIFIED_SYNC_MAX_CHANGES_{source_token}", "VERIFIED_SYNC_MAX_CHANGES_PER_SOURCE"]
        for key in env_keys:
            raw_value = os.getenv(key)
            if raw_value in (None, ""):
                continue
            try:
                return max(0, int(str(raw_value).strip()))
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _select_limited_candidate_ids(verification_candidates_by_id, *, new_content_items, newly_completed_items, limit):
        if limit is None or len(verification_candidates_by_id) <= limit:
            return None

        ordered_ids = []
        seen = set()

        def _push(content_id):
            cid = str(content_id or "").strip()
            if not cid or cid in seen or cid not in verification_candidates_by_id:
                return
            ordered_ids.append(cid)
            seen.add(cid)

        for content_id, *_ in newly_completed_items:
            _push(content_id)
        for item in new_content_items:
            if isinstance(item, dict):
                _push(item.get("content_id"))
        for content_id in verification_candidates_by_id.keys():
            _push(content_id)

        return set(ordered_ids[:limit])

    @staticmethod
    def _partial_verified_subset_enabled():
        raw_value = str(os.getenv("VERIFIED_SYNC_APPLY_VERIFIED_SUBSET") or "").strip().lower()
        return raw_value in {"1", "true", "t", "yes", "y", "on"}

    @staticmethod
    def _filter_change_sets_by_ids(
        *,
        selected_ids,
        all_content_today,
        ongoing_today,
        hiatus_today,
        finished_today,
        new_content_items,
        newly_completed_items,
        pending_cdc_records,
        verification_candidates_by_id,
    ):
        allowed_ids = {str(content_id or "").strip() for content_id in (selected_ids or []) if str(content_id or "").strip()}
        return {
            "all_content_today": {
                content_id: entry for content_id, entry in all_content_today.items() if content_id in allowed_ids
            },
            "ongoing_today": {
                content_id: entry for content_id, entry in ongoing_today.items() if content_id in allowed_ids
            },
            "hiatus_today": {
                content_id: entry for content_id, entry in hiatus_today.items() if content_id in allowed_ids
            },
            "finished_today": {
                content_id: entry for content_id, entry in finished_today.items() if content_id in allowed_ids
            },
            "new_content_items": [
                item for item in new_content_items if str(item.get("content_id") or "").strip() in allowed_ids
            ],
            "newly_completed_items": [
                item for item in newly_completed_items if str(item[0] or "").strip() in allowed_ids
            ],
            "pending_cdc_records": [
                item for item in pending_cdc_records if str(item[0] or "").strip() in allowed_ids
            ],
            "verification_candidates_by_id": {
                content_id: item
                for content_id, item in verification_candidates_by_id.items()
                if content_id in allowed_ids
            },
        }

    def build_prefetch_context_from_snapshot(self, snapshot):
        existing_rows = []
        if isinstance(snapshot, dict):
            raw_rows = snapshot.get("existing_rows")
            if isinstance(raw_rows, list):
                existing_rows = [dict(row) for row in raw_rows if isinstance(row, dict)]
        return {"sync_snapshot": self._build_sync_snapshot(existing_rows)}

    @staticmethod
    def _serialize_verification_value(value):
        if hasattr(value, "isoformat"):
            return value.isoformat()
        if isinstance(value, dict):
            return {
                str(key): ContentCrawler._serialize_verification_value(raw_value)
                for key, raw_value in value.items()
            }
        if isinstance(value, set):
            return [
                ContentCrawler._serialize_verification_value(item)
                for item in sorted(value, key=lambda item: str(item))
            ]
        if isinstance(value, (list, tuple)):
            return [
                ContentCrawler._serialize_verification_value(item)
                for item in value
            ]
        return value

    def build_default_content_url(self, content_id, content_data=None):
        cid = str(content_id or "").strip()
        if not cid:
            return ""
        if self.source_name == "naver_webtoon":
            return f"https://m.comic.naver.com/webtoon/list?titleId={cid}"
        if self.source_name == "naver_series":
            return f"https://series.naver.com/novel/detail.series?productNo={cid}"
        if self.source_name == "kakao_page":
            return f"https://page.kakao.com/content/{cid}"
        if self.source_name == "ridi":
            return f"https://ridibooks.com/books/{cid}"
        if self.source_name == "laftel":
            return f"https://laftel.net/item/{cid}"
        return ""

    def resolve_verification_content_url(self, content_id, content_data):
        if isinstance(content_data, dict):
            direct_url = str(content_data.get("content_url") or "").strip()
            if direct_url:
                return direct_url
            platform_url = str(content_data.get("platform_url") or "").strip()
            if platform_url:
                return platform_url
            meta = content_data.get("meta")
            if isinstance(meta, dict):
                common = meta.get("common")
                if isinstance(common, dict):
                    meta_url = str(common.get("content_url") or "").strip()
                    if meta_url:
                        return meta_url
                    meta_url = str(common.get("url") or "").strip()
                    if meta_url:
                        return meta_url
        return self.build_default_content_url(content_id, content_data=content_data)

    @staticmethod
    def _build_sync_snapshot(rows):
        snapshot = {}
        for row in rows:
            content_id = str(row["content_id"])
            snapshot[content_id] = {
                "content_type": row.get("content_type"),
                "title": row.get("title"),
                "normalized_title": row.get("normalized_title") or "",
                "normalized_authors": row.get("normalized_authors") or "",
                "status": row.get("status"),
                "meta_json": canonicalize_json(row.get("meta") or {}),
                "search_document": row.get("search_document") or "",
                "novel_genre_group": row.get("novel_genre_group"),
                "novel_genre_groups_json": canonicalize_json(row.get("novel_genre_groups") or []),
            }
        return snapshot

    @staticmethod
    def _coerce_snapshot_rows(rows):
        return [dict(row) for row in (rows or []) if isinstance(row, dict)]

    @staticmethod
    def _coerce_override_rows(rows):
        normalized = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            if isinstance(item.get("override_completed_at"), str):
                item["override_completed_at"] = parse_iso_naive_kst(item.get("override_completed_at"))
            normalized.append(item)
        return normalized

    def _build_snapshot_state(self, *, existing_rows, override_rows, prefetch_context=None):
        normalized_existing_rows = self._coerce_snapshot_rows(existing_rows)
        normalized_override_rows = self._coerce_override_rows(override_rows)

        db_status_map = {
            str(row["content_id"]): row.get("status")
            for row in normalized_existing_rows
            if row.get("content_id") is not None
        }
        override_map = {
            str(row["content_id"]): row
            for row in normalized_override_rows
            if row.get("content_id") is not None
        }

        db_state_before_sync = {}
        for content_id in set(db_status_map.keys()) | set(override_map.keys()):
            db_state_before_sync[content_id] = resolve_final_state(
                db_status_map.get(content_id),
                override_map.get(content_id),
            )

        sync_snapshot = self._build_sync_snapshot(normalized_existing_rows)
        resolved_prefetch_context = dict(prefetch_context or {})
        resolved_prefetch_context.setdefault("sync_snapshot", sync_snapshot)

        return {
            "existing_rows": normalized_existing_rows,
            "override_rows": normalized_override_rows,
            "db_status_map": db_status_map,
            "override_map": override_map,
            "db_state_before_sync": db_state_before_sync,
            "sync_snapshot": sync_snapshot,
            "prefetch_context": resolved_prefetch_context,
        }

    @staticmethod
    def _attach_prefetch_context(snapshot_state, prefetch_context):
        resolved_prefetch_context = dict(prefetch_context or {})
        resolved_prefetch_context.setdefault("sync_snapshot", snapshot_state.get("sync_snapshot") or {})
        snapshot_state["prefetch_context"] = resolved_prefetch_context
        return snapshot_state

    def _load_snapshot_state(self, conn, cursor):
        cursor.execute(
            """
            SELECT
                content_id,
                content_type,
                title,
                normalized_title,
                normalized_authors,
                status,
                meta,
                search_document,
                novel_genre_group,
                novel_genre_groups
            FROM contents
            WHERE source = %s
            """,
            (self.source_name,),
        )
        existing_rows = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            "SELECT content_id, override_status, override_completed_at "
            "FROM admin_content_overrides WHERE source = %s",
            (self.source_name,),
        )
        override_rows = [dict(row) for row in cursor.fetchall()]

        snapshot_state = self._build_snapshot_state(
            existing_rows=existing_rows,
            override_rows=override_rows,
        )
        prefetch_context = self.build_prefetch_context(
            conn,
            cursor,
            snapshot_state["db_status_map"],
            snapshot_state["override_map"],
            snapshot_state["db_state_before_sync"],
        )
        if not isinstance(prefetch_context, dict):
            prefetch_context = {}
        return self._attach_prefetch_context(snapshot_state, prefetch_context)

    @staticmethod
    def normalize_sync_result(sync_result):
        if isinstance(sync_result, dict):
            inserted_count = int(sync_result.get("inserted_count") or 0)
            updated_count = int(sync_result.get("updated_count") or 0)
            unchanged_count = int(sync_result.get("unchanged_count") or 0)
            write_skipped_count = int(sync_result.get("write_skipped_count") or 0)
            normalized = {
                "inserted_count": max(0, inserted_count),
                "updated_count": max(0, updated_count),
                "unchanged_count": max(0, unchanged_count),
                "write_skipped_count": max(0, write_skipped_count),
            }
            for key, value in sync_result.items():
                if key in normalized:
                    continue
                normalized[key] = value
            return normalized

        try:
            inserted_count = int(sync_result or 0)
        except (TypeError, ValueError):
            inserted_count = 0

        return {
            "inserted_count": max(0, inserted_count),
            "updated_count": 0,
            "unchanged_count": 0,
            "write_skipped_count": 0,
        }

    def build_verification_candidate(
        self,
        *,
        content_id,
        content_data,
        expected_status,
        change_kind,
        previous_status=None,
    ):
        cid = str(content_id or "").strip()
        if not cid:
            return None

        entry = dict(content_data) if isinstance(content_data, dict) else {}
        title = str(entry.get("title") or entry.get("titleName") or "").strip() or cid

        return {
            "content_id": cid,
            "source_name": self.source_name,
            "title": title,
            "expected_status": str(expected_status or "").strip(),
            "previous_status": str(previous_status or "").strip() or None,
            "content_url": self.resolve_verification_content_url(cid, entry),
            "change_kinds": [str(change_kind or "").strip()] if change_kind else [],
            "source_item": self._serialize_verification_value(entry),
        }

    def seed_webtoon_publication_dates(self, cursor):
        """
        Seed default publication dates for webtoons from first-seen timestamp.

        public_at default rule:
        - if admin_content_metadata row does not exist
        - and content_type is webtoon
        - then set public_at = contents.created_at

        admin_content_metadata.admin_id is NOT NULL, so this uses the first
        available user id (preferring admin role) as the actor.
        """
        cursor.execute(
            """
            WITH preferred_user AS (
                SELECT id
                FROM users
                ORDER BY
                    CASE WHEN role = 'admin' THEN 0 ELSE 1 END,
                    id ASC
                LIMIT 1
            )
            INSERT INTO admin_content_metadata (
                content_id,
                source,
                public_at,
                reason,
                admin_id,
                updated_at
            )
            SELECT
                c.content_id,
                c.source,
                c.created_at,
                'auto_from_created_at',
                u.id,
                NOW()
            FROM contents c
            JOIN preferred_user u ON TRUE
            LEFT JOIN admin_content_metadata m
              ON m.content_id = c.content_id
             AND m.source = c.source
            WHERE c.source = %s
              AND c.content_type = 'webtoon'
              AND COALESCE(c.is_deleted, FALSE) = FALSE
              AND m.content_id IS NULL
            ON CONFLICT (content_id, source) DO NOTHING
            RETURNING content_id
            """,
            (self.source_name,),
        )
        rows = cursor.fetchall()
        return len(rows) if rows else 0

    @abstractmethod
    async def fetch_all_data(self):
        """
        소스에서 모든 콘텐츠 데이터를 비동기적으로 가져옵니다.
        """
        raise NotImplementedError

    @abstractmethod
    def synchronize_database(self, conn, all_content_today, ongoing_today, hiatus_today, finished_today):
        """
        데이터베이스를 최신 상태로 동기화합니다.
        NOTE: commit/rollback은 ContentCrawler.run_daily_check()에서 강제합니다.
        """
        raise NotImplementedError

    async def evaluate_verification_gate(self, verification_gate, write_plan):
        if verification_gate is None:
            return None

        verdict = verification_gate(write_plan)
        if inspect.isawaitable(verdict):
            verdict = await verdict

        if not isinstance(verdict, dict):
            verdict = {}

        gate_status = str(
            verdict.get("gate")
            or verdict.get("status")
            or "passed"
        ).strip().lower()
        if gate_status in {"pass", "ok", "approved"}:
            gate_status = "passed"
        elif gate_status in {"fail", "error", "deny", "denied"}:
            gate_status = "blocked"
        elif gate_status not in {"passed", "blocked", "skipped", "not_applicable"}:
            gate_status = "passed"

        apply_allowed = verdict.get("apply_allowed")
        if apply_allowed is None:
            apply_allowed = gate_status in {"passed", "not_applicable"}

        normalized = {
            "status": gate_status,
            "mode": str(verdict.get("mode") or "pass_through"),
            "reason": str(verdict.get("reason") or gate_status),
            "message": str(verdict.get("message") or ""),
            "apply_allowed": bool(apply_allowed),
        }
        for key, value in verdict.items():
            if key in {"gate", "status", "mode", "reason", "message", "apply_allowed"}:
                continue
            normalized[key] = self._serialize_verification_value(value)
        return normalized

    def _build_remote_apply_payload(
        self,
        *,
        all_content_today,
        ongoing_today,
        hiatus_today,
        finished_today,
        pending_cdc_records,
        skip_database_sync,
    ):
        return self._serialize_verification_value(
            {
                "source_name": self.source_name,
                "all_content_today": all_content_today,
                "ongoing_today": ongoing_today,
                "hiatus_today": hiatus_today,
                "finished_today": finished_today,
                "pending_cdc_records": [
                    {
                        "content_id": content_id,
                        "final_completed_at": (
                            final_completed_at.isoformat()
                            if hasattr(final_completed_at, "isoformat")
                            else final_completed_at
                        ),
                        "resolved_by": resolved_by,
                    }
                    for content_id, final_completed_at, resolved_by in pending_cdc_records
                ],
                "skip_database_sync": bool(skip_database_sync),
            }
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
        write_cursor = None
        try:
            write_cursor = get_cursor(conn)

            cdc_events_inserted_count = 0
            cdc_events_inserted_items = []
            for content_id, final_completed_at, resolved_by in pending_cdc_records:
                inserted = record_content_completed_event(
                    conn,
                    content_id=content_id,
                    source=self.source_name,
                    final_completed_at=final_completed_at,
                    resolved_by=resolved_by,
                )
                if inserted:
                    cdc_events_inserted_count += 1
                    cdc_events_inserted_items.append(content_id)

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

            default_publication_seeded_count = 0
            default_publication_seed_error = None
            try:
                default_publication_seeded_count = self.seed_webtoon_publication_dates(write_cursor)
            except Exception as seed_error:
                default_publication_seed_error = str(seed_error)

            conn.commit()
            result = {
                "added": added,
                "cdc_events_inserted_count": cdc_events_inserted_count,
                "cdc_events_inserted_items": cdc_events_inserted_items,
                "default_publication_seeded_count": default_publication_seeded_count,
                **sync_stats,
            }
            if default_publication_seed_error:
                result["default_publication_seed_error"] = default_publication_seed_error
            return result
        finally:
            if write_cursor:
                try:
                    write_cursor.close()
                except Exception:
                    pass

    async def _finalize_daily_check(
        self,
        *,
        conn,
        snapshot_state,
        fetch_result,
        verification_gate=None,
        write_enabled=True,
        allow_deferred_apply=False,
    ):
        db_status_map = snapshot_state["db_status_map"]
        override_map = snapshot_state["override_map"]
        db_state_before_sync = snapshot_state["db_state_before_sync"]

        if len(fetch_result) == 5:
            ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta = fetch_result
        else:
            ongoing_today, hiatus_today, finished_today, all_content_today = fetch_result
            fetch_meta = {}

        if not isinstance(fetch_meta, dict):
            fetch_meta = {"raw_meta": fetch_meta}

        def _normalize_key_map(map_data):
            return {str(k): v for k, v in map_data.items()}

        ongoing_today = _normalize_key_map(ongoing_today)
        hiatus_today = _normalize_key_map(hiatus_today)
        finished_today = _normalize_key_map(finished_today)
        all_content_today = _normalize_key_map(all_content_today)

        current_status_map = {}
        for content_id in all_content_today.keys():
            cid = str(content_id)
            if cid in finished_today:
                current_status_map[cid] = "완결"
            elif cid in hiatus_today:
                current_status_map[cid] = "휴재"
            elif cid in ongoing_today:
                current_status_map[cid] = "연재중"

        for content_id, previous_state in db_state_before_sync.items():
            current_status_map.setdefault(content_id, previous_state["final_status"])

        current_final_state_map = {}
        for content_id in set(current_status_map.keys()) | set(override_map.keys()):
            current_final_state_map[content_id] = resolve_final_state(
                current_status_map.get(content_id),
                override_map.get(content_id),
            )

        newly_completed_items = []
        new_content_items = []
        pending_cdc_records = []
        verification_candidates_by_id = {}

        def _record_verification_candidate(
            *,
            content_id,
            content_data,
            expected_status,
            change_kind,
            previous_status=None,
        ):
            candidate = self.build_verification_candidate(
                content_id=content_id,
                content_data=content_data,
                expected_status=expected_status,
                change_kind=change_kind,
                previous_status=previous_status,
            )
            if candidate is None:
                return

            existing = verification_candidates_by_id.get(candidate["content_id"])
            if existing is None:
                verification_candidates_by_id[candidate["content_id"]] = candidate
                return

            merged_change_kinds = set(existing.get("change_kinds") or [])
            merged_change_kinds.update(candidate.get("change_kinds") or [])
            existing["change_kinds"] = sorted(merged_change_kinds)
            if candidate.get("expected_status"):
                existing["expected_status"] = candidate["expected_status"]
            if candidate.get("previous_status") and not existing.get("previous_status"):
                existing["previous_status"] = candidate["previous_status"]
            if candidate.get("content_url") and not existing.get("content_url"):
                existing["content_url"] = candidate["content_url"]
            if candidate.get("title") and existing.get("title") == existing.get("content_id"):
                existing["title"] = candidate["title"]
            if candidate.get("source_item") and not existing.get("source_item"):
                existing["source_item"] = candidate["source_item"]

        for content_id, content_data in all_content_today.items():
            if content_id in db_status_map:
                continue
            current_final_state = current_final_state_map.get(content_id, {})
            candidate = self.build_verification_candidate(
                content_id=content_id,
                content_data=content_data,
                expected_status=current_final_state.get("final_status") or current_status_map.get(content_id),
                change_kind="new_content",
                previous_status=None,
            )
            if candidate is None:
                continue
            new_content_items.append(candidate)
            verification_candidates_by_id[candidate["content_id"]] = candidate

        for content_id, current_final_state in current_final_state_map.items():
            previous_final_state = db_state_before_sync.get(content_id, {"final_status": None})

            if previous_final_state.get("final_status") != "완결" and current_final_state["final_status"] == "완결":
                final_completed_at = current_final_state.get("final_completed_at")
                display_completed_at = (
                    final_completed_at.isoformat()
                    if hasattr(final_completed_at, "isoformat")
                    else final_completed_at
                )

                newly_completed_items.append(
                    (
                        content_id,
                        self.source_name,
                        display_completed_at,
                        current_final_state.get("resolved_by"),
                    )
                )

                pending_cdc_records.append(
                    (content_id, final_completed_at, current_final_state.get("resolved_by"))
                )
                _record_verification_candidate(
                    content_id=content_id,
                    content_data=all_content_today.get(content_id),
                    expected_status=current_final_state.get("final_status"),
                    change_kind="newly_completed",
                    previous_status=previous_final_state.get("final_status"),
                )

        total_candidate_count = len(verification_candidates_by_id)
        candidate_limit = self._verification_candidate_limit()
        skipped_candidate_count = 0
        selected_candidate_ids = self._select_limited_candidate_ids(
            verification_candidates_by_id,
            new_content_items=new_content_items,
            newly_completed_items=newly_completed_items,
            limit=candidate_limit,
        )
        if selected_candidate_ids is not None:
            skipped_candidate_count = max(0, total_candidate_count - len(selected_candidate_ids))
            all_content_today = {
                content_id: entry
                for content_id, entry in all_content_today.items()
                if content_id in selected_candidate_ids
            }
            ongoing_today = {
                content_id: entry
                for content_id, entry in ongoing_today.items()
                if content_id in selected_candidate_ids
            }
            hiatus_today = {
                content_id: entry
                for content_id, entry in hiatus_today.items()
                if content_id in selected_candidate_ids
            }
            finished_today = {
                content_id: entry
                for content_id, entry in finished_today.items()
                if content_id in selected_candidate_ids
            }
            new_content_items = [
                item for item in new_content_items if str(item.get("content_id") or "").strip() in selected_candidate_ids
            ]
            newly_completed_items = [
                item for item in newly_completed_items if str(item[0] or "").strip() in selected_candidate_ids
            ]
            pending_cdc_records = [
                item for item in pending_cdc_records if str(item[0] or "").strip() in selected_candidate_ids
            ]
            verification_candidates_by_id = {
                content_id: item
                for content_id, item in verification_candidates_by_id.items()
                if content_id in selected_candidate_ids
            }

        resolved_by_counts = {}
        for _, _, _, resolved_by in newly_completed_items:
            resolved_by_counts[resolved_by] = resolved_by_counts.get(resolved_by, 0) + 1

        db_count = len(db_status_map)
        fetched_count = len(all_content_today)

        fetched_count_override = fetch_meta.get("fetched_count")
        if isinstance(fetched_count_override, int) and fetched_count_override >= 0:
            fetched_count = fetched_count_override

        health_db_count = db_count
        expected_count = fetch_meta.get("expected_count")
        health_db_override = fetch_meta.get("health_db_count")
        if isinstance(health_db_override, int) and health_db_override > 0:
            health_db_count = health_db_override
        elif isinstance(expected_count, int) and expected_count > 0:
            health_db_count = expected_count

        no_ratio = bool(fetch_meta.get("force_no_ratio"))
        ratio = None if no_ratio or health_db_count is None else fetched_count / max(health_db_count, 1)
        health_info = {
            "db_count": db_count,
            "fetched_count": fetched_count,
            "health_db_count": health_db_count,
            "fetch_ratio": ratio,
            "min_ratio_threshold": config.CRAWLER_FETCH_HEALTH_MIN_RATIO,
        }
        is_degraded_fetch = False
        skip_reason = None
        fetch_errors = fetch_meta.get("errors")
        if fetch_errors is not None:
            health_info["fetch_errors"] = fetch_errors
        if fetch_errors:
            is_degraded_fetch = True
            skip_reason = "fetch_errors"
        if ratio is not None and ratio < config.CRAWLER_FETCH_HEALTH_MIN_RATIO:
            is_degraded_fetch = True
            skip_reason = skip_reason or "fetch_ratio_below_threshold"

        if fetch_meta.get("is_suspicious_empty"):
            health_info["is_suspicious_empty"] = True
            if ratio is None:
                ratio = 0.0
                health_info["fetch_ratio"] = ratio
        notes = fetch_meta.get("health_notes")
        if isinstance(notes, list):
            health_info["notes"] = notes
        skip_database_sync = bool(fetch_meta.get("skip_database_sync"))

        fetch_meta["is_degraded_fetch"] = is_degraded_fetch
        fetch_meta["fetch_health"] = health_info

        status_label = fetch_meta.get("status") or "ok"
        summary = fetch_meta.get("summary")
        candidate_limit_note = None
        if skipped_candidate_count > 0:
            candidate_limit_note = (
                "VERIFICATION_CANDIDATE_LIMIT_APPLIED:"
                f"selected={len(verification_candidates_by_id)}:"
                f"skipped={skipped_candidate_count}:"
                f"limit={candidate_limit}"
            )
            health_notes = fetch_meta.setdefault("health_notes", [])
            if isinstance(health_notes, list):
                health_notes.append(candidate_limit_note)
            fetch_meta["verification_candidate_limit"] = {
                "limit": candidate_limit,
                "selected_count": len(verification_candidates_by_id),
                "skipped_count": skipped_candidate_count,
                "total_count": total_candidate_count,
            }
            if status_label == "ok":
                status_label = "warn"
        if is_degraded_fetch and status_label == "ok":
            status_label = "warn"
        if summary is None:
            if candidate_limit_note:
                summary = {
                    "crawler": self.source_name,
                    "reason": "candidate_limit_applied",
                    "message": "crawler run completed with verification candidate limit applied",
                }
            else:
                summary = {
                    "crawler": self.source_name,
                    "reason": status_label,
                    "message": "crawler run completed",
                }
        elif candidate_limit_note and summary.get("reason") == "ok":
            summary = {
                "crawler": self.source_name,
                "reason": "candidate_limit_applied",
                "message": "crawler run completed with verification candidate limit applied",
            }

        cdc_info = {
            "cdc_mode": "final_state",
            "newly_completed_count": len(newly_completed_items),
            "resolved_by_counts": resolved_by_counts,
            "cdc_events_inserted_count": 0,
            "cdc_events_inserted_items": [],
            "cdc_skipped": is_degraded_fetch,
            "skip_reason": skip_reason,
            "health": health_info,
            "fetch_meta": fetch_meta,
            "status": status_label,
            "summary": summary,
            "db_sync_skipped": skip_database_sync,
            "inserted_count": 0,
            "updated_count": 0,
            "unchanged_count": 0,
            "write_skipped_count": 0,
            "candidate_total_count": total_candidate_count,
            "candidate_selected_count": len(verification_candidates_by_id),
            "candidate_skipped_count": skipped_candidate_count,
        }

        write_plan = {
            "source_name": self.source_name,
            "status": status_label,
            "summary": summary,
            "health": health_info,
            "fetch_meta": fetch_meta,
            "fetched_count": fetched_count,
            "all_content_today": all_content_today,
            "ongoing_today": ongoing_today,
            "hiatus_today": hiatus_today,
            "finished_today": finished_today,
            "new_content_items": new_content_items,
            "newly_completed_items": newly_completed_items,
            "verification_candidates": list(verification_candidates_by_id.values()),
            "snapshot_existing_rows": list((self.get_prefetch_context() or {}).get("snapshot_existing_rows") or []),
            "platform_links": list((self.get_prefetch_context() or {}).get("platform_links") or []),
            "watchlist_rows": list((self.get_prefetch_context() or {}).get("watchlist_rows") or []),
            "pending_cdc_records": [
                {
                    "content_id": content_id,
                    "final_completed_at": final_completed_at,
                    "resolved_by": resolved_by,
                }
                for content_id, final_completed_at, resolved_by in pending_cdc_records
            ],
            "new_contents_count": len(new_content_items),
        }

        selected_candidate_count = len(verification_candidates_by_id)
        verification = await self.evaluate_verification_gate(verification_gate, write_plan)
        if verification is not None and self._partial_verified_subset_enabled():
            verification_items = verification.get("items")
            if isinstance(verification_items, list):
                verified_candidate_ids = {
                    str(item.get("content_id") or "").strip()
                    for item in verification_items
                    if (
                        isinstance(item, dict)
                        and item.get("ok")
                        and not item.get("watchlist_recheck")
                        and str(item.get("reason") or "").strip().lower() != "filtered_out"
                        and not item.get("exclude_reason")
                        and str(item.get("content_id") or "").strip()
                    )
                }
                failed_count = sum(
                    1
                    for item in verification_items
                    if isinstance(item, dict) and not item.get("ok")
                )
                if failed_count > 0:
                    filtered_sets = self._filter_change_sets_by_ids(
                        selected_ids=verified_candidate_ids,
                        all_content_today=all_content_today,
                        ongoing_today=ongoing_today,
                        hiatus_today=hiatus_today,
                        finished_today=finished_today,
                        new_content_items=new_content_items,
                        newly_completed_items=newly_completed_items,
                        pending_cdc_records=pending_cdc_records,
                        verification_candidates_by_id=verification_candidates_by_id,
                    )
                    all_content_today = filtered_sets["all_content_today"]
                    ongoing_today = filtered_sets["ongoing_today"]
                    hiatus_today = filtered_sets["hiatus_today"]
                    finished_today = filtered_sets["finished_today"]
                    new_content_items = filtered_sets["new_content_items"]
                    newly_completed_items = filtered_sets["newly_completed_items"]
                    pending_cdc_records = filtered_sets["pending_cdc_records"]
                    verification_candidates_by_id = filtered_sets["verification_candidates_by_id"]

                    resolved_by_counts = {}
                    for _, _, _, resolved_by in newly_completed_items:
                        resolved_by_counts[resolved_by] = resolved_by_counts.get(resolved_by, 0) + 1

                    skipped_after_verification = max(0, selected_candidate_count - len(verification_candidates_by_id))
                    cdc_info["newly_completed_count"] = len(newly_completed_items)
                    cdc_info["resolved_by_counts"] = resolved_by_counts
                    cdc_info["candidate_selected_count"] = len(verification_candidates_by_id)
                    cdc_info["candidate_skipped_count"] = skipped_candidate_count + skipped_after_verification
                    cdc_info["status"] = "warn"
                    cdc_info["summary"] = {
                        "crawler": self.source_name,
                        "reason": "verified_subset_applied",
                        "message": (
                            f"verification matched {len(verification_candidates_by_id)}/{selected_candidate_count} "
                            "selected changes; applying verified subset only"
                        ),
                    }
                    verification = {
                        **verification,
                        "gate": "passed",
                        "status": "passed",
                        "reason": "verified_subset",
                        "message": (
                            f"{self.source_name} verified {len(verification_candidates_by_id)}/{selected_candidate_count} "
                            "selected changes; unverified changes were skipped"
                        ),
                        "apply_allowed": True,
                        "verified_count": len(verification_candidates_by_id),
                        "failed_count": failed_count,
                    }

        if verification is not None:
            cdc_info["verification"] = verification

        apply_allowed = bool(verification.get("apply_allowed", True)) if verification else True
        would_apply = not is_degraded_fetch and apply_allowed and write_enabled
        apply_result = "applied"
        if not apply_allowed:
            apply_result = "blocked"
        elif not write_enabled:
            apply_result = "dry_run"
        elif is_degraded_fetch:
            apply_result = "skipped"
        elif conn is None and allow_deferred_apply:
            apply_result = "deferred"

        apply_payload = None
        if conn is not None and would_apply:
            write_result = self._apply_write_phase(
                conn,
                pending_cdc_records=pending_cdc_records,
                skip_database_sync=skip_database_sync,
                all_content_today=all_content_today,
                ongoing_today=ongoing_today,
                hiatus_today=hiatus_today,
                finished_today=finished_today,
            )
            added = write_result.pop("added", 0)
            cdc_info.update(write_result)
        else:
            added = 0
            cdc_info["cdc_skipped"] = True
            if apply_result != "deferred":
                cdc_info["cdc_events_inserted_count"] = 0
                cdc_info["cdc_events_inserted_items"] = []
                cdc_info["default_publication_seeded_count"] = 0
            if not cdc_info.get("skip_reason"):
                if not apply_allowed:
                    cdc_info["skip_reason"] = "verification_blocked"
                elif not write_enabled:
                    cdc_info["skip_reason"] = "dry_run"
                elif apply_result == "deferred":
                    cdc_info["skip_reason"] = "remote_apply_pending"
            if not skip_database_sync and apply_result != "deferred":
                cdc_info["db_sync_skipped"] = True
            if verification and not verification.get("message"):
                if not apply_allowed:
                    verification["message"] = "verification gate blocked database apply"
                elif not write_enabled:
                    verification["message"] = "dry-run mode skipped database apply"
                elif apply_result == "deferred":
                    verification["message"] = "verification passed; awaiting remote database apply"
            if apply_result == "deferred":
                apply_payload = self._build_remote_apply_payload(
                    all_content_today=all_content_today,
                    ongoing_today=ongoing_today,
                    hiatus_today=hiatus_today,
                    finished_today=finished_today,
                    pending_cdc_records=pending_cdc_records,
                    skip_database_sync=skip_database_sync,
                )

        if skip_database_sync and apply_result == "applied":
            cdc_info["db_sync_skipped"] = True
        cdc_info["apply_result"] = apply_result

        return added, newly_completed_items, cdc_info, apply_payload

    async def prepare_remote_daily_check(
        self,
        snapshot,
        *,
        verification_gate=None,
        write_enabled=True,
    ):
        snapshot_data = snapshot if isinstance(snapshot, dict) else {}
        snapshot_state = self._build_snapshot_state(
            existing_rows=snapshot_data.get("existing_rows") or [],
            override_rows=snapshot_data.get("override_rows") or [],
            prefetch_context=self.build_prefetch_context_from_snapshot(snapshot_data),
        )

        try:
            self._prefetch_context = snapshot_state["prefetch_context"]
            fetch_result = await self.fetch_all_data()
            return await self._finalize_daily_check(
                conn=None,
                snapshot_state=snapshot_state,
                fetch_result=fetch_result,
                verification_gate=verification_gate,
                write_enabled=write_enabled,
                allow_deferred_apply=write_enabled,
            )
        finally:
            self._prefetch_context = {}

    def apply_remote_daily_check_payload(self, conn, payload):
        payload = dict(payload or {})
        if str(payload.get("source_name") or self.source_name) != self.source_name:
            raise ValueError("remote apply payload source does not match crawler")

        read_cursor = get_cursor(conn)
        try:
            read_cursor.execute(
                """
                SELECT
                    content_id,
                    content_type,
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    meta,
                    search_document,
                    novel_genre_group,
                    novel_genre_groups
                FROM contents
                WHERE source = %s
                """,
                (self.source_name,),
            )
            existing_rows = [dict(row) for row in read_cursor.fetchall()]
        finally:
            read_cursor.close()

        self._prefetch_context = {"sync_snapshot": self._build_sync_snapshot(existing_rows)}
        try:
            pending_cdc_records = []
            for row in payload.get("pending_cdc_records") or []:
                if not isinstance(row, dict):
                    continue
                pending_cdc_records.append(
                    (
                        str(row.get("content_id") or "").strip(),
                        parse_iso_naive_kst(row.get("final_completed_at")),
                        row.get("resolved_by"),
                    )
                )

            return self._apply_write_phase(
                conn,
                pending_cdc_records=pending_cdc_records,
                skip_database_sync=bool(payload.get("skip_database_sync")),
                all_content_today=payload.get("all_content_today") or {},
                ongoing_today=payload.get("ongoing_today") or {},
                hiatus_today=payload.get("hiatus_today") or {},
                finished_today=payload.get("finished_today") or {},
            )
        finally:
            self._prefetch_context = {}

    async def run_daily_check(self, conn, *, verification_gate=None, write_enabled=True):
        """
        일일 데이터 점검 및 CDC 이벤트 기록 프로세스 실행.

        1) DB 스냅샷 로드 (Final State)
        2) 원격 데이터 수집(fetch_all_data)
        3) 신규 완결 감지 및 CDC 이벤트 기록 (Final-State CDC)
        4) DB 동기화(synchronize_database)

        트랜잭션 경계:
        - 성공 시 run_daily_check에서 1회 commit
        - 실패 시 rollback
        """
        read_cursor = None
        write_cursor = None
        try:
            read_cursor = get_cursor(conn)
            snapshot_state = self._load_snapshot_state(conn, read_cursor)
            self._prefetch_context = snapshot_state["prefetch_context"]

            conn.rollback()
            read_cursor.close()
            read_cursor = None

            fetch_result = await self.fetch_all_data()
            added, newly_completed_items, cdc_info, _ = await self._finalize_daily_check(
                conn=conn,
                snapshot_state=snapshot_state,
                fetch_result=fetch_result,
                verification_gate=verification_gate,
                write_enabled=write_enabled,
                allow_deferred_apply=False,
            )
            return added, newly_completed_items, cdc_info

            # 1) Load previous crawler status snapshot (raw)
            read_cursor.execute(
                """
                SELECT
                    content_id,
                    content_type,
                    title,
                    normalized_title,
                    normalized_authors,
                    status,
                    meta,
                    search_document
                FROM contents
                WHERE source = %s
                """,
                (self.source_name,),
            )
            existing_rows = [dict(row) for row in read_cursor.fetchall()]
            db_status_map = {str(row["content_id"]): row["status"] for row in existing_rows}
            db_sync_snapshot = self._build_sync_snapshot(existing_rows)

            # 2) Load overrides (overlay)
            read_cursor.execute(
                "SELECT content_id, override_status, override_completed_at "
                "FROM admin_content_overrides WHERE source = %s",
                (self.source_name,),
            )
            override_map = {str(row["content_id"]): row for row in read_cursor.fetchall()}

            # 3) Resolve previous final states
            db_state_before_sync = {}
            for content_id in set(db_status_map.keys()) | set(override_map.keys()):
                db_state_before_sync[content_id] = resolve_final_state(
                    db_status_map.get(content_id), override_map.get(content_id)
                )
            prefetch_context = self.build_prefetch_context(
                conn,
                read_cursor,
                db_status_map,
                override_map,
                db_state_before_sync,
            )
            if not isinstance(prefetch_context, dict):
                prefetch_context = {}
            prefetch_context.setdefault("sync_snapshot", db_sync_snapshot)
            self._prefetch_context = prefetch_context

            # End the read phase before any network I/O so no transaction is left open.
            conn.rollback()
            read_cursor.close()
            read_cursor = None

            # 4) Fetch today's data
            fetch_result = await self.fetch_all_data()
            if len(fetch_result) == 5:
                ongoing_today, hiatus_today, finished_today, all_content_today, fetch_meta = fetch_result
            else:
                ongoing_today, hiatus_today, finished_today, all_content_today = fetch_result
                fetch_meta = {}

            if not isinstance(fetch_meta, dict):
                fetch_meta = {"raw_meta": fetch_meta}

            def _normalize_key_map(map_data):
                return {str(k): v for k, v in map_data.items()}

            ongoing_today = _normalize_key_map(ongoing_today)
            hiatus_today = _normalize_key_map(hiatus_today)
            finished_today = _normalize_key_map(finished_today)
            all_content_today = _normalize_key_map(all_content_today)

            # 5) Build current raw status map (from today's fetch)
            current_status_map = {}
            for content_id in all_content_today.keys():
                cid = str(content_id)
                if cid in finished_today:
                    current_status_map[cid] = "완결"
                elif cid in hiatus_today:
                    current_status_map[cid] = "휴재"
                elif cid in ongoing_today:
                    current_status_map[cid] = "연재중"

            # Fill missing ids with previous known final status (stability for partial fetch)
            for content_id, previous_state in db_state_before_sync.items():
                current_status_map.setdefault(content_id, previous_state["final_status"])

            # 6) Resolve current final states
            current_final_state_map = {}
            for content_id in set(current_status_map.keys()) | set(override_map.keys()):
                current_final_state_map[content_id] = resolve_final_state(
                    current_status_map.get(content_id), override_map.get(content_id)
                )

            # 7) Final-State CDC: newly completed + record events
            newly_completed_items = []
            new_content_items = []
            cdc_events_inserted_count = 0
            cdc_events_inserted_items = []
            pending_cdc_records = []
            verification_candidates_by_id = {}

            def _record_verification_candidate(
                *,
                content_id,
                content_data,
                expected_status,
                change_kind,
                previous_status=None,
            ):
                candidate = self.build_verification_candidate(
                    content_id=content_id,
                    content_data=content_data,
                    expected_status=expected_status,
                    change_kind=change_kind,
                    previous_status=previous_status,
                )
                if candidate is None:
                    return

                existing = verification_candidates_by_id.get(candidate["content_id"])
                if existing is None:
                    verification_candidates_by_id[candidate["content_id"]] = candidate
                    return

                merged_change_kinds = set(existing.get("change_kinds") or [])
                merged_change_kinds.update(candidate.get("change_kinds") or [])
                existing["change_kinds"] = sorted(merged_change_kinds)
                if candidate.get("expected_status"):
                    existing["expected_status"] = candidate["expected_status"]
                if candidate.get("previous_status") and not existing.get("previous_status"):
                    existing["previous_status"] = candidate["previous_status"]
                if candidate.get("content_url") and not existing.get("content_url"):
                    existing["content_url"] = candidate["content_url"]
                if candidate.get("title") and existing.get("title") == existing.get("content_id"):
                    existing["title"] = candidate["title"]
                if candidate.get("source_item") and not existing.get("source_item"):
                    existing["source_item"] = candidate["source_item"]

            for content_id, content_data in all_content_today.items():
                if content_id in db_status_map:
                    continue
                current_final_state = current_final_state_map.get(content_id, {})
                candidate = self.build_verification_candidate(
                    content_id=content_id,
                    content_data=content_data,
                    expected_status=current_final_state.get("final_status") or current_status_map.get(content_id),
                    change_kind="new_content",
                    previous_status=None,
                )
                if candidate is None:
                    continue
                new_content_items.append(candidate)
                verification_candidates_by_id[candidate["content_id"]] = candidate

            for content_id, current_final_state in current_final_state_map.items():
                previous_final_state = db_state_before_sync.get(content_id, {"final_status": None})

                if previous_final_state.get("final_status") != "완결" and current_final_state["final_status"] == "완결":
                    final_completed_at = current_final_state.get("final_completed_at")
                    display_completed_at = (
                        final_completed_at.isoformat()
                        if hasattr(final_completed_at, "isoformat")
                        else final_completed_at
                    )

                    newly_completed_items.append(
                        (
                            content_id,
                            self.source_name,
                            display_completed_at,
                            current_final_state.get("resolved_by"),
                        )
                    )

                    pending_cdc_records.append(
                        (content_id, final_completed_at, current_final_state.get("resolved_by"))
                    )
                    _record_verification_candidate(
                        content_id=content_id,
                        content_data=all_content_today.get(content_id),
                        expected_status=current_final_state.get("final_status"),
                        change_kind="newly_completed",
                        previous_status=previous_final_state.get("final_status"),
                    )

            resolved_by_counts = {}
            for _, _, _, resolved_by in newly_completed_items:
                resolved_by_counts[resolved_by] = resolved_by_counts.get(resolved_by, 0) + 1

            db_count = len(db_status_map)
            fetched_count = len(all_content_today)

            if isinstance(fetch_meta, dict):
                fetched_count_override = fetch_meta.get("fetched_count")
                if isinstance(fetched_count_override, int) and fetched_count_override >= 0:
                    fetched_count = fetched_count_override

            health_db_count = db_count
            if isinstance(fetch_meta, dict):
                expected_count = fetch_meta.get("expected_count")
                health_db_override = fetch_meta.get("health_db_count")
                if isinstance(health_db_override, int) and health_db_override > 0:
                    health_db_count = health_db_override
                elif isinstance(expected_count, int) and expected_count > 0:
                    health_db_count = expected_count

            no_ratio = bool(fetch_meta.get("force_no_ratio")) if isinstance(fetch_meta, dict) else False
            ratio = None if no_ratio or health_db_count is None else fetched_count / max(health_db_count, 1)
            health_info = {
                "db_count": db_count,
                "fetched_count": fetched_count,
                "health_db_count": health_db_count,
                "fetch_ratio": ratio,
                "min_ratio_threshold": config.CRAWLER_FETCH_HEALTH_MIN_RATIO,
            }
            is_degraded_fetch = False
            skip_reason = None
            fetch_errors = fetch_meta.get("errors") if isinstance(fetch_meta, dict) else None
            if fetch_errors is not None:
                health_info["fetch_errors"] = fetch_errors
            if fetch_errors:
                is_degraded_fetch = True
                skip_reason = "fetch_errors"
            if ratio is not None and ratio < config.CRAWLER_FETCH_HEALTH_MIN_RATIO:
                is_degraded_fetch = True
                skip_reason = skip_reason or "fetch_ratio_below_threshold"

            if isinstance(fetch_meta, dict):
                if fetch_meta.get("is_suspicious_empty"):
                    health_info["is_suspicious_empty"] = True
                    if ratio is None:
                        ratio = 0.0
                        health_info["fetch_ratio"] = ratio
                notes = fetch_meta.get("health_notes")
                if isinstance(notes, list):
                    health_info["notes"] = notes
            skip_database_sync = bool(fetch_meta.get("skip_database_sync")) if isinstance(fetch_meta, dict) else False

            fetch_meta["is_degraded_fetch"] = is_degraded_fetch
            fetch_meta["fetch_health"] = health_info

            status_label = "ok"
            summary = None
            if isinstance(fetch_meta, dict):
                status_label = fetch_meta.get("status") or status_label
                summary = fetch_meta.get("summary")
            if is_degraded_fetch and status_label == "ok":
                status_label = "warn"
            if summary is None:
                summary = {
                    "crawler": self.source_name,
                    "reason": status_label,
                    "message": "crawler run completed",
                }

            cdc_info = {
                "cdc_mode": "final_state",
                "newly_completed_count": len(newly_completed_items),
                "resolved_by_counts": resolved_by_counts,
                "cdc_events_inserted_count": cdc_events_inserted_count,
                "cdc_events_inserted_items": cdc_events_inserted_items,
                "cdc_skipped": is_degraded_fetch,
                "skip_reason": skip_reason,
                "health": health_info,
                "fetch_meta": fetch_meta,
                "status": status_label,
                "summary": summary,
                "db_sync_skipped": skip_database_sync,
                "inserted_count": 0,
                "updated_count": 0,
                "unchanged_count": 0,
                "write_skipped_count": 0,
            }

            write_plan = {
                "source_name": self.source_name,
                "status": status_label,
                "summary": summary,
                "health": health_info,
                "fetch_meta": fetch_meta,
                "fetched_count": fetched_count,
                "all_content_today": all_content_today,
                "ongoing_today": ongoing_today,
                "hiatus_today": hiatus_today,
                "finished_today": finished_today,
                "new_content_items": new_content_items,
                "newly_completed_items": newly_completed_items,
                "verification_candidates": list(verification_candidates_by_id.values()),
                "snapshot_existing_rows": list((self.get_prefetch_context() or {}).get("snapshot_existing_rows") or []),
                "platform_links": list((self.get_prefetch_context() or {}).get("platform_links") or []),
                "watchlist_rows": list((self.get_prefetch_context() or {}).get("watchlist_rows") or []),
                "pending_cdc_records": [
                    {
                        "content_id": content_id,
                        "final_completed_at": final_completed_at,
                        "resolved_by": resolved_by,
                    }
                    for content_id, final_completed_at, resolved_by in pending_cdc_records
                ],
                "new_contents_count": len(new_content_items),
            }

            verification = await self.evaluate_verification_gate(verification_gate, write_plan)
            if verification is not None:
                cdc_info["verification"] = verification

            apply_allowed = bool(verification.get("apply_allowed", True)) if verification else True
            apply_result = "applied"
            if not apply_allowed:
                apply_result = "blocked"
            elif not write_enabled:
                apply_result = "dry_run"
            elif is_degraded_fetch:
                apply_result = "skipped"

            if not is_degraded_fetch and apply_allowed and write_enabled:
                # Start a fresh transaction for the write phase.
                write_cursor = get_cursor(conn)

                for content_id, final_completed_at, resolved_by in pending_cdc_records:
                    inserted = record_content_completed_event(
                        conn,
                        content_id=content_id,
                        source=self.source_name,
                        final_completed_at=final_completed_at,
                        resolved_by=resolved_by,
                    )
                    if inserted:
                        cdc_events_inserted_count += 1
                        cdc_events_inserted_items.append(content_id)

                cdc_info["cdc_events_inserted_count"] = cdc_events_inserted_count
                cdc_info["cdc_events_inserted_items"] = cdc_events_inserted_items

                # 8) DB sync (commit is enforced here, not in crawler implementations)
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
                    cdc_info.update(sync_stats)

                # 8.5) Seed default publication dates for webtoons (best effort).
                default_publication_seeded_count = 0
                default_publication_seed_error = None
                try:
                    default_publication_seeded_count = self.seed_webtoon_publication_dates(write_cursor)
                except Exception as seed_error:
                    default_publication_seed_error = str(seed_error)
                cdc_info["default_publication_seeded_count"] = default_publication_seeded_count
                if default_publication_seed_error:
                    cdc_info["default_publication_seed_error"] = default_publication_seed_error

                # 9) Single commit here (forced)
                conn.commit()
            else:
                added = 0
                cdc_info["cdc_skipped"] = True
                cdc_info["cdc_events_inserted_count"] = 0
                cdc_info["cdc_events_inserted_items"] = []
                cdc_info["default_publication_seeded_count"] = 0
                if not cdc_info.get("skip_reason"):
                    if not apply_allowed:
                        cdc_info["skip_reason"] = "verification_blocked"
                    elif not write_enabled:
                        cdc_info["skip_reason"] = "dry_run"
                if not skip_database_sync:
                    cdc_info["db_sync_skipped"] = True
                if verification and not verification.get("message"):
                    if not apply_allowed:
                        verification["message"] = "verification gate blocked database apply"
                    elif not write_enabled:
                        verification["message"] = "dry-run mode skipped database apply"

            if skip_database_sync and apply_result == "applied":
                cdc_info["db_sync_skipped"] = True
            cdc_info["apply_result"] = apply_result

            return added, newly_completed_items, cdc_info

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        finally:
            self._prefetch_context = {}
            if read_cursor:
                try:
                    read_cursor.close()
                except Exception:
                    pass
            if write_cursor:
                try:
                    write_cursor.close()
                except Exception:
                    pass
