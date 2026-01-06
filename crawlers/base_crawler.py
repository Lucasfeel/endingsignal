#crawlers/base_crawler.py
from abc import ABC, abstractmethod

from database import get_cursor
from services.cdc_event_service import record_content_completed_event
from services.final_state_resolver import resolve_final_state
import config


class ContentCrawler(ABC):
    """
    모든 콘텐츠 크롤러를 위한 추상 기본 클래스입니다.
    각 크롤러는 이 클래스를 상속받아 특정 콘텐츠 소스에 대한
    데이터 수집, 동기화, 점검 로직을 구현해야 합니다.
    """

    def __init__(self, source_name):
        self.source_name = source_name

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

    async def run_daily_check(self, conn):
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
        cursor = None
        try:
            cursor = get_cursor(conn)

            # 1) Load previous crawler status snapshot (raw)
            cursor.execute("SELECT content_id, status FROM contents WHERE source = %s", (self.source_name,))
            db_status_map = {str(row["content_id"]): row["status"] for row in cursor.fetchall()}

            # 2) Load overrides (overlay)
            cursor.execute(
                "SELECT content_id, override_status, override_completed_at "
                "FROM admin_content_overrides WHERE source = %s",
                (self.source_name,),
            )
            override_map = {str(row["content_id"]): row for row in cursor.fetchall()}

            # 3) Resolve previous final states
            db_state_before_sync = {}
            for content_id in set(db_status_map.keys()) | set(override_map.keys()):
                db_state_before_sync[content_id] = resolve_final_state(
                    db_status_map.get(content_id), override_map.get(content_id)
                )

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
            cdc_events_inserted_count = 0
            cdc_events_inserted_items = []
            pending_cdc_records = []

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

            if not is_degraded_fetch:
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
            }

            # 8) DB sync (commit is enforced here, not in crawler implementations)
            added = self.synchronize_database(conn, all_content_today, ongoing_today, hiatus_today, finished_today)

            # 9) Single commit here (forced)
            conn.commit()

            return added, newly_completed_items, cdc_info

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
