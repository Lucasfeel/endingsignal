from datetime import datetime

import services.admin_override_service as admin_service


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.last_result = None

    def execute(self, query, params):
        if "SELECT status FROM contents" in query:
            content_id, source = params
            status = self.db.contents.get((content_id, source))
            self.last_result = [] if status is None else [{'status': status}]
        elif "SELECT override_status, override_completed_at" in query:
            content_id, source = params
            row = self.db.overrides.get((content_id, source))
            self.last_result = [] if row is None else [row]
        elif "INSERT INTO admin_content_overrides" in query:
            content_id, source, override_status, override_completed_at, reason, admin_id = params
            key = (content_id, source)
            row = self.db.overrides.get(key, {'id': len(self.db.overrides) + 1, 'content_id': content_id, 'source': source, 'created_at': self.db.now})
            row.update({
                'override_status': override_status,
                'override_completed_at': override_completed_at,
                'reason': reason,
                'admin_id': admin_id,
                'updated_at': self.db.now,
            })
            self.db.overrides[key] = row
            self.last_result = [row]
        else:
            raise NotImplementedError(query)

    def fetchone(self):
        if not self.last_result:
            return None
        return self.last_result[0]

    def fetchall(self):
        return self.last_result or []

    def close(self):
        pass


class FakeDB:
    def __init__(self, contents, overrides=None, now=None):
        self.contents = contents
        self.overrides = overrides or {}
        self.now = now
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_scheduled_override_does_not_record_event(monkeypatch):
    now = datetime(2025, 12, 17, 12, 0, 0)
    db = FakeDB({('CID', 'SRC'): '연재중'}, now=now)

    recorded_events = []

    monkeypatch.setattr(admin_service, 'get_cursor', lambda conn: FakeCursor(conn))
    monkeypatch.setattr(
        admin_service,
        'record_content_completed_event',
        lambda *args, **kwargs: recorded_events.append(kwargs) or True,
    )

    result = admin_service.upsert_override_and_record_event(
        db,
        admin_id=1,
        content_id='CID',
        source='SRC',
        override_status='완결',
        override_completed_at=datetime(2025, 12, 30, 0, 0, 0),
        reason='scheduled completion',
        now=now,
    )

    assert result['event_recorded'] is False
    assert recorded_events == []
    assert result['new_final_state']['final_status'] == '연재중'
    assert result['new_final_state']['resolved_by'] == 'crawler'
    assert result['final_state']['is_scheduled_completion'] is True
    assert result['final_state']['scheduled_completed_at'] == datetime(
        2025, 12, 30, 0, 0, 0
    ).isoformat()
