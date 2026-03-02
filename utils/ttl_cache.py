import threading
import time
from collections import OrderedDict


class TTLCache:
    def __init__(self, max_entries=500):
        self.max_entries = max(1, int(max_entries))
        self._lock = threading.RLock()
        self._entries = OrderedDict()

    def get(self, key):
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at <= now:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return value

    def set(self, key, value, ttl_seconds):
        ttl = float(ttl_seconds)
        if ttl <= 0:
            return
        expires_at = time.monotonic() + ttl
        with self._lock:
            self._entries[key] = (expires_at, value)
            self._entries.move_to_end(key)
            self._prune_expired_locked()
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def clear(self):
        with self._lock:
            self._entries.clear()

    def _prune_expired_locked(self):
        now = time.monotonic()
        expired_keys = [
            key
            for key, (expires_at, _value) in self._entries.items()
            if expires_at <= now
        ]
        for key in expired_keys:
            self._entries.pop(key, None)
