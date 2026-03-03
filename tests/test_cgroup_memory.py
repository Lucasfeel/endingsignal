import utils.cgroup_memory as cgroup_memory


def test_read_memory_limit_bytes_prefers_v2_numeric(monkeypatch):
    values = {
        "/sys/fs/cgroup/memory.max": "536870912",
    }
    monkeypatch.setattr(cgroup_memory, "_read_text_file", lambda path: values.get(path))

    assert cgroup_memory.read_memory_limit_bytes() == 536870912


def test_read_memory_limit_bytes_treats_v2_max_as_unlimited(monkeypatch):
    values = {
        "/sys/fs/cgroup/memory.max": "max",
    }
    monkeypatch.setattr(cgroup_memory, "_read_text_file", lambda path: values.get(path))

    assert cgroup_memory.read_memory_limit_bytes() is None


def test_read_memory_limit_bytes_falls_back_to_v1_and_ignores_huge(monkeypatch):
    values = {
        "/sys/fs/cgroup/memory.max": None,
        "/sys/fs/cgroup/memory/memory.limit_in_bytes": str((1 << 60) + 99),
    }
    monkeypatch.setattr(cgroup_memory, "_read_text_file", lambda path: values.get(path))

    assert cgroup_memory.read_memory_limit_bytes() is None


def test_get_memory_snapshot_computes_ratio(monkeypatch):
    monkeypatch.setattr(cgroup_memory, "read_memory_limit_bytes", lambda: 100)
    monkeypatch.setattr(cgroup_memory, "read_memory_usage_bytes", lambda: 25)

    snapshot = cgroup_memory.get_memory_snapshot()

    assert snapshot["limit_bytes"] == 100
    assert snapshot["usage_bytes"] == 25
    assert snapshot["usage_ratio"] == 0.25


def test_read_memory_usage_bytes_falls_back_to_v1(monkeypatch):
    values = {
        "/sys/fs/cgroup/memory.current": None,
        "/sys/fs/cgroup/memory/memory.usage_in_bytes": "1234",
    }
    monkeypatch.setattr(cgroup_memory, "_read_text_file", lambda path: values.get(path))

    assert cgroup_memory.read_memory_usage_bytes() == 1234
