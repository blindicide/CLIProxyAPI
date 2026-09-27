"""Service memory against its cgroup limit (early warning before MemoryHigh/MemoryMax bite)."""
from __future__ import annotations

from pathlib import Path

from app import memory_status


def _fake(tmp_path: Path, rss_kb: int, high: str, maximum: str) -> tuple[Path, Path]:
    proc, cgroup = tmp_path / "proc", tmp_path / "cgroup"
    (proc / "self").mkdir(parents=True)
    (proc / "self" / "status").write_text(f"Name:\tuvicorn\nVmRSS:\t{rss_kb} kB\n")
    (proc / "self" / "cgroup").write_text("0::/system.slice/cproxy-ui.service\n")
    group = cgroup / "system.slice" / "cproxy-ui.service"
    group.mkdir(parents=True)
    (group / "memory.high").write_text(high + "\n")
    (group / "memory.max").write_text(maximum + "\n")
    return proc, cgroup


def test_under_threshold_no_warning(tmp_path):
    status = memory_status(*_fake(tmp_path, 100 * 1024, str(600 * 2**20), str(900 * 2**20)))
    assert status["limit_kind"] == "memory.high" and status["limit_bytes"] == 600 * 2**20
    assert status["used_fraction"] == round(100 / 600, 3) and status["warning"] is None


def test_warns_at_seventy_percent(tmp_path):
    status = memory_status(*_fake(tmp_path, 450 * 1024, str(600 * 2**20), str(900 * 2**20)))
    assert status["used_fraction"] == 0.75
    assert "75% of its memory.high limit (450 of 600 MiB)" in status["warning"]
    assert "archive" in status["warning"]


def test_falls_back_to_memory_max_and_handles_no_limit(tmp_path):
    status = memory_status(*_fake(tmp_path / "a", 100 * 1024, "max", str(200 * 2**20)))
    assert status["limit_kind"] == "memory.max" and status["used_fraction"] == 0.5
    status = memory_status(*_fake(tmp_path / "b", 100 * 1024, "max", "max"))
    assert status["limit_bytes"] is None and status["used_fraction"] is None and status["warning"] is None


def test_unreadable_proc_is_not_fatal(tmp_path):
    status = memory_status(tmp_path / "nope", tmp_path / "nope")
    assert status == {"rss_bytes": None, "limit_bytes": None, "limit_kind": None, "used_fraction": None, "warning": None}


async def test_health_carries_memory_status(make_app, api_client):
    async with api_client(make_app()) as client:
        memory = (await client.get("/api/health")).json()["ingest"]["storage"]["memory"]
    assert memory["rss_bytes"] > 0 and set(memory) == {"rss_bytes", "limit_bytes", "limit_kind", "used_fraction", "warning"}
