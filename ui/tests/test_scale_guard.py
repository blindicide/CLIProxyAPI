"""The scale-test harness must refuse or abort instead of risking a host OOM (2026-09-27 incident)."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location("scale_test", Path(__file__).resolve().parents[1] / "tools" / "scale_test.py")
scale = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(scale)

PLENTY = 64_000.0


def test_default_run_is_small_and_allowed():
    est = scale.preflight(2_000, False, 500, 1500, PLENTY)
    assert est["service"] < 200 and est["total"] < 500


def test_on_host_record_cap():
    scale.preflight(scale.ON_HOST_MAX_RECORDS, False, 500, 1500, PLENTY)
    with pytest.raises(SystemExit, match="exceeds the on-host limit of 20,000"):
        scale.preflight(scale.ON_HOST_MAX_RECORDS + 1, False, 500, 1500, PLENTY)
    with pytest.raises(SystemExit, match="OOM-killed other processes"):
        scale.preflight(200_000, False, 500, 1500, PLENTY)


def test_off_host_still_respects_memory_budgets():
    with pytest.raises(SystemExit, match="per-process peak .* exceeds --rss-budget-mb 500"):
        scale.preflight(200_000, True, 500, 1500, PLENTY)
    with pytest.raises(SystemExit, match="exceeds --total-budget-mb"):
        scale.preflight(200_000, True, 20_000, 1_000, PLENTY)
    est = scale.preflight(1_000_000, True, 20_000, 32_000, PLENTY)
    assert est["service"] > 14_000  # a 1M run genuinely needs a big machine


def test_busy_host_is_refused():
    with pytest.raises(SystemExit, match="more than half of MemAvailable"):
        scale.preflight(20_000, False, 500, 1500, 800)


def test_estimate_matches_the_incident():
    # The OOM-killed run peaked at ~3.0 GB for ~200k records; the estimate must not be lower.
    assert scale.estimate_mib(200_000)["service"] >= 2_900


def test_cli_refuses_before_allocating(tmp_path):
    result = subprocess.run([sys.executable, str(Path(scale.__file__)), "--records", "200000", "--workdir", str(tmp_path / "w")],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode != 0 and "on-host limit" in result.stderr
    assert not (tmp_path / "w").exists()  # nothing was created, let alone allocated


def test_watchdog_kills_a_process_over_budget():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        dog = scale.Watchdog(rss_budget=0.01, min_available=0)
        dog.watch(child.pid, "service")
        dog.check()
        assert dog.tripped and "service RSS" in dog.tripped
        assert child.wait(timeout=10) == -9
    finally:
        if child.poll() is None:
            child.kill()


def test_watchdog_trips_on_low_host_memory():
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        dog = scale.Watchdog(rss_budget=PLENTY, min_available=10**9)
        dog.watch(child.pid, "oracle")
        dog.check()
        assert "MemAvailable" in dog.tripped
        assert child.wait(timeout=10) == -9
    finally:
        if child.poll() is None:
            child.kill()
