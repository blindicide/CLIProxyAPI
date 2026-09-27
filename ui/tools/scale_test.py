#!/usr/bin/env python3
"""Scale test: run a throwaway cproxy-ui against a large synthetic history and measure it.

Never touches production: the app code is copied into a scratch work dir, the history there
is synthetic (built from the committed redacted fixture), and the management API is a local
mock that also trickles new usage records into the queue during the test, so ingestion runs
under read load. The production service and cproxy's real usage queue are not contacted.

    venv/bin/python tools/scale_test.py --workdir /path/to/scratch            # 2,000 records (default)
    venv/bin/python tools/scale_test.py --records 20000 --workdir /path/to/scratch

MEMORY SAFETY (after a 200k run OOM-killed other processes on the shared 7.8 GB host,
2026-09-27): the whole history is held in memory by the service and by the oracle, and the
peak is far above steady state (measured ~15 KB RSS per record for the service). So:
  * at most 20,000 records unless --off-host is given;
  * before allocating anything, the per-process peak is estimated and the run is refused if
    any process would exceed --rss-budget-mb (500), the total would exceed --total-budget-mb
    (1500), or the total would exceed half of the host's MemAvailable;
  * while running, a watchdog polls every child's RSS and MemAvailable and kills the test
    as soon as a budget is crossed or MemAvailable drops below --min-available-mb (1024).
Larger runs (200k, 1M) belong on a dedicated machine with enough RAM for the estimate:
    venv/bin/python tools/scale_test.py --off-host --records 1000000 \
        --rss-budget-mb 20000 --total-budget-mb 32000 --workdir /tmp/cproxy-ui-scale

Reports: history generation + startup time, RSS, cold/warm latency per endpoint, concurrent
dashboard-poll latency (p50/p95/max), ingest throughput under load, export size, and an
independent recomputation of the totals (tools/verify_totals.py) at that scale.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

UI = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(UI))

import httpx  # noqa: E402  (from the ui venv)

from ingest import iso_utc, normalize_record, utc_now  # noqa: E402

MGMT_KEY = "scale-test-management-key"
CLIENT_KEYS = ["sk-scale-test-key-alpha-000000000000", "sk-scale-test-key-beta-1111111111111"]
# Rough shape of observed cproxy traffic: (model, weight, uncached in, cache read, cache write, out, reasoning)
PROFILE = [
    ("claude-sonnet-4-6", 55, 900, 19000, 1500, 300, 0),
    ("claude-opus-5", 15, 1500, 30000, 2500, 600, 150),
    ("claude-haiku-4-5-20251001", 15, 700, 0, 0, 60, 0),
    ("claude-sonnet-5", 8, 400, 5000, 800, 120, 0),
    ("claude-opus-5-5", 5, 2000, 40000, 3000, 900, 300),
    ("claude-3-7-sonnet-20250219", 2, 300, 0, 0, 40, 0),
]
USER_AGENTS = ["node-fetch", "claude-cli/2.1.0 (external, cli)", "OpenAI/Python 1.99.0", "curl/8.5.0"]
IPS = ["127.0.0.1", "10.0.0.5", "203.0.113.7"]


def synth_record(base: dict, rng: random.Random, i: int, ts) -> dict:
    model, _, unc, read, write, out, reasoning = rng.choices(PROFILE, weights=[p[1] for p in PROFILE])[0]
    failed = rng.random() < 0.15
    jitter = lambda v: int(v * rng.uniform(0.5, 1.5))  # noqa: E731
    unc, read, write, out, reasoning = (0, 0, 0, 0, 0) if failed else (jitter(unc), jitter(read), jitter(write), jitter(out), min(jitter(reasoning), out))
    r = copy.deepcopy(base)
    r.update(
        timestamp=ts.isoformat(), model=model, alias=model, failed=failed, stream=rng.random() < 0.5,
        latency_ms=rng.randint(200, 20000), ttft_ms=0 if failed else rng.randint(150, 4000),
        user_agent=rng.choice(USER_AGENTS), client_ip=rng.choice(IPS), resolved_client_ip=None,
        api_key=rng.choice(CLIENT_KEYS), execution_id=f"scale-{i:08d}", request_id=f"{i:08x}", trace_id=f"{i:08x}",
        session_id=f"session-{i // 20:07d}", fail={"status_code": 400 if failed else 200, "body": "invalid_request_error" if failed else ""},
        token_breakdown={"schema_version": 2, "quality": "complete", "total_tokens": unc + read + write + out,
                         "input": {"total_tokens": unc + read + write, "uncached_tokens": unc, "cache_read_tokens": read, "cache_write_tokens": write},
                         "output": {"total_tokens": out, "non_reasoning_tokens": out - reasoning, "reasoning_tokens": reasoning}, "unclassified_tokens": 0},
        tokens={"input_tokens": unc, "output_tokens": out, "reasoning_tokens": reasoning, "cached_tokens": read, "cache_read_tokens": read,
                "cache_read_tokens_present": True, "cache_creation_tokens": write, "total_tokens": unc + read + write + out},
    )
    r["resolved_client_ip"] = r["client_ip"]
    return r


def build_history(path: Path, base: dict, n: int, days: int, seed: int) -> None:
    rng = random.Random(seed)
    now = utc_now()
    offsets = sorted(rng.uniform(0, days * 86400) for _ in range(n))[::-1]  # oldest first
    names = {}
    from ingest import key_hash
    for index, key in enumerate(CLIENT_KEYS, start=1):
        names[key_hash(key)] = f"key-{index}"
    with path.open("w", encoding="utf-8") as handle:
        for i, offset in enumerate(offsets):
            ts = now - timedelta(seconds=offset)
            record = normalize_record(synth_record(base, rng, i, ts), names, ingested_at=ts + timedelta(seconds=2))
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


class MockManagement:
    """Tiny management API: usage queue fed by a producer thread, plus static endpoints."""

    def __init__(self, base: dict, rps: float, seed: int) -> None:
        self.queue: deque = deque()
        self.lock = threading.Lock()
        self.produced = self.popped = 0
        self.base, self.rps, self.rng = base, rps, random.Random(seed + 1)
        self.stop = threading.Event()

    def producer(self) -> None:
        i = 10_000_000
        while not self.stop.wait(1 / self.rps if self.rps else 3600):
            with self.lock:
                self.queue.append(synth_record(self.base, self.rng, i, utc_now()))
                self.produced += 1
            i += 1

    def handler(self):
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _json(self, body, status=200):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                if self.headers.get("Authorization") != f"Bearer {MGMT_KEY}":
                    return self._json({"error": "unauthorized"}, 401)
                path = self.path.split("?")[0].removeprefix("/v0/management/")
                if path == "usage-queue":
                    count = int((self.path.split("count=") + ["1"])[1].split("&")[0])
                    with mock.lock:
                        items = [mock.queue.popleft() for _ in range(min(count, len(mock.queue)))]
                        mock.popped += len(items)
                    return self._json(items)
                static = {
                    "api-keys": {"api-keys": CLIENT_KEYS},
                    "config": {"redis-usage-queue-retention-seconds": 60},
                    "auth-files": {"files": [{"id": "scale.json", "label": "scale-test", "provider": "claude", "failed": 0, "cooldowns": [], "disabled": False}]},
                    "api-key-usage": {},
                    "quota/providers": {"providers": []},
                    "model-definitions/claude": {"models": [{"id": p[0]} for p in PROFILE]},
                }
                return self._json(static.get(path, {"error": "not found"}), 200 if path in static else 404)

        return Handler


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def rss_mib(pid: int) -> float:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024
    return float("nan")


ON_HOST_MAX_RECORDS = 20_000
# Measured peaks (RSS per record, plus a fixed base) from the 2026-09-27 runs on this host.
SERVICE_BYTES_PER_RECORD = 15_000
SERVICE_BASE_MIB = 80
ORACLE_BYTES_PER_RECORD = 9_000
ORACLE_BASE_MIB = 60  # measured 62 MiB at 2k records
HARNESS_BASE_MIB = 60


def mem_available_mib() -> float:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024
    return 0.0


def estimate_mib(records: int) -> dict:
    service = SERVICE_BASE_MIB + records * SERVICE_BYTES_PER_RECORD / 2**20
    oracle = ORACLE_BASE_MIB + records * ORACLE_BYTES_PER_RECORD / 2**20
    return {"service": round(service), "oracle": round(oracle), "harness": HARNESS_BASE_MIB, "total": round(service + oracle + HARNESS_BASE_MIB)}


def preflight(records: int, off_host: bool, rss_budget: float, total_budget: float, available: float) -> dict:
    """Refuse (SystemExit with the reason) before anything is allocated."""
    if records <= 0:
        raise SystemExit("refusing: --records must be positive")
    if records > ON_HOST_MAX_RECORDS and not off_host:
        raise SystemExit(
            f"refusing: {records:,} records exceeds the on-host limit of {ON_HOST_MAX_RECORDS:,}. The history is held in memory "
            f"(~{estimate_mib(records)['total']:,} MiB estimated); a 200k run OOM-killed other processes on this shared host. "
            "Run larger tests on a dedicated machine with --off-host (see the module docstring)."
        )
    est = estimate_mib(records)
    biggest = max(est["service"], est["oracle"])
    if biggest > rss_budget:
        raise SystemExit(f"refusing: estimated per-process peak {biggest} MiB exceeds --rss-budget-mb {rss_budget:.0f} (estimate {est})")
    if est["total"] > total_budget:
        raise SystemExit(f"refusing: estimated total {est['total']} MiB exceeds --total-budget-mb {total_budget:.0f} (estimate {est})")
    if est["total"] > available / 2:
        raise SystemExit(f"refusing: estimated total {est['total']} MiB is more than half of MemAvailable ({available:.0f} MiB); the host is too busy")
    return est


class Watchdog(threading.Thread):
    """Kills watched processes if any exceeds the per-process budget or the host runs low."""

    def __init__(self, rss_budget: float, min_available: float, interval: float = 0.5) -> None:
        super().__init__(daemon=True)
        self.rss_budget, self.min_available, self.interval = rss_budget, min_available, interval
        self.pids: dict[int, str] = {}
        self.peaks: dict[str, float] = {}
        self.tripped: str | None = None
        self.done = threading.Event()

    def watch(self, pid: int, name: str) -> None:
        self.pids[pid] = name

    def check(self) -> None:
        for pid, name in list(self.pids.items()):
            try:
                rss = rss_mib(pid)
            except (FileNotFoundError, ProcessLookupError):
                self.pids.pop(pid, None)
                continue
            self.peaks[name] = max(self.peaks.get(name, 0.0), rss)
            if rss > self.rss_budget and not self.tripped:
                self.tripped = f"{name} RSS {rss:.0f} MiB exceeded the {self.rss_budget:.0f} MiB budget"
        available = mem_available_mib()
        if available < self.min_available and not self.tripped:
            self.tripped = f"host MemAvailable fell to {available:.0f} MiB (< {self.min_available:.0f})"
        if self.tripped:
            # Children first, this process last, so nothing is orphaned holding memory.
            for pid in sorted(self.pids, key=lambda p: p == os.getpid()):
                try:
                    os.kill(pid, 9)
                except ProcessLookupError:
                    pass

    def run(self) -> None:
        while not self.done.wait(self.interval):
            self.check()


def pct(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, round(p / 100 * len(ordered)) - 1))] if ordered else None


async def timed(client, path):
    start = time.perf_counter()
    response = await client.get(path)
    return (time.perf_counter() - start) * 1000, response


async def measure(base_url: str, viewers: int, duration: float) -> dict:
    out: dict = {}
    async with httpx.AsyncClient(base_url=base_url, timeout=120) as client:
        for window in ("24h", "7d", "30d", "all"):
            cold, response = await timed(client, f"/api/analytics?window={window}")
            warm, _ = await timed(client, f"/api/analytics?window={window}")
            out[f"analytics_{window}_ms"] = {"cold": round(cold, 1), "warm_cached": round(warm, 1), "requests": response.json()["summary"]["requests"]}
        for path in ("/api/requests?limit=100", "/api/quota", "/api/health", "/"):
            ms, response = await timed(client, path)
            out[f"{path}_ms"] = round(ms, 1)
        ms, response = await timed(client, "/api/export.csv?window=all")
        out["export_all"] = {"ms": round(ms, 1), "bytes": len(response.content), "rows": response.text.count("\n") - 1}

        # Concurrent viewers, each doing the dashboard's 30 s poll back to back (worst case).
        latencies: list[float] = []
        stop_at = time.perf_counter() + duration

        async def viewer(index: int) -> None:
            windows = ("24h", "7d", "30d", "all")
            while time.perf_counter() < stop_at:
                for path in (f"/api/analytics?window={windows[index % 4]}", "/api/requests?limit=100", "/api/quota"):
                    ms, response = await timed(client, path)
                    response.raise_for_status()
                    latencies.append(ms)

        await asyncio.gather(*(viewer(i) for i in range(viewers)))
        out["concurrent"] = {"viewers": viewers, "duration_s": duration, "requests": len(latencies),
                             "p50_ms": round(statistics.median(latencies), 1), "p95_ms": round(pct(latencies, 95), 1), "max_ms": round(max(latencies), 1)}
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--records", type=int, default=2_000, help=f"history size (max {ON_HOST_MAX_RECORDS:,} unless --off-host)")
    parser.add_argument("--off-host", action="store_true", help="allow more than the on-host limit (dedicated machine only)")
    parser.add_argument("--rss-budget-mb", type=float, default=500, help="max RSS of any single process")
    parser.add_argument("--total-budget-mb", type=float, default=1500, help="max estimated RSS of all processes together")
    parser.add_argument("--min-available-mb", type=float, default=1024, help="abort if host MemAvailable drops below this")
    parser.add_argument("--days", type=int, default=90, help="history span")
    parser.add_argument("--viewers", type=int, default=10, help="concurrent dashboard viewers")
    parser.add_argument("--duration", type=float, default=15.0, help="seconds of concurrent load")
    parser.add_argument("--ingest-rps", type=float, default=5.0, help="new usage records per second during the test")
    parser.add_argument("--workdir", required=True, help="scratch directory (created; never the live ui/)")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    available = mem_available_mib()
    estimate = preflight(args.records, args.off_host, args.rss_budget_mb, args.total_budget_mb, available)
    print(f"preflight ok: {args.records:,} records, estimated peak {estimate} MiB, MemAvailable {available:.0f} MiB", file=sys.stderr)

    work = Path(args.workdir).resolve()
    if work == UI or UI in work.parents:
        raise SystemExit("refusing: workdir must be outside the live ui/ directory")
    if work.exists():
        shutil.rmtree(work / "app", ignore_errors=True)
    app_dir = work / "app"
    app_dir.mkdir(parents=True)
    for name in ("app.py", "ingest.py", "pricing.py", "analytics.py", "export.py", "version.py", "dashboard.html"):
        shutil.copy2(UI / name, app_dir / name)
    base = json.loads((UI / "tests" / "fixtures" / "usage_queue_sample.redacted.json").read_text())[0]

    result: dict = {"records": args.records, "days": args.days, "estimate_mib": estimate, "mem_available_mib_at_start": round(available)}
    watchdog = Watchdog(args.rss_budget_mb, args.min_available_mb)
    watchdog.watch(os.getpid(), "harness")
    watchdog.start()
    t0 = time.perf_counter()
    build_history(app_dir / "requests.jsonl", base, args.records, args.days, args.seed)
    result["generate_s"] = round(time.perf_counter() - t0, 1)
    result["file_mib"] = round((app_dir / "requests.jsonl").stat().st_size / 2**20, 1)

    mock = MockManagement(base, args.ingest_rps, args.seed)
    server = ThreadingHTTPServer(("127.0.0.1", free_port()), mock.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = free_port()
    env = {**os.environ, "CPROXY_MANAGEMENT_URL": f"http://127.0.0.1:{server.server_address[1]}/v0/management", "CPROXY_MANAGEMENT_KEY": MGMT_KEY, "PYTHONDONTWRITEBYTECODE": "1"}
    uvicorn = Path(sys.executable).with_name("uvicorn")
    t0 = time.perf_counter()
    proc = subprocess.Popen([str(uvicorn), "app:app", "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"], cwd=app_dir, env=env)
    watchdog.watch(proc.pid, "service")
    base_url = f"http://127.0.0.1:{port}"
    try:
        while True:
            try:
                if httpx.get(f"{base_url}/api/health", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if watchdog.tripped:
                raise SystemExit(f"aborted by memory watchdog: {watchdog.tripped}")
            if proc.poll() is not None or time.perf_counter() - t0 > 600:
                raise SystemExit("instance did not start")
            time.sleep(0.2)
        result["startup_s"] = round(time.perf_counter() - t0, 1)
        result["rss_mib_after_load"] = round(rss_mib(proc.pid), 1)
        producer = threading.Thread(target=mock.producer, daemon=True)
        producer.start()
        try:
            result.update(asyncio.run(measure(base_url, args.viewers, args.duration)))
        except Exception:
            if watchdog.tripped:
                raise SystemExit(f"aborted by memory watchdog: {watchdog.tripped}")
            raise
        result["rss_mib_after_queries"] = round(rss_mib(proc.pid), 1)
        mock.stop.set()
        time.sleep(3)  # let the poller drain the last produced records
        health = httpx.get(f"{base_url}/api/health", timeout=30).json()
        result["ingest_under_load"] = {"produced": mock.produced, "popped": mock.popped, "left_in_queue": len(mock.queue),
                                       "stored": health["ingest"]["records"] - args.records, "status": health["status"]}
        if watchdog.tripped:
            raise SystemExit(f"aborted by memory watchdog: {watchdog.tripped}")
        oracle = subprocess.Popen([sys.executable, str(UI / "tools" / "verify_totals.py"), "--url", base_url, "--file", str(app_dir / "requests.jsonl"), "--json"],
                                  stdout=subprocess.PIPE, text=True)
        watchdog.watch(oracle.pid, "oracle")
        stdout, _ = oracle.communicate(timeout=1800)
        if watchdog.tripped:
            raise SystemExit(f"aborted by memory watchdog: {watchdog.tripped}")
        report = json.loads(stdout)
        result["independent_totals"] = {"ok": report["ok"], "windows": {w["window"]: w["match"] for w in report["windows"]}}
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=60)
        server.shutdown()
        watchdog.done.set()
        result["peak_rss_mib"] = {name: round(v) for name, v in watchdog.peaks.items()}

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        for key, value in result.items():
            print(f"{key:28} {value}")
    ok = result["independent_totals"]["ok"] and result["ingest_under_load"]["left_in_queue"] == 0 and result["ingest_under_load"]["stored"] == result["ingest_under_load"]["produced"]
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
