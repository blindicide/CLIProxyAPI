#!/usr/bin/env python3
"""Chaos / recovery drills for cproxy-ui, with proof instead of claims.

Every drill runs against a throwaway copy of the app in --workdir, with a local mock of the
cproxy management API (it never contacts production or cproxy's real usage queue), injects a
real fault - SIGKILL, ENOSPC, EROFS, clock jumps, a second instance - and then asserts:

  (A) conservation   datastore.audit: live U archives == total_ingested
  (B) no duplicates  no record id appears twice in the live file
  (C) loss bound     (ingest drills) every record the mock queue handed out is durable,
                     except records from a pop that was in flight when a SIGKILL landed -
                     cproxy's queue pops destructively with no ack, so that window cannot be
                     closed, only bounded; drills without kills must lose nothing at all

    venv/bin/python tools/chaos_drill.py --workdir /path/to/scratch            # all drills
    venv/bin/python tools/chaos_drill.py --workdir ... --only kill_ingest disk_full

Disk-full and read-only drills mount a small tmpfs under --workdir (needs passwordless sudo
for mount/umount; skipped with a note otherwise). Memory stays small: a few hundred records
per drill, one service process (~60 MiB) at a time. Exit 0 only if every drill passes.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

UI = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(UI))
sys.path.insert(0, str(UI / "tools"))

import httpx  # noqa: E402  (ui venv)

import datastore  # noqa: E402
from scale_test import build_history, synth_record  # noqa: E402

MGMT_KEY = "chaos-drill-management-key"
APP_FILES = ("app.py", "ingest.py", "pricing.py", "analytics.py", "export.py", "version.py", "archiver.py", "lifetime.py", "dashboard.html")
INFLIGHT_SECONDS = 3.0  # a pop counts as "in flight" at a kill if it happened this recently


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class MockQueue:
    """Management API mock that logs every popped record id with its pop time."""

    def __init__(self, seed: int = 7) -> None:
        self.base = json.loads((UI / "tests" / "fixtures" / "usage_queue_sample.redacted.json").read_text())[0]
        self.rng = random.Random(seed)
        self.queue: deque = deque()
        self.lock = threading.Lock()
        self.popped: dict[str, float] = {}
        self.produced = 0
        self.counter = 50_000_000
        self.rps = 0.0
        self.stop = threading.Event()
        self.server = ThreadingHTTPServer(("127.0.0.1", free_port()), self._handler())
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        threading.Thread(target=self._produce, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}/v0/management"

    def _produce(self) -> None:
        while not self.stop.is_set():
            if self.rps <= 0:
                time.sleep(0.05)
                continue
            from ingest import utc_now

            with self.lock:
                self.counter += 1
                self.queue.append(synth_record(self.base, self.rng, self.counter, utc_now()))
                self.produced += 1
            time.sleep(1 / self.rps)

    def pending(self) -> int:
        with self.lock:
            return len(self.queue)

    def close(self) -> None:
        self.stop.set()
        self.server.shutdown()

    def _handler(self):
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
                    now = time.time()
                    with mock.lock:
                        items = [mock.queue.popleft() for _ in range(min(count, len(mock.queue)))]
                        for item in items:
                            mock.popped[f"{item['execution_id']}:{item['request_id']}"] = now
                    return self._json(items)
                static = {"api-keys": {"api-keys": ["sk-chaos-a"]}, "config": {"redis-usage-queue-retention-seconds": 60},
                          "auth-files": {"files": []}, "api-key-usage": {}, "quota/providers": {"providers": []},
                          "model-definitions/claude": {"models": []}}
                return self._json(static.get(path, {}), 200 if path in static else 404)

        return Handler


class Workspace:
    def __init__(self, root: Path, name: str) -> None:
        self.dir = root / name
        if self.dir.exists():
            shutil.rmtree(self.dir)
        self.dir.mkdir(parents=True)
        self.copy_app(self.dir)

    @staticmethod
    def copy_app(target: Path) -> None:
        for name in APP_FILES:
            shutil.copy2(UI / name, target / name)
        (target / "tools").mkdir(exist_ok=True)
        shutil.copy2(UI / "tools" / "datastore.py", target / "tools" / "datastore.py")
        (target / "data").mkdir(exist_ok=True)
        (target / "requests.jsonl").touch()


class Service:
    """The real app under uvicorn, as a separate process that can really be SIGKILLed."""

    def __init__(self, app_dir: Path, mock: MockQueue, log: Path) -> None:
        self.port = free_port()
        env = {**os.environ, "CPROXY_MANAGEMENT_URL": mock.url, "CPROXY_MANAGEMENT_KEY": MGMT_KEY, "PYTHONDONTWRITEBYTECODE": "1"}
        uvicorn = Path(sys.executable).with_name("uvicorn")
        self.log = open(log, "ab")
        self.proc = subprocess.Popen([str(uvicorn), "app:app", "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning"],
                                     cwd=app_dir, env=env, stdout=self.log, stderr=self.log)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def health(self, strict: bool = False) -> tuple[int, dict]:
        response = httpx.get(f"{self.url}/api/health" + ("?strict=1" if strict else ""), timeout=5)
        return response.status_code, response.json()

    def wait_ready(self, timeout: float = 30) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                return False
            try:
                if self.health()[0] == 200:
                    return True
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        return False

    def kill(self) -> float:
        at = time.time()
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=30)
        self.log.close()
        return at

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=60)
        self.log.close()


def drain_and_stop(app_dir: Path, mock: MockQueue, log: Path) -> dict:
    """Start the service, let it drain everything, stop it gracefully; return final health."""
    mock.rps = 0
    service = Service(app_dir, mock, log)
    if not service.wait_ready():
        service.stop()
        raise RuntimeError(f"service did not start; see {log}")
    deadline = time.time() + 30
    while mock.pending() and time.time() < deadline:
        time.sleep(0.2)
    time.sleep(2.5)  # one more poll cycle
    health = service.health()[1]
    service.stop()
    return health


def invariants(app_dir: Path) -> dict:
    audit = datastore.audit(app_dir)
    live = datastore.record_ids((app_dir / "requests.jsonl").read_bytes())
    return {
        "conservation": audit["ok"] and audit["unique_records"] == audit["total_ingested"],
        "no_duplicates": len(live) == len(set(live)),
        "audit": audit,
    }


def durable_ids(app_dir: Path) -> set:
    ids = set(datastore.record_ids((app_dir / "requests.jsonl").read_bytes()))
    import gzip

    for path in datastore.archive_files(app_dir):
        with gzip.open(path, "rb") as handle:
            ids |= datastore.scan(handle)[1]
    return ids


def loss_report(mock: MockQueue, app_dir: Path, kills: list[float]) -> dict:
    on_disk = durable_ids(app_dir)
    missing = {rid: t for rid, t in mock.popped.items() if rid not in on_disk}
    unexplained = [rid for rid, t in missing.items() if not any(0 <= k - t <= INFLIGHT_SECONDS for k in kills)]
    return {"popped": len(mock.popped), "durable": len(set(mock.popped) & on_disk), "lost_in_flight": len(missing),
            "unexplained_losses": len(unexplained), "kills": len(kills)}


# --------------------------------------------------------------------------- drills


def drill_kill_ingest(root: Path, rounds: int = 12) -> dict:
    ws = Workspace(root, "kill_ingest")
    mock = MockQueue()
    rng = random.Random(3)
    kills = []
    try:
        mock.rps = 40
        for _ in range(rounds):
            service = Service(ws.dir, mock, ws.dir / "service.log")
            if not service.wait_ready():
                service.stop()
                return {"ok": False, "error": "service did not come back after a SIGKILL", "log": str(ws.dir / "service.log")}
            time.sleep(rng.uniform(0.3, 2.5))
            kills.append(service.kill())
        final = drain_and_stop(ws.dir, mock, ws.dir / "service.log")
        inv = invariants(ws.dir)
        loss = loss_report(mock, ws.dir, kills)
        ok = inv["conservation"] and inv["no_duplicates"] and loss["unexplained_losses"] == 0 and loss["durable"] > 0
        return {"ok": ok, **inv, "loss": loss, "corrupt_lines_after": final["ingest"]["corrupt_lines"], "final_status": final["status"]}
    finally:
        mock.close()


def drill_kill_before_state_save(root: Path) -> dict:
    """SIGKILL after records were fsync'd but before the state file was saved: the counter lags
    the disk until the next start reconciles it."""
    ws = Workspace(root, "kill_before_state_save")
    mock = MockQueue()
    try:
        mock.rps = 50
        time.sleep(1.0)
        code = _driver(ws.dir, f"""
        import ingest
        ingest.RecordStore.save_state = lambda self: die()
        import app as appmod
        a = appmod.create_app(management_url={mock.url!r}, management_key={MGMT_KEY!r}, data_dir=APP_DIR, start_poller=False)
        asyncio.run(a.state.drain_once(a))
        sys.exit(3)
        """)
        mock.rps = 0
        on_disk_before_restart = len(datastore.record_ids((ws.dir / "requests.jsonl").read_bytes()))
        state_path = ws.dir / "data" / "ingest_state.json"
        counted_before = json.loads(state_path.read_text()).get("total_ingested", 0) if state_path.exists() else 0
        drain_and_stop(ws.dir, mock, ws.dir / "service.log")
        inv = invariants(ws.dir)
        loss = loss_report(mock, ws.dir, kills=[])
        ok = code == -signal.SIGKILL and on_disk_before_restart > counted_before and inv["conservation"] and inv["no_duplicates"] and loss["lost_in_flight"] == 0
        return {"ok": ok, "killed": code == -signal.SIGKILL, "on_disk_at_kill": on_disk_before_restart,
                "counted_at_kill": counted_before, **inv, "loss": loss}
    finally:
        mock.close()


def drill_kill_after_pop(root: Path) -> dict:
    """SIGKILL after cproxy's queue handed out a batch but before it was written: the one
    unavoidable loss window (the queue has no ack). Proves the loss is exactly that batch,
    bounded by the pop size, and that nothing else is affected."""
    ws = Workspace(root, "kill_after_pop")
    mock = MockQueue()
    try:
        mock.rps = 50
        time.sleep(1.0)
        code = _driver(ws.dir, f"""
        import ingest
        ingest.RecordStore.append = lambda self, records: die()
        import app as appmod
        a = appmod.create_app(management_url={mock.url!r}, management_key={MGMT_KEY!r}, data_dir=APP_DIR, start_poller=False)
        asyncio.run(a.state.drain_once(a))
        sys.exit(3)
        """)
        kill_at = time.time()
        drain_and_stop(ws.dir, mock, ws.dir / "service.log")
        inv = invariants(ws.dir)
        loss = loss_report(mock, ws.dir, kills=[kill_at])
        import ingest

        ok = (code == -signal.SIGKILL and inv["conservation"] and inv["no_duplicates"] and loss["unexplained_losses"] == 0
              and 0 < loss["lost_in_flight"] <= ingest.POP_BATCH)
        return {"ok": ok, "killed": code == -signal.SIGKILL, "pop_batch": ingest.POP_BATCH, **inv, "loss": loss,
                "note": "the lost records are exactly the batch in flight; cproxy's queue pops without ack"}
    finally:
        mock.close()


def _history(ws: Workspace, n: int, days: int) -> None:
    base = json.loads((UI / "tests" / "fixtures" / "usage_queue_sample.redacted.json").read_text())[0]
    build_history(ws.dir / "requests.jsonl", base, n, days, 11)
    (ws.dir / "data" / "ingest_state.json").write_text(json.dumps({"total_ingested": n}))


def _driver(app_dir: Path, body: str) -> int:
    """Run python code in a fresh process with the workspace app importable; returns the exit code."""
    code = textwrap.dedent(f"""
        import asyncio, os, signal, sys
        from pathlib import Path
        sys.path.insert(0, {str(app_dir)!r})
        sys.path.insert(0, {str(app_dir / "tools")!r})
        os.chdir({str(app_dir)!r})
        import httpx
        APP_DIR = Path({str(app_dir)!r})
        def die():
            os.kill(os.getpid(), signal.SIGKILL)
    """) + textwrap.dedent(body)
    return subprocess.run([sys.executable, "-c", code], env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, timeout=300).returncode


ARCHIVE_KILLS = {
    "after_prepare": """
        import archiver
        archiver.commit = lambda ui, plan: die()
    """,
    "mid_copy": """
        import archiver
        def half(source, live):
            data = source.read_bytes()
            with open(live, "r+b") as dst:
                dst.write(data[: len(data) // 2]); dst.flush(); os.fsync(dst.fileno())
            die()
        archiver._copy_in_place = half
    """,
    "after_commit": """
        import archiver
        real = archiver.commit
        def commit_then_die(ui, plan):
            real(ui, plan); die()
        archiver.commit = commit_then_die
    """,
}


def drill_kill_archive(root: Path) -> dict:
    results = {}
    mock = MockQueue()
    try:
        for point, patch in ARCHIVE_KILLS.items():
            ws = Workspace(root, f"kill_archive_{point}")
            _history(ws, 600, 400)
            code = _driver(ws.dir, patch + """
        import app as appmod
        a = appmod.create_app(management_key="k", client=httpx.AsyncClient(), data_dir=APP_DIR, start_poller=False)
        asyncio.run(a.state.archive_once(a))
        sys.exit(3)  # not reached if the kill landed
            """)
            final = drain_and_stop(ws.dir, mock, ws.dir / "service.log")
            inv = invariants(ws.dir)
            leftovers = [p.name for p in (ws.dir / "data").iterdir() if p.name.startswith("requests.jsonl.rewrite")]
            ok = code == -signal.SIGKILL and inv["conservation"] and inv["no_duplicates"] and not leftovers and final["ingest"]["corrupt_lines"] == 0
            results[point] = {"ok": ok, "killed": code == -signal.SIGKILL, **inv, "journal_leftovers": leftovers,
                              "corrupt_lines_after": final["ingest"]["corrupt_lines"]}
    finally:
        mock.close()
    return {"ok": all(r["ok"] for r in results.values()), "points": results}


RESTORE_KILLS = {
    "mid_tmp_write": """
        import datastore, shutil
        def partial(src, dst, length=0):
            dst.write(src.read(1000)); dst.flush(); die()
        datastore.shutil.copyfileobj = partial
    """,
    "before_replace": """
        import datastore
        datastore.os.replace = lambda a, b: die()
    """,
}


def drill_kill_restore(root: Path) -> dict:
    results = {}
    for point, patch in RESTORE_KILLS.items():
        try:
            results[point] = _kill_restore_point(root, point, patch)
        except Exception as exc:  # one broken point must not hide the others
            results[point] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": all(r["ok"] for r in results.values()), "points": results}


def _kill_restore_point(root: Path, point: str, patch: str) -> dict:
    if True:
        ws = Workspace(root, f"kill_restore_{point}")
        _history(ws, 400, 20)
        backup = datastore.backup(ws.dir)["backup"]
        live = ws.dir / "requests.jsonl"
        lines = live.read_bytes().splitlines(keepends=True)
        live.write_bytes(b"".join(lines[:-100]))  # the live file lost 100 records
        before = live.read_bytes()
        code = _driver(ws.dir, patch + f"""
        datastore.restore(APP_DIR, Path({backup!r}), check_service=False)
        sys.exit(3)  # not reached if the kill landed
        """)
        untouched = live.read_bytes() == before
        # A clean restore afterwards completes the job.
        result = datastore.restore(ws.dir, Path(backup), check_service=False)
        inv = invariants(ws.dir)
        ok = code == -signal.SIGKILL and untouched and result["records_after"] == 400 and inv["conservation"] and inv["no_duplicates"]
        return {"ok": ok, "killed": code == -signal.SIGKILL, "live_untouched_by_killed_restore": untouched,
                "records_after_clean_restore": result["records_after"], **inv}


def _sudo(*args: str) -> bool:
    return subprocess.run(["sudo", "-n", *args], capture_output=True).returncode == 0


def _fs_drill(root: Path, name: str, break_fs, repair_fs, size: str) -> dict:
    mnt = root / name
    if mnt.exists():
        shutil.rmtree(mnt)
    mnt.mkdir(parents=True)
    if not _sudo("mount", "-t", "tmpfs", "-o", f"size={size},uid={os.getuid()},gid={os.getgid()},mode=700", "tmpfs", str(mnt)):
        return {"ok": True, "skipped": "passwordless sudo mount unavailable"}
    mock = MockQueue()
    try:
        Workspace.copy_app(mnt)
        mock.rps = 30
        service = Service(mnt, mock, root / f"{name}.service.log")
        if not service.wait_ready():
            service.stop()
            return {"ok": False, "error": "service did not start"}
        time.sleep(3)
        if break_fs(mnt) is False:
            return {"ok": False, "error": "fault injection failed (the filesystem could not be broken)"}
        deadline, degraded = time.time() + 60, None
        while time.time() < deadline:
            status, body = service.health(strict=True)
            if body["ingest"]["pending_writes"] > 0:
                degraded = {"strict_status": status, "pending_writes": body["ingest"]["pending_writes"], "last_write_error": body["ingest"]["last_write_error"]}
                break
            time.sleep(0.5)
        time.sleep(3)  # keep ingesting into memory while the disk is broken
        repair_fs(mnt)
        deadline, recovered = time.time() + 60, False
        while time.time() < deadline:
            status, body = service.health(strict=True)
            if body["ingest"]["pending_writes"] == 0 and status == 200:
                recovered = True
                break
            time.sleep(0.5)
        mock.rps = 0
        time.sleep(3)
        service.stop()
        inv = invariants(mnt)
        loss = loss_report(mock, mnt, kills=[])
        ok = bool(degraded) and degraded["strict_status"] == 503 and recovered and inv["conservation"] and inv["no_duplicates"] and loss["lost_in_flight"] == 0
        return {"ok": ok, "while_broken": degraded, "recovered": recovered, **inv, "loss": loss}
    finally:
        mock.close()
        subprocess.run(["sudo", "-n", "umount", str(mnt)], capture_output=True)


def drill_disk_full(root: Path) -> dict:
    def fill(mnt: Path) -> None:
        with open(mnt / "filler", "wb") as handle:
            try:
                while True:
                    handle.write(b"\0" * 65536)
            except OSError:
                return True

    return _fs_drill(root, "disk_full", fill, lambda mnt: (mnt / "filler").unlink(), "8m")


def drill_read_only(root: Path) -> dict:
    return _fs_drill(root, "read_only", lambda mnt: _sudo("mount", "-o", "remount,ro", str(mnt)),
                     lambda mnt: _sudo("mount", "-o", "remount,rw", str(mnt)), "8m")


def drill_clock_jumps(root: Path) -> dict:
    ws = Workspace(root, "clock_jumps")
    mock = MockQueue()
    try:
        out = ws.dir / "clock.json"
        mock.rps = 20
        code = _driver(ws.dir, f"""
        import json, time
        from datetime import timedelta
        import app as appmod, ingest
        real = ingest.utc_now
        offset = [timedelta(0)]
        def shifted():
            return real() + offset[0]
        appmod.utc_now = ingest.utc_now = shifted
        a = appmod.create_app(management_url={mock.url!r}, management_key={MGMT_KEY!r}, data_dir=APP_DIR, start_poller=False)
        async def run():
            windows = []
            for jump in (timedelta(0), timedelta(hours=2), -timedelta(hours=2), timedelta(days=400), timedelta(0)):
                offset[0] = jump
                await a.state.drain_once(a)
                windows.append(a.state.store.state["loss_windows_total"])
                if jump == timedelta(days=400):
                    await a.state.archive_once(a, shifted())
                time.sleep(1.5)
            return windows
        windows = asyncio.run(run())
        json.dump({{"loss_windows_after_each_drain": windows, "archived_total": a.state.store.state["archived_total"]}}, open({str(out)!r}, "w"))
        """)
        mock.rps = 0
        report = json.loads(out.read_text()) if out.exists() else {}
        drain_and_stop(ws.dir, mock, ws.dir / "service.log")
        inv = invariants(ws.dir)
        loss = loss_report(mock, ws.dir, kills=[])
        false_alarms = report.get("loss_windows_after_each_drain", [None])[-1]
        ok = code == 0 and inv["conservation"] and inv["no_duplicates"] and loss["lost_in_flight"] == 0 and false_alarms == 0
        return {"ok": ok, **report, **inv, "loss": loss, "note": "clock jumps are not outages; they must not raise loss windows or lose records"}
    finally:
        mock.close()


def drill_second_instance(root: Path) -> dict:
    ws = Workspace(root, "second_instance")
    mock = MockQueue()
    try:
        mock.rps = 20
        first = Service(ws.dir, mock, ws.dir / "first.log")
        if not first.wait_ready():
            first.stop()
            return {"ok": False, "error": "first instance did not start"}
        second = Service(ws.dir, mock, ws.dir / "second.log")
        second_up = second.wait_ready(timeout=10)
        second_exit = second.proc.poll()
        if second.proc.poll() is None:
            second.stop()
        first_ok = first.health()[0] == 200
        first.stop()
        mock.rps = 0
        drain_and_stop(ws.dir, mock, ws.dir / "first.log")
        inv = invariants(ws.dir)
        loss = loss_report(mock, ws.dir, kills=[])
        refused = not second_up and second_exit not in (None, 0)
        ok = refused and first_ok and inv["conservation"] and inv["no_duplicates"] and loss["lost_in_flight"] == 0
        return {"ok": ok, "second_instance_refused": refused, "second_exit_code": second_exit, "first_unaffected": first_ok, **inv, "loss": loss,
                "second_log_tail": (ws.dir / "second.log").read_text()[-300:]}
    finally:
        mock.close()


DRILLS = {
    "kill_ingest": drill_kill_ingest,
    "kill_before_state_save": drill_kill_before_state_save,
    "kill_after_pop": drill_kill_after_pop,
    "kill_archive": drill_kill_archive,
    "kill_restore": drill_kill_restore,
    "disk_full": drill_disk_full,
    "read_only": drill_read_only,
    "clock_jumps": drill_clock_jumps,
    "second_instance": drill_second_instance,
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--workdir", required=True, help="scratch directory (never the live ui/)")
    parser.add_argument("--only", nargs="*", choices=list(DRILLS), help="run a subset")
    args = parser.parse_args(argv)
    root = Path(args.workdir).resolve()
    if root == UI or UI in root.parents:
        raise SystemExit("refusing: workdir must be outside the live ui/ directory")
    root.mkdir(parents=True, exist_ok=True)
    report, ok = {}, True
    for name in args.only or DRILLS:
        started = time.time()
        try:
            result = DRILLS[name](root)
        except Exception as exc:  # a crashed drill is a failed drill
            result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        result["seconds"] = round(time.time() - started, 1)
        report[name] = result
        ok &= result["ok"]
        print(f"{'PASS' if result['ok'] else 'FAIL'}  {name:<16} {result['seconds']:>6.1f}s", file=sys.stderr)
    print(json.dumps({"ok": ok, "drills": report}, indent=2, default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
