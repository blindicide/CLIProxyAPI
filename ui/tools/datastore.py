#!/usr/bin/env python3
"""Backup, verify, restore and archive cproxy-ui's request history (requests.jsonl).

Policy: history is never truncated automatically. Every operation that changes
requests.jsonl keeps the previous file next to it and verifies record ids before
switching, so a mistake is always recoverable.

    datastore.py backup                    # consistent snapshot -> data/backups/*.tar.gz (keeps newest --keep)
    datastore.py verify <backup.tar.gz>    # manifest checksum + record count
    datastore.py restore <backup.tar.gz>   # MERGE (union by id) into requests.jsonl; service must be stopped
    datastore.py archive --before DATE     # move records older than DATE (UTC) to data/archive; service stopped
    datastore.py status                    # sizes, record counts, newest backup

restore/archive refuse to run while cproxy-ui is active: requests.jsonl is bind-mounted
into the running service, so it can only be replaced while the service is stopped. The
queue keeps unread records for its retention (60 s by default), so keep that stop short;
cproxy-ui reports any longer gap as a loss window.

Standard library only; exit 0 on success, 1 on verification failure, 2 on usage errors.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

UI = Path(__file__).resolve().parent.parent
BACKUP_PREFIX = "cproxy-ui-backup-"
DEFAULT_KEEP = 30


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def complete_lines(data: bytes) -> bytes:
    """Everything up to the last newline: a line still being appended is left out."""
    cut = data.rfind(b"\n")
    return data[: cut + 1] if cut >= 0 else b""


def record_ids(data: bytes) -> list[str]:
    ids = []
    for line in data.decode("utf-8", errors="replace").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("id"):
            ids.append(record["id"])
    return ids


def fsync_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def service_active(unit: str = "cproxy-ui") -> bool:
    try:
        result = subprocess.run(["systemctl", "is-active", "--quiet", unit], check=False)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def backup(ui: Path, keep: int = DEFAULT_KEEP) -> dict:
    live = ui / "requests.jsonl"
    data = complete_lines(live.read_bytes()) if live.exists() else b""
    state_path = ui / "data" / "ingest_state.json"
    state = state_path.read_bytes() if state_path.exists() else b"{}\n"
    ids = record_ids(data)
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "records": len(set(ids)),
        "lines": len(ids),
        "requests_sha256": hashlib.sha256(data).hexdigest(),
        "requests_bytes": len(data),
    }
    target = ui / "data" / "backups" / f"{BACKUP_PREFIX}{now_stamp()}.tar.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, payload in (("requests.jsonl", data), ("ingest_state.json", state), ("MANIFEST.json", json.dumps(manifest, indent=2).encode())):
            info = tarfile.TarInfo(name)
            info.size, info.mtime, info.mode = len(payload), int(time.time()), 0o600
            tar.addfile(info, io.BytesIO(payload))
    tmp = target.with_suffix(".tmp")
    fsync_write(tmp, buffer.getvalue())
    os.replace(tmp, target)
    verified = verify(target)
    pruned = prune(target.parent, keep)
    return {"backup": str(target), **manifest, "verified": verified["ok"], "pruned": pruned}


def read_backup(path: Path) -> tuple[dict, bytes]:
    with tarfile.open(path, mode="r:gz") as tar:
        manifest = json.loads(tar.extractfile("MANIFEST.json").read())
        data = tar.extractfile("requests.jsonl").read()
    return manifest, data


def verify(path: Path) -> dict:
    try:
        manifest, data = read_backup(path)
    except (OSError, KeyError, tarfile.TarError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"unreadable backup: {exc}"}
    problems = []
    if hashlib.sha256(data).hexdigest() != manifest["requests_sha256"]:
        problems.append("requests.jsonl checksum does not match the manifest")
    if len(set(record_ids(data))) != manifest["records"]:
        problems.append("record count does not match the manifest")
    return {"ok": not problems, "records": manifest["records"], "created_at": manifest["created_at"], "problems": problems}


def prune(directory: Path, keep: int) -> list[str]:
    backups = sorted(directory.glob(f"{BACKUP_PREFIX}*.tar.gz"))
    doomed = backups[: max(0, len(backups) - keep)] if keep > 0 else []
    for path in doomed:
        path.unlink()
    return [p.name for p in doomed]


def _require_stopped(check_service: bool) -> None:
    if check_service and service_active():
        raise SystemExit("refusing: cproxy-ui is running. Stop it first (sudo systemctl stop cproxy-ui), then start it right after.")


def _replace_live(ui: Path, new_data: bytes, label: str) -> Path:
    """Swap in new_data, keeping the previous file as requests.jsonl.<label>-<stamp>."""
    live = ui / "requests.jsonl"
    keep = ui / f"requests.jsonl.{label}-{now_stamp()}"
    tmp = ui / "requests.jsonl.tmp"
    fsync_write(tmp, new_data)
    if live.exists():
        os.link(live, keep)  # the previous file stays reachable under its new name
    os.replace(tmp, live)
    return keep


def restore(ui: Path, backup_path: Path, check_service: bool = True) -> dict:
    _require_stopped(check_service)
    result = verify(backup_path)
    if not result["ok"]:
        raise SystemExit(f"refusing: backup does not verify: {result}")
    _, backup_data = read_backup(backup_path)
    live = ui / "requests.jsonl"
    live_data = live.read_bytes() if live.exists() else b""
    if live_data and not live_data.endswith(b"\n"):
        live_data += b"\n"
    live_ids = set(record_ids(live_data))
    added = []
    for line in backup_data.decode("utf-8").splitlines(keepends=True):
        try:
            rid = json.loads(line).get("id")
        except (json.JSONDecodeError, AttributeError):
            continue
        if rid and rid not in live_ids:
            live_ids.add(rid)
            added.append(line.encode("utf-8"))
    merged = live_data + b"".join(added)
    expected = set(record_ids(live_data)) | set(record_ids(backup_data))
    if set(record_ids(merged)) != expected:
        raise SystemExit("refusing: merged file does not contain exactly live + backup records")
    kept = _replace_live(ui, merged, "pre-restore") if added else None
    return {"restored_from": str(backup_path), "records_before": len(set(record_ids(live_data))), "records_added": len(added),
            "records_after": len(expected), "previous_file": str(kept) if kept else None}


def archive(ui: Path, before: str, check_service: bool = True) -> dict:
    _require_stopped(check_service)
    cutoff = datetime.fromisoformat(before).replace(tzinfo=timezone.utc) if "T" not in before else datetime.fromisoformat(before.replace("Z", "+00:00"))
    live = ui / "requests.jsonl"
    data = live.read_bytes()
    keep_lines, old_lines = [], []
    for line in data.decode("utf-8", errors="replace").splitlines(keepends=True):
        if not line.endswith("\n"):
            line += "\n"
        try:
            ts = json.loads(line).get("timestamp")
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else None
        except (json.JSONDecodeError, AttributeError, ValueError):
            dt = None
        (old_lines if dt is not None and dt < cutoff else keep_lines).append(line)
    if not old_lines:
        return {"archived": 0, "remaining": len(keep_lines), "archive": None, "previous_file": None}
    old_data, new_data = "".join(old_lines).encode(), "".join(keep_lines).encode()
    if set(record_ids(old_data)) | set(record_ids(new_data)) != set(record_ids(data)) or set(record_ids(old_data)) & set(record_ids(new_data)):
        raise SystemExit("refusing: archive split does not partition the records")
    target = ui / "data" / "archive" / f"requests-before-{cutoff:%Y%m%d}-{now_stamp()}.jsonl.gz"
    fsync_write(target, gzip.compress(old_data))
    if gzip.decompress(target.read_bytes()) != old_data:
        raise SystemExit("refusing: archive read-back mismatch")
    kept = _replace_live(ui, new_data, "pre-archive")
    return {"archived": len(set(record_ids(old_data))), "remaining": len(set(record_ids(new_data))), "archive": str(target), "previous_file": str(kept)}


def status(ui: Path) -> dict:
    live = ui / "requests.jsonl"
    backups = sorted((ui / "data" / "backups").glob(f"{BACKUP_PREFIX}*.tar.gz"))
    disk = os.statvfs(ui)
    return {
        "requests_bytes": live.stat().st_size if live.exists() else 0,
        "records": len(set(record_ids(live.read_bytes()))) if live.exists() else 0,
        "backups": len(backups),
        "newest_backup": backups[-1].name if backups else None,
        "disk_free_bytes": disk.f_bavail * disk.f_frsize,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--ui", default=str(UI), help="the ui/ directory (default: this checkout)")
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup")
    b.add_argument("--keep", type=int, default=DEFAULT_KEEP, help="backups to keep (0 = keep all)")
    sub.add_parser("verify").add_argument("backup")
    r = sub.add_parser("restore")
    r.add_argument("backup")
    a = sub.add_parser("archive")
    a.add_argument("--before", required=True, help="UTC date or ISO timestamp; older records are archived")
    sub.add_parser("status")
    args = parser.parse_args(argv)
    ui = Path(args.ui)
    if args.command == "backup":
        result = backup(ui, args.keep)
    elif args.command == "verify":
        result = verify(Path(args.backup))
    elif args.command == "restore":
        result = restore(ui, Path(args.backup))
    elif args.command == "archive":
        result = archive(ui, args.before)
    else:
        result = status(ui)
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok", result.get("verified", True)) else 1


if __name__ == "__main__":
    sys.exit(main())
