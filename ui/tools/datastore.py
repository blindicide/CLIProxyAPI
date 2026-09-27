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
import shutil
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


# Everything below streams: memory stays at one line plus a set of record ids, whatever the
# size of the history (reading files whole is what made the 2026-09-27 scale test OOM).
CHUNK = 1 << 20


def complete_lines(data: bytes) -> bytes:
    """Everything up to the last newline: a line still being appended is left out."""
    cut = data.rfind(b"\n")
    return data[: cut + 1] if cut >= 0 else b""


def complete_length(path: Path) -> int:
    """Byte length of the file up to and including its last newline (read from the end)."""
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return 0
    with open(path, "rb") as handle:
        position = size
        while position > 0:
            start = max(0, position - CHUNK)
            handle.seek(start)
            cut = handle.read(position - start).rfind(b"\n")
            if cut >= 0:
                return start + cut + 1
            position = start
    return 0


def _line_id(line: bytes) -> str | None:
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return record.get("id") if isinstance(record, dict) and record.get("id") else None


def iter_lines(handle, limit: int | None = None):
    """Yield lines (bytes) from a binary file object, stopping after ``limit`` bytes."""
    remaining = limit
    while remaining is None or remaining > 0:
        line = handle.readline() if remaining is None else handle.readline(remaining)
        if not line:
            return
        if remaining is not None:
            remaining -= len(line)
        yield line


def scan(handle, limit: int | None = None) -> tuple[str, set, int]:
    """(sha256 of the bytes, set of record ids, lines carrying an id) in one streaming pass."""
    digest, ids, lines = hashlib.sha256(), set(), 0
    for line in iter_lines(handle, limit):
        digest.update(line)
        rid = _line_id(line)
        if rid:
            ids.add(rid)
            lines += 1
    return digest.hexdigest(), ids, lines


def record_ids(data: bytes) -> list[str]:
    """Ids in an in-memory buffer (small inputs and tests)."""
    return [rid for rid in (_line_id(line) for line in data.splitlines()) if rid]


class _Limited(io.RawIOBase):
    """Read-only view of the first ``size`` bytes of a file (for tarfile.addfile)."""

    def __init__(self, handle, size: int) -> None:
        self.handle, self.left = handle, size

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        n = min(len(buffer), self.left)
        if n <= 0:
            return 0
        chunk = self.handle.read(n)
        buffer[: len(chunk)] = chunk
        self.left -= len(chunk)
        return len(chunk)


def _fsync(path: Path) -> None:
    with open(path, "rb+") as handle:
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


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


def _tar_add(tar: tarfile.TarFile, name: str, size: int, fileobj) -> None:
    info = tarfile.TarInfo(name)
    info.size, info.mtime, info.mode = size, int(time.time()), 0o600
    tar.addfile(info, fileobj)


def backup(ui: Path, keep: int = DEFAULT_KEEP) -> dict:
    live = ui / "requests.jsonl"
    # Fix the snapshot length first: the file is append-only, so bytes [0, length) never change
    # while the service keeps writing after them.
    length = complete_length(live)
    if length:
        with open(live, "rb") as handle:
            sha, ids, lines = scan(handle, length)
    else:
        sha, ids, lines = hashlib.sha256(b"").hexdigest(), set(), 0
    state_path = ui / "data" / "ingest_state.json"
    state = state_path.read_bytes() if state_path.exists() else b"{}\n"
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "records": len(ids),
        "lines": lines,
        "requests_sha256": sha,
        "requests_bytes": length,
    }
    target = ui / "data" / "backups" / f"{BACKUP_PREFIX}{now_stamp()}.tar.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    with tarfile.open(tmp, mode="w:gz") as tar:
        if length:
            with open(live, "rb") as handle:
                _tar_add(tar, "requests.jsonl", length, _Limited(handle, length))
        else:
            _tar_add(tar, "requests.jsonl", 0, io.BytesIO(b""))
        _tar_add(tar, "ingest_state.json", len(state), io.BytesIO(state))
        payload = json.dumps(manifest, indent=2).encode()
        _tar_add(tar, "MANIFEST.json", len(payload), io.BytesIO(payload))
    _fsync(tmp)
    os.replace(tmp, target)
    verified = verify(target)
    pruned = prune(target.parent, keep)
    return {"backup": str(target), **manifest, "verified": verified["ok"], "pruned": pruned}


def read_backup(path: Path) -> tuple[dict, bytes]:
    """Manifest and full requests.jsonl of a backup, in memory (small backups and tests only)."""
    with tarfile.open(path, mode="r:gz") as tar:
        manifest = json.loads(tar.extractfile("MANIFEST.json").read())
        data = tar.extractfile("requests.jsonl").read()
    return manifest, data


def _scan_backup(path: Path) -> tuple[dict, str, set]:
    with tarfile.open(path, mode="r:gz") as tar:
        manifest = json.loads(tar.extractfile("MANIFEST.json").read())
        sha, ids, _ = scan(tar.extractfile("requests.jsonl"))
    return manifest, sha, ids


def verify(path: Path) -> dict:
    try:
        manifest, sha, ids = _scan_backup(path)
    except (OSError, KeyError, tarfile.TarError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"unreadable backup: {exc}"}
    problems = []
    if sha != manifest["requests_sha256"]:
        problems.append("requests.jsonl checksum does not match the manifest")
    if len(ids) != manifest["records"]:
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


def _replace_live(ui: Path, tmp: Path, label: str) -> Path:
    """Swap the fsync'd ``tmp`` in, keeping the previous file as requests.jsonl.<label>-<stamp>."""
    live = ui / "requests.jsonl"
    keep = ui / f"requests.jsonl.{label}-{now_stamp()}"
    if live.exists():
        os.link(live, keep)  # the previous file stays reachable under its new name
    os.replace(tmp, live)
    return keep


def _ids_of(path: Path) -> set:
    if not path.exists():
        return set()
    with open(path, "rb") as handle:
        return scan(handle)[1]


def restore(ui: Path, backup_path: Path, check_service: bool = True) -> dict:
    _require_stopped(check_service)
    result = verify(backup_path)
    if not result["ok"]:
        raise SystemExit(f"refusing: backup does not verify: {result}")
    live = ui / "requests.jsonl"
    live_ids = _ids_of(live)
    tmp = ui / "requests.jsonl.tmp"
    added = 0
    backup_ids = set()
    with open(tmp, "wb") as out:
        if live.exists():
            with open(live, "rb") as handle:
                shutil.copyfileobj(handle, out, CHUNK)
            if live.stat().st_size and complete_length(live) != live.stat().st_size:
                out.write(b"\n")  # never glue a restored line onto a torn tail
        seen = set(live_ids)
        with tarfile.open(backup_path, mode="r:gz") as tar:
            for line in iter_lines(tar.extractfile("requests.jsonl")):
                rid = _line_id(line)
                if not rid:
                    continue
                backup_ids.add(rid)
                if rid not in seen:
                    seen.add(rid)
                    out.write(line if line.endswith(b"\n") else line + b"\n")
                    added += 1
        out.flush()
        os.fsync(out.fileno())
    if added == 0:
        tmp.unlink()
        return {"restored_from": str(backup_path), "records_before": len(live_ids), "records_added": 0,
                "records_after": len(live_ids), "previous_file": None}
    if _ids_of(tmp) != live_ids | backup_ids:
        tmp.unlink()
        raise SystemExit("refusing: merged file does not contain exactly live + backup records")
    os.chmod(tmp, 0o600)
    kept = _replace_live(ui, tmp, "pre-restore")
    return {"restored_from": str(backup_path), "records_before": len(live_ids), "records_added": added,
            "records_after": len(live_ids | backup_ids), "previous_file": str(kept)}


def archive(ui: Path, before: str, check_service: bool = True) -> dict:
    _require_stopped(check_service)
    cutoff = datetime.fromisoformat(before).replace(tzinfo=timezone.utc) if "T" not in before else datetime.fromisoformat(before.replace("Z", "+00:00"))
    live = ui / "requests.jsonl"
    target = ui / "data" / "archive" / f"requests-before-{cutoff:%Y%m%d}-{now_stamp()}.jsonl.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    gz_tmp, live_tmp = target.with_suffix(".tmp"), ui / "requests.jsonl.tmp"
    old_ids, keep_ids, all_ids = set(), set(), set()
    with open(live, "rb") as handle, gzip.open(gz_tmp, "wb") as old_out, open(live_tmp, "wb") as keep_out:
        for line in iter_lines(handle):
            if not line.endswith(b"\n"):
                line += b"\n"
            rid = _line_id(line)
            dt = None
            if rid:
                all_ids.add(rid)
                try:
                    ts = json.loads(line).get("timestamp")
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else None
                except (ValueError, AttributeError):
                    dt = None
            if dt is not None and dt < cutoff:
                old_out.write(line)
                old_ids.add(rid)
            else:
                keep_out.write(line)  # recent, id-less and unparseable lines always stay
                if rid:
                    keep_ids.add(rid)
        keep_out.flush()
        os.fsync(keep_out.fileno())
    if not old_ids:
        gz_tmp.unlink()
        live_tmp.unlink()
        return {"archived": 0, "remaining": len(keep_ids), "archive": None, "previous_file": None}
    _fsync(gz_tmp)
    with gzip.open(gz_tmp, "rb") as handle:
        archived_back = scan(handle)[1]
    if archived_back != old_ids or _ids_of(live_tmp) != keep_ids or old_ids | keep_ids != all_ids or old_ids & keep_ids:
        gz_tmp.unlink()
        live_tmp.unlink()
        raise SystemExit("refusing: archive split does not partition the records")
    os.replace(gz_tmp, target)
    os.chmod(live_tmp, 0o600)
    kept = _replace_live(ui, live_tmp, "pre-archive")
    return {"archived": len(old_ids), "remaining": len(keep_ids), "archive": str(target), "previous_file": str(kept)}


def status(ui: Path) -> dict:
    live = ui / "requests.jsonl"
    backups = sorted((ui / "data" / "backups").glob(f"{BACKUP_PREFIX}*.tar.gz"))
    disk = os.statvfs(ui)
    return {
        "requests_bytes": live.stat().st_size if live.exists() else 0,
        "records": len(_ids_of(live)),
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
