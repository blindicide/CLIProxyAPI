"""Automatic archiving of old usage records: move, verify, never delete.

Records older than ``ARCHIVE_AFTER_DAYS`` leave requests.jsonl only after all of these hold:

1. a fresh backup of the whole history was just written and verifies;
2. the old records were streamed into ``data/archive/requests-before-<date>-<UTC>.jsonl.gz``,
   fsync'd, and read back with exactly the expected ids;
3. every archived id is also present in that fresh backup;
4. the replacement live content (kept records + anything appended meanwhile) was written to a
   journal file, fsync'd, and its ids equal kept + appended exactly.

Only then is requests.jsonl rewritten, in place (it is bind-mounted into the service sandbox,
so it cannot be renamed over), under the service's write lock and an exclusive history lock
(the backup timer takes it shared). A marker file makes an interrupted rewrite finish on the
next start. Any failed check deletes only this job's temporary files, leaves the history as it
was, and is reported as ``last_error``.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ingest import iso_utc, parse_ts
from tools import datastore

logger = logging.getLogger("cproxy-ui.archiver")

ARCHIVE_AFTER_DAYS = int(os.getenv("CPROXY_UI_ARCHIVE_AFTER_DAYS", "180"))
REWRITE = "requests.jsonl.rewrite"
REWRITE_OK = REWRITE + ".ok"


class ArchiveError(RuntimeError):
    """A verification step failed; nothing was removed from requests.jsonl."""


def _paths(ui: Path) -> tuple[Path, Path, Path]:
    return ui / "requests.jsonl", ui / "data" / REWRITE, ui / "data" / REWRITE_OK


def _copy_in_place(source: Path, live: Path) -> None:
    with open(source, "rb") as src, open(live, "r+b") as dst:
        shutil.copyfileobj(src, dst, datastore.CHUNK)
        dst.truncate()
        dst.flush()
        os.fsync(dst.fileno())


def recover_interrupted_rewrite(ui: Path) -> str | None:
    """Finish (or discard) a rewrite that a crash interrupted. Call before loading the store."""
    live, journal, marker = _paths(ui)
    if marker.exists() and journal.exists():
        # The journal is the complete, verified new content; the old records are in a verified
        # archive. Re-applying it is idempotent.
        _copy_in_place(journal, live)
        marker.unlink()
        journal.unlink()
        logger.warning("completed an interrupted archive rewrite of %s", live.name)
        return "completed"
    if journal.exists():
        # Crash before the journal was verified: requests.jsonl was never touched.
        journal.unlink()
        marker.unlink(missing_ok=True)
        return "discarded"
    return None


def _timestamp(line: bytes) -> datetime | None:
    try:
        record = json.loads(line)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parse_ts(record.get("timestamp")) if isinstance(record, dict) else None


def prepare(ui: Path, cutoff: datetime, keep_backups: int = datastore.DEFAULT_KEEP) -> dict[str, Any] | None:
    """Steps 1-3 (no lock on appends): returns the plan, or None when nothing is old enough."""
    live, journal, _ = _paths(ui)
    backup = datastore.backup(ui, keep_backups)
    if not backup["verified"]:
        raise ArchiveError(f"fresh backup does not verify: {backup['backup']}")
    _, _, backup_ids = datastore._scan_backup(Path(backup["backup"]))

    length = datastore.complete_length(live)
    target = ui / "data" / "archive" / f"requests-before-{cutoff:%Y%m%d}-{datastore.now_stamp()}.jsonl.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    gz_tmp = target.with_suffix(".tmp")
    old_ids: set = set()
    keep_ids: set = set()
    prefix_ids: set = set()
    try:
        with open(live, "rb") as handle, gzip.open(gz_tmp, "wb") as old_out, open(journal, "wb") as keep_out:
            for line in datastore.iter_lines(handle, length):
                rid = datastore._line_id(line)
                if rid:
                    prefix_ids.add(rid)
                ts = _timestamp(line) if rid else None
                if rid and ts is not None and ts < cutoff:
                    old_out.write(line)
                    old_ids.add(rid)
                else:
                    keep_out.write(line)  # recent, undated and unparseable lines always stay
                    if rid:
                        keep_ids.add(rid)
        if not old_ids:
            gz_tmp.unlink()
            journal.unlink()
            return None
        datastore._fsync(gz_tmp)
        try:
            with gzip.open(gz_tmp, "rb") as handle:
                archived_back = datastore.scan(handle)[1]
        except (OSError, EOFError, zlib.error) as exc:
            raise ArchiveError(f"archive read-back failed: {exc}") from exc
        if archived_back != old_ids:
            raise ArchiveError("archive read-back does not contain exactly the records to archive")
        if old_ids & keep_ids or (old_ids | keep_ids) != prefix_ids:
            raise ArchiveError("archive split does not partition the records")
        if not old_ids <= backup_ids:
            raise ArchiveError("fresh backup is missing records that would be archived")
        os.replace(gz_tmp, target)  # the verified archive is now on disk, before anything is removed
    except BaseException:
        gz_tmp.unlink(missing_ok=True)
        journal.unlink(missing_ok=True)
        raise
    return {"archive": target, "length": length, "old_ids": old_ids, "keep_ids": keep_ids, "backup": backup["backup"]}


def commit(ui: Path, plan: dict[str, Any]) -> None:
    """Step 4, called with appends stopped and the exclusive history lock held."""
    live, journal, marker = _paths(ui)
    try:
        # Records appended since prepare() read the file: carry them over verbatim.
        with open(live, "rb") as handle, open(journal, "ab") as out:
            handle.seek(plan["length"])
            tail = handle.read()
            if tail and not tail.endswith(b"\n"):
                tail += b"\n"
            out.write(tail)
            out.flush()
            os.fsync(out.fileno())
        tail_ids = set(datastore.record_ids(tail))
        with open(journal, "rb") as handle:
            journal_ids = datastore.scan(handle)[1]
        if journal_ids != plan["keep_ids"] | tail_ids or journal_ids & plan["old_ids"]:
            raise ArchiveError("replacement content does not match kept + appended records")
    except BaseException:
        journal.unlink(missing_ok=True)
        raise
    marker.write_text("journal verified; apply it to requests.jsonl\n")
    datastore._fsync(marker)
    _copy_in_place(journal, live)
    with open(live, "rb") as handle:
        if datastore.scan(handle)[1] != journal_ids:
            # Leave marker + journal: the next start re-applies the verified journal.
            raise ArchiveError("requests.jsonl does not match the journal after rewrite; will re-apply on restart")
    marker.unlink()
    journal.unlink()


def cutoff_for(now: datetime, days: int = ARCHIVE_AFTER_DAYS) -> datetime:
    return now - timedelta(days=days)


def has_old_records(records: list[dict[str, Any]], cutoff: datetime) -> bool:
    return any((ts := parse_ts(r.get("timestamp"))) is not None and ts < cutoff for r in records)


def summary(plan: dict[str, Any], now: datetime) -> dict[str, Any]:
    return {"archived": len(plan["old_ids"]), "kept": len(plan["keep_ids"]), "archive": str(plan["archive"]),
            "backup": plan["backup"], "at": iso_utc(now)}
