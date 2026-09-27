"""Backup / verify / restore / archive of requests.jsonl (tools/datastore.py)."""
from __future__ import annotations

import copy
import gzip
import importlib.util
import io
import json
import os
import tarfile
from datetime import timedelta
from pathlib import Path

import pytest

from ingest import iso_utc, normalize_record, utc_now

_SPEC = importlib.util.spec_from_file_location("datastore", Path(__file__).resolve().parents[1] / "tools" / "datastore.py")
ds = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(ds)


def _records(sample, n, *, prefix="r", age_days=0):
    out = []
    for i in range(n):
        raw = copy.deepcopy(sample[i % 2])
        raw["execution_id"] = f"{prefix}-{i}"
        raw["timestamp"] = iso_utc(utc_now() - timedelta(days=age_days, minutes=i))
        out.append(normalize_record(raw, {}))
    return out


def _write(ui: Path, records, tail: str = "") -> None:
    (ui / "data").mkdir(parents=True, exist_ok=True)
    (ui / "requests.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records) + tail)
    (ui / "data" / "ingest_state.json").write_text('{"total_ingested": %d}\n' % len(records))


def _ids(ui: Path) -> list[str]:
    return ds.record_ids((ui / "requests.jsonl").read_bytes())


def test_backup_is_consistent_and_verifies(tmp_path, sample):
    records = _records(sample, 5)
    _write(tmp_path, records, tail='{"id":"half-written",')  # append in progress
    result = ds.backup(tmp_path)
    assert result["verified"] and result["records"] == 5
    manifest, data = ds.read_backup(Path(result["backup"]))
    assert data.endswith(b"\n") and b"half-written" not in data
    assert oct(os.stat(result["backup"]).st_mode & 0o777) == "0o600"
    assert ds.verify(Path(result["backup"])) == {"ok": True, "records": 5, "created_at": manifest["created_at"], "problems": []}


def test_verify_detects_tampering(tmp_path, sample):
    _write(tmp_path, _records(sample, 3))
    path = Path(ds.backup(tmp_path)["backup"])
    with tarfile.open(path, "r:gz") as tar:
        members = {m.name: tar.extractfile(m).read() for m in tar.getmembers()}
    tampered = members["requests.jsonl"].replace(b'"failed": false', b'"failed": true', 1)
    assert tampered != members["requests.jsonl"]
    members["requests.jsonl"] = tampered
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, payload in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    path.write_bytes(buffer.getvalue())
    result = ds.verify(path)
    assert not result["ok"] and "checksum" in result["problems"][0]
    assert not ds.verify(tmp_path / "missing.tar.gz")["ok"]


def test_restore_merges_and_never_loses_newer_records(tmp_path, sample):
    old = _records(sample, 4, prefix="old")
    _write(tmp_path, old)
    backup_path = Path(ds.backup(tmp_path)["backup"])
    # Later: the live file lost two old records (e.g. bad edit) but gained new ones.
    newer = _records(sample, 3, prefix="new")
    _write(tmp_path, old[2:] + newer)
    result = ds.restore(tmp_path, backup_path, check_service=False)
    assert result["records_added"] == 2 and result["records_after"] == 7
    assert sorted(_ids(tmp_path)) == sorted(r["id"] for r in old + newer)
    previous = Path(result["previous_file"])
    assert sorted(ds.record_ids(previous.read_bytes())) == sorted(r["id"] for r in old[2:] + newer)
    # Restoring again is a no-op: nothing to add, file untouched.
    again = ds.restore(tmp_path, backup_path, check_service=False)
    assert again["records_added"] == 0 and again["previous_file"] is None


def test_restore_and_archive_refuse_while_service_runs(tmp_path, sample, monkeypatch):
    _write(tmp_path, _records(sample, 2))
    backup_path = Path(ds.backup(tmp_path)["backup"])
    monkeypatch.setattr(ds, "service_active", lambda unit="cproxy-ui": True)
    with pytest.raises(SystemExit, match="cproxy-ui is running"):
        ds.restore(tmp_path, backup_path)
    with pytest.raises(SystemExit, match="cproxy-ui is running"):
        ds.archive(tmp_path, "2030-01-01")


def test_archive_partitions_history_and_keeps_previous_file(tmp_path, sample):
    old = _records(sample, 4, prefix="old", age_days=90)
    recent = _records(sample, 3, prefix="recent")
    _write(tmp_path, old + recent, tail="{torn")
    cutoff = (utc_now() - timedelta(days=30)).strftime("%Y-%m-%d")
    result = ds.archive(tmp_path, cutoff, check_service=False)
    assert result["archived"] == 4 and result["remaining"] == 3
    archived_ids = ds.record_ids(gzip.decompress(Path(result["archive"]).read_bytes()))
    assert sorted(archived_ids) == sorted(r["id"] for r in old)
    assert sorted(_ids(tmp_path)) == sorted(r["id"] for r in recent)
    assert "{torn" in (tmp_path / "requests.jsonl").read_text()  # unparseable lines are never dropped
    assert len(ds.record_ids(Path(result["previous_file"]).read_bytes())) == 7
    assert ds.archive(tmp_path, cutoff, check_service=False)["archived"] == 0


def test_backup_rotation_keeps_newest(tmp_path, sample, monkeypatch):
    _write(tmp_path, _records(sample, 1))
    stamps = iter(f"20260927T0000{i:02d}Z" for i in range(10))
    monkeypatch.setattr(ds, "now_stamp", lambda: next(stamps))
    for _ in range(5):
        result = ds.backup(tmp_path, keep=3)
    names = sorted(p.name for p in (tmp_path / "data" / "backups").glob("*.tar.gz"))
    assert names == [f"cproxy-ui-backup-20260927T0000{i:02d}Z.tar.gz" for i in (2, 3, 4)]
    assert result["pruned"] == ["cproxy-ui-backup-20260927T000001Z.tar.gz"]


def test_cli_status_and_exit_codes(tmp_path, sample, capsys):
    _write(tmp_path, _records(sample, 2))
    assert ds.main(["--ui", str(tmp_path), "backup"]) == 0
    assert ds.main(["--ui", str(tmp_path), "status"]) == 0
    capsys.readouterr()
    status = ds.status(tmp_path)
    assert status["records"] == 2 and status["backups"] == 1
    assert ds.main(["--ui", str(tmp_path), "verify", str(tmp_path / "nope.tar.gz")]) == 1


async def test_health_reports_storage_and_backup_freshness(make_app, api_client, tmp_path, sample):
    app = make_app()
    async with api_client(app) as client:
        storage = (await client.get("/api/health")).json()["ingest"]["storage"]
    assert storage["backups"] == 0 and storage["newest_backup_at"] is None
    assert "no backup of requests.jsonl yet" in storage["warnings"]
    assert storage["disk_free_bytes"] > 0

    _write(tmp_path, _records(sample, 2))
    ds.backup(tmp_path)
    async with api_client(app) as client:
        storage = (await client.get("/api/health")).json()["ingest"]["storage"]
    assert storage["backups"] == 1 and storage["newest_backup_age_s"] < 120
    assert storage["requests_file_bytes"] > 0
    assert not any("backup" in w for w in storage["warnings"])
