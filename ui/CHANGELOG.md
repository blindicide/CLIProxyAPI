# cproxy-ui changelog

Versions are shown in the dashboard header/footer, `/api/health` and
`/api/analytics` (`ui/version.py`).

## Unreleased

- `tools/verify_totals.py`: independent recomputation of the analytics from
  `requests.jsonl` (`ad577bb`).
- `tools/datastore.py` backup / verify / merge-restore / archive / status, a
  sandboxed daily backup timer (keeps 30), and storage + backup freshness in
  `/api/health` with dashboard warnings. History is still never truncated
  automatically.
- The store and the oracle stream `requests.jsonl` instead of reading it
  whole: load-time peak 9.9 → 3.3 KB/record (store), 13.8 → 1.3 KB/record
  (oracle). Memory-guarded `tools/scale_test.py` (20k records max on this host).
- `tools/datastore.py` streams every operation (backup memory no longer grows
  with the history; snapshots stay consistent while the file is appended to).
- cgroup memory caps: cproxy-ui `MemoryHigh=600M`/`MemoryMax=900M`, backup unit
  `MemoryMax=256M`; `/api/health` reports RSS against the limit and the dashboard
  warns at 70%.
- Automatic archiving: records older than 180 days move daily into
  `data/archive/*.jsonl.gz` only after a fresh verified backup and a
  read-back-verified archive exist; journaled in-place rewrite with crash
  recovery; any failed check keeps everything and warns. `datastore.py audit`
  proves live ∪ archives == total ingested.

## 0.2.0 — 2026-09-27 (tag `cproxy-ui-v0.2`)

Data safety
- A crash mid-write can no longer glue the next record onto a torn line and
  lose it; failed writes (disk full) keep records in memory and retry
  (`1263ca7`).
- A failing `/api-keys` refresh no longer aborts the drain, so records are not
  left to expire in cproxy's queue (`8549748`).
- Shutdown persists an in-flight queue pop instead of cancelling it; every
  deploy restart used to risk dropping those records (`24a2923`).
- Spans lost to cproxy's usage-queue retention (drainer away longer than
  `redis-usage-queue-retention-seconds`) are detected, persisted and shown; the
  retention is read live from cproxy's config (`d9cf5e2`).

Security
- Hash-pinned Content-Security-Policy for the dashboard, baseline security
  headers everywhere, `no-store` on `/api/*`, HEAD support for monitors
  (`9052090`).
- systemd sandbox: secrets and other projects invisible to the service, code
  read-only, only `requests.jsonl` + `data/` writable, localhost-only network;
  `systemd-analyze security` 9.2 → 1.1 (`464a79e`).

Performance
- Per-record derivation cache, 5 s analytics result cache, aggregation in the
  threadpool so the poller is never blocked: 50k records, window=all
  919 → 365 ms (`df461d2`).
- About 3× less memory per stored record (9.3 → 3.2 KB) through interned,
  compact in-memory records (`a991bcd`).

Features
- `GET /api/export.csv?window=` with a spreadsheet formula-injection guard,
  and a CSV button in the dashboard (`ff31bd2`).

Accessibility
- WCAG AA text contrast in both themes, scoped and captioned tables, charts
  that announce their data, visible keyboard focus (`80cd5ef`).

Correctness and hardening (after the version bump, before tagging)
- Hourly buckets carry the UTC offset and are ordered by real time, so the
  repeated hour on a DST fall-back day is not merged (`ab02be3`).
- Quota windows whose reset time has passed since the last observed upstream
  response are flagged (`reset_passed`) and shown as "unknown" instead of
  presenting the old utilisation as current.
- Runtime and test dependencies split; tests move to pytest >= 9.0.3
  (PYSEC-2026-1845, predictable `/tmp/pytest-of-<user>`); `requirements.lock`
  pins the exact tested set, which pip-audit reports clean.
- Responses over 1 KB are gzip-compressed by the app (nginx does not compress
  here): a 30 s dashboard poll drops from ~64 KB to ~7.7 KB.
- Upstream API-key usage rows skip malformed entries; tests for the live
  quota shape, key masking, backlog draining and schema drift (coverage 98%).
- Dashboard JavaScript unit tests, mutation-checked (`3df96f2`).

Docs
- Developer README (`9d88151`); operator note on queue retention (`5873278`);
  public-exposure operator decision with tested basic-auth / allowlist snippets.

## 0.1.0 — 2026-09-27 (tag `cproxy-ui-v0.1`)

Initial release: usage-queue drain with dedupe and key masking, Anthropic
list-price cost estimation, analytics API, dashboard with quota panel, charts
and tables; systemd unit and nginx vhosts for cproxy-ui.net.a.blindicide.ru and
the `/` split on cproxy.net.a.blindicide.ru.
