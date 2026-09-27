# cproxy-ui changelog

Versions are shown in the dashboard header/footer, `/api/health` and
`/api/analytics` (`ui/version.py`).

## Unreleased

- `tools/smoke.py`: one-command acceptance check (services, strict health,
  pricing, analytics, dashboard version, `/v1/models` = 17, independent totals,
  conservation audit; `--completion` optional). `/api/health?strict=1` answers
  503 when degraded, for monitors.
- Archive runs record `prepare_s` and `lock_held_s`; `/api/health` and the
  dashboard warn when the lock was held longer than a third of the queue
  retention (measured at 20k records: 3.3 s prepare, 0.41 s locked). Archive
  failures now also raise a dashboard banner.
- Test hang guard (`tests/timeout_guard.py`): a test stuck past 60 s fails
  with a `TimeoutError` traceback and the run continues; faulthandler hard-exits
  as a last resort. Suite checked stable over 8 runs and per file in isolation.
- Chaos drills (`tools/chaos_drill.py`) and the fixes they forced: failed
  partial writes are rolled back (no duplicate lines after ENOSPC); loss windows
  use a monotonic gap within a process (no false alarms on clock jumps);
  one instance per data dir (`data/.instance.lock`, read-only fd), with crash
  recovery and counter reconciliation under that lock at startup; unique
  pre-restore/pre-archive names; `archive_due` tolerates a clock that went
  back; `POP_BATCH` 50 → 10 (the proven worst-case loss per SIGKILL).
- Property/fuzz tests (Hypothesis) and the bugs they found: the key scrub
  rewrote serialised JSON and crashed on keys like `"`, `,`, `1` or `id` (it now
  replaces inside string values only, keys ≥ 8 chars); a non-string `model`
  crashed pricing; infinite token counts overflowed; NaN/Infinity from queue
  JSON reached responses (strict JSON 500). `normalize_record` now types every
  field at the boundary, token counts above 10^9 are treated as corrupt, and
  a record that still fails to normalise is kept in `data/quarantine.jsonl`
  instead of aborting its batch.
- `requests.jsonl` is written ASCII-only (`\uXXXX` escapes), so a record is
  always exactly one physical line even for readers that split on U+2028/`\x85`.

## 0.3.0 — 2026-09-27 (tag `cproxy-ui-v0.3`)

History: never lost, archived not deleted
- Backup / verify / merge-restore / archive / status (`tools/datastore.py`), a
  sandboxed daily backup timer keeping 30, and storage + backup freshness in
  `/api/health` with dashboard warnings (`216b4ed`).
- Automatic archiving: records older than 180 days move daily into
  `data/archive/*.jsonl.gz`, only after a fresh verified backup and a
  read-back-verified archive exist; journaled in-place rewrite with crash
  recovery under the service's write lock and a shared history lock; any
  failed check keeps everything and warns. `datastore.py audit` proves
  live ∪ archives == total ingested (`ce1cf49`).
- `window=all` spans the archives through the same aggregation as live
  records (summary, percentiles, per-day series, every table); rows carry
  `archived_requests`, responses a `coverage` block, and the dashboard labels
  archive rows and the boundary. 24h/7d/30d stay live-only (threshold floor
  31 days); the CSV export of `all` includes archived rows (`b94214c`,
  superseding the unreleased lifetime block of `635e043`).

Memory (after a 200k-record scale test OOM-killed other processes on the
shared host, 2026-09-27)
- The store and the oracle stream `requests.jsonl`: load-time peak 9.9 → 3.3
  KB/record (store), 13.8 → 1.3 KB/record (oracle) (`d3d0deb`).
- Every datastore operation streams; cgroup caps cproxy-ui
  `MemoryHigh=600M`/`MemoryMax=900M` and backup `MemoryMax=256M`; `/api/health`
  reports RSS against the limit and warns at 70% (`e53cfca`).
- Memory-guarded `tools/scale_test.py`: 20k records max on this host,
  preflight refusal and an RSS watchdog; larger runs are off-host only
  (`03897a2`). Measured at 20k: service peak 140 MiB, p95 626 ms with 10
  viewers polling back to back.

Verification
- `tools/verify_totals.py`: stdlib-only oracle with its own price table;
  recomputes every window from `requests.jsonl` + archives and compares with
  `/api/analytics` (`ad577bb`, extended in `b94214c`).

Docs
- Public-dashboard exposure recorded as an operator decision, with tested
  basic-auth / IP-allowlist snippets (`70bea40`, tagged as 0.2.0).

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
