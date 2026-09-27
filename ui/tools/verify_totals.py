#!/usr/bin/env python3
"""Independently recompute cproxy-ui totals from requests.jsonl + data/archive and compare with /api/analytics.

Deliberately imports nothing from the app: the price table below is a second, hand-copied
encoding of the official Anthropic list prices (mandate section 4, as of 2026-09-27) and the
cost formula is re-implemented from the spec. If this oracle and the app ever disagree, one of
them is wrong. Standard library only.

    venv/bin/python tools/verify_totals.py                      # live service, all windows
    venv/bin/python tools/verify_totals.py --window 24h --json  # machine-readable

Exit status 0 when every compared figure matches, 1 on any mismatch, 2 on usage/IO errors.
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# (input, output, cache write 5m, cache read) USD per 1M tokens; None = unpriced legacy.
PRICES = {
    "claude-fable-5-1": (10.00, 50.00, 12.50, 0.25),
    "claude-fable-5": (10.00, 50.00, 12.50, 1.00),
    "claude-opus-5-5": (4.00, 20.00, 5.00, 0.20),
    "claude-opus-5": (5.00, 25.00, 6.25, 0.50),
    "claude-opus-4-8": (5.00, 25.00, 6.25, 0.50),
    "claude-opus-4-7": (5.00, 25.00, 6.25, 0.50),
    "claude-opus-4-6": (5.00, 25.00, 6.25, 0.50),
    "claude-opus-4-5-20251101": (5.00, 25.00, 6.25, 0.50),
    "claude-opus-4-1-20250805": (15.00, 75.00, 18.75, 1.50),
    "claude-opus-4-20250514": (15.00, 75.00, 18.75, 1.50),
    "claude-sonnet-5": (2.00, 10.00, 2.50, 0.20),
    "claude-sonnet-4-6": (3.00, 15.00, 3.75, 0.30),
    "claude-sonnet-4-5-20250929": (3.00, 15.00, 3.75, 0.30),
    "claude-sonnet-4-20250514": (3.00, 15.00, 3.75, 0.30),
    "claude-haiku-4-5-20251001": (1.00, 5.00, 1.25, 0.10),
    "claude-3-5-haiku-20241022": (0.80, 4.00, 1.00, 0.08),
    "claude-3-7-sonnet-20250219": None,
}
WINDOWS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30), "all": None}
SUFFIX = re.compile(r"-(\d{8}|latest)$")
COST_TOLERANCE = 1e-8  # the API rounds the total to 8 decimals


def price_row(model):
    model = (model or "").strip().lower()
    if model in PRICES:
        return PRICES[model]
    base = SUFFIX.sub("", model)
    for known, row in PRICES.items():
        if known == base or SUFFIX.sub("", known) == base:
            return row
    return None


def num(value):
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def buckets(record):
    """(uncached, cache_read, cache_write, output, reasoning, unclassified) per the spec."""
    if "_buckets" in record:
        return record["_buckets"]
    tb = record.get("token_breakdown") or {}
    if tb.get("quality") == "complete" and isinstance(tb.get("input"), dict) and isinstance(tb.get("output"), dict):
        i, o = tb["input"], tb["output"]
        return num(i.get("uncached_tokens")), num(i.get("cache_read_tokens")), num(i.get("cache_write_tokens")), num(o.get("total_tokens")), num(o.get("reasoning_tokens")), 0
    t = record.get("tokens") or {}
    unclassified = num(tb.get("unclassified_tokens")) if tb else 0
    return num(t.get("input_tokens")), num(t.get("cache_read_tokens")), num(t.get("cache_creation_tokens")), num(t.get("output_tokens")), num(t.get("reasoning_tokens")), unclassified


def parse_time(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def load(path, seen=None):
    """Stream the file (.jsonl or .jsonl.gz), keeping only what the recomputation needs."""
    seen, records = (set() if seen is None else seen), []
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict) or not record.get("id") or record["id"] in seen:
                continue
            seen.add(record["id"])
            records.append({
                "id": record["id"], "timestamp": record.get("timestamp"), "ingested_at": record.get("ingested_at"),
                "model": record.get("model"), "failed": bool(record.get("failed")), "_buckets": buckets(record),
            })
    return records


def recompute(records, window, now):
    span = WINDOWS[window]
    out = {"requests": 0, "success": 0, "failed": 0, "input_tokens": 0, "output_tokens": 0, "cache_read_tokens": 0,
           "cache_write_tokens": 0, "reasoning_tokens": 0, "priced_requests": 0, "unpriced_requests": 0, "cost": 0.0}
    per_model = {}
    for record in records:
        ts = parse_time(record.get("timestamp"))
        if span is not None and (ts is None or ts < now - span):
            continue
        uncached, read, write, output, reasoning, unclassified = buckets(record)
        out["requests"] += 1
        out["failed" if record.get("failed") else "success"] += 1
        out["input_tokens"] += uncached + read + write + unclassified
        out["output_tokens"] += output
        out["cache_read_tokens"] += read
        out["cache_write_tokens"] += write
        out["reasoning_tokens"] += reasoning
        model = str(record.get("model") or "unknown")
        m = per_model.setdefault(model, {"requests": 0, "cost": 0.0, "priced": 0})
        m["requests"] += 1
        row = price_row(model)
        if row is None:
            out["unpriced_requests"] += 1
            continue
        inp, outp, cw5, cr = row
        cost = ((uncached + unclassified) * inp + read * cr + write * cw5 + output * outp) / 1_000_000
        out["cost"] += cost
        out["priced_requests"] += 1
        m["cost"] += cost
        m["priced"] += 1
    return out, per_model


def compare(analytics, records):
    """List of (field, expected_from_file, reported_by_api) mismatches for one window."""
    now = parse_time(analytics["generated_at"])
    # Only records the API could have seen when it computed this result.
    visible = [r for r in records if (parse_time(r.get("ingested_at")) or now) <= now]
    mine, per_model = recompute(visible, analytics["window"], now)
    summary = analytics["summary"]
    problems = []
    for field in ("requests", "success", "failed", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "reasoning_tokens", "priced_requests", "unpriced_requests"):
        if mine[field] != summary[field]:
            problems.append((field, mine[field], summary[field]))
    reported = summary["estimated_cost_usd"]
    if mine["priced_requests"] == 0:
        expected_cost = 0.0 if mine["requests"] == 0 else None
        if reported != expected_cost:
            problems.append(("estimated_cost_usd", expected_cost, reported))
    elif reported is None or abs(mine["cost"] - reported) > COST_TOLERANCE:
        problems.append(("estimated_cost_usd", round(mine["cost"], 8), reported))
    api_models = {row["model"]: row for row in analytics["per_model"]}
    if set(api_models) != set(per_model):
        problems.append(("per_model.models", sorted(per_model), sorted(api_models)))
    for model, m in per_model.items():
        row = api_models.get(model)
        if row is None:
            continue
        if row["requests"] != m["requests"]:
            problems.append((f"per_model[{model}].requests", m["requests"], row["requests"]))
        expected = m["cost"] if m["priced"] else None
        got = row["cost_usd"]
        if (expected is None) != (got is None) or (expected is not None and abs(expected - got) > COST_TOLERANCE):
            problems.append((f"per_model[{model}].cost_usd", expected, got))
    day_cost = sum(row["cost_usd"] or 0 for row in analytics["per_day"])
    if mine["priced_requests"] and abs(day_cost - mine["cost"]) > 1e-6:
        problems.append(("per_day.cost_sum", round(mine["cost"], 8), day_cost))
    return mine, problems


def load_lifetime(live_path):
    """Live records first, then every archive, each id counted once."""
    seen = set()
    records = load(live_path, seen)
    for archive in sorted((Path(live_path).parent / "data" / "archive").glob("requests-*.jsonl.gz")):
        records += load(archive, seen)
    return records


def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as response:  # local tool, not a relay path
        return json.loads(response.read().decode("utf-8"))


def main(argv=None):
    here = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://127.0.0.1:24688", help="cproxy-ui base URL")
    parser.add_argument("--file", default=str(here / "requests.jsonl"), help="path to requests.jsonl")
    parser.add_argument("--window", choices=[*WINDOWS, "every"], default="every")
    parser.add_argument("--json", action="store_true", help="print a JSON report")
    args = parser.parse_args(argv)
    windows = list(WINDOWS) if args.window == "every" else [args.window]
    report, failed = [], False
    for window in windows:
        try:
            analytics = fetch(f"{args.url.rstrip('/')}/api/analytics?window={window}")
            # Live file + archives (read after the API answered, so nothing it saw is missing).
            # Archives only hold records older than the 24h/7d/30d windows, so filtering by
            # timestamp gives the same sets the app uses; window=all spans both.
            records = load_lifetime(args.file)
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        mine, problems = compare(analytics, records)
        failed |= bool(problems)
        report.append({"window": window, "generated_at": analytics["generated_at"], "requests": mine["requests"],
                       "cost_usd": round(mine["cost"], 8), "match": not problems,
                       "mismatches": [{"field": f, "recomputed": a, "api": b} for f, a, b in problems]})
    if args.json:
        print(json.dumps({"ok": not failed, "windows": report}, indent=2))
    else:
        for item in report:
            status = "MATCH" if item["match"] else "MISMATCH"
            requests = "—" if item["requests"] is None else item["requests"]
            cost = "—" if item["cost_usd"] is None else f"{item['cost_usd']:.8f}"
            print(f"{item['window']:>8}  {status:8}  requests={requests:<6} cost_usd={cost}  (api generated_at {item['generated_at']})")
            for m in item["mismatches"]:
                print(f"        {m['field']}: recomputed={m['recomputed']} api={m['api']}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
