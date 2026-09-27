"""Official Anthropic API list prices and per-record cost estimation.

cproxy relays a Claude subscription, so nothing here is billed. Every figure is the
"list-price equivalent": what the same traffic would cost on the pay-as-you-go API.
"""
from __future__ import annotations

import re
from typing import Any

SOURCE_URL = "https://platform.claude.com/docs/en/about-claude/pricing"
AS_OF = "2026-09-27"
CURRENCY = "USD"
UNIT = "per 1M tokens"
BASIS = "list-price equivalent, not billed (requests run on a Claude subscription)"

# Usage records only expose the total cache-write count, never the 5m/1h TTL split,
# so cache writes are priced at the 5-minute rate (the Anthropic default TTL).
CACHE_WRITE_TTL_ASSUMPTION = "5m"

PRICED = "priced"
UNPRICED_LEGACY = "unpriced_legacy"

# (input, output, cache write 5m, cache write 1h, cache read) USD per 1M tokens.
_ROWS: dict[str, tuple[str, tuple[float, float, float, float, float] | None]] = {
    "claude-fable-5-1": ("Claude Fable 5.1", (10.00, 50.00, 12.50, 20.00, 0.25)),
    "claude-fable-5": ("Claude Fable 5", (10.00, 50.00, 12.50, 20.00, 1.00)),
    "claude-opus-5-5": ("Claude Opus 5.5", (4.00, 20.00, 5.00, 8.00, 0.20)),
    "claude-opus-5": ("Claude Opus 5", (5.00, 25.00, 6.25, 10.00, 0.50)),
    "claude-opus-4-8": ("Claude Opus 4.8", (5.00, 25.00, 6.25, 10.00, 0.50)),
    "claude-opus-4-7": ("Claude Opus 4.7", (5.00, 25.00, 6.25, 10.00, 0.50)),
    "claude-opus-4-6": ("Claude Opus 4.6", (5.00, 25.00, 6.25, 10.00, 0.50)),
    "claude-opus-4-5-20251101": ("Claude Opus 4.5", (5.00, 25.00, 6.25, 10.00, 0.50)),
    "claude-opus-4-1-20250805": ("Claude Opus 4.1", (15.00, 75.00, 18.75, 30.00, 1.50)),
    "claude-opus-4-20250514": ("Claude Opus 4", (15.00, 75.00, 18.75, 30.00, 1.50)),
    "claude-sonnet-5": ("Claude Sonnet 5", (2.00, 10.00, 2.50, 4.00, 0.20)),
    "claude-sonnet-4-6": ("Claude Sonnet 4.6", (3.00, 15.00, 3.75, 6.00, 0.30)),
    "claude-sonnet-4-5-20250929": ("Claude Sonnet 4.5", (3.00, 15.00, 3.75, 6.00, 0.30)),
    "claude-sonnet-4-20250514": ("Claude Sonnet 4", (3.00, 15.00, 3.75, 6.00, 0.30)),
    "claude-haiku-4-5-20251001": ("Claude Haiku 4.5", (1.00, 5.00, 1.25, 2.00, 0.10)),
    "claude-3-5-haiku-20241022": ("Claude Haiku 3.5", (0.80, 4.00, 1.00, 1.60, 0.08)),
    # Sonnet 3.7 is served by cproxy but absent from the current official pricing page.
    # Never invent a price for it: cost renders as "—" and is excluded from totals.
    "claude-3-7-sonnet-20250219": ("Claude Sonnet 3.7", None),
}

_DATE_SUFFIX = re.compile(r"-(\d{8}|latest)$")


def _build_table() -> dict[str, dict[str, Any]]:
    table: dict[str, dict[str, Any]] = {}
    for model_id, (name, prices) in _ROWS.items():
        row: dict[str, Any] = {"id": model_id, "display_name": name}
        if prices is None:
            row.update(
                price_status=UNPRICED_LEGACY,
                input=None,
                output=None,
                cache_write_5m=None,
                cache_write_1h=None,
                cache_read=None,
                note="not on the current official Anthropic pricing page; excluded from cost totals",
            )
        else:
            inp, out, cw5, cw1, cr = prices
            row.update(price_status=PRICED, input=inp, output=out, cache_write_5m=cw5, cache_write_1h=cw1, cache_read=cr, note=None)
        table[model_id] = row
    return table


PRICING: dict[str, dict[str, Any]] = _build_table()


def resolve_model(model: str | None) -> str | None:
    """Map a served model id (possibly date-suffixed or an alias) onto its pricing row id."""
    if not isinstance(model, str) or not model:
        return None
    model = model.strip().lower()
    if model in PRICING:
        return model
    base = _DATE_SUFFIX.sub("", model)
    if base in PRICING:
        return base
    # Date-suffixed ids map to their family row, e.g. claude-opus-4-5 -> claude-opus-4-5-20251101.
    for model_id in PRICING:
        if _DATE_SUFFIX.sub("", model_id) == base:
            return model_id
    return None


def price_for(model: str | None) -> dict[str, Any] | None:
    model_id = resolve_model(model)
    return PRICING.get(model_id) if model_id else None


# A single request cannot plausibly exceed this many tokens; larger values are corrupt data and
# are ignored (like negatives and non-numbers) rather than priced into absurd costs.
MAX_TOKENS = 10**9


def _int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return number if 0 <= number <= MAX_TOKENS else 0


def token_usage(record: dict[str, Any]) -> dict[str, Any]:
    """Normalised, non-overlapping token buckets for one usage record.

    Prefers ``token_breakdown`` when its quality is ``complete``; otherwise falls back to the
    flat ``tokens`` object (Claude semantics: ``input_tokens`` excludes cache reads/writes and
    ``output_tokens`` includes thinking) and flags ``cost_quality`` accordingly.
    """
    breakdown = record.get("token_breakdown") if isinstance(record.get("token_breakdown"), dict) else {}
    tokens = record.get("tokens") if isinstance(record.get("tokens"), dict) else {}
    quality = breakdown.get("quality")
    unclassified = 0
    if quality == "complete" and isinstance(breakdown.get("input"), dict) and isinstance(breakdown.get("output"), dict):
        inp, out = breakdown["input"], breakdown["output"]
        uncached = _int(inp.get("uncached_tokens"))
        cache_read = _int(inp.get("cache_read_tokens"))
        cache_write = _int(inp.get("cache_write_tokens"))
        output = _int(out.get("total_tokens"))
        reasoning = _int(out.get("reasoning_tokens"))
        cost_quality = "complete"
    else:
        uncached = _int(tokens.get("input_tokens"))
        cache_read = _int(tokens.get("cache_read_tokens"))
        cache_write = _int(tokens.get("cache_creation_tokens"))
        output = _int(tokens.get("output_tokens"))
        reasoning = _int(tokens.get("reasoning_tokens"))
        if breakdown:
            # Unclassified tokens are billed conservatively on the input side.
            unclassified = _int(breakdown.get("unclassified_tokens"))
            cost_quality = f"fallback_tokens:{quality or 'unknown'}"
        else:
            cost_quality = "fallback_tokens:no_breakdown"
    input_total = uncached + cache_read + cache_write + unclassified
    return {
        "input_total": input_total,
        "uncached_input": uncached,
        "cache_read": cache_read,
        "cache_write": cache_write,
        "output": output,
        "reasoning": reasoning,
        "unclassified": unclassified,
        "total": input_total + output,
        "cost_quality": cost_quality,
    }


def record_cost(record: dict[str, Any]) -> dict[str, Any]:
    """USD list-price equivalent for one record.

    cost = (uncached_input + unclassified) * input + cache_read * cache_read_rate
         + cache_write * cache_write_5m + output * output   (all per 1M tokens)
    ``output`` includes reasoning/thinking tokens, which Anthropic bills as output.
    Returns ``cost_usd = None`` for unpriced or unknown models.
    """
    usage = token_usage(record)
    model = record.get("model") or record.get("alias")
    row = price_for(model)
    result: dict[str, Any] = {
        "cost_usd": None,
        "cost_quality": usage["cost_quality"],
        "price_model": row["id"] if row else None,
        "price_status": row["price_status"] if row else "unknown_model",
        "usage": usage,
    }
    if not row or row["price_status"] != PRICED:
        return result
    cost = (
        (usage["uncached_input"] + usage["unclassified"]) * row["input"]
        + usage["cache_read"] * row["cache_read"]
        + usage["cache_write"] * row["cache_write_5m"]
        + usage["output"] * row["output"]
    ) / 1_000_000
    result["cost_usd"] = cost
    return result


def pricing_payload() -> dict[str, Any]:
    return {
        "source_url": SOURCE_URL,
        "as_of": AS_OF,
        "currency": CURRENCY,
        "unit": UNIT,
        "basis": BASIS,
        "cache_write_ttl_assumption": CACHE_WRITE_TTL_ASSUMPTION,
        "cache_write_ttl_note": "usage records expose no 5m/1h TTL split; cache writes are priced at the 5m rate",
        "reasoning_note": "reasoning/thinking tokens are billed as output",
        "unclassified_note": "unclassified tokens (non-complete token_breakdown) are priced at the input rate",
        "formula": "(uncached_input + unclassified) * input + cache_read * cache_read + cache_write * cache_write_5m + output * output, / 1e6",
        "models": list(PRICING.values()),
    }
