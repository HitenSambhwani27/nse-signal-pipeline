"""Read-only instrument-cache diagnostics. Does not refresh or call Kite."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

# Bump when the on-disk instruments_cache.json shape changes. Ingest startup
# treats a missing/older version as needs_refresh. Never copy a local-PC cache
# onto the VM to "fix" a mismatch — rebuild from the Kite master instead.
CACHE_SCHEMA_VERSION = 1


def durable_instrument_key(
    *,
    exchange: str | None,
    tradingsymbol: str | None,
    name: str | None = None,
    instrument_type: str | None = None,
    expiry: str | None = None,
    strike: float | None = None,
) -> str:
    """Stable identity for persisted UI state. Never key user state on tokens."""
    exch = (exchange or "NSE").upper()
    kind = (instrument_type or "").upper()
    symbol = (tradingsymbol or "").upper()
    underlying = (name or tradingsymbol or "").upper()
    if kind in {"CE", "PE"}:
        strike_s = "" if strike is None else f"{float(strike):g}"
        return f"{exch}:{underlying}:{kind}:{expiry or ''}:{strike_s}"
    if kind == "FUT":
        return f"{exch}:{underlying}:FUT:{expiry or ''}"
    return f"{exch}:{symbol}"


def _parse_expiry(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def instrument_cache_health(
    cache: dict[str, Any],
    *,
    today: date | None = None,
    option_expiry_count: int = 1,
    future_contract_count: int = 2,
) -> dict[str, Any]:
    today = today or date.today()
    options = list(cache.get("options") or [])
    futures = list(cache.get("futures") or [])
    expired_options = [
        o.get("tradingsymbol")
        for o in options
        if (exp := _parse_expiry(o.get("expiry"))) is not None and exp < today
    ]
    expired_futures = [
        f.get("tradingsymbol")
        for f in futures
        if (exp := _parse_expiry(f.get("expiry"))) is not None and exp < today
    ]
    option_expiries = sorted(
        {exp for o in options if (exp := _parse_expiry(o.get("expiry"))) is not None and exp >= today}
    )
    future_expiries = sorted(
        {exp for f in futures if (exp := _parse_expiry(f.get("expiry"))) is not None and exp >= today}
    )
    missing_current_options = bool(options) and not option_expiries
    missing_current_futures = bool(futures) and not future_expiries
    missing_next_options = bool(options) and option_expiry_count >= 2 and len(option_expiries) < 2
    missing_next_futures = bool(futures) and future_contract_count >= 2 and len(future_expiries) < 2
    needs_refresh = bool(
        expired_options
        or expired_futures
        or missing_current_options
        or missing_current_futures
        or missing_next_options
        or missing_next_futures
    )
    schema_version = cache.get("schema_version")
    schema_ok = schema_version == CACHE_SCHEMA_VERSION
    if not schema_ok:
        needs_refresh = True
    status = "ok"
    if not schema_ok:
        status = "schema_stale"
    elif needs_refresh:
        status = "stale_expiries"
    return {
        "updated_at": cache.get("updated_at"),
        "schema_version": schema_version,
        "schema_ok": schema_ok,
        "expected_schema_version": CACHE_SCHEMA_VERSION,
        "counts": cache.get("counts") or {},
        "expired_option_count": len(expired_options),
        "expired_future_count": len(expired_futures),
        "current_option_expiries": [d.isoformat() for d in option_expiries],
        "current_future_expiries": [d.isoformat() for d in future_expiries],
        "missing_current_options": missing_current_options,
        "missing_current_futures": missing_current_futures,
        "missing_next_options": missing_next_options,
        "missing_next_futures": missing_next_futures,
        "needs_refresh": needs_refresh,
        "status": status,
    }


def cache_file_needs_refresh(
    cache: dict[str, Any] | None,
    *,
    membership_stale: bool,
    cache_exists: bool,
    today: date | None = None,
    option_expiry_count: int = 1,
    future_contract_count: int = 2,
) -> bool:
    """True when ingest startup should rebuild the instrument master. Does not call Kite."""
    if membership_stale or not cache_exists or cache is None:
        return True
    health = instrument_cache_health(
        cache,
        today=today,
        option_expiry_count=option_expiry_count,
        future_contract_count=future_contract_count,
    )
    return bool(health["needs_refresh"])


def lookup_instrument_meta(cache: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    want = symbol.strip()
    want_u = want.upper()
    for section in ("equity_depth", "equity_quote", "index", "equity"):
        mapping = cache.get(section) or {}
        if want in mapping:
            return mapping[want]
        for key, row in mapping.items():
            if str(key).upper() == want_u:
                return row
            if str(row.get("tradingsymbol") or "").upper() == want_u:
                return row
    for row in list(cache.get("options") or []) + list(cache.get("futures") or []):
        if str(row.get("tradingsymbol") or "").upper() == want_u:
            return row
    return None


def _rows_for_underlying(rows: list[dict[str, Any]], name: str) -> list[dict[str, Any]]:
    want = name.upper()
    return [r for r in rows if str(r.get("name") or "").upper() == want]


def _type_count(rows: list[dict[str, Any]], instrument_type: str) -> int:
    want = instrument_type.upper()
    return sum(1 for r in rows if str(r.get("instrument_type") or "").upper() == want)


def coverage_from_cache(cache: dict[str, Any], membership: dict[str, Any] | None = None) -> dict[str, Any]:
    nifty100 = list((membership or {}).get("nifty100") or [])
    depth = set((cache.get("equity_depth") or {}).keys())
    quote = set((cache.get("equity_quote") or {}).keys())
    missing_depth = [s for s in nifty100 if s not in depth]
    options = list(cache.get("options") or [])
    futures = list(cache.get("futures") or [])
    index = cache.get("index") or {}
    nifty_opt = _rows_for_underlying(options, "NIFTY")
    bank_opt = _rows_for_underlying(options, "BANKNIFTY")
    nifty_fut = _rows_for_underlying(futures, "NIFTY")
    bank_fut = _rows_for_underlying(futures, "BANKNIFTY")
    return {
        "nifty100_listed": len(nifty100),
        "subscribed_equity_depth": len(depth),
        "subscribed_equity_quote": len(quote),
        "nifty100_missing_from_depth": missing_depth,
        "index_symbols": sorted(index.keys()),
        "nifty_spot": "NIFTY 50" in index,
        "banknifty_spot": "NIFTY BANK" in index,
        "options_subscribed": len(options),
        "futures_subscribed": len(futures),
        "nifty_options": len(nifty_opt),
        "nifty_option_ce": _type_count(nifty_opt, "CE"),
        "nifty_option_pe": _type_count(nifty_opt, "PE"),
        "nifty_option_expiries": sorted({str(o.get("expiry")) for o in nifty_opt if o.get("expiry")}),
        "nifty_option_strikes": sorted({float(o["strike"]) for o in nifty_opt if o.get("strike") is not None}),
        "banknifty_options": len(bank_opt),
        "banknifty_option_ce": _type_count(bank_opt, "CE"),
        "banknifty_option_pe": _type_count(bank_opt, "PE"),
        "banknifty_option_expiries": sorted({str(o.get("expiry")) for o in bank_opt if o.get("expiry")}),
        "banknifty_option_strikes": sorted({float(o["strike"]) for o in bank_opt if o.get("strike") is not None}),
        "nifty_futures": len(nifty_fut),
        "nifty_future_expiries": sorted({str(f.get("expiry")) for f in nifty_fut if f.get("expiry")}),
        "banknifty_futures": len(bank_fut),
        "banknifty_future_expiries": sorted({str(f.get("expiry")) for f in bank_fut if f.get("expiry")}),
        "option_expiries": sorted({str(o.get("expiry")) for o in options if o.get("expiry")}),
        "future_expiries": sorted({str(f.get("expiry")) for f in futures if f.get("expiry")}),
        "option_underlyings": sorted({str(o.get("name")) for o in options if o.get("name")}),
        "fo_eligible_nifty100": list(cache.get("fo_eligible_nifty100") or []),
        "stock_options": sum(
            1
            for o in options
            if str(o.get("name") or "").upper() not in {"NIFTY", "BANKNIFTY"}
        ),
        "stock_options_full": sum(
            1
            for o in options
            if str(o.get("name") or "").upper() not in {"NIFTY", "BANKNIFTY"}
            and str(o.get("subscribe_mode") or "full").lower() != "quote"
        ),
        "stock_options_quote": sum(
            1
            for o in options
            if str(o.get("name") or "").upper() not in {"NIFTY", "BANKNIFTY"}
            and str(o.get("subscribe_mode") or "").lower() == "quote"
        ),
        "stock_futures": sum(
            1
            for f in futures
            if str(f.get("name") or "").upper() not in {"NIFTY", "BANKNIFTY"}
        ),
        "note": (
            "Index options/futures plus optional NIFTY100∩F&O stock derivatives. "
            "Configured universe is not the live ingest universe until the next restart."
        ),
    }
