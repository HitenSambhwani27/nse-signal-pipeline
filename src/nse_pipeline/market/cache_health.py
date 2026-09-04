"""Read-only instrument-cache diagnostics. Does not refresh or call Kite."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any


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


def instrument_cache_health(cache: dict[str, Any], *, today: date | None = None) -> dict[str, Any]:
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
    needs_refresh = bool(expired_options or expired_futures)
    return {
        "updated_at": cache.get("updated_at"),
        "counts": cache.get("counts") or {},
        "expired_option_count": len(expired_options),
        "expired_future_count": len(expired_futures),
        "needs_refresh": needs_refresh,
        "status": "stale_expiries" if needs_refresh else "ok",
    }


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
        "note": "Equity F&O eligibility is out of scope; this repo subscribes index options only.",
    }
