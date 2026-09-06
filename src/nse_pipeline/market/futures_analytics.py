"""Futures quotes, basis, and price/OI interpretation. Not trader-position proof."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

DEFAULT_BASIS_MAX_AGE_SECONDS = 10.0


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _ratio(numer: float | None, denom: float | None) -> float | None:
    if numer is None or denom is None or denom == 0:
        return None
    return float(numer) / float(denom)


def basis(future_price: float | None, spot_price: float | None) -> float | None:
    if future_price is None or spot_price is None:
        return None
    return float(future_price) - float(spot_price)


def basis_pct(future_price: float | None, spot_price: float | None) -> float | None:
    gap = basis(future_price, spot_price)
    if gap is None or spot_price in (None, 0):
        return None
    return (gap / float(spot_price)) * 100.0


def parse_observation_ts(value: Any) -> datetime | None:
    """Parse an observation timestamp. Does not substitute wall-clock time."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def observation_age_seconds(first: Any, second: Any) -> float | None:
    left = parse_observation_ts(first)
    right = parse_observation_ts(second)
    if left is None or right is None:
        return None
    return abs((left - right).total_seconds())


def basis_freshness(
    *,
    future_price: float | None,
    spot_price: float | None,
    futures_as_of: Any,
    spot_as_of: Any,
    max_age_seconds: float = DEFAULT_BASIS_MAX_AGE_SECONDS,
) -> dict[str, Any]:
    """
    Match latest futures and latest spot by observation freshness, not equality.

    Status:
      missing — either price is absent (no invented prices)
      fresh   — both prices exist and |fut_ts - spot_ts| <= tolerance
      stale   — both prices exist but timestamps differ by more than tolerance,
                or timestamps cannot be compared
    """
    age = observation_age_seconds(futures_as_of, spot_as_of)
    if future_price is None or spot_price is None:
        return {
            "basis": None,
            "basis_pct": None,
            "basis_status": "missing",
            "data_age_seconds": age,
        }
    if age is None or age > float(max_age_seconds):
        return {
            "basis": None,
            "basis_pct": None,
            "basis_status": "stale",
            "data_age_seconds": age,
        }
    return {
        "basis": basis(future_price, spot_price),
        "basis_pct": basis_pct(future_price, spot_price),
        "basis_status": "fresh",
        "data_age_seconds": age,
    }


def price_oi_interpretation(
    price_change: float | None, oi_change: float | None
) -> dict[str, Any]:
    """
    Standard four-state table. Interpretation of price vs OI, not proof of
    individual trader positions.
    """
    if price_change is None or oi_change is None:
        return {
            "label": None,
            "pattern": None,
            "kind": "inferred",
            "note": "insufficient price/OI change",
        }
    px_up = price_change > 0
    px_down = price_change < 0
    oi_up = oi_change > 0
    oi_down = oi_change < 0
    if px_up and oi_up:
        label, pattern = "long_buildup", "price_up_oi_up"
    elif px_up and oi_down:
        label, pattern = "short_covering", "price_up_oi_down"
    elif px_down and oi_up:
        label, pattern = "short_buildup", "price_down_oi_up"
    elif px_down and oi_down:
        label, pattern = "long_unwinding", "price_down_oi_down"
    else:
        label, pattern = "neutral", "unchanged"
    return {
        "label": label,
        "pattern": pattern,
        "kind": "inferred",
        "note": "Descriptive market-structure label, not a position or identity claim.",
    }


def futures_snapshot(
    quote: dict[str, Any] | None,
    *,
    meta: dict[str, Any] | None = None,
    spot: float | None,
    spot_as_of: str | None,
    spot_symbol: str | None,
    max_age_seconds: float = DEFAULT_BASIS_MAX_AGE_SECONDS,
) -> dict[str, Any]:
    q = quote or {}
    meta = meta or {}
    last = _f(q.get("last_price"))
    oi = _f(q.get("oi"))
    oi_change = _f(q.get("oi_delta") if "oi_delta" in q else q.get("oi_change"))
    prev_oi = oi - oi_change if oi is not None and oi_change is not None else None
    px_change = _f(q.get("price_delta") if "price_delta" in q else q.get("change"))
    fut_ts = q.get("timestamp")
    freshness = basis_freshness(
        future_price=last,
        spot_price=spot,
        futures_as_of=fut_ts,
        spot_as_of=spot_as_of,
        max_age_seconds=max_age_seconds,
    )
    basis_val = freshness["basis"]
    basis_p = freshness["basis_pct"]
    return {
        "symbol": q.get("symbol") or meta.get("tradingsymbol"),
        "underlying": meta.get("name"),
        "expiry": meta.get("expiry"),
        "lot_size": meta.get("lot_size"),
        "tick_size": meta.get("tick_size"),
        "ltp": last,
        "price_change": px_change,
        "price_change_pct": _ratio(px_change, last - px_change if last is not None and px_change is not None else None),
        "volume": q.get("volume"),
        "volume_delta": q.get("volume_delta"),
        "oi": q.get("oi"),
        "oi_change": oi_change,
        "oi_change_pct": _ratio(oi_change, prev_oi),
        "last_quantity": q.get("last_quantity"),
        "best_bid": q.get("best_bid_price", q.get("best_bid")),
        "best_ask": q.get("best_ask_price", q.get("best_ask")),
        "spread": q.get("spread"),
        "mid_price": q.get("mid_price"),
        "bid_depth_5": q.get("bid_depth_5"),
        "ask_depth_5": q.get("ask_depth_5"),
        "depth_imbalance": q.get("depth_imbalance"),
        "spot_symbol": spot_symbol,
        "spot": spot,
        "spot_as_of": spot_as_of,
        "futures_as_of": fut_ts,
        "data_age_seconds": freshness["data_age_seconds"],
        "basis": basis_val,
        "basis_pct": basis_p,
        "basis_status": freshness["basis_status"],
        "price_oi": price_oi_interpretation(px_change, oi_change),
        "kind": "observed+derived+inferred",
    }
