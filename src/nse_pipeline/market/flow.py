"""Order-flow proxies from observable book and last price. No aggressor identity."""

from __future__ import annotations

from typing import Any


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


def aggressive_side_proxy(
    last_price: float | None,
    best_bid: float | None,
    best_ask: float | None,
    *,
    tolerance_bps: float,
) -> dict[str, Any]:
    """
    Compare last_price to displayed bid/ask.

    last_price at/above ask (within tolerance) → aggressive_buy_proxy
    last_price at/below bid → aggressive_sell_proxy
    otherwise unknown.

    This is not executed buy/sell volume and not a real aggressor flag.
    """
    if last_price is None or best_bid is None or best_ask is None:
        return {
            "label": None,
            "kind": "inferred",
            "note": "bid/ask or last_price missing",
        }
    mid = (float(best_bid) + float(best_ask)) / 2.0
    tol = abs(mid) * (float(tolerance_bps) / 10_000.0) if mid else 0.0
    if last_price >= float(best_ask) - tol:
        label = "aggressive_buy_proxy"
    elif last_price <= float(best_bid) + tol:
        label = "aggressive_sell_proxy"
    else:
        label = "unknown"
    return {
        "label": label,
        "last_price": last_price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "tolerance_bps": tolerance_bps,
        "kind": "inferred",
        "note": "Buy/sell-pressure proxy from trade price vs displayed book, not actual aggressor identity.",
    }


def displayed_depth_changes(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    prev = previous or {}
    bid_now = _f(current.get("bid_depth_5")) or 0.0
    ask_now = _f(current.get("ask_depth_5")) or 0.0
    bid_prev = _f(prev.get("bid_depth_5"))
    ask_prev = _f(prev.get("ask_depth_5"))
    if bid_prev is None and ask_prev is None:
        return {
            "bid_quantity_added": None,
            "bid_quantity_removed": None,
            "ask_quantity_added": None,
            "ask_quantity_removed": None,
            "depth_imbalance_change": None,
            "spread_change": None,
            "best_bid_movement": None,
            "best_ask_movement": None,
            "kind": "derived",
            "note": "observed displayed-depth changes; not discrete exchange orders",
        }
    bid_prev = bid_prev or 0.0
    ask_prev = ask_prev or 0.0
    bid_delta = bid_now - bid_prev
    ask_delta = ask_now - ask_prev
    imb_now = _f(current.get("depth_imbalance"))
    imb_prev = _f(prev.get("depth_imbalance"))
    sp_now = _f(current.get("spread"))
    sp_prev = _f(prev.get("spread"))
    bb_now = _f(current.get("best_bid_price", current.get("best_bid")))
    bb_prev = _f(prev.get("best_bid_price", prev.get("best_bid")))
    ba_now = _f(current.get("best_ask_price", current.get("best_ask")))
    ba_prev = _f(prev.get("best_ask_price", prev.get("best_ask")))
    return {
        "bid_quantity_added": bid_delta if bid_delta > 0 else 0.0,
        "bid_quantity_removed": -bid_delta if bid_delta < 0 else 0.0,
        "ask_quantity_added": ask_delta if ask_delta > 0 else 0.0,
        "ask_quantity_removed": -ask_delta if ask_delta < 0 else 0.0,
        "depth_imbalance_change": (imb_now - imb_prev) if imb_now is not None and imb_prev is not None else None,
        "spread_change": (sp_now - sp_prev) if sp_now is not None and sp_prev is not None else None,
        "best_bid_movement": (bb_now - bb_prev) if bb_now is not None and bb_prev is not None else None,
        "best_ask_movement": (ba_now - ba_prev) if ba_now is not None and ba_prev is not None else None,
        "kind": "derived",
        "note": "observed displayed-depth changes; not discrete exchange orders",
    }


def liquidity_events(
    changes: dict[str, Any],
    *,
    depth_shock_pct: float,
    spread_widen_pct: float,
    previous: dict[str, Any] | None,
) -> list[str]:
    events: list[str] = []
    prev = previous or {}
    bid_prev = _f(prev.get("bid_depth_5")) or 0.0
    ask_prev = _f(prev.get("ask_depth_5")) or 0.0
    added = (changes.get("bid_quantity_added") or 0) + (changes.get("ask_quantity_added") or 0)
    removed = (changes.get("bid_quantity_removed") or 0) + (changes.get("ask_quantity_removed") or 0)
    if added > 0 and added >= removed:
        events.append("liquidity_added")
    if removed > 0 and removed > added:
        events.append("liquidity_withdrawn")
    if bid_prev > 0 and (changes.get("bid_quantity_removed") or 0) >= bid_prev * (depth_shock_pct / 100.0):
        events.append("bid_depth_shock")
    if ask_prev > 0 and (changes.get("ask_quantity_removed") or 0) >= ask_prev * (depth_shock_pct / 100.0):
        events.append("ask_depth_shock")
    sp_prev = _f(prev.get("spread"))
    sp_change = _f(changes.get("spread_change"))
    if sp_prev not in (None, 0) and sp_change is not None:
        if sp_change >= abs(sp_prev) * (spread_widen_pct / 100.0):
            events.append("spread_widening")
        if sp_change <= -abs(sp_prev) * (spread_widen_pct / 100.0):
            events.append("spread_compression")
    return events


def price_impact_note(
    notional: float | None,
    price_change: float | None,
    events: list[str],
    large_trade: str | None,
) -> str | None:
    if large_trade not in {"large", "very_large", "extreme"}:
        return None
    px = abs(price_change) if price_change is not None else None
    if px is None:
        return None
    if px < 1e-9:
        text = "large observed trade + small price response"
    else:
        text = "large observed trade + strong price response"
    if "liquidity_withdrawn" in events:
        text += " + liquidity withdrawal"
    if "bid_depth_shock" in events or "ask_depth_shock" in events:
        text += " + depth imbalance shift"
    return text
