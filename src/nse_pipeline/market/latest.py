"""Latest per-instrument snapshot. One row in SQLite — not a tick history table."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from nse_pipeline.market.derived import (
    ask_depth_n,
    best_ask,
    best_bid,
    bid_depth_n,
    depth_imbalance,
    mid_price,
    numeric_delta,
    spread,
)
from nse_pipeline.storage.schemas import NormalizedTick


def _depth_levels(prices: Any, quantities: Any, orders: Any = None) -> list[list[float | int]] | None:
    if not prices or not quantities:
        return None
    px = list(prices)
    qty = list(quantities)
    ords = list(orders) if orders else [None] * len(px)
    out: list[list[float | int]] = []
    for i, price in enumerate(px):
        if price is None:
            continue
        q = qty[i] if i < len(qty) else None
        o = ords[i] if i < len(ords) else None
        if q is None and o is None:
            out.append([float(price)])
        elif o is None:
            out.append([float(price), int(q)])
        else:
            out.append([float(price), int(q or 0), int(o)])
    return out or None


def _parse_snapshot_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    if isinstance(value, str) and value.strip():
        try:
            ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    return None


def snapshot_from_tick(
    tick: NormalizedTick,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    bid_px, bid_qty = best_bid(tick.bid_prices, tick.bid_quantities)
    ask_px, ask_qty = best_ask(tick.ask_prices, tick.ask_quantities)
    bid5 = bid_depth_n(tick.bid_quantities, n=5)
    ask5 = ask_depth_n(tick.ask_quantities, n=5)
    prev = previous or {}
    volume_delta = numeric_delta(tick.volume, prev.get("volume"))
    if volume_delta is not None and volume_delta < 0:
        volume_delta = None
    oi_delta = numeric_delta(tick.oi, prev.get("oi"))
    price_delta = numeric_delta(tick.last_price, prev.get("last_price"))
    ts = tick.timestamp
    ingested = tick.ingested_at or datetime.now(timezone.utc)
    ltt = tick.last_trade_time
    return {
        "instrument_token": tick.instrument_token,
        "symbol": tick.symbol,
        "exchange": tick.exchange,
        "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
        "ingested_at": ingested.isoformat() if hasattr(ingested, "isoformat") else str(ingested),
        "last_price": tick.last_price,
        "last_quantity": tick.last_quantity,
        "volume": tick.volume,
        "average_price": tick.average_price,
        "oi": tick.oi,
        "total_buy_quantity": tick.total_buy_quantity,
        "total_sell_quantity": tick.total_sell_quantity,
        "best_bid_price": bid_px,
        "best_bid_quantity": bid_qty,
        "best_ask_price": ask_px,
        "best_ask_quantity": ask_qty,
        "bid_depth_5": bid5,
        "ask_depth_5": ask5,
        "spread": spread(bid_px, ask_px),
        "mid_price": mid_price(bid_px, ask_px),
        "depth_imbalance": depth_imbalance(bid5, ask5),
        "volume_delta": int(volume_delta) if volume_delta is not None else None,
        "oi_delta": int(oi_delta) if oi_delta is not None else None,
        "price_delta": price_delta,
        "ohlc_open": tick.ohlc_open,
        "ohlc_high": tick.ohlc_high,
        "ohlc_low": tick.ohlc_low,
        "ohlc_close": tick.ohlc_close,
        "last_trade_time": ltt.isoformat() if ltt is not None and hasattr(ltt, "isoformat") else (str(ltt) if ltt else None),
        "bid_levels": _depth_levels(tick.bid_prices, tick.bid_quantities, getattr(tick, "bid_orders", None)),
        "ask_levels": _depth_levels(tick.ask_prices, tick.ask_quantities, getattr(tick, "ask_orders", None)),
    }


class LatestQuoteTracker:
    """In-process latest map. Persist to SQLite in batches, never per-tick."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latest: dict[int, dict[str, Any]] = {}
        self._dirty: set[int] = set()

    def observe(self, tick: NormalizedTick) -> dict[str, Any]:
        with self._lock:
            previous = self._latest.get(tick.instrument_token)
            if previous is not None:
                prev_ts = _parse_snapshot_ts(previous.get("timestamp"))
                if prev_ts is not None and tick.timestamp < prev_ts:
                    return previous
            snap = snapshot_from_tick(tick, previous)
            self._latest[tick.instrument_token] = snap
            self._dirty.add(tick.instrument_token)
            return snap

    def drain_dirty(self) -> list[dict[str, Any]]:
        with self._lock:
            tokens = list(self._dirty)
            self._dirty.clear()
            return [self._latest[t] for t in tokens if t in self._latest]


def public_quote(row: dict[str, Any], *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """API DTO. Book totals are visible depth, not executed buy/sell."""
    out = {
        "symbol": row.get("symbol"),
        "instrument_token": row.get("instrument_token"),
        "exchange": row.get("exchange"),
        "timestamp": row.get("timestamp"),
        "last_price": row.get("last_price"),
        "change": row.get("price_delta"),
        "volume": row.get("volume"),
        "volume_delta": row.get("volume_delta"),
        "oi": row.get("oi"),
        "oi_change": row.get("oi_delta"),
        "last_quantity": row.get("last_quantity"),
        "best_bid": row.get("best_bid_price"),
        "best_ask": row.get("best_ask_price"),
        "spread": row.get("spread"),
        "mid_price": row.get("mid_price"),
        "buy_quantity": row.get("total_buy_quantity"),
        "sell_quantity": row.get("total_sell_quantity"),
        "bid_depth_5": row.get("bid_depth_5"),
        "ask_depth_5": row.get("ask_depth_5"),
        "depth_imbalance": row.get("depth_imbalance"),
    }
    if meta:
        out["instrument_type"] = meta.get("instrument_type")
        out["underlying"] = meta.get("name") or meta.get("underlying")
        out["expiry"] = meta.get("expiry")
        out["strike"] = meta.get("strike")
        out["lot_size"] = meta.get("lot_size")
        out["tick_size"] = meta.get("tick_size")
        out["subscribe_mode"] = meta.get("subscribe_mode")
    return out
