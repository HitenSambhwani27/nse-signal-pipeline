"""Normalize a Kite ticker dict into a tick *observation* (not a unique trade)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from nse_pipeline.market.timestamps import parse_kite_datetime
from nse_pipeline.storage.schemas import NormalizedTick

_IST = ZoneInfo("Asia/Kolkata")


def parse_depth_side(
    levels: list[dict[str, Any]] | None,
) -> tuple[list[float], list[int], list[int]]:
    prices: list[float] = []
    quantities: list[int] = []
    orders: list[int] = []
    if not levels:
        return prices, quantities, orders
    for level in levels:
        prices.append(float(level.get("price", 0.0) or 0.0))
        quantities.append(int(level.get("quantity", 0) or 0))
        orders.append(int(level.get("orders", 0) or 0))
    return prices, quantities, orders


def _as_datetime(value: Any) -> datetime | None:
    """Kite tick timestamps only. Naive values are IST wall-clock, not UTC."""
    return parse_kite_datetime(value)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_tick(
    tick: dict[str, Any],
    token_symbol_map: dict[int, str],
    exchange_by_token: dict[int, str],
    *,
    ingested_at: datetime | None = None,
) -> NormalizedTick | None:
    """
    Convert a Kite on_ticks dict into a NormalizedTick observation.

    One callback is not assumed to be one unique exchange trade.
    last_quantity is the last observed traded quantity on this packet.
    volume is Kite's cumulative session volume_traded.
    total_buy_quantity / total_sell_quantity are visible book totals, not fills.
    """
    token = tick.get("instrument_token")
    if token is None:
        return None

    token_int = int(token)
    symbol = token_symbol_map.get(token_int)
    if not symbol:
        return None

    ingested = ingested_at or datetime.now(timezone.utc)
    exchange_ts = _as_datetime(tick.get("exchange_timestamp")) or _as_datetime(
        tick.get("timestamp")
    )
    event_ts = exchange_ts if exchange_ts is not None else ingested
    last_trade_time = _as_datetime(tick.get("last_trade_time"))
    trade_date = event_ts.astimezone(_IST).date().isoformat()

    depth = tick.get("depth") or {}
    bid_prices, bid_quantities, bid_orders = parse_depth_side(depth.get("buy"))
    ask_prices, ask_quantities, ask_orders = parse_depth_side(depth.get("sell"))

    ohlc = tick.get("ohlc") if isinstance(tick.get("ohlc"), dict) else {}

    oi_value = tick.get("oi")
    oi_int = int(oi_value) if oi_value is not None else None

    return NormalizedTick(
        timestamp=event_ts,
        instrument_token=token_int,
        symbol=symbol,
        exchange=exchange_by_token.get(token_int, "NSE"),
        last_price=float(tick.get("last_price", 0.0) or 0.0),
        volume=int(tick.get("volume_traded", tick.get("volume", 0)) or 0),
        last_quantity=int(tick.get("last_traded_quantity", tick.get("last_quantity", 0)) or 0),
        average_price=float(tick.get("average_traded_price", tick.get("average_price", 0.0)) or 0.0),
        oi=oi_int,
        bid_prices=bid_prices,
        bid_quantities=bid_quantities,
        bid_orders=bid_orders,
        ask_prices=ask_prices,
        ask_quantities=ask_quantities,
        ask_orders=ask_orders,
        ingested_at=ingested,
        exchange_timestamp=exchange_ts,
        last_trade_time=last_trade_time,
        trade_date=trade_date,
        total_buy_quantity=_int_or_none(tick.get("total_buy_quantity")),
        total_sell_quantity=_int_or_none(tick.get("total_sell_quantity")),
        ohlc_open=_float_or_none(ohlc.get("open")) if ohlc else None,
        ohlc_high=_float_or_none(ohlc.get("high")) if ohlc else None,
        ohlc_low=_float_or_none(ohlc.get("low")) if ohlc else None,
        ohlc_close=_float_or_none(ohlc.get("close")) if ohlc else None,
    )
