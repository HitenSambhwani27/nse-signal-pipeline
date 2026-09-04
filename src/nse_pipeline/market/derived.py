"""Deterministic book/trade deltas from observed tick fields. No classifiers."""

from __future__ import annotations

from typing import Sequence


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return number


def _as_int(value: object) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    return int(number)


def best_bid(
    prices: Sequence[float] | None,
    quantities: Sequence[int] | None,
) -> tuple[float | None, int | None]:
    return _first_live_level(prices, quantities)


def best_ask(
    prices: Sequence[float] | None,
    quantities: Sequence[int] | None,
) -> tuple[float | None, int | None]:
    return _first_live_level(prices, quantities)


def _first_live_level(
    prices: Sequence[float] | None,
    quantities: Sequence[int] | None,
) -> tuple[float | None, int | None]:
    if not prices or not quantities:
        return None, None
    for price, qty in zip(prices, quantities):
        p = _as_float(price)
        q = _as_int(qty)
        if p is not None and p > 0 and q is not None and q > 0:
            return p, q
    return None, None


def bid_depth_n(quantities: Sequence[int] | None, n: int = 5) -> int:
    if not quantities:
        return 0
    total = 0
    for qty in list(quantities)[:n]:
        parsed = _as_int(qty)
        if parsed is not None and parsed > 0:
            total += parsed
    return total


def ask_depth_n(quantities: Sequence[int] | None, n: int = 5) -> int:
    return bid_depth_n(quantities, n=n)


def spread(best_bid_price: float | None, best_ask_price: float | None) -> float | None:
    if best_bid_price is None or best_ask_price is None:
        return None
    if best_bid_price <= 0 or best_ask_price <= 0:
        return None
    return float(best_ask_price) - float(best_bid_price)


def mid_price(best_bid_price: float | None, best_ask_price: float | None) -> float | None:
    if best_bid_price is None or best_ask_price is None:
        return None
    return (float(best_bid_price) + float(best_ask_price)) / 2.0


def depth_imbalance(bid_depth: int, ask_depth: int) -> float | None:
    total = int(bid_depth) + int(ask_depth)
    if total <= 0:
        return None
    return (int(bid_depth) - int(ask_depth)) / float(total)


def numeric_delta(current: float | int | None, previous: float | int | None) -> float | None:
    """current - previous. None if either side missing. Session reset (curr < prev for volume) → None."""
    if current is None or previous is None:
        return None
    try:
        cur = float(current)
        prev = float(previous)
    except (TypeError, ValueError):
        return None
    return cur - prev
