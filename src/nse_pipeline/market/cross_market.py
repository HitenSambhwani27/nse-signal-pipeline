"""Descriptive spot/futures/options relationships. Not prediction probabilities."""

from __future__ import annotations

from typing import Any


def _sign(value: float | None) -> str | None:
    if value is None:
        return None
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "unchanged"


def describe_cross_market(
    *,
    spot_change: float | None,
    future_oi_change: float | None,
    basis: float | None,
    prev_basis: float | None,
    call_oi_change: float | None,
    put_oi_change: float | None,
    volume_level: str | None,
    liquidity_events: list[str],
) -> list[str]:
    notes: list[str] = []
    spot = _sign(spot_change)
    foi = _sign(future_oi_change)
    if spot == "up" and foi == "up":
        notes.append("spot ↑ + future OI ↑")
    if spot == "up" and foi == "down":
        notes.append("spot ↑ + future OI ↓")
    if spot == "down" and foi == "up":
        notes.append("spot ↓ + future OI ↑")
    if spot == "down" and foi == "down":
        notes.append("spot ↓ + future OI ↓")
    if basis is not None and prev_basis is not None:
        if abs(basis) > abs(prev_basis):
            notes.append("future basis widening")
        elif abs(basis) < abs(prev_basis):
            notes.append("future basis narrowing")
    if call_oi_change is not None and call_oi_change > 0:
        notes.append("call OI build-up")
    if call_oi_change is not None and call_oi_change < 0:
        notes.append("call OI unwinding")
    if put_oi_change is not None and put_oi_change > 0:
        notes.append("put OI build-up")
    if put_oi_change is not None and put_oi_change < 0:
        notes.append("put OI unwinding")
    if volume_level in {"burst", "extreme"} and spot in {"up", "down"}:
        notes.append("volume burst + price movement")
    if any(e.endswith("_shock") for e in liquidity_events) and spot in {"up", "down"}:
        notes.append("depth shock + price movement")
    return notes
