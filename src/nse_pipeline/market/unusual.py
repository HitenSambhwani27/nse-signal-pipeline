"""Deterministic unusual-activity score. Not ML, HFT, or participant detection."""

from __future__ import annotations

from typing import Any


def unusual_activity_score(
    *,
    large_trade: str | None,
    trade_notional_percentile: float | None,
    volume_level: str | None,
    volume_ratio: float | None,
    activity_level: str | None,
    oi_delta: float | None,
    liquidity_events: list[str],
    aggressor: str | None,
    high_score: float,
) -> dict[str, Any]:
    score = 0.0
    reasons: list[str] = []
    if large_trade == "extreme":
        score += 30
        reasons.append("trade notional in extreme percentile")
    elif large_trade == "very_large":
        score += 22
        reasons.append("trade notional above very-large percentile")
    elif large_trade == "large":
        score += 14
        reasons.append("trade notional above large percentile")
    if trade_notional_percentile is not None and trade_notional_percentile >= 99:
        reasons.append(f"trade size above {trade_notional_percentile:.0f}th percentile")
    if volume_level == "extreme":
        score += 25
    elif volume_level == "burst":
        score += 18
    elif volume_level == "elevated":
        score += 10
    if volume_ratio is not None and volume_ratio >= 2:
        reasons.append(f"volume rate {volume_ratio:.1f}x time-of-day baseline")
    if activity_level in {"burst", "extreme"}:
        score += 15
        reasons.append("observation frequency above burst threshold")
    elif activity_level == "elevated":
        score += 8
    if oi_delta is not None and abs(oi_delta) > 0:
        score += 6
        reasons.append("OI changed versus previous observation")
    if "ask_depth_shock" in liquidity_events:
        score += 12
        reasons.append("ask depth dropped sharply")
    if "bid_depth_shock" in liquidity_events:
        score += 12
        reasons.append("bid depth dropped sharply")
    if "liquidity_withdrawn" in liquidity_events:
        score += 8
        reasons.append("displayed liquidity withdrawn")
    if "spread_widening" in liquidity_events:
        score += 6
        reasons.append("spread widened")
    if aggressor == "aggressive_buy_proxy":
        score += 4
        reasons.append("last price near/at displayed ask (buy-pressure proxy)")
    if aggressor == "aggressive_sell_proxy":
        score += 4
        reasons.append("last price near/at displayed bid (sell-pressure proxy)")
    score = min(100.0, score)
    if score >= high_score:
        level = "high"
    elif score >= high_score * 0.5:
        level = "elevated"
    elif score > 0:
        level = "watch"
    else:
        level = "normal"
    return {
        "activity_level": level,
        "activity_score": round(score, 1),
        "reasons": reasons,
        "kind": "inferred",
        "note": (
            "Deterministic score from observable/derived metrics. "
            "Not a machine-learning probability or a participant-identity claim."
        ),
    }
