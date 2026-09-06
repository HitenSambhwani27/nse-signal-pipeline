"""Market-activity observations. Not unique exchange trades."""

from __future__ import annotations

from typing import Any, Sequence


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


def trade_notional(price: float | None, last_quantity: float | None) -> float | None:
    if price is None or last_quantity is None:
        return None
    return float(price) * float(last_quantity)


def trade_size_lots(last_quantity: float | None, lot_size: float | None) -> float | None:
    if last_quantity is None or lot_size in (None, 0):
        return None
    return float(last_quantity) / float(lot_size)


def percentile_rank(value: float | None, sample: Sequence[float]) -> float | None:
    if value is None or not sample:
        return None
    ordered = sorted(float(x) for x in sample)
    if not ordered:
        return None
    below = sum(1 for x in ordered if x <= float(value))
    return 100.0 * below / len(ordered)


def classify_by_percentile(
    percentile: float | None,
    *,
    large: float,
    very_large: float,
    extreme: float,
    baseline_available: bool,
) -> str | None:
    if not baseline_available or percentile is None:
        return None
    if percentile >= extreme:
        return "extreme"
    if percentile >= very_large:
        return "very_large"
    if percentile >= large:
        return "large"
    return "normal"


def classify_ratio(
    ratio: float | None,
    *,
    elevated: float,
    burst: float,
    extreme: float,
    baseline_available: bool,
) -> str | None:
    if not baseline_available or ratio is None:
        return None
    if ratio >= extreme:
        return "extreme"
    if ratio >= burst:
        return "burst"
    if ratio >= elevated:
        return "elevated"
    return "normal"


def tod_bucket(timestamp_iso: str | None, bucket_minutes: int = 15) -> str | None:
    if not timestamp_iso:
        return None
    text = timestamp_iso[11:16] if len(timestamp_iso) >= 16 else ""
    if len(text) < 5 or text[2] != ":":
        return None
    hour = int(text[0:2])
    minute = int(text[3:5])
    total = hour * 60 + minute
    floored = (total // bucket_minutes) * bucket_minutes
    return f"{floored // 60:02d}:{floored % 60:02d}"


def market_activity_row(
    quote: dict[str, Any],
    *,
    lot_size: float | None,
    notional_sample: Sequence[float],
    size_sample: Sequence[float],
    volume_rate: float | None,
    baseline_volume_rate: float | None,
    activity_rate: float | None,
    baseline_activity_rate: float | None,
    thresholds: dict[str, float],
    baseline_min: int,
) -> dict[str, Any]:
    price = _f(quote.get("last_price"))
    qty = _f(quote.get("last_quantity"))
    notional = trade_notional(price, qty)
    lots = trade_size_lots(qty, lot_size)
    baseline_ok = len(notional_sample) >= baseline_min and len(size_sample) >= baseline_min
    n_pct = percentile_rank(notional, notional_sample) if baseline_ok else None
    s_pct = percentile_rank(qty, size_sample) if baseline_ok else None
    vol_ratio = (
        volume_rate / baseline_volume_rate
        if volume_rate is not None and baseline_volume_rate not in (None, 0)
        else None
    )
    act_ratio = (
        activity_rate / baseline_activity_rate
        if activity_rate is not None and baseline_activity_rate not in (None, 0)
        else None
    )
    vol_base_ok = baseline_volume_rate is not None
    act_base_ok = baseline_activity_rate is not None
    return {
        "timestamp": quote.get("timestamp"),
        "instrument": quote.get("symbol"),
        "price": price,
        "last_quantity": qty,
        "trade_notional": notional,
        "trade_size_lots": lots,
        "volume": quote.get("volume"),
        "volume_delta": quote.get("volume_delta"),
        "oi": quote.get("oi"),
        "oi_delta": quote.get("oi_delta") if "oi_delta" in quote else quote.get("oi_change"),
        "best_bid": quote.get("best_bid_price", quote.get("best_bid")),
        "best_ask": quote.get("best_ask_price", quote.get("best_ask")),
        "mid": quote.get("mid_price"),
        "spread": quote.get("spread"),
        "bid_depth": quote.get("bid_depth_5"),
        "ask_depth": quote.get("ask_depth_5"),
        "depth_imbalance": quote.get("depth_imbalance"),
        "trade_notional_percentile": n_pct,
        "trade_size_percentile": s_pct,
        "large_trade": classify_by_percentile(
            n_pct,
            large=thresholds["large_trade_percentile"],
            very_large=thresholds["very_large_trade_percentile"],
            extreme=thresholds["extreme_trade_percentile"],
            baseline_available=baseline_ok,
        ),
        "volume_rate": volume_rate,
        "baseline_volume_rate": baseline_volume_rate,
        "volume_ratio": vol_ratio,
        "volume_level": classify_ratio(
            vol_ratio,
            elevated=thresholds["volume_elevated_ratio"],
            burst=thresholds["volume_burst_ratio"],
            extreme=thresholds["volume_extreme_ratio"],
            baseline_available=vol_base_ok,
        ),
        "activity_rate": activity_rate,
        "baseline_activity_rate": baseline_activity_rate,
        "activity_ratio": act_ratio,
        "activity_level": classify_ratio(
            act_ratio,
            elevated=thresholds["activity_elevated_ratio"],
            burst=thresholds["activity_burst_ratio"],
            extreme=thresholds["volume_extreme_ratio"],
            baseline_available=act_base_ok,
        ),
        "baseline_available": baseline_ok,
        "tod_bucket": tod_bucket(quote.get("timestamp"), int(thresholds.get("tod_bucket_minutes", 15))),
        "kind": "market_data_observation",
        "note": "last_quantity is the latest observed trade size on this snapshot, not a unique exchange trade id.",
    }
