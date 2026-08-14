"""
Stage 2A — equity feature engineering.

Depth path (Nifty 100, subscribe_mode=full): spread, depth ratio, OFI stub, VWAP.
Quote path (Nifty 500 \\ Nifty 100): LTP/volume/VWAP only — no depth/OFI.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def compute_ofi(
    *,
    bid_price_prev: float | None,
    bid_qty_prev: float | None,
    ask_price_prev: float | None,
    ask_qty_prev: float | None,
    bid_price: float | None,
    bid_qty: float | None,
    ask_price: float | None,
    ask_qty: float | None,
) -> float | None:
    """
    Cont-style order-flow imbalance stub (best-level).

    Replace with your Phase 1 OFI formula. Returns None if depth is unavailable.
    """
    if None in (
        bid_price_prev,
        bid_qty_prev,
        ask_price_prev,
        ask_qty_prev,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
    ):
        return None

    # Bid contribution
    if bid_price > bid_price_prev:
        bid_ofi = bid_qty
    elif bid_price == bid_price_prev:
        bid_ofi = bid_qty - bid_qty_prev
    else:
        bid_ofi = -bid_qty_prev

    # Ask contribution
    if ask_price < ask_price_prev:
        ask_ofi = ask_qty
    elif ask_price == ask_price_prev:
        ask_ofi = ask_qty - ask_qty_prev
    else:
        ask_ofi = -ask_qty_prev

    return float(bid_ofi - ask_ofi)


def _level1(prices: Any, qtys: Any) -> tuple[float | None, float | None]:
    try:
        if prices is None or qtys is None or len(prices) == 0 or len(qtys) == 0:
            return None, None
        return float(prices[0]), float(qtys[0])
    except (TypeError, ValueError, IndexError):
        return None, None


def _bucket_end(ts: pd.Timestamp, minutes: int) -> pd.Timestamp:
    # Floor to bucket then add minutes → exclusive end label as bucket close.
    floored = ts.floor(f"{minutes}min")
    return floored + pd.Timedelta(minutes=minutes)


def compute_equity_features(
    ticks: pd.DataFrame,
    *,
    symbol: str,
    subscribe_mode: str,
    bucket_minutes: int = 5,
    lot_size: int | None = None,
    source: str | None = None,
) -> list[dict[str, Any]]:
    """
    Build one feature row per time bucket from cleaned ticks.

    `subscribe_mode` is 'full' (depth) or 'quote'.
    Historical rows skip depth-dependent features (OFI, spread, depth ratio)
    rather than null-filling them.
    """
    if ticks.empty:
        return []

    df = ticks.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["bucket"] = df["timestamp"].map(lambda t: _bucket_end(t, bucket_minutes))

    inferred_source = source
    if inferred_source is None and "source" in df.columns:
        vals = {str(s) for s in df["source"].dropna().unique()}
        if vals == {"historical"}:
            inferred_source = "historical"
        elif "live" in vals:
            inferred_source = "live"
        else:
            inferred_source = "live"
    inferred_source = inferred_source or "live"
    skip_depth = inferred_source == "historical"
    completeness = "historical_partial" if skip_depth else "live_full"
    if not skip_depth and subscribe_mode != "full":
        completeness = "live_quote"

    # Session VWAP: historical uses typical price (H+L+C)/3 × volume;
    # live uses last_quantity / volume-delta × LTP.
    if skip_depth and {"high", "low", "close"}.issubset(df.columns):
        typical = (
            df["high"].astype(float) + df["low"].astype(float) + df["close"].astype(float)
        ) / 3.0
        trade_qty = df["volume"].fillna(0).astype(float).clip(lower=0)
        # Minute bars already store cumulative-or-bar volume depending on Kite;
        # prefer bar volume via diff when the series is non-decreasing.
        vol_delta = trade_qty.diff()
        if vol_delta.fillna(0).ge(0).all():
            trade_qty = vol_delta.fillna(trade_qty.iloc[0]).clip(lower=0)
        df["_trade_qty"] = trade_qty
        df["_pv"] = typical * df["_trade_qty"]
    else:
        qty = df["last_quantity"].fillna(0).astype(float).clip(lower=0)
        vol = df["volume"].fillna(0).astype(float)
        vol_delta = vol.diff().fillna(vol.iloc[0]).clip(lower=0)
        trade_qty = np.where(qty > 0, qty, vol_delta)
        df["_trade_qty"] = trade_qty
        df["_pv"] = df["last_price"].astype(float) * df["_trade_qty"]
    df["_cum_pv"] = df["_pv"].cumsum()
    df["_cum_qty"] = df["_trade_qty"].cumsum().replace(0, np.nan)
    df["vwap"] = df["_cum_pv"] / df["_cum_qty"]

    rows: list[dict[str, Any]] = []
    prev_bid_p = prev_bid_q = prev_ask_p = prev_ask_q = None

    ofi_vals: list[float | None] = []
    compute_depth = (not skip_depth) and subscribe_mode == "full"
    for _, row in df.iterrows():
        if not compute_depth:
            ofi_vals.append(None)
            continue
        bid_p, bid_q = _level1(row.get("bid_prices"), row.get("bid_quantities"))
        ask_p, ask_q = _level1(row.get("ask_prices"), row.get("ask_quantities"))
        ofi = compute_ofi(
            bid_price_prev=prev_bid_p,
            bid_qty_prev=prev_bid_q,
            ask_price_prev=prev_ask_p,
            ask_qty_prev=prev_ask_q,
            bid_price=bid_p,
            bid_qty=bid_q,
            ask_price=ask_p,
            ask_qty=ask_q,
        )
        ofi_vals.append(ofi)
        if bid_p is not None:
            prev_bid_p, prev_bid_q = bid_p, bid_q
        if ask_p is not None:
            prev_ask_p, prev_ask_q = ask_p, ask_q
    df["_ofi"] = ofi_vals

    track = "equity_depth" if subscribe_mode == "full" else "equity_quote"

    for bucket_ts, grp in df.groupby("bucket", sort=True):
        last = grp.iloc[-1]
        ltp = float(last["last_price"])
        vwap = float(last["vwap"]) if pd.notna(last["vwap"]) else None
        vwap_dev_bps = ((ltp / vwap - 1.0) * 10000.0) if vwap and vwap > 0 else None

        features: dict[str, Any] = {
            "ltp": ltp,
            "volume": int(last.get("volume") or 0),
            "vwap": vwap,
            "vwap_deviation_bps": vwap_dev_bps,
            "bucket_minutes": bucket_minutes,
            "subscribe_mode": subscribe_mode,
            "lot_size": lot_size,
            "tick_count": int(len(grp)),
            "source": inferred_source,
            "feature_completeness": completeness,
        }

        if compute_depth:
            bid_p, bid_q = _level1(last.get("bid_prices"), last.get("bid_quantities"))
            ask_p, ask_q = _level1(last.get("ask_prices"), last.get("ask_quantities"))
            spread_abs = (ask_p - bid_p) if bid_p is not None and ask_p is not None else None
            mid = ((ask_p + bid_p) / 2.0) if bid_p is not None and ask_p is not None else None
            spread_bps = (spread_abs / mid * 10000.0) if spread_abs is not None and mid else None
            depth_ratio = (
                (bid_q / ask_q) if bid_q is not None and ask_q not in (None, 0) else None
            )
            ofi_sum = float(np.nansum([x for x in grp["_ofi"] if x is not None]))
            features.update(
                {
                    "best_bid": bid_p,
                    "best_ask": ask_p,
                    "best_bid_qty": bid_q,
                    "best_ask_qty": ask_q,
                    "spread_abs": spread_abs,
                    "spread_bps": spread_bps,
                    "depth_ratio_bid_ask": depth_ratio,
                    "ofi_bucket_sum": ofi_sum,
                    "ofi_stub": True,
                }
            )
        elif not skip_depth:
            features.update(
                {
                    "best_bid": None,
                    "best_ask": None,
                    "spread_abs": None,
                    "spread_bps": None,
                    "depth_ratio_bid_ask": None,
                    "ofi_bucket_sum": None,
                    "quote_only": True,
                }
            )
        else:
            features["depth_features_skipped"] = [
                "depth_ratio_bid_ask",
                "spread_bps",
                "spread_abs",
                "ofi_bucket_sum",
            ]

        rows.append(
            {
                "timestamp": pd.Timestamp(bucket_ts).isoformat(),
                "trade_date": str(pd.Timestamp(bucket_ts).date()),
                "symbol": symbol,
                "track": track,
                "source": inferred_source,
                "feature_completeness": completeness,
                "features": features,
            }
        )

    return rows
