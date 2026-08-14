"""
Stage 3A — Triple-Barrier / legacy flat labeling.

Volatility is estimated on each instrument's own price series (options: own
premium). No lookahead: vol at T uses only returns strictly before T.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd

from nse_pipeline.config import Settings, TripleBarrierSettings


LabelName = Literal["up", "down", "flat"]


@dataclass
class LabelResult:
    symbol: str
    timestamp: pd.Timestamp
    track: str
    mode: str
    label: LabelName
    label_code: float  # +1 / 0 / -1
    fwd_ret_pct: float
    thr_pct: float
    thr_raw_pct: float | None
    floor_bound: bool
    cap_bound: bool
    vol: float | None
    hour_ist: int
    # Why floor applied: missing_vol | below_min | None
    floor_reason: str | None = None


def _bucket_closes_from_ticks(
    ticks: pd.DataFrame, *, bucket_minutes: int
) -> pd.Series:
    """Last LTP per bucket end, UTC-indexed."""
    if ticks.empty:
        return pd.Series(dtype=float)
    df = ticks.sort_values("timestamp").copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["bucket"] = df["timestamp"].dt.floor(f"{bucket_minutes}min") + pd.Timedelta(
        minutes=bucket_minutes
    )
    closes = df.groupby("bucket", sort=True)["last_price"].last().astype(float)
    closes = closes[closes > 0]
    return closes


def _return_vol(
    closes: pd.Series,
    *,
    method: str,
    window: int,
    min_periods: int,
) -> pd.Series:
    """
    Per-timestamp vol (decimal std of returns) using only prior returns.

    At index t, vol_t is computed from returns ending at or before the previous bar.
    """
    rets = closes.pct_change()
    # Shift so vol at t does not include the return into t (strictly prior).
    prior_rets = rets.shift(1)

    if method == "rolling_std":
        vol = prior_rets.rolling(window=window, min_periods=min_periods).std()
    elif method == "ewma":
        # ewm on prior returns; adjust=False for causal recursive estimate
        vol = prior_rets.ewm(span=window, min_periods=min_periods, adjust=False).std()
    else:
        raise ValueError(f"Unknown vol_method: {method}")
    return vol


def _clip_threshold(
    thr_raw_pct: float | None,
    tb: TripleBarrierSettings,
) -> tuple[float, bool, bool, str | None]:
    """Return (thr_used_pct, floor_bound, cap_bound, floor_reason)."""
    if thr_raw_pct is None or not np.isfinite(thr_raw_pct) or thr_raw_pct <= 0:
        return tb.min_threshold_pct, True, False, "missing_vol"
    floor_bound = thr_raw_pct < tb.min_threshold_pct
    cap_bound = thr_raw_pct > tb.max_threshold_pct
    thr = float(np.clip(thr_raw_pct, tb.min_threshold_pct, tb.max_threshold_pct))
    floor_reason = "below_min" if floor_bound else None
    return thr, floor_bound, cap_bound, floor_reason


def label_from_return(fwd_ret_pct: float, thr_pct: float) -> tuple[LabelName, float]:
    if fwd_ret_pct >= thr_pct:
        return "up", 1.0
    if fwd_ret_pct <= -thr_pct:
        return "down", -1.0
    return "flat", 0.0


def label_series_triple_barrier(
    closes: pd.Series,
    *,
    symbol: str,
    track: str,
    horizon_bars: int,
    settings: Settings,
) -> list[LabelResult]:
    """Volatility-scaled barriers; options use own premium series (caller passes closes)."""
    tb = settings.signal.triple_barrier
    if track == "options" and tb.options_label_mode != "raw_premium":
        raise NotImplementedError(
            f"options_label_mode={tb.options_label_mode!r} is Phase B "
            "(delta_residual) and is not implemented yet"
        )
    vol = _return_vol(
        closes,
        method=tb.vol_method,
        window=tb.vol_window,
        min_periods=settings.signal.vol_min_periods,
    )
    results: list[LabelResult] = []
    times = list(closes.index)
    if len(times) < 2:
        return results
    step = times[1] - times[0]

    for i, ts in enumerate(times):
        expected = ts + step * horizon_bars
        if expected not in closes.index:
            continue
        p0 = float(closes.iloc[i])
        p1 = float(closes.loc[expected])
        if p0 <= 0:
            continue
        fwd = (p1 / p0 - 1.0) * 100.0

        v = vol.loc[ts] if ts in vol.index else np.nan
        if v is not None and np.isfinite(v) and v > 0:
            thr_raw = float(v) * tb.barrier_multiplier * 100.0
        else:
            thr_raw = None
        thr, floor_bound, cap_bound, floor_reason = _clip_threshold(thr_raw, tb)
        name, code = label_from_return(fwd, thr)
        ts_ist = pd.Timestamp(ts).tz_convert("Asia/Kolkata")
        results.append(
            LabelResult(
                symbol=symbol,
                timestamp=pd.Timestamp(ts),
                track=track,
                mode="triple_barrier",
                label=name,
                label_code=code,
                fwd_ret_pct=fwd,
                thr_pct=thr,
                thr_raw_pct=thr_raw,
                floor_bound=floor_bound,
                cap_bound=cap_bound,
                vol=float(v) if v is not None and np.isfinite(v) else None,
                hour_ist=int(ts_ist.hour),
                floor_reason=floor_reason,
            )
        )
    return results


def label_series_legacy(
    closes: pd.Series,
    *,
    symbol: str,
    track: str,
    horizon_bars: int,
    threshold_pct: float,
) -> list[LabelResult]:
    """Flat ±threshold_pct vs close at T+horizon_bars."""
    results: list[LabelResult] = []
    times = list(closes.index)
    if len(times) < 2:
        return results
    step = times[1] - times[0]

    for i, ts in enumerate(times):
        expected = ts + step * horizon_bars
        if expected not in closes.index:
            continue
        p0 = float(closes.iloc[i])
        p1 = float(closes.loc[expected])
        if p0 <= 0:
            continue
        fwd = (p1 / p0 - 1.0) * 100.0
        name, code = label_from_return(fwd, threshold_pct)
        ts_ist = pd.Timestamp(ts).tz_convert("Asia/Kolkata")
        results.append(
            LabelResult(
                symbol=symbol,
                timestamp=pd.Timestamp(ts),
                track=track,
                mode="legacy_flat",
                label=name,
                label_code=code,
                fwd_ret_pct=fwd,
                thr_pct=threshold_pct,
                thr_raw_pct=threshold_pct,
                floor_bound=False,
                cap_bound=False,
                vol=None,
                hour_ist=int(ts_ist.hour),
            )
        )
    return results


def summarize_labels(results: list[LabelResult]) -> dict[str, Any]:
    if not results:
        return {
            "n": 0,
            "up": 0,
            "down": 0,
            "flat": 0,
            "floor_bound_n": 0,
            "cap_bound_n": 0,
            "floor_bound_pct": None,
            "cap_bound_pct": None,
            "floor_missing_vol_n": 0,
            "floor_missing_vol_pct": None,
            "floor_below_min_n": 0,
            "floor_below_min_pct": None,
            "dynamic_used_n": 0,
            "dynamic_used_pct": None,
            "by_hour": {},
        }
    n = len(results)
    up = sum(1 for r in results if r.label == "up")
    down = sum(1 for r in results if r.label == "down")
    flat = sum(1 for r in results if r.label == "flat")
    floor_n = sum(1 for r in results if r.floor_bound)
    cap_n = sum(1 for r in results if r.cap_bound)
    dynamic_n = sum(1 for r in results if not r.floor_bound and not r.cap_bound)
    floor_missing = sum(1 for r in results if r.floor_reason == "missing_vol")
    floor_below = sum(1 for r in results if r.floor_reason == "below_min")
    by_hour: dict[int, dict[str, int]] = {}
    for r in results:
        bucket = by_hour.setdefault(
            r.hour_ist,
            {
                "up": 0,
                "down": 0,
                "flat": 0,
                "n": 0,
                "floor_bound": 0,
                "cap_bound": 0,
                "dynamic": 0,
                "floor_missing_vol": 0,
                "floor_below_min": 0,
            },
        )
        bucket[r.label] += 1
        bucket["n"] += 1
        if r.floor_bound:
            bucket["floor_bound"] += 1
            if r.floor_reason == "missing_vol":
                bucket["floor_missing_vol"] += 1
            elif r.floor_reason == "below_min":
                bucket["floor_below_min"] += 1
        elif r.cap_bound:
            bucket["cap_bound"] += 1
        else:
            bucket["dynamic"] += 1
    return {
        "n": n,
        "up": up,
        "down": down,
        "flat": flat,
        "up_pct": 100.0 * up / n,
        "down_pct": 100.0 * down / n,
        "flat_pct": 100.0 * flat / n,
        "floor_bound_n": floor_n,
        "cap_bound_n": cap_n,
        "floor_bound_pct": 100.0 * floor_n / n,
        "cap_bound_pct": 100.0 * cap_n / n,
        "floor_missing_vol_n": floor_missing,
        "floor_missing_vol_pct": 100.0 * floor_missing / n,
        "floor_below_min_n": floor_below,
        "floor_below_min_pct": 100.0 * floor_below / n,
        "dynamic_used_n": dynamic_n,
        "dynamic_used_pct": 100.0 * dynamic_n / n,
        "by_hour": by_hour,
    }


def build_five_min_closes(ticks: pd.DataFrame, bucket_minutes: int) -> pd.Series:
    return _bucket_closes_from_ticks(ticks, bucket_minutes=bucket_minutes)
