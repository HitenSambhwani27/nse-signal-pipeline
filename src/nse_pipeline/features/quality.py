"""
Stage 2D — data-quality gate for ticks before feature computation.

Rejected ticks are never silently dropped: callers must log via quality_log /
ingestion_meta. Circuit-halt style events use a distinct event_type from
WebSocket disconnects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class QualityReject:
    timestamp: str
    symbol: str
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityResult:
    clean: pd.DataFrame
    rejects: list[QualityReject]
    halt_flags: list[QualityReject]


def _first_level(values: Any) -> float | None:
    if values is None:
        return None
    try:
        if len(values) == 0:
            return None
        return float(values[0])
    except (TypeError, ValueError):
        return None


def filter_bad_ticks(
    df: pd.DataFrame,
    *,
    symbol: str,
    subscribe_mode: str,
    max_tick_return_pct: float,
    require_depth: bool = False,
) -> QualityResult:
    """
    Filter impossible / stale / crossed-book ticks.

    Rules:
    - drop null or non-positive LTP
    - drop non-increasing timestamps (stale/out-of-order)
    - drop abs return vs previous kept LTP above max_tick_return_pct
    - depth mode: drop crossed book (best bid > best ask) when both sides present
    - freeze heuristic: long run of unchanged LTP with flat volume →
      stock_quote_freeze flag (per-symbol quote freeze / possible individual
      price-band limit — NOT a market-wide NSE circuit breaker; row is kept)
    """
    if df.empty:
        return QualityResult(clean=df.copy(), rejects=[], halt_flags=[])

    work = df.sort_values("timestamp").reset_index(drop=True)
    keep_mask = np.ones(len(work), dtype=bool)
    rejects: list[QualityReject] = []
    halt_flags: list[QualityReject] = []  # stock_quote_freeze events (name kept for API)

    prev_ts: pd.Timestamp | None = None
    prev_ltp: float | None = None
    unchanged_run = 0
    prev_volume: int | None = None

    for idx, row in work.iterrows():
        ts = pd.Timestamp(row["timestamp"])
        ts_str = ts.isoformat()
        ltp = row.get("last_price")
        volume = int(row.get("volume") or 0)

        if ltp is None or (isinstance(ltp, float) and np.isnan(ltp)) or float(ltp) <= 0:
            keep_mask[idx] = False
            rejects.append(
                QualityReject(ts_str, symbol, "null_or_nonpositive_ltp", {"ltp": ltp})
            )
            continue

        ltp_f = float(ltp)

        if prev_ts is not None and ts <= prev_ts:
            keep_mask[idx] = False
            rejects.append(
                QualityReject(
                    ts_str,
                    symbol,
                    "stale_or_out_of_order_timestamp",
                    {"prev_ts": prev_ts.isoformat(), "ts": ts_str},
                )
            )
            continue

        if prev_ltp is not None and prev_ltp > 0:
            ret_pct = abs(ltp_f / prev_ltp - 1.0) * 100.0
            if ret_pct > max_tick_return_pct:
                keep_mask[idx] = False
                rejects.append(
                    QualityReject(
                        ts_str,
                        symbol,
                        "impossible_jump",
                        {
                            "prev_ltp": prev_ltp,
                            "ltp": ltp_f,
                            "ret_pct": ret_pct,
                            "max_tick_return_pct": max_tick_return_pct,
                        },
                    )
                )
                continue

        if subscribe_mode == "full" or require_depth:
            bid = _first_level(row.get("bid_prices"))
            ask = _first_level(row.get("ask_prices"))
            if bid is not None and ask is not None and bid > ask and bid > 0 and ask > 0:
                keep_mask[idx] = False
                rejects.append(
                    QualityReject(
                        ts_str,
                        symbol,
                        "crossed_book",
                        {"best_bid": bid, "best_ask": ask},
                    )
                )
                continue

        # Halt heuristic: LTP unchanged and volume flat for many consecutive ticks.
        if (
            prev_ltp is not None
            and abs(ltp_f - prev_ltp) < 1e-12
            and prev_volume is not None
            and volume == prev_volume
        ):
            unchanged_run += 1
        else:
            unchanged_run = 0

        if unchanged_run == 50:
            # Per-symbol freeze suspect (illiquid print or individual price-band).
            # Do NOT treat as market-wide NSE circuit breaker.
            halt_flags.append(
                QualityReject(
                    ts_str,
                    symbol,
                    "unchanged_ltp_and_volume_run",
                    {
                        "unchanged_ticks": unchanged_run,
                        "ltp": ltp_f,
                        "event_type": "stock_quote_freeze",
                        "note": (
                            "Per-symbol quote freeze suspect — not a market-wide "
                            "NSE circuit breaker; distinct from websocket disconnect."
                        ),
                    },
                )
            )

        prev_ts = ts
        prev_ltp = ltp_f
        prev_volume = volume

    clean = work.loc[keep_mask].reset_index(drop=True)
    return QualityResult(clean=clean, rejects=rejects, halt_flags=halt_flags)
