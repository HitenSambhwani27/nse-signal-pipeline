"""Request-count / runtime estimates for Part 1 (no API calls)."""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

from nse_pipeline.config import Settings
from nse_pipeline.historical.chunks import calendar_days, max_days_for_interval
from nse_pipeline.historical.universe import cache_counts


def _ceil_div(n: int, d: int) -> int:
    return int(math.ceil(n / d)) if n > 0 else 0


def estimate_backfill(
    settings: Settings,
    *,
    start: date,
    end: date,
    minute_lookback_days: int | None,
) -> dict[str, Any]:
    """
    Two scenarios:

    (a) minute-level for the full [start, end] range across the equity+spot universe
    (b) daily for the full range + minute-level for the last `minute_window` days
        (default 365 if minute_lookback_days is None in scenario b comparison)
    """
    counts = cache_counts(settings)
    n_eq = counts.get("equity_depth", 0) + counts.get("equity_quote", 0)
    n_spot = counts.get("index", 0)
    n_opt = counts.get("options", 0)
    n_fut = counts.get("futures", 0)
    n_eq_spot = n_eq + n_spot

    hist = settings.historical
    cal = calendar_days(start, end)
    minute_cap = max_days_for_interval(hist, hist.minute_interval)
    day_cap = max_days_for_interval(hist, hist.daily_interval)

    req_minute_full = n_eq_spot * _ceil_div(cal, minute_cap)
    req_daily_full = n_eq_spot * _ceil_div(cal, day_cap)

    window_b = minute_lookback_days if minute_lookback_days is not None else 365
    minute_start_b = max(start, end - timedelta(days=window_b - 1))
    cal_b = calendar_days(minute_start_b, end)
    req_minute_b = n_eq_spot * _ceil_div(cal_b, minute_cap)

    # F&O: contract-lifetime bound — assume 1–2 chunks (live contract only).
    fo_chunks = 2
    req_fo = (n_opt + n_fut) * fo_chunks

    sleep = hist.sleep_seconds
    req_a = req_minute_full + req_daily_full + req_fo
    req_b = req_daily_full + req_minute_b + req_fo

    def eta(requests: int) -> dict[str, float]:
        seconds = requests * sleep
        return {
            "sleep_seconds": round(seconds, 1),
            "sleep_minutes": round(seconds / 60.0, 1),
            "note": (
                "Wall-clock is sleep plus Kite latency; budget ~1.5–2× sleep "
                "as a conservative range."
            ),
        }

    return {
        "universe": counts,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "calendar_days_full": cal,
        "kite_historical_rate_limit_per_second": hist.rate_limit_per_second,
        "sleep_seconds_per_request": sleep,
        "interval_caps": hist.interval_max_days,
        "scenario_a_full_minute": {
            "description": (
                f"Daily + {hist.minute_interval} for every equity/spot from "
                f"{start} to {end}"
            ),
            "minute_requests": req_minute_full,
            "daily_requests": req_daily_full,
            "fo_requests_lifetime_bound": req_fo,
            "total_requests": req_a,
            "eta": eta(req_a),
        },
        "scenario_b_daily_full_minute_recent": {
            "description": (
                f"Daily {start}→{end} plus {hist.minute_interval} for last "
                f"{window_b} calendar days ({minute_start_b}→{end})"
            ),
            "minute_window_days": window_b,
            "minute_start": minute_start_b.isoformat(),
            "minute_requests": req_minute_b,
            "daily_requests": req_daily_full,
            "fo_requests_lifetime_bound": req_fo,
            "total_requests": req_b,
            "eta": eta(req_b),
        },
        "fo_ceiling_note": (
            "Options/futures requests are contract-lifetime-bound (live "
            "contracts only). Nifty weekly depth is days-to-weeks; Bank Nifty "
            "monthly up to ~one month. Do not compare F&O coverage to the "
            "equity 2022-01-01 benchmark."
        ),
        "excluded": [
            "equity options",
            "single-stock futures",
            "Nifty monthly options",
        ],
    }
