"""Real OHLC bars from compacted Parquet.

Session OHLC (Kite quote fields ohlc_open/high/low/close) is the day's
running session range. It is NOT a per-bar candle and is never used here.

Bar OHLC (columns open/high/low/close on candles.parquet or historical
candle-materialized ticks) is a real interval candle and may be resampled
onto NSE session-aligned buckets.

Live ticks without bar OHLC are aggregated from observed last_price only.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from nse_pipeline.config import Settings
from nse_pipeline.storage.duckdb_store import DuckDBTickStore

IST = ZoneInfo("Asia/Kolkata")
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)

CHART_INTERVALS: dict[str, int | None] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "10m": 600,
    "15m": 900,
    "30m": 1800,
    "60m": 3600,
    "1D": None,
}

# Kite session-range fields. Never treat these as bar OHLC.
SESSION_OHLC_COLUMNS = ("ohlc_open", "ohlc_high", "ohlc_low", "ohlc_close")
BAR_OHLC_COLUMNS = ("open", "high", "low", "close")


def normalize_interval(value: str | None) -> str | None:
    if value is None:
        return "1m"
    text = str(value).strip()
    if not text:
        return "1m"
    aliases = {
        "1M": "1m",
        "3M": "3m",
        "5M": "5m",
        "10M": "10m",
        "15M": "15m",
        "30M": "30m",
        "60M": "60m",
        "1d": "1D",
        "D": "1D",
        "day": "1D",
        "1day": "1D",
    }
    text = aliases.get(text, text)
    if text not in CHART_INTERVALS:
        return None
    return text


def _as_ist(value: Any) -> datetime | None:
    if value is None or (isinstance(value, float) and value != value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(IST).to_pydatetime()


def nse_session_bucket(ts: Any, interval: str) -> datetime | None:
    """
    NSE regular-session bucket start in Asia/Kolkata.

    Bars align to 09:15 IST, not UTC epoch. Observations before 09:15 or
    after 15:30 IST are dropped. Empty buckets are not invented.
    """
    resolved = normalize_interval(interval)
    if resolved is None:
        return None
    ist = _as_ist(ts)
    if ist is None:
        return None
    open_dt = datetime.combine(ist.date(), SESSION_OPEN, tzinfo=IST)
    close_dt = datetime.combine(ist.date(), SESSION_CLOSE, tzinfo=IST)
    if ist < open_dt or ist > close_dt:
        return None
    if resolved == "1D":
        return open_dt
    seconds = CHART_INTERVALS[resolved]
    assert seconds is not None
    elapsed = (ist - open_dt).total_seconds()
    session_len = (close_dt - open_dt).total_seconds()
    if elapsed >= session_len:
        elapsed = session_len - 1e-9
    start = open_dt + timedelta(seconds=int(elapsed // seconds) * seconds)
    if start >= close_dt:
        return None
    return start


def _iso(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and value != value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def frame_has_bar_ohlc(frame: pd.DataFrame) -> bool:
    """True when columns open/high/low/close are present and populated.

    Ignores session-range ohlc_* fields.
    """
    if frame is None or frame.empty:
        return False
    cols = {str(c).lower() for c in frame.columns}
    if not set(BAR_OHLC_COLUMNS) <= cols:
        return False
    subset = frame.loc[:, list(BAR_OHLC_COLUMNS)]
    return bool(subset.dropna(how="any").shape[0])


def session_aggregate_ohlc(frame: pd.DataFrame, interval: str, *, mode: str) -> pd.DataFrame:
    """Group observed rows onto NSE session buckets. No empty-bar fill."""
    if frame is None or frame.empty:
        return pd.DataFrame()
    work = frame.copy()
    work["_bucket"] = work["timestamp"].map(lambda ts: nse_session_bucket(ts, interval))
    work = work[work["_bucket"].notna()]
    if work.empty:
        return pd.DataFrame()
    work = work.sort_values("timestamp")
    rows: list[dict[str, Any]] = []
    for bucket, group in work.groupby("_bucket", sort=True):
        g = group.sort_values("timestamp")
        if mode == "bar":
            open_px = _num(g["open"].iloc[0])
            high = _num(g["high"].max())
            low = _num(g["low"].min())
            close = _num(g["close"].iloc[-1])
            volume = g["volume"].sum() if "volume" in g.columns else None
        else:
            px = g["last_price"] if "last_price" in g.columns else g.get("close")
            if px is None:
                continue
            px = px.dropna()
            if px.empty:
                continue
            open_px = _num(px.iloc[0])
            high = _num(px.max())
            low = _num(px.min())
            close = _num(px.iloc[-1])
            volume = None
            if "volume" in g.columns:
                first_v = _num(g["volume"].iloc[0])
                last_v = _num(g["volume"].iloc[-1])
                if first_v is not None and last_v is not None:
                    volume = max(last_v - first_v, 0.0)
        if None in (open_px, high, low, close):
            continue
        oi = g["oi"].iloc[-1] if "oi" in g.columns else None
        rows.append(
            {
                "timestamp": bucket,
                "open": open_px,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "oi": oi,
            }
        )
    return pd.DataFrame(rows)


def candle_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if frame is None or frame.empty:
        return rows
    for rec in frame.to_dict(orient="records"):
        open_px = _num(rec.get("open"))
        high = _num(rec.get("high"))
        low = _num(rec.get("low"))
        close = _num(rec.get("close"))
        if None in (open_px, high, low, close):
            continue
        rows.append(
            {
                "timestamp": _iso(rec.get("timestamp")),
                "open": open_px,
                "high": high,
                "low": low,
                "close": close,
                "volume": rec.get("volume"),
                "oi": rec.get("oi"),
            }
        )
    return rows


def observation_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if frame is None or frame.empty:
        return rows
    for rec in frame.to_dict(orient="records"):
        rows.append(
            {
                "timestamp": _iso(rec.get("timestamp")),
                "last_price": rec.get("last_price"),
                "volume": rec.get("volume"),
                "oi": rec.get("oi"),
                "oi_delta": rec.get("oi_delta"),
                "volume_delta": rec.get("volume_delta"),
                "trade_notional": rec.get("trade_notional"),
                "depth_imbalance": rec.get("depth_imbalance"),
                "spread": rec.get("spread"),
            }
        )
    return rows


def load_historical_observations(
    settings: Settings,
    symbol: str,
    *,
    lookback_days: int,
) -> tuple[list[dict[str, Any]], str | None]:
    with DuckDBTickStore(settings) as store:
        dates = store.list_dates_for_symbol(symbol, limit=lookback_days)
        frame = store.read_symbol_observations(symbol, dates)
    if frame.empty:
        return [], None
    return observation_rows(frame), "compacted_ticks"


def load_ohlc_candles(
    settings: Settings,
    symbol: str,
    *,
    interval: str | None,
    lookback_days: int,
) -> dict[str, Any]:
    resolved = normalize_interval(interval)
    if resolved is None:
        return {
            "candles": [],
            "candles_status": "unavailable",
            "candles_reason": "unsupported_interval",
            "candles_source": None,
            "interval": interval,
        }
    with DuckDBTickStore(settings) as store:
        dates = store.list_dates_for_symbol(symbol, limit=lookback_days)
        if not dates:
            return {
                "candles": [],
                "candles_status": "unavailable",
                "candles_reason": "no_compacted_data",
                "candles_source": None,
                "interval": resolved,
            }
        sources = store.read_symbol_ohlc_frames(symbol, dates)

    daily = sources.get("daily")
    candles = sources.get("candles")
    ticks = sources.get("ticks")
    frame = pd.DataFrame()
    source = None

    # 1) Genuine daily bar OHLC for 1D.
    if resolved == "1D" and frame_has_bar_ohlc(daily):
        frame = session_aggregate_ohlc(daily, resolved, mode="bar")
        source = "daily"
    # 2) Stored bar candles (Kite historical / candles.parquet).
    if frame.empty and frame_has_bar_ohlc(candles):
        frame = session_aggregate_ohlc(candles, resolved, mode="bar")
        source = "compacted_candles"
    # 3) Tick files that already carry bar OHLC (not session ohlc_*).
    if frame.empty and frame_has_bar_ohlc(ticks):
        frame = session_aggregate_ohlc(ticks, resolved, mode="bar")
        source = "compacted_ticks"
    # 4) Live/compacted ticks: last_price only.
    if frame.empty and ticks is not None and not ticks.empty and "last_price" in ticks.columns:
        frame = session_aggregate_ohlc(ticks, resolved, mode="last_price")
        source = "compacted_ticks"
    if frame.empty and resolved == "1D" and daily is not None and not daily.empty:
        price_col = "last_price" if "last_price" in daily.columns else "close"
        if price_col in daily.columns:
            work = daily.rename(columns={price_col: "last_price"}) if price_col != "last_price" else daily
            frame = session_aggregate_ohlc(work, resolved, mode="last_price")
            source = "daily"

    candles_out = candle_rows(frame)
    if not candles_out:
        return {
            "candles": [],
            "candles_status": "unavailable",
            "candles_reason": "insufficient_ohlc",
            "candles_source": source,
            "interval": resolved,
        }
    return {
        "candles": candles_out,
        "candles_status": "ok",
        "candles_reason": None,
        "candles_source": source,
        "interval": resolved,
    }
