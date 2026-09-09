"""Real OHLC bars from compacted Parquet.

Session OHLC (Kite quote fields ohlc_open/high/low/close) is the day's
running session range. It is NOT a per-bar candle and is never used here.

Bar OHLC (columns open/high/low/close on candles.parquet or historical
candle-materialized ticks) is a real interval candle and may be resampled
onto NSE session-aligned buckets.

Live ticks without bar OHLC are aggregated from observed last_price only.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Sequence
from zoneinfo import ZoneInfo
import json

import pandas as pd

from nse_pipeline.config import Settings
from nse_pipeline.market.timestamps import AMBIGUOUS_UTC_HOUR, canonical_observation, parse_kite_datetime

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
    observed, repair = canonical_observation(value)
    if observed is None:
        observed = parse_kite_datetime(value)
        repair = None
    if observed is None or repair == AMBIGUOUS_UTC_HOUR:
        return None
    return observed.astimezone(IST)


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
    return ts.floor("s").isoformat()


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
    if frame is None or frame.empty:
        return []
    work = frame.copy()
    stamp_col = "bucket_start" if "bucket_start" in work.columns else "timestamp"
    ts = pd.to_datetime(work[stamp_col], utc=True).dt.floor("s")
    iso = ts.dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    complete = (
        work["bar_complete"]
        if "bar_complete" in work.columns
        else work["complete"]
        if "complete" in work.columns
        else True
    )
    out = pd.DataFrame(
        {
            "timestamp": iso,
            "t": iso,
            "open": pd.to_numeric(work["open"], errors="coerce"),
            "high": pd.to_numeric(work["high"], errors="coerce"),
            "low": pd.to_numeric(work["low"], errors="coerce"),
            "close": pd.to_numeric(work["close"], errors="coerce"),
            "volume": work["volume"] if "volume" in work.columns else None,
            "oi": work["oi"] if "oi" in work.columns else None,
            "vwap": work["vwap"] if "vwap" in work.columns else None,
            "trades_observed": work["trades_observed"] if "trades_observed" in work.columns else None,
            "complete": complete,
        }
    )
    out["o"] = out["open"]
    out["h"] = out["high"]
    out["l"] = out["low"]
    out["c"] = out["close"]
    out["v"] = out["volume"]
    out = out.dropna(subset=["open", "high", "low", "close"], how="any")
    if "complete" in out.columns:
        out["complete"] = out["complete"].fillna(True).astype(bool)
    return json.loads(out.to_json(orient="records"))


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
    from nse_pipeline.storage.duckdb_store import DuckDBTickStore

    with DuckDBTickStore(settings) as store:
        dates = store.list_dates_for_symbol(symbol, limit=lookback_days)
        frame = store.read_symbol_observations(symbol, dates)
    if frame.empty:
        return [], None
    return observation_rows(frame), "compacted_ticks"


def rollup_1m_bars(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Server-side rollup from 1m bars. Missing constituents → incomplete, not invented."""
    resolved = normalize_interval(interval)
    if resolved is None or frame is None or frame.empty:
        return pd.DataFrame()
    work = frame
    stamps = pd.to_datetime(work["bucket_start"], utc=True).dt.tz_convert(IST)
    if resolved == "1m":
        out = work.copy()
        out["timestamp"] = stamps
        out["bucket_start"] = stamps
        return out
    seconds = CHART_INTERVALS[resolved]
    if not seconds:
        return pd.DataFrame()
    expected_minutes = int(seconds) // 60
    midnight = stamps.dt.floor("D")
    open_dt = midnight + pd.Timedelta(hours=SESSION_OPEN.hour, minutes=SESSION_OPEN.minute)
    close_dt = midnight + pd.Timedelta(hours=SESSION_CLOSE.hour, minutes=SESSION_CLOSE.minute)
    elapsed = (stamps - open_dt).dt.total_seconds()
    in_session = (stamps >= open_dt) & (stamps <= close_dt)
    work = work.loc[in_session].copy()
    if work.empty:
        return pd.DataFrame()
    work["_ist"] = stamps.loc[work.index]
    work = work.sort_values("_ist")
    elapsed = elapsed.loc[work.index]
    open_dt = open_dt.loc[work.index]
    work["_rbucket"] = open_dt + pd.to_timedelta(
        (elapsed // int(seconds)).astype("int64") * int(seconds), unit="s"
    )
    if "volume" in work.columns:
        typical = work["vwap"] if "vwap" in work.columns else work["close"]
        vol = pd.to_numeric(work["volume"], errors="coerce")
        work["_vw_num"] = pd.to_numeric(typical, errors="coerce") * vol
        work["_vw_den"] = vol
    grouped = work.groupby("_rbucket", sort=True)
    volume_sum = grouped["volume"].sum(min_count=1) if "volume" in work.columns else None
    trades = grouped["trades_observed"].sum() if "trades_observed" in work.columns else grouped.size()
    complete_count = grouped["bar_complete"].all() if "bar_complete" in work.columns else True
    size = grouped.size()
    vw_num = grouped["_vw_num"].sum() if "_vw_num" in work.columns else None
    vw_den = grouped["_vw_den"].sum() if "_vw_den" in work.columns else None
    vwap = (vw_num / vw_den).where(vw_den > 0) if vw_num is not None and vw_den is not None else None
    oi = grouped["oi"].last() if "oi" in work.columns else None
    out = pd.DataFrame(
        {
            "bucket_start": size.index,
            "timestamp": size.index,
            "open": grouped["open"].first(),
            "high": grouped["high"].max(),
            "low": grouped["low"].min(),
            "close": grouped["close"].last(),
            "volume": volume_sum,
            "oi": oi,
            "vwap": vwap,
            "trades_observed": trades.astype(int),
            "bar_complete": (size >= expected_minutes) & complete_count
            if not isinstance(complete_count, bool)
            else (size >= expected_minutes),
        }
    ).reset_index(drop=True)
    return out.dropna(subset=["open", "high", "low", "close"], how="any")


def _coverage(
    *,
    interval: str,
    dates: list[str],
    candles: list[dict[str, Any]],
) -> dict[str, Any]:
    observed_set = {
        str(row.get("timestamp") or row.get("t"))
        for row in candles
        if row.get("timestamp") or row.get("t")
    }
    gaps: list[dict[str, str]] = []
    expected_n = 0
    seconds = CHART_INTERVALS.get(interval)
    for session_date in dates:
        day = date.fromisoformat(str(session_date)[:10])
        open_dt = datetime.combine(day, SESSION_OPEN, tzinfo=IST)
        close_dt = datetime.combine(day, SESSION_CLOSE, tzinfo=IST)
        if interval == "1D":
            idx = pd.DatetimeIndex([open_dt])
        elif not seconds:
            continue
        else:
            idx = pd.date_range(open_dt, close_dt, freq=f"{int(seconds)}s", inclusive="left")
        expected_n += int(len(idx))
        if len(idx) == 0:
            continue
        iso_list = idx.tz_convert("UTC").floor("s").strftime("%Y-%m-%dT%H:%M:%S+00:00").tolist()
        gap_start: str | None = None
        gap_end: str | None = None
        for iso in iso_list:
            if iso in observed_set:
                if gap_start is not None and gap_end is not None:
                    gaps.append({"from": gap_start, "to": gap_end})
                gap_start = None
                gap_end = None
                continue
            if gap_start is None:
                gap_start = iso
            gap_end = iso
        if gap_start is not None and gap_end is not None:
            gaps.append({"from": gap_start, "to": gap_end})
    observed_n = len(candles)
    completeness = (observed_n / expected_n) if expected_n else None
    status = "ok"
    if expected_n == 0 or observed_n == 0:
        status = "unavailable"
    elif gaps:
        status = "partial"
    return {
        "expected_bars": expected_n,
        "observed_bars": observed_n,
        "completeness": completeness,
        "gaps": gaps,
        "session_dates": dates,
        "status": status,
    }


def _filter_range(
    frame: pd.DataFrame, *, start: datetime | None, end: datetime | None
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame
    if start is None and end is None:
        return frame
    work = frame.copy()
    col = "bucket_start" if "bucket_start" in work.columns else "timestamp"
    stamps = pd.to_datetime(work[col], utc=True).dt.tz_convert(IST)
    mask = pd.Series(True, index=work.index)
    if start is not None:
        start_ist = start.astimezone(IST) if start.tzinfo else start.replace(tzinfo=IST)
        mask &= stamps >= start_ist
    if end is not None:
        end_ist = end.astimezone(IST) if end.tzinfo else end.replace(tzinfo=IST)
        mask &= stamps <= end_ist
    return work[mask]


def load_ohlc_candles(
    settings: Settings,
    symbol: str,
    *,
    interval: str | None,
    lookback_days: int,
    start: datetime | None = None,
    end: datetime | None = None,
    max_points: int | None = None,
) -> dict[str, Any]:
    """Read RM-1 bars only. Never aggregates compacted ticks at request time."""
    from nse_pipeline.storage.bars import list_bar_dates, read_daily_bars, read_minute_bars

    resolved = normalize_interval(interval)
    if resolved is None:
        return {
            "candles": [],
            "candles_status": "unavailable",
            "candles_reason": "unsupported_interval",
            "candles_source": None,
            "interval": interval,
            "coverage": {
                "expected_bars": 0,
                "observed_bars": 0,
                "completeness": None,
                "gaps": [],
                "session_dates": [],
                "status": "unavailable",
            },
        }
    dates = list_bar_dates(settings, symbol, limit=lookback_days)
    if resolved == "1D":
        frame = read_daily_bars(settings, symbol)
        source = "bars_daily"
        if frame.empty:
            return {
                "candles": [],
                "candles_status": "unavailable",
                "candles_reason": "bars_not_built",
                "candles_source": None,
                "interval": resolved,
                "coverage": {
                    "expected_bars": 0,
                    "observed_bars": 0,
                    "completeness": None,
                    "gaps": [],
                    "session_dates": [],
                    "status": "unavailable",
                },
            }
        if "session_date" in frame.columns:
            dates = [str(v) for v in frame["session_date"].tolist() if v]
            if lookback_days:
                dates = dates[-int(lookback_days) :]
                frame = frame[frame["session_date"].astype(str).isin(dates)]
        frame = _filter_range(frame, start=start, end=end)
        candles_out = candle_rows(frame)
        coverage = _coverage(interval="1D", dates=dates, candles=candles_out)
        status = coverage["status"] if candles_out else "unavailable"
        reason = None if candles_out else "bars_not_built"
        if max_points:
            candles_out = candles_out[-int(max_points) :]
        coverage["requested_from"] = _iso(start)
        coverage["requested_to"] = _iso(end)
        coverage["returned_from"] = candles_out[0]["t"] if candles_out else None
        coverage["returned_to"] = candles_out[-1]["t"] if candles_out else None
        coverage["returned_bars"] = len(candles_out)
        return {
            "candles": candles_out,
            "candles_status": status if candles_out else "unavailable",
            "candles_reason": reason,
            "candles_source": source,
            "interval": resolved,
            "coverage": coverage,
            "source": source,
        }

    if not dates:
        return {
            "candles": [],
            "candles_status": "unavailable",
            "candles_reason": "bars_not_built",
            "candles_source": None,
            "interval": resolved,
            "coverage": {
                "expected_bars": 0,
                "observed_bars": 0,
                "completeness": None,
                "gaps": [],
                "session_dates": [],
                "status": "unavailable",
            },
        }
    minute = read_minute_bars(settings, symbol, dates)
    minute = _filter_range(minute, start=start, end=end)
    if minute.empty:
        return {
            "candles": [],
            "candles_status": "unavailable",
            "candles_reason": "bars_not_built",
            "candles_source": None,
            "interval": resolved,
            "coverage": {
                "expected_bars": 0,
                "observed_bars": 0,
                "completeness": None,
                "gaps": [],
                "session_dates": dates,
                "status": "unavailable",
            },
        }
    rolled = rollup_1m_bars(minute, resolved)
    source = "bars_1m" if resolved == "1m" else "bars_1m_rollup"
    candles_out = candle_rows(rolled)
    coverage = _coverage(interval=resolved, dates=dates, candles=candles_out)
    if max_points and len(candles_out) > int(max_points):
        candles_out = candles_out[-int(max_points) :]
    coverage["requested_from"] = _iso(start)
    coverage["requested_to"] = _iso(end)
    coverage["returned_from"] = candles_out[0]["t"] if candles_out else None
    coverage["returned_to"] = candles_out[-1]["t"] if candles_out else None
    coverage["returned_bars"] = len(candles_out)
    status = coverage["status"]
    return {
        "candles": candles_out,
        "candles_status": status,
        "candles_reason": None if candles_out else "bars_not_built",
        "candles_source": source,
        "interval": resolved,
        "coverage": coverage,
        "source": source,
    }


def load_bar_series(
    settings: Settings,
    symbol: str,
    *,
    fields: Sequence[str] | str | None,
    interval: str | None,
    lookback_days: int,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, Any]:
    """Volume/OI (and close) series from RM-1. Does not scan ticks."""
    payload = load_ohlc_candles(
        settings,
        symbol,
        interval=interval,
        lookback_days=lookback_days,
        start=start,
        end=end,
    )
    wanted: list[str]
    if isinstance(fields, str):
        wanted = [part.strip() for part in fields.split(",") if part.strip()]
    elif fields:
        wanted = [str(item).strip() for item in fields if str(item).strip()]
    else:
        wanted = ["volume", "oi"]
    series: dict[str, list[dict[str, Any]]] = {}
    mapping = {
        "volume": "volume",
        "oi": "oi",
        "vwap": "vwap",
        "close": "close",
        "trades_observed": "trades_observed",
    }
    for field in wanted:
        if field == "activity":
            series[field] = []
            continue
        key = mapping.get(field)
        if key is None:
            continue
        series[field] = [
            {"t": row.get("t") or row.get("timestamp"), field: row.get(key)}
            for row in payload.get("candles") or []
        ]
    return {
        "series": series,
        "interval": payload.get("interval"),
        "source": payload.get("candles_source"),
        "series_status": payload.get("candles_status"),
        "series_reason": payload.get("candles_reason"),
        "coverage": payload.get("coverage"),
    }
