"""RM-1 bar store: 1-minute and daily Parquet from compacted ticks.

Request paths must not call this to aggregate ticks. Compaction and backfill do.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from nse_pipeline.config import SessionSettings, Settings
from nse_pipeline.market.ohlc import (
    CHART_INTERVALS,
    SESSION_CLOSE,
    SESSION_OPEN,
    _num,
    nse_session_bucket,
)
from nse_pipeline.market.timestamps import AMBIGUOUS_UTC_HOUR, canonical_observation, parse_kite_datetime

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

BAR_COLUMNS = (
    "bucket_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "vwap",
    "trades_observed",
    "bar_complete",
)

DAILY_EXTRA = ("session_date", "previous_close", "settlement_price")


def bars_root(settings: Settings) -> Path:
    return settings.paths.data_dir / "bars"


def minute_bar_path(settings: Settings, session_date: str, symbol: str) -> Path:
    return bars_root(settings) / session_date / _safe_symbol(symbol) / "1m.parquet"


def daily_bar_path(settings: Settings, symbol: str) -> Path:
    return bars_root(settings) / "daily" / f"{_safe_symbol(symbol)}.parquet"


def _safe_symbol(symbol: str) -> str:
    text = str(symbol or "").strip()
    if not text or any(part in text for part in ("..", "/", "\\")):
        raise ValueError("invalid bar symbol")
    return text


def _usable_price(value: Any) -> float | None:
    number = _num(value)
    if number is None or number <= 0:
        return None
    return number


def _nonneg(value: Any) -> float | None:
    number = _num(value)
    if number is None or number < 0:
        return None
    return number


def observation_ist(value: Any, ingested_at: Any = None) -> datetime | None:
    """Canonical observation instant in Asia/Kolkata. Does not guess ambiguous hours."""
    observed, repair = canonical_observation(value, ingested_at=ingested_at)
    if observed is None:
        kite = parse_kite_datetime(value)
        if kite is None:
            return None
        observed, repair = kite, None
    if repair == AMBIGUOUS_UTC_HOUR:
        return None
    return observed.astimezone(IST)


def expected_session_buckets(
    session_date: str,
    interval: str,
    *,
    session: SessionSettings | None = None,
) -> list[datetime]:
    """NSE cash-session bucket starts. Empty buckets are not bars."""
    day = date.fromisoformat(session_date)
    open_hhmm = SESSION_OPEN
    close_hhmm = SESSION_CLOSE
    if session is not None:
        oh, om = (int(p) for p in str(session.market_open).split(":"))
        ch, cm = (int(p) for p in str(session.market_close).split(":"))
        open_hhmm = time(oh, om)
        close_hhmm = time(ch, cm)
    open_dt = datetime.combine(day, open_hhmm, tzinfo=IST)
    close_dt = datetime.combine(day, close_hhmm, tzinfo=IST)
    if interval == "1D":
        return [open_dt]
    seconds = CHART_INTERVALS.get(interval)
    if not seconds:
        return []
    buckets: list[datetime] = []
    cursor = open_dt
    while cursor < close_dt:
        buckets.append(cursor)
        cursor = cursor + timedelta(seconds=int(seconds))
    return buckets


def _to_utc_aware(frame: pd.DataFrame) -> pd.DataFrame:
    """Persist bar timestamps as UTC instants so naive parquet round-trips stay unambiguous."""
    if frame is None or frame.empty or "bucket_start" not in frame.columns:
        return frame
    out = frame.copy()
    out["bucket_start"] = pd.to_datetime(out["bucket_start"], utc=True)
    return out


def _bar_as_ist(value: Any) -> datetime | None:
    if value is None or (isinstance(value, float) and value != value):
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(IST).to_pydatetime()


def _normalize_bar_times(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty or "bucket_start" not in frame.columns:
        return frame
    out = frame.copy()
    ts = pd.to_datetime(out["bucket_start"], utc=True)
    out["bucket_start"] = ts.dt.tz_convert(IST)
    return out


def atomic_replace_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    _to_utc_aware(frame).to_parquet(tmp, index=False)
    os.replace(tmp, path)


def _prepare_ticks(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    work = frame.copy()
    ingested = work["ingested_at"] if "ingested_at" in work.columns else None
    instants: list[datetime | None] = []
    for i, ts in enumerate(work["timestamp"].tolist()):
        extra = None if ingested is None else ingested.iloc[i]
        instants.append(observation_ist(ts, extra))
    work["_ist"] = instants
    work = work[work["_ist"].notna()].copy()
    if work.empty:
        return work
    work["_price"] = work["last_price"].map(_usable_price) if "last_price" in work.columns else None
    work = work[work["_price"].notna()].copy()
    work = work.sort_values("_ist")
    work["_bucket"] = work["_ist"].map(lambda ts: nse_session_bucket(ts, "1m"))
    work = work[work["_bucket"].notna()]
    return work


def _bar_volume_and_vwap(
    group: pd.DataFrame,
    prev_cum_volume: float | None,
) -> tuple[float | None, float | None, float | None]:
    """Bar volume from last_quantity, else cumulative session volume delta. Never fake zero."""
    qty_sum = None
    qty_notional = 0.0
    qty_weight = 0.0
    if "last_quantity" in group.columns:
        qtys = [_nonneg(v) for v in group["last_quantity"].tolist()]
        prices = group["_price"].tolist()
        valid = [(p, q) for p, q in zip(prices, qtys) if q is not None and q > 0]
        if valid:
            qty_sum = float(sum(q for _p, q in valid))
            qty_notional = float(sum(p * q for p, q in valid))
            qty_weight = qty_sum
    last_cum = None
    if "volume" in group.columns:
        for value in reversed(group["volume"].tolist()):
            last_cum = _nonneg(value)
            if last_cum is not None:
                break
    bar_volume = qty_sum
    if bar_volume is None and last_cum is not None:
        first_cum = None
        for value in group["volume"].tolist():
            first_cum = _nonneg(value)
            if first_cum is not None:
                break
        if prev_cum_volume is not None:
            bar_volume = max(last_cum - prev_cum_volume, 0.0)
        elif first_cum is not None:
            intra = last_cum - first_cum
            # Intra-bucket delta only. Never treat session cumulative as bar volume.
            bar_volume = intra if intra > 0 else None
    vwap = None
    if qty_weight > 0:
        vwap = qty_notional / qty_weight
    elif bar_volume is not None and bar_volume > 0 and "volume" in group.columns:
        weighted = 0.0
        weight = 0.0
        prev = prev_cum_volume
        for rec in group.to_dict(orient="records"):
            cum = _nonneg(rec.get("volume"))
            px = rec.get("_price")
            if cum is None or px is None:
                continue
            if prev is None:
                prev = cum
                continue
            delta = max(cum - prev, 0.0)
            if delta > 0:
                weighted += float(px) * delta
                weight += delta
            prev = cum
        if weight > 0:
            vwap = weighted / weight
    if bar_volume == 0.0 and qty_sum is None:
        bar_volume = None
    return bar_volume, vwap, last_cum


def aggregate_1m_bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Deterministic 1-minute OHLCV from compacted observations. No empty-bar fill."""
    work = _prepare_ticks(frame)
    if work.empty:
        return pd.DataFrame(columns=list(BAR_COLUMNS))
    session_last = pd.Timestamp(work["_ist"].max())
    rows: list[dict[str, Any]] = []
    prev_cum: float | None = None
    for bucket, group in work.groupby("_bucket", sort=True):
        g = group.sort_values("_ist")
        prices = g["_price"].tolist()
        open_px = prices[0]
        high = max(prices)
        low = min(prices)
        close = prices[-1]
        volume, vwap, last_cum = _bar_volume_and_vwap(g, prev_cum)
        oi = None
        if "oi" in g.columns:
            for value in reversed(g["oi"].tolist()):
                number = _num(value)
                if number is not None and number >= 0:
                    oi = number
                    break
        bucket_end = pd.Timestamp(bucket) + timedelta(minutes=1)
        last_in_bucket = pd.Timestamp(g["_ist"].iloc[-1])
        bar_complete = not (session_last == last_in_bucket and last_in_bucket < bucket_end)
        rows.append(
            {
                "bucket_start": bucket,
                "open": open_px,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "oi": oi,
                "vwap": vwap,
                "trades_observed": int(len(g)),
                "bar_complete": bool(bar_complete),
            }
        )
        if last_cum is not None:
            prev_cum = last_cum
    return pd.DataFrame(rows, columns=list(BAR_COLUMNS))


def aggregate_daily_bar(
    frame: pd.DataFrame,
    *,
    session_date: str,
    previous_close: float | None = None,
) -> pd.DataFrame:
    """One daily bar from compacted observations. Does not roll up 1m/5m."""
    work = _prepare_ticks(frame)
    if work.empty:
        return pd.DataFrame(columns=list(BAR_COLUMNS) + list(DAILY_EXTRA))
    work = work.sort_values("_ist")
    prices = work["_price"].tolist()
    volume, vwap, _last_cum = _bar_volume_and_vwap(work, None)
    oi = None
    if "oi" in work.columns:
        for value in reversed(work["oi"].tolist()):
            number = _num(value)
            if number is not None and number >= 0:
                oi = number
                break
    open_dt = expected_session_buckets(session_date, "1D")[0]
    expected = expected_session_buckets(session_date, "1m")
    observed_minutes = int(work["_bucket"].nunique())
    complete = bool(expected) and observed_minutes >= len(expected)
    row = {
        "bucket_start": open_dt,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": volume,
        "oi": oi,
        "vwap": vwap,
        "trades_observed": int(len(work)),
        "bar_complete": complete,
        "session_date": session_date,
        "previous_close": previous_close,
        "settlement_price": None,
    }
    return pd.DataFrame([row], columns=list(BAR_COLUMNS) + list(DAILY_EXTRA))


def _validate_1m(frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    missing = [c for c in BAR_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f"1m bar schema missing {missing}")
    if frame["bucket_start"].duplicated().any():
        raise ValueError("duplicate 1m bucket_start")
    for _, rec in frame.iterrows():
        if None in (rec["open"], rec["high"], rec["low"], rec["close"]):
            raise ValueError("1m bar missing OHLC")


@dataclass
class BarBuildResult:
    session_date: str
    symbol: str
    minute_rows: int = 0
    daily_rows: int = 0
    minute_path: Path | None = None
    daily_path: Path | None = None
    skipped_reason: str | None = None


def materialize_symbol_session(
    settings: Settings,
    session_date: str,
    symbol: str,
    *,
    ticks: pd.DataFrame | None = None,
    previous_close: float | None = None,
) -> BarBuildResult:
    """Write closed-session 1m + daily bars from compacted ticks. Idempotent."""
    symbol = _safe_symbol(symbol)
    if ticks is None:
        path = settings.paths.compacted_dir / session_date / symbol / "ticks.parquet"
        if not path.is_file():
            return BarBuildResult(
                session_date, symbol, skipped_reason="no_compacted_ticks"
            )
        ticks = pd.read_parquet(path)
    if ticks is None or ticks.empty:
        return BarBuildResult(session_date, symbol, skipped_reason="empty_ticks")
    minute = aggregate_1m_bars(ticks)
    if minute.empty:
        return BarBuildResult(session_date, symbol, skipped_reason="no_session_observations")
    _validate_1m(minute)
    out_1m = minute_bar_path(settings, session_date, symbol)
    atomic_replace_parquet(out_1m, minute)
    daily_path = daily_bar_path(settings, symbol)
    if previous_close is None and daily_path.exists():
        prior_store = pd.read_parquet(daily_path)
        if "session_date" in prior_store.columns:
            prior = prior_store[prior_store["session_date"].astype(str) < session_date]
            if not prior.empty:
                previous_close = _usable_price(
                    prior.sort_values("session_date").iloc[-1].get("close")
                )
    daily = aggregate_daily_bar(
        ticks, session_date=session_date, previous_close=previous_close
    )
    if daily_path.exists():
        existing = pd.read_parquet(daily_path)
        if "session_date" in existing.columns:
            existing = existing[existing["session_date"] != session_date]
        elif "bucket_start" in existing.columns:
            existing = existing[
                pd.to_datetime(existing["bucket_start"], utc=True)
                .dt.tz_convert(IST)
                .dt.date.astype(str)
                != session_date
            ]
        combined = pd.concat([existing, daily], ignore_index=True)
    else:
        combined = daily
    combined = combined.sort_values("bucket_start")
    atomic_replace_parquet(daily_path, combined)
    return BarBuildResult(
        session_date=session_date,
        symbol=symbol,
        minute_rows=int(len(minute)),
        daily_rows=int(len(daily)),
        minute_path=out_1m,
        daily_path=daily_path,
    )


def list_compacted_sessions(settings: Settings) -> list[str]:
    root = settings.paths.compacted_dir
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir() and len(p.name) == 10)


def list_compacted_symbols(settings: Settings, session_date: str) -> list[str]:
    day = settings.paths.compacted_dir / session_date
    if not day.exists():
        return []
    return sorted(
        p.name
        for p in day.iterdir()
        if p.is_dir() and (p / "ticks.parquet").is_file()
    )


def backfill_compacted_bars(
    settings: Settings,
    *,
    dates: list[str] | None = None,
    symbols: list[str] | None = None,
) -> list[BarBuildResult]:
    """Idempotent T1 → T2/T3 backfill. Does not rewrite compacted ticks."""
    wanted_dates = dates or list_compacted_sessions(settings)
    wanted_symbols = set(symbols) if symbols else None
    results: list[BarBuildResult] = []
    for session_date in wanted_dates:
        names = list_compacted_symbols(settings, session_date)
        for symbol in names:
            if wanted_symbols is not None and symbol not in wanted_symbols:
                continue
            try:
                result = materialize_symbol_session(settings, session_date, symbol)
            except Exception as exc:  # noqa: BLE001 — record skip, keep going
                logger.exception("bar materialise failed %s/%s", session_date, symbol)
                result = BarBuildResult(
                    session_date, symbol, skipped_reason=f"error:{type(exc).__name__}"
                )
            results.append(result)
    return results


def summarize_bar_build(results: list[BarBuildResult]) -> dict[str, Any]:
    built = [r for r in results if r.skipped_reason is None]
    skipped = [r for r in results if r.skipped_reason]
    skip_reasons: dict[str, int] = {}
    for row in skipped:
        skip_reasons[row.skipped_reason or "unknown"] = skip_reasons.get(row.skipped_reason or "unknown", 0) + 1
    return {
        "sessions": sorted({r.session_date for r in results}),
        "sessions_materialized": sorted({r.session_date for r in built}),
        "symbol_days": len(results),
        "symbol_days_built": len(built),
        "symbols": sorted({r.symbol for r in built}),
        "symbols_skipped": sorted({r.symbol for r in skipped}),
        "minute_rows": int(sum(r.minute_rows for r in built)),
        "daily_rows": int(sum(r.daily_rows for r in built)),
        "skips": skip_reasons,
    }


def list_bar_dates(settings: Settings, symbol: str, *, limit: int | None = None) -> list[str]:
    root = bars_root(settings)
    if not root.exists():
        return []
    wanted = _safe_symbol(symbol)
    found: list[str] = []
    days = sorted(
        (p for p in root.iterdir() if p.is_dir() and p.name != "daily" and len(p.name) == 10),
        reverse=True,
    )
    for day in days:
        if (day / wanted / "1m.parquet").is_file():
            found.append(day.name)
        if limit is not None and len(found) >= int(limit):
            break
    return list(reversed(found))


def read_minute_bars(settings: Settings, symbol: str, dates: list[str]) -> pd.DataFrame:
    wanted = _safe_symbol(symbol)
    paths = [
        minute_bar_path(settings, session_date, wanted)
        for session_date in dates
        if minute_bar_path(settings, session_date, wanted).is_file()
    ]
    if not paths:
        return pd.DataFrame(columns=list(BAR_COLUMNS))
    import pyarrow.dataset as ds

    table = ds.dataset([str(path) for path in paths], format="parquet").to_table()
    frame = table.to_pandas()
    return _normalize_bar_times(frame).sort_values("bucket_start")


def read_daily_bars(settings: Settings, symbol: str) -> pd.DataFrame:
    path = daily_bar_path(settings, _safe_symbol(symbol))
    if not path.is_file():
        return pd.DataFrame(columns=list(BAR_COLUMNS) + list(DAILY_EXTRA))
    return _normalize_bar_times(pd.read_parquet(path)).sort_values("bucket_start")
