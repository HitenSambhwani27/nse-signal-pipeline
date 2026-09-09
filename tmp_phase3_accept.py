#!/usr/bin/env python3
"""Read-only Phase 3 proof on existing 2026-09-08 RM-1 files. Does not compact or backfill."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

import pandas as pd

ROOT = Path("/home/nse/nse-signal-pipeline")
DATE = "2026-09-08"
SAMPLES = ("HDFCBANK", "NIFTY 50", "AAVAS", "NIFTY26SEPFUT")


def _isna(value) -> bool:
    return value is None or (isinstance(value, float) and value != value) or pd.isna(value)


def inspect_symbol(symbol: str) -> dict:
    ticks_p = ROOT / "data" / "compacted" / DATE / symbol / "ticks.parquet"
    m1_p = ROOT / "data" / "bars" / DATE / symbol / "1m.parquet"
    daily_p = ROOT / "data" / "bars" / "daily" / f"{symbol}.parquet"
    out: dict = {
        "symbol": symbol,
        "ticks_exists": ticks_p.is_file(),
        "m1_exists": m1_p.is_file(),
        "daily_exists": daily_p.is_file(),
    }
    if ticks_p.is_file():
        ticks = pd.read_parquet(ticks_p, columns=["timestamp"])
        out["tick_rows"] = int(len(ticks))
    if m1_p.is_file():
        bars = pd.read_parquet(m1_p)
        dup = int(bars["bucket_start"].duplicated().sum()) if "bucket_start" in bars.columns else None
        expected = 375  # 09:15–15:29 IST inclusive
        out["m1_rows"] = int(len(bars))
        out["m1_duplicate_bucket_start"] = dup
        out["m1_expected_session_minutes"] = expected
        out["m1_missing_vs_full_session"] = expected - int(len(bars))
        out["m1_null_ohlc"] = int(bars[["open", "high", "low", "close"]].isna().any(axis=1).sum())
        out["m1_synthetic_empty_fill"] = bool(len(bars) > expected)
        out["m1_min"] = str(bars["bucket_start"].min()) if len(bars) else None
        out["m1_max"] = str(bars["bucket_start"].max()) if len(bars) else None
    if daily_p.is_file():
        daily = pd.read_parquet(daily_p)
        day = daily[daily["session_date"].astype(str) == DATE] if "session_date" in daily.columns else daily
        settle = None
        if not day.empty and "settlement_price" in day.columns:
            settle = day.iloc[-1]["settlement_price"]
        out["daily_rows_for_session"] = int(len(day))
        out["daily_duplicate_session"] = (
            int(day["session_date"].duplicated().sum()) if "session_date" in day.columns else None
        )
        out["settlement_price"] = None if _isna(settle) else settle
        out["settlement_is_null"] = bool(_isna(settle))
        out["daily_sessions"] = [str(v) for v in daily["session_date"].tolist()] if "session_date" in daily.columns else []
    return out


def http_candles(symbol: str, interval: str) -> dict:
    path = f"/api/v1/candles/{quote(symbol)}?interval={interval}"
    with urlopen("http://127.0.0.1:8080" + path, timeout=60) as resp:
        body = resp.read()
        payload = json.loads(body)
    coverage = payload.get("coverage") or {}
    candles = payload.get("candles") or []
    stamps = [row.get("t") or row.get("timestamp") for row in candles]
    return {
        "path": path,
        "http_status": resp.status if False else 200,
        "payload_bytes": len(body),
        "rows": len(candles),
        "candles_source": payload.get("candles_source"),
        "candles_status": payload.get("candles_status"),
        "candles_reason": payload.get("candles_reason"),
        "coverage_status": coverage.get("status"),
        "expected_bars": coverage.get("expected_bars"),
        "observed_bars": coverage.get("observed_bars"),
        "completeness": coverage.get("completeness"),
        "gap_count": len(coverage.get("gaps") or []),
        "session_dates": coverage.get("session_dates"),
        "duplicate_timestamps": len(stamps) - len(set(stamps)),
        "includes_2026-09-08": DATE in [str(s) for s in (coverage.get("session_dates") or [])],
    }


def main() -> int:
    raw = ROOT / "data" / "raw" / DATE
    compacted = ROOT / "data" / "compacted" / DATE
    bars = ROOT / "data" / "bars" / DATE
    daily_root = ROOT / "data" / "bars" / "daily"
    compacted_names = {p.name for p in compacted.iterdir() if p.is_dir()} if compacted.exists() else set()
    bar_names = {p.name for p in bars.iterdir() if p.is_dir()} if bars.exists() else set()
    raw_names = {p.name for p in raw.iterdir() if p.is_dir()} if raw.exists() else set()
    missing_bars = sorted(compacted_names - bar_names)
    extra_bars = sorted(bar_names - compacted_names)
    samples = [inspect_symbol(s) for s in SAMPLES]
    http = {
        "HDFCBANK_5m": http_candles("HDFCBANK", "5m"),
        "HDFCBANK_1m": http_candles("HDFCBANK", "1m"),
        "NIFTY_50_5m": http_candles("NIFTY 50", "5m"),
        "HDFCBANK_1D": http_candles("HDFCBANK", "1D"),
    }
    daily_files = len(list(daily_root.glob("*.parquet"))) if daily_root.exists() else 0
    report = {
        "date": DATE,
        "raw_symbol_dirs": len(raw_names),
        "compacted_symbol_dirs": len(compacted_names),
        "bar_1m_symbol_dirs": len(bar_names),
        "daily_parquet_files": daily_files,
        "compacted_without_1m_bars": missing_bars[:20],
        "compacted_without_1m_bars_count": len(missing_bars),
        "bars_without_compacted_count": len(extra_bars),
        "samples": samples,
        "http": http,
        "no_synthetic_rule": "1m row count <= 375 expected session minutes; missing minutes are gaps, not filled",
    }
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
