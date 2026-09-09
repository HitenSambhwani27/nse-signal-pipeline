#!/usr/bin/env python3
"""Phase 3 VM: time RM-1 stages and HTTP /candles. No Kite, no ingest restart."""

from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

ROOT = Path("/home/nse/nse-signal-pipeline")


def pctl(samples: list[float], q: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return float("nan")
    idx = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def http_get(path: str) -> tuple[int, bytes, float]:
    t0 = time.perf_counter()
    with urlopen("http://127.0.0.1:8080" + path, timeout=60) as resp:
        body = resp.read()
        ms = (time.perf_counter() - t0) * 1000
        return resp.status, body, ms


def stage_profile(settings, symbol: str, interval: str, lookback_days: int) -> dict:
    from nse_pipeline.market.ohlc import candle_rows, load_ohlc_candles, rollup_1m_bars, _coverage
    from nse_pipeline.storage.bars import list_bar_dates, read_minute_bars

    marks: dict[str, float] = {}
    t0 = time.perf_counter()
    dates = list_bar_dates(settings, symbol, limit=lookback_days)
    marks["list_dates_ms"] = (time.perf_counter() - t0) * 1000
    t1 = time.perf_counter()
    minute = read_minute_bars(settings, symbol, dates)
    marks["read_parquet_ms"] = (time.perf_counter() - t1) * 1000
    t2 = time.perf_counter()
    rolled = rollup_1m_bars(minute, interval)
    marks["rollup_ms"] = (time.perf_counter() - t2) * 1000
    t3 = time.perf_counter()
    candles = candle_rows(rolled)
    marks["candle_rows_ms"] = (time.perf_counter() - t3) * 1000
    t4 = time.perf_counter()
    coverage = _coverage(interval=interval, dates=dates, candles=candles)
    marks["coverage_ms"] = (time.perf_counter() - t4) * 1000
    t5 = time.perf_counter()
    payload = load_ohlc_candles(settings, symbol, interval=interval, lookback_days=lookback_days)
    marks["load_ohlc_total_ms"] = (time.perf_counter() - t5) * 1000
    blob = json.dumps(payload, default=str)
    marks["json_dumps_ms"] = 0.0
    t6 = time.perf_counter()
    blob = json.dumps(payload, default=str)
    marks["json_dumps_ms"] = (time.perf_counter() - t6) * 1000
    return {
        "symbol": symbol,
        "interval": interval,
        "sessions": dates,
        "session_count": len(dates),
        "minute_rows": int(len(minute)),
        "candle_rows": len(candles),
        "payload_bytes": len(blob.encode("utf-8")),
        "stages_ms": marks,
        "candles_source": payload.get("candles_source"),
        "candles_status": payload.get("candles_status"),
    }


def http_bench(symbol: str, interval: str, n_cold: int = 1, n_warm: int = 20) -> dict:
    path = f"/api/v1/candles/{quote(symbol)}?interval={interval}"
    cold = []
    status = 0
    body = b""
    payload = {}
    for _ in range(n_cold):
        status, body, ms = http_get(path)
        cold.append(ms)
        payload = json.loads(body)
    warm = []
    for _ in range(n_warm):
        status, body, ms = http_get(path)
        warm.append(ms)
        payload = json.loads(body)
    return {
        "path": path,
        "http_status": status,
        "payload_bytes": len(body),
        "rows": len(payload.get("candles") or []),
        "session_dates": (payload.get("coverage") or {}).get("session_dates"),
        "candles_source": payload.get("candles_source"),
        "candles_status": payload.get("candles_status"),
        "cold_ms": cold,
        "cold_p50": pctl(cold, 0.5),
        "cold_p95": pctl(cold, 0.95),
        "warm_ms_n": len(warm),
        "warm_p50": pctl(warm, 0.5),
        "warm_p95": pctl(warm, 0.95),
        "targets_ms": {"five_m_10s_cold_p95": 120, "five_m_10s_warm_p95": 40},
    }


def main() -> int:
    import sys

    sys.path.insert(0, str(ROOT / "src"))
    from nse_pipeline.config import load_settings

    settings = load_settings()
    symbol = "HDFCBANK"
    interval = "5m"
    lookback = int(settings.analytics.chart_lookback_days)
    # Discard first in-process import/parquet open, then profile.
    stage_profile(settings, symbol, interval, lookback)
    profile = stage_profile(settings, symbol, interval, lookback)
    http = http_bench(symbol, interval, n_cold=3, n_warm=25)
    out = {"in_process": profile, "http": http}
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
