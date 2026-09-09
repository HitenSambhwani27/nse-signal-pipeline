"""Phase 3 RM-1 bars: aggregation, gaps, rollups, /candles, /series. No tick-scan reads."""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.market.ohlc import load_ohlc_candles, rollup_1m_bars
from nse_pipeline.storage.bars import (
    BAR_COLUMNS,
    aggregate_1m_bars,
    aggregate_daily_bar,
    atomic_replace_parquet,
    backfill_compacted_bars,
    expected_session_buckets,
    materialize_symbol_session,
    minute_bar_path,
    read_daily_bars,
    read_minute_bars,
    summarize_bar_build,
)
from nse_pipeline.storage.compaction import compact_date
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_last_session_fallback import _read_model
from tests.test_phase1_semantics import test_freshness_live_stale_last_session_no_data
from tests.test_phase2_session_reference import test_quote_path_consumes_rm2
from tests.test_timestamp_provenance import test_kite_naive_ist_is_localized_not_labeled_utc

IST = ZoneInfo("Asia/Kolkata")


def _ist(h: int, m: int, s: int = 0, day: int = 4) -> datetime:
    return datetime(2026, 9, day, h, m, s, tzinfo=IST)


def _tick(
    when: datetime,
    last_price: float,
    *,
    volume: float | None = None,
    last_quantity: float | None = None,
    oi: float | None = None,
    ingested_at: datetime | None = None,
) -> dict:
    return {
        "timestamp": when,
        "last_price": last_price,
        "volume": volume,
        "last_quantity": last_quantity,
        "oi": oi,
        "ingested_at": ingested_at or when,
    }


def _write_compacted(settings, date_str: str, symbol: str, rows: list[dict]) -> None:
    folder = settings.paths.compacted_dir / date_str / symbol
    folder.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(folder / "ticks.parquet", index=False)


def test_1m_ohlc_first_last_high_low() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 1), 100.0, last_quantity=1, volume=1),
            _tick(_ist(9, 15, 20), 108.0, last_quantity=1, volume=2),
            _tick(_ist(9, 15, 40), 97.0, last_quantity=1, volume=3),
            _tick(_ist(9, 15, 50), 103.0, last_quantity=1, volume=4),
        ]
    )
    bars = aggregate_1m_bars(frame)
    assert list(bars.columns) == list(BAR_COLUMNS)
    assert len(bars) == 1
    row = bars.iloc[0]
    assert row["open"] == 100.0
    assert row["high"] == 108.0
    assert row["low"] == 97.0
    assert row["close"] == 103.0
    assert pd.Timestamp(row["bucket_start"]).tz_convert(IST) == _ist(9, 15)


def test_cumulative_volume_is_delta_not_session_total() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 0), 10.0, volume=1000),
            _tick(_ist(9, 15, 30), 11.0, volume=1010),
            _tick(_ist(9, 16, 0), 12.0, volume=1010),
            _tick(_ist(9, 16, 20), 13.0, volume=1030),
        ]
    )
    bars = aggregate_1m_bars(frame)
    assert list(bars["volume"]) == [10.0, 20.0]
    assert bars.iloc[0]["volume"] != 1010.0


def test_last_quantity_volume_and_vwap() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 0), 100.0, last_quantity=10, volume=9999, oi=50),
            _tick(_ist(9, 15, 30), 110.0, last_quantity=10, volume=10010, oi=55),
        ]
    )
    bars = aggregate_1m_bars(frame)
    row = bars.iloc[0]
    assert row["volume"] == 20.0
    assert row["vwap"] == 105.0
    assert row["oi"] == 55.0
    assert row["trades_observed"] == 2


def test_vwap_and_volume_null_without_quantity_information() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 0), 100.0, volume=None, last_quantity=None, oi=None),
            _tick(_ist(9, 15, 10), 101.0, volume=None, last_quantity=None, oi=None),
        ]
    )
    row = aggregate_1m_bars(frame).iloc[0]
    assert pd.isna(row["volume"]) or row["volume"] is None
    assert pd.isna(row["vwap"]) or row["vwap"] is None
    assert pd.isna(row["oi"]) or row["oi"] is None
    assert row["trades_observed"] == 2


def test_oi_zero_is_kept_not_fabricated() -> None:
    frame = pd.DataFrame([_tick(_ist(9, 15, 0), 100.0, last_quantity=1, oi=0)])
    assert aggregate_1m_bars(frame).iloc[0]["oi"] == 0.0


def test_trades_observed_skips_invalid_prices() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 0), 0.0, last_quantity=1),
            _tick(_ist(9, 15, 1), 100.0, last_quantity=1),
            {**_tick(_ist(9, 15, 2), 101.0, last_quantity=1), "last_price": None},
        ]
    )
    bars = aggregate_1m_bars(frame)
    assert len(bars) == 1
    assert bars.iloc[0]["trades_observed"] == 1
    assert bars.iloc[0]["open"] == 100.0


def test_bar_complete_only_last_open_bucket_is_incomplete() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 10), 100.0, last_quantity=1),
            _tick(_ist(9, 16, 5), 101.0, last_quantity=1),
        ]
    )
    bars = aggregate_1m_bars(frame)
    assert bool(bars.iloc[0]["bar_complete"]) is True
    assert bool(bars.iloc[1]["bar_complete"]) is False


def test_missing_minute_is_gap_not_synthetic() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 15, 0), 100.0, last_quantity=1),
            _tick(_ist(9, 17, 0), 102.0, last_quantity=1),
        ]
    )
    bars = aggregate_1m_bars(frame)
    starts = [
        pd.Timestamp(ts).tz_convert(IST).strftime("%H:%M") for ts in bars["bucket_start"]
    ]
    assert starts == ["09:15", "09:17"]
    assert "09:16" not in starts


def test_timezone_session_boundaries() -> None:
    frame = pd.DataFrame(
        [
            _tick(_ist(9, 14, 59), 1.0, last_quantity=1),
            _tick(_ist(9, 15, 0), 2.0, last_quantity=1),
            _tick(_ist(15, 30, 0), 3.0, last_quantity=1),
            _tick(_ist(15, 31, 0), 4.0, last_quantity=1),
            _tick(datetime(2026, 9, 4, 3, 44, tzinfo=ZoneInfo("UTC")), 5.0, last_quantity=1),
            _tick(datetime(2026, 9, 4, 3, 45, 1, tzinfo=ZoneInfo("UTC")), 6.0, last_quantity=1),
        ]
    )
    bars = aggregate_1m_bars(frame)
    minutes = {
        pd.Timestamp(ts).tz_convert(IST).strftime("%H:%M") for ts in bars["bucket_start"]
    }
    assert "09:14" not in minutes
    assert "15:31" not in minutes
    assert "09:15" in minutes
    assert "15:29" in minutes
    opens = {
        pd.Timestamp(row.bucket_start).tz_convert(IST).strftime("%H:%M"): row.open
        for row in bars.itertuples()
    }
    assert opens["09:15"] == 2.0


def test_idempotent_atomic_closed_session(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    rows = [
        _tick(_ist(9, 15, 0), 100.0, last_quantity=2, volume=2, oi=9),
        _tick(_ist(9, 16, 0), 101.0, last_quantity=3, volume=5, oi=10),
    ]
    _write_compacted(settings, "2026-09-04", "HDFCBANK", rows)
    first = materialize_symbol_session(settings, "2026-09-04", "HDFCBANK")
    path = minute_bar_path(settings, "2026-09-04", "HDFCBANK")
    assert first.skipped_reason is None
    assert path.exists()
    assert not path.with_name("1m.parquet.tmp").exists()
    before = pd.read_parquet(path)
    second = materialize_symbol_session(settings, "2026-09-04", "HDFCBANK")
    after = pd.read_parquet(path)
    assert second.minute_rows == first.minute_rows
    assert after["bucket_start"].equals(before["bucket_start"])
    assert after.duplicated(subset=["bucket_start"]).sum() == 0
    atomic_replace_parquet(path, after)
    assert path.exists()
    assert not list(path.parent.glob("*.tmp"))


def test_daily_bar_null_semantics(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    sparse = [_tick(_ist(9, 15, 0), 50.0)]
    _write_compacted(settings, "2026-09-04", "INFY", sparse)
    result = materialize_symbol_session(settings, "2026-09-04", "INFY")
    assert result.daily_rows == 1
    daily = read_daily_bars(settings, "INFY")
    row = daily.iloc[0]
    assert row["open"] == 50.0
    assert row["close"] == 50.0
    assert row["session_date"] == "2026-09-04"
    assert row["settlement_price"] is None or pd.isna(row["settlement_price"])
    assert row["volume"] is None or pd.isna(row["volume"])
    assert row["vwap"] is None or pd.isna(row["vwap"])
    assert bool(row["bar_complete"]) is False
    expected = expected_session_buckets("2026-09-04", "1m")
    assert int(row["trades_observed"]) < len(expected)


def test_daily_from_ticks_not_from_intraday_rollup() -> None:
    ticks = pd.DataFrame(
        [
            _tick(_ist(9, 15, 0), 10.0, last_quantity=1),
            _tick(_ist(15, 29, 0), 20.0, last_quantity=1),
        ]
    )
    daily = aggregate_daily_bar(ticks, session_date="2026-09-04")
    assert daily.iloc[0]["open"] == 10.0
    assert daily.iloc[0]["high"] == 20.0
    assert daily.iloc[0]["low"] == 10.0
    assert daily.iloc[0]["close"] == 20.0


def test_rollups_3m_to_60m() -> None:
    rows = []
    px = 100.0
    for minute in range(60):
        stamp = _ist(9, 15) + timedelta(minutes=minute)
        rows.append(
            {
                "bucket_start": stamp,
                "open": px,
                "high": px + 1,
                "low": px - 1,
                "close": px + 0.5,
                "volume": 2.0,
                "oi": 10.0 + minute,
                "vwap": px,
                "trades_observed": 1,
                "bar_complete": True,
            }
        )
        px += 0.5
    frame = pd.DataFrame(rows)
    expected_counts = {"3m": 20, "5m": 12, "10m": 6, "15m": 4, "30m": 2, "60m": 1}
    for interval, count in expected_counts.items():
        rolled = rollup_1m_bars(frame, interval)
        assert len(rolled) == count, interval
        assert bool(rolled.iloc[0]["bar_complete"]) is True
        assert rolled.iloc[0]["open"] == 100.0
        assert rolled.iloc[-1]["close"] == frame.iloc[count * (int(interval.rstrip("m"))) - 1]["close"]


def test_incomplete_rollup_is_not_invented_complete() -> None:
    frame = pd.DataFrame(
        [
            {
                "bucket_start": _ist(9, 15),
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1.0,
                "oi": None,
                "vwap": 1.0,
                "trades_observed": 1,
                "bar_complete": True,
            },
            {
                "bucket_start": _ist(9, 16),
                "open": 2.0,
                "high": 2.0,
                "low": 2.0,
                "close": 2.0,
                "volume": 1.0,
                "oi": None,
                "vwap": 2.0,
                "trades_observed": 1,
                "bar_complete": True,
            },
        ]
    )
    rolled = rollup_1m_bars(frame, "5m")
    assert len(rolled) == 1
    assert rolled.iloc[0]["open"] == 1.0
    assert rolled.iloc[0]["close"] == 2.0
    assert bool(rolled.iloc[0]["bar_complete"]) is False


def test_candles_and_series_read_bars_not_ticks(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    _write_compacted(
        settings,
        "2026-09-04",
        "HDFCBANK",
        [
            _tick(_ist(9, 15, 0), 100.0, last_quantity=4, volume=4, oi=8),
            _tick(_ist(9, 16, 0), 110.0, last_quantity=6, volume=10, oi=9),
        ],
    )
    materialize_symbol_session(settings, "2026-09-04", "HDFCBANK")

    def _boom(*_args, **_kwargs):
        raise AssertionError("candle/series path must not open the tick store")

    monkeypatch.setattr("nse_pipeline.storage.duckdb_store.DuckDBTickStore", _boom)

    client = TestClient(create_app(settings))
    candles = client.get("/api/v1/candles/HDFCBANK?interval=1m").json()
    assert candles["candles_status"] == "partial"
    assert candles["candles_source"] == "bars_1m"
    assert candles["candles_reason"] is None
    assert candles["coverage"]["session_dates"] == ["2026-09-04"]
    assert candles["coverage"]["gaps"]
    assert candles["candles"][0]["o"] == 100.0
    assert candles["candles"][0]["complete"] is True
    series = client.get("/api/v1/series/HDFCBANK?fields=volume,oi&interval=1m").json()
    assert series["series_status"] == "partial"
    assert series["series_source"] == "bars_1m"
    assert series["series"]["volume"][0]["volume"] == 4.0
    assert series["series"]["oi"][-1]["oi"] == 9.0
    rolled = client.get("/api/v1/candles/HDFCBANK?interval=5m").json()
    assert rolled["candles_source"] == "bars_1m_rollup"
    assert rolled["candles"][0]["complete"] is False


def test_unavailable_bar_contract(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    client = TestClient(create_app(settings))
    payload = client.get("/api/v1/candles/MISSING?interval=5m").json()
    assert payload["candles"] == []
    assert payload["candles_status"] == "unavailable"
    assert payload["candles_reason"] == "bars_not_built"
    series = client.get("/api/v1/series/MISSING?fields=volume,oi").json()
    assert series["series_status"] == "unavailable"
    assert series["series_reason"] == "bars_not_built"


def test_coverage_metadata_and_partial_session(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_compacted(
        settings,
        "2026-09-04",
        "NIFTY 50",
        [_tick(_ist(9, 15, 0), 100.0, last_quantity=1), _tick(_ist(9, 20, 0), 101.0, last_quantity=1)],
    )
    materialize_symbol_session(settings, "2026-09-04", "NIFTY 50")
    payload = load_ohlc_candles(settings, "NIFTY 50", interval="1m", lookback_days=10)
    coverage = payload["coverage"]
    assert coverage["status"] == "partial"
    assert coverage["expected_bars"] == len(expected_session_buckets("2026-09-04", "1m"))
    assert coverage["observed_bars"] == 2
    assert coverage["completeness"] < 1.0
    assert coverage["gaps"]
    assert coverage["session_dates"] == ["2026-09-04"]
    assert coverage["returned_bars"] == 2


def test_compact_date_materializes_rm1(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    raw = settings.paths.raw_dir / "2026-09-04" / "RELIANCE"
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                **_tick(_ist(9, 15, 0), 100.0, last_quantity=1, volume=1),
                "instrument_token": 1,
                "symbol": "RELIANCE",
                "exchange": "NSE",
            },
            {
                **_tick(_ist(9, 16, 0), 101.0, last_quantity=1, volume=2),
                "instrument_token": 1,
                "symbol": "RELIANCE",
                "exchange": "NSE",
            },
        ]
    ).to_parquet(raw / "ticks_0915.parquet", index=False)
    results = compact_date(settings, "2026-09-04")
    assert results[0].rows == 2
    built = materialize_symbol_session(settings, "2026-09-04", "RELIANCE")
    assert built.minute_rows >= 1
    assert minute_bar_path(settings, "2026-09-04", "RELIANCE").exists()
    again = backfill_compacted_bars(settings, dates=["2026-09-04"], symbols=["RELIANCE"])
    summary = summarize_bar_build(again)
    assert summary["minute_rows"] == built.minute_rows


def test_read_path_performance_from_realistic_bars(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    symbol = "HDFCBANK"
    dates = []
    for i in range(10):
        day = datetime(2026, 8, 24) + timedelta(days=i)
        date_str = day.date().isoformat()
        dates.append(date_str)
        rows = []
        px = 1600.0 + i
        for minute in expected_session_buckets(date_str, "1m"):
            rows.append(
                {
                    "bucket_start": minute,
                    "open": px,
                    "high": px + 1,
                    "low": px - 1,
                    "close": px,
                    "volume": 100.0,
                    "oi": None,
                    "vwap": px,
                    "trades_observed": 3,
                    "bar_complete": True,
                }
            )
        path = minute_bar_path(settings, date_str, symbol)
        atomic_replace_parquet(path, pd.DataFrame(rows))
    daily_rows = []
    start = datetime(2024, 1, 2)
    for i in range(500):
        day = start + timedelta(days=i)
        date_str = day.date().isoformat()
        open_dt = datetime.combine(day.date(), datetime.min.time().replace(hour=9, minute=15), tzinfo=IST)
        daily_rows.append(
            {
                "bucket_start": open_dt,
                "open": 1600.0,
                "high": 1610.0,
                "low": 1590.0,
                "close": 1605.0,
                "volume": 1000.0,
                "oi": None,
                "vwap": 1602.0,
                "trades_observed": 375,
                "bar_complete": True,
                "session_date": date_str,
                "previous_close": None,
                "settlement_price": None,
            }
        )
    from nse_pipeline.storage.bars import daily_bar_path

    atomic_replace_parquet(daily_bar_path(settings, symbol), pd.DataFrame(daily_rows))

    def _p95(samples: list[float]) -> float:
        ordered = sorted(samples)
        idx = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * 0.95)))
        return ordered[idx]

    # First call includes interpreter/parquet first-open; keep it as cold_first.
    t0 = time.perf_counter()
    payload = load_ohlc_candles(settings, symbol, interval="5m", lookback_days=10)
    cold_first = (time.perf_counter() - t0) * 1000
    assert payload["candles"]
    assert payload["candles_source"] == "bars_1m_rollup"
    cold = []
    for _ in range(5):
        t0 = time.perf_counter()
        payload = load_ohlc_candles(settings, symbol, interval="5m", lookback_days=10)
        cold.append((time.perf_counter() - t0) * 1000)
    warm = []
    for _ in range(8):
        t0 = time.perf_counter()
        warm_payload = load_ohlc_candles(settings, symbol, interval="5m", lookback_days=10)
        warm.append((time.perf_counter() - t0) * 1000)
    daily_times = []
    for _ in range(5):
        t0 = time.perf_counter()
        daily = load_ohlc_candles(settings, symbol, interval="1D", lookback_days=500, max_points=500)
        daily_times.append((time.perf_counter() - t0) * 1000)
    stats = {
        "five_m_rows": len(warm_payload["candles"]),
        "five_m_cold_first_ms": cold_first,
        "five_m_cold_p50": sorted(cold)[len(cold) // 2],
        "five_m_cold_p95": _p95(cold),
        "five_m_warm_p50": sorted(warm)[len(warm) // 2],
        "five_m_warm_p95": _p95(warm),
        "daily_rows": len(daily["candles"]),
        "daily_p50": sorted(daily_times)[len(daily_times) // 2],
        "daily_p95": _p95(daily_times),
        "strategy": "parquet_rm1_dataset_rollup",
        "targets_ms": {"five_m_cold_p95": 120, "five_m_warm_p95": 40, "daily_p95": 80},
    }
    print("phase3_perf", stats)
    assert len(warm_payload["candles"]) == 750
    assert payload["candles_source"] == "bars_1m_rollup"
    assert daily["candles_source"] == "bars_daily"
    assert len(daily["candles"]) == 500
    # Wall-clock SLOs are measured on the VM. This only guards the old 32 s tick scan.
    assert cold_first < 15_000.0
    assert stats["five_m_warm_p50"] < 15_000.0
    assert stats["daily_p50"] < 15_000.0


def test_phase1_timestamp_regression(tmp_path: Path) -> None:
    test_kite_naive_ist_is_localized_not_labeled_utc()
    test_freshness_live_stale_last_session_no_data(tmp_path)


def test_phase2_session_reference_regression(tmp_path: Path) -> None:
    test_quote_path_consumes_rm2(tmp_path)


def test_maturity_gate_still_sixty_days(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=_ist(10, 32, day=7), seed_history=True)
    maturity = read.maturity()["maturity"]["equity"]
    assert maturity["probability_permitted"] is False
    assert "/60 pooled days" in maturity["display"]
