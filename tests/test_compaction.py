"""Tests for daily tick compaction + DuckDB query layer."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from nse_pipeline.config import (
    CompactionSettings,
    FeaturesSettings,
    FuturesSettings,
    HistoricalSettings,
    IngestionSettings,
    KiteCredentials,
    OptionsSettings,
    PathsSettings,
    ResilienceSettings,
    SessionSettings,
    Settings,
    SignalSettings,
    TripleBarrierSettings,
    UniverseSettings,
)
from nse_pipeline.storage.compaction import compact_date, compact_symbol_day
from nse_pipeline.storage.duckdb_store import DuckDBTickStore


def _settings(tmp: Path) -> Settings:
    raw = tmp / "raw"
    compacted = tmp / "compacted"
    return Settings(
        paths=PathsSettings(
            data_dir=tmp,
            raw_dir=raw,
            compacted_dir=compacted,
            features_dir=tmp / "features",
            models_dir=tmp / "models",
            sqlite_db=tmp / "test.db",
            logs_dir=tmp / "logs",
            instruments_cache=tmp / "cache.json",
            membership_dir=tmp / "membership",
        ),
        universe=UniverseSettings(
            nifty100_csv_url="x",
            nifty500_csv_url="x",
            nifty100_csv_fallback_url="x",
            nifty500_csv_fallback_url="x",
            membership_max_age_days=7,
        ),
        index_symbols=[],
        options=OptionsSettings(exchange="NFO", underlyings=[]),
        futures=FuturesSettings(
            exchange="NFO", underlyings=[], contract_count=2, subscribe_mode="full"
        ),
        ingestion=IngestionSettings(
            flush_interval_seconds=45, flush_max_rows=5000, websocket_mode="full"
        ),
        compaction=CompactionSettings(archive_minute_files=False, archive_subdir="_minute_parts"),
        session=SessionSettings(
            timezone="Asia/Kolkata",
            market_open="09:15",
            market_close="15:30",
            open_grace_minutes=15,
            close_grace_minutes=15,
        ),
        historical=HistoricalSettings(interval="5minute", lookback_days=1),
        features=FeaturesSettings(
            oi_bucket_minutes=5,
            max_tick_return_pct=5.0,
            options_max_tick_return_pct=25.0,
            basis_days_per_year=365,
        ),
        signal=SignalSettings(
            candle_interval_minutes=5,
            threshold_pct=0.15,
            label_mode="triple_barrier",
            triple_barrier=TripleBarrierSettings(
                vol_method="ewma",
                vol_window=20,
                barrier_multiplier=1.0,
                min_threshold_pct=0.05,
                max_threshold_pct=5.0,
                path_rule="close_vs_barriers",
                options_label_mode="raw_premium",
            ),
            vol_min_periods=5,
        ),
        resilience=ResilienceSettings(2.0, 60.0, 2.0),
        kite=KiteCredentials("", "", ""),
    )


def _write_minute(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_compact_symbol_day_merges_and_is_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    symbol_dir = settings.paths.raw_dir / "2026-08-12" / "RELIANCE"
    ts0 = datetime(2026, 8, 12, 5, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 8, 12, 5, 1, tzinfo=timezone.utc)
    base = {
        "instrument_token": 1,
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "last_price": 100.0,
        "volume": 1,
        "last_quantity": 1,
        "average_price": 100.0,
        "oi": None,
        "bid_prices": [99.0],
        "bid_quantities": [10],
        "bid_orders": [1],
        "ask_prices": [101.0],
        "ask_quantities": [10],
        "ask_orders": [1],
    }
    _write_minute(symbol_dir / "ticks_0500.parquet", [{**base, "timestamp": ts0}])
    _write_minute(symbol_dir / "ticks_0501.parquet", [{**base, "timestamp": ts1, "last_price": 101.0}])

    out_dir = settings.paths.compacted_dir / "2026-08-12" / "RELIANCE"
    r1 = compact_symbol_day(
        raw_symbol_dir=symbol_dir,
        compacted_symbol_dir=out_dir,
        archive_minute_files=False,
        archive_subdir="_minute_parts",
    )
    assert r1.rows == 2
    assert r1.output_path is not None and r1.output_path.exists()

    r2 = compact_symbol_day(
        raw_symbol_dir=symbol_dir,
        compacted_symbol_dir=out_dir,
        archive_minute_files=False,
        archive_subdir="_minute_parts",
    )
    assert r2.rows == 2

    with DuckDBTickStore(settings) as store:
        df = store.read_ticks("2026-08-12", "RELIANCE")
        assert len(df) == 2
        assert list(df["last_price"]) == [100.0, 101.0]


def test_compact_date_skips_candle_only_folders(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    day = "2026-08-03"
    only_candles = settings.paths.raw_dir / day / "FOO"
    only_candles.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": [datetime(2026, 8, 3, tzinfo=timezone.utc)],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0],
            "oi": [None],
        }
    ).to_parquet(only_candles / "candles.parquet", index=False)

    results = compact_date(settings, day)
    assert len(results) == 1
    assert results[0].skipped_reason == "no_minute_parts"
