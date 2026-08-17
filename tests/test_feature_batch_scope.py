"""Scoped feature batch must not touch other tracks' parquet or sqlite rows."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from nse_pipeline.features.batch import run_feature_batch, symbols_for_tracks
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings


def _tick_row(symbol: str, ts: datetime, px: float) -> dict:
    return {
        "timestamp": ts,
        "instrument_token": 1,
        "symbol": symbol,
        "exchange": "NSE",
        "last_price": px,
        "volume": 10,
        "last_quantity": 1,
        "average_price": px,
        "oi": None,
        "bid_prices": [px - 0.5],
        "bid_quantities": [10],
        "bid_orders": [1],
        "ask_prices": [px + 0.5],
        "ask_quantities": [10],
        "ask_orders": [1],
        "source": "historical",
    }


def test_symbols_for_tracks_depth_only() -> None:
    cache = {
        "equity_depth": {"RELIANCE": {}},
        "equity_quote": {"AARTIIND": {}},
        "options": [{"tradingsymbol": "NIFTY25AUG25000CE"}],
        "futures": [{"tradingsymbol": "NIFTY25AUGFUT"}],
    }
    assert symbols_for_tracks(cache, ("equity_depth",)) == {"RELIANCE"}
    assert symbols_for_tracks(cache, None) is None


def test_depth_track_skips_quote_symbol_and_preserves_other_logs(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    day = "2026-08-13"
    ts = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    for symbol in ("RELIANCE", "AARTIIND"):
        out = settings.paths.compacted_dir / day / symbol
        out.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([_tick_row(symbol, ts, 100.0)]).to_parquet(
            out / "ticks.parquet", index=False
        )
    settings.paths.instruments_cache.write_text(
        json.dumps(
            {
                "equity_depth": {
                    "RELIANCE": {
                        "instrument_token": 1,
                        "tradingsymbol": "RELIANCE",
                        "exchange": "NSE",
                        "lot_size": 1,
                    }
                },
                "equity_quote": {
                    "AARTIIND": {
                        "instrument_token": 2,
                        "tradingsymbol": "AARTIIND",
                        "exchange": "NSE",
                        "lot_size": 1,
                    }
                },
                "options": [],
                "futures": [],
                "index": {},
            }
        ),
        encoding="utf-8",
    )
    store = SQLiteStore(settings.paths.sqlite_db)
    store.insert_feature_logs(
        [
            {
                "timestamp": ts.isoformat(),
                "trade_date": day,
                "symbol": "AARTIIND",
                "track": "equity_quote",
                "features": {"ltp": 1},
                "actual_outcome": None,
                "source": "historical",
                "feature_completeness": "historical_partial",
            }
        ]
    )
    summary = run_feature_batch(settings, day, tracks=("equity_depth",))
    assert summary["by_track"].get("equity_depth") == summary["feature_rows"]
    assert summary["symbols_seen"] == 1
    with store.connection() as conn:
        symbols = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT symbol FROM feature_log WHERE trade_date = ?",
                (day,),
            )
        }
    assert symbols == {"RELIANCE", "AARTIIND"}


def test_quote_workers_match_sequential_and_preserve_other_track(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    day = "2026-08-13"
    ts = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    for symbol in ("AARTIIND", "ZYDUSLIFE", "RELIANCE"):
        out = settings.paths.compacted_dir / day / symbol
        out.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([_tick_row(symbol, ts, 100.0)]).to_parquet(
            out / "ticks.parquet", index=False
        )
    settings.paths.instruments_cache.write_text(
        json.dumps(
            {
                "equity_depth": {
                    "RELIANCE": {
                        "instrument_token": 1,
                        "tradingsymbol": "RELIANCE",
                        "exchange": "NSE",
                        "lot_size": 1,
                    }
                },
                "equity_quote": {
                    "AARTIIND": {
                        "instrument_token": 2,
                        "tradingsymbol": "AARTIIND",
                        "exchange": "NSE",
                        "lot_size": 1,
                    },
                    "ZYDUSLIFE": {
                        "instrument_token": 3,
                        "tradingsymbol": "ZYDUSLIFE",
                        "exchange": "NSE",
                        "lot_size": 1,
                    },
                },
                "options": [],
                "futures": [],
                "index": {},
            }
        ),
        encoding="utf-8",
    )
    store = SQLiteStore(settings.paths.sqlite_db)
    store.insert_feature_logs(
        [
            {
                "timestamp": ts.isoformat(),
                "trade_date": day,
                "symbol": "RELIANCE",
                "track": "equity_depth",
                "features": {"ltp": 1},
                "actual_outcome": None,
                "source": "historical",
                "feature_completeness": "historical_partial",
            }
        ]
    )
    sequential = run_feature_batch(settings, day, tracks=("equity_quote",), workers=1)
    parallel = run_feature_batch(settings, day, tracks=("equity_quote",), workers=2)
    assert sequential["feature_rows"] == parallel["feature_rows"]
    assert sequential["symbols_seen"] == parallel["symbols_seen"] == 2
    with store.connection() as conn:
        by_track = dict(
            conn.execute(
                "SELECT track, COUNT(*) FROM feature_log WHERE trade_date = ? "
                "GROUP BY track",
                (day,),
            )
        )
    assert by_track["equity_depth"] == 1
    assert by_track["equity_quote"] == parallel["feature_rows"]
