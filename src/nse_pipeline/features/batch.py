"""
Stage 2E — batch feature job over compacted Parquet via DuckDB.

Idempotent per trade_date: deletes existing feature_log / quality_log rows for
that date before inserting fresh ones.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import pandas as pd

from nse_pipeline.broker.instruments import load_instrument_cache
from nse_pipeline.config import Settings
from nse_pipeline.features.equity import compute_equity_features
from nse_pipeline.features.futures import (
    bucket_futures_series,
    compute_futures_features,
)
from nse_pipeline.features.options import (
    bucket_option_series,
    compute_option_features_for_symbol,
    compute_pcr_by_bucket,
)
from nse_pipeline.features.quality import filter_bad_ticks
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)

UNDERLYING_SPOT = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
}


def _spot_series_by_bucket(
    store: DuckDBTickStore,
    date_str: str,
    spot_symbol: str,
    bucket_minutes: int,
) -> dict[pd.Timestamp, float]:
    if spot_symbol not in store.list_symbols(date_str):
        logger.warning("Spot %s not in compacted day %s", spot_symbol, date_str)
        return {}
    ticks = store.read_ticks(date_str, spot_symbol)
    if ticks.empty:
        return {}
    ticks = ticks.sort_values("timestamp").copy()
    ticks["timestamp"] = pd.to_datetime(ticks["timestamp"], utc=True)
    ticks["bucket"] = ticks["timestamp"].dt.floor(f"{bucket_minutes}min") + pd.Timedelta(
        minutes=bucket_minutes
    )
    last = ticks.groupby("bucket", sort=True)["last_price"].last()
    return {pd.Timestamp(k): float(v) for k, v in last.items()}


def _build_meta(cache: dict[str, Any]) -> dict[str, dict[str, Any]]:
    meta: dict[str, dict[str, Any]] = {}
    for symbol, info in cache.get("equity_depth", {}).items():
        meta[symbol] = {**info, "bucket_kind": "equity", "subscribe_mode": "full"}
    for symbol, info in cache.get("equity_quote", {}).items():
        meta[symbol] = {**info, "bucket_kind": "equity", "subscribe_mode": "quote"}
    for info in cache.get("options", []):
        meta[info["tradingsymbol"]] = {
            **info,
            "bucket_kind": "options",
            "subscribe_mode": "full",
            "underlying": info.get("name"),
            "option_type": info.get("instrument_type"),
        }
    for info in cache.get("futures", []):
        meta[info["tradingsymbol"]] = {
            **info,
            "bucket_kind": "futures",
            "subscribe_mode": info.get("subscribe_mode", "full"),
            "underlying": info.get("name"),
        }
    return meta


def run_feature_batch(settings: Settings, date_str: str) -> dict[str, Any]:
    """Compute equity / options / futures features for one compacted trade date."""
    cache = load_instrument_cache(settings.paths.instruments_cache)
    store_sql = SQLiteStore(settings.paths.sqlite_db)
    bucket_minutes = settings.features.oi_bucket_minutes
    max_jump = settings.features.max_tick_return_pct
    options_max_jump = settings.features.options_max_tick_return_pct
    meta = _build_meta(cache)

    feature_rows: list[dict[str, Any]] = []
    quality_buffer: list[dict[str, Any]] = []
    ingest_halt_buffer: list[dict[str, Any]] = []

    with DuckDBTickStore(settings) as store:
        symbols = store.list_symbols(date_str)
        if not symbols:
            raise FileNotFoundError(
                f"No compacted ticks for {date_str}. Run scripts/03_run_compaction.py first."
            )

        spot_cache = {
            spot: _spot_series_by_bucket(store, date_str, spot, bucket_minutes)
            for spot in UNDERLYING_SPOT.values()
        }

        def apply_quality(
            ticks: pd.DataFrame,
            symbol: str,
            mode: str,
            require_depth: bool,
            max_tick_return_pct: float,
        ) -> pd.DataFrame:
            result = filter_bad_ticks(
                ticks,
                symbol=symbol,
                subscribe_mode=mode,
                max_tick_return_pct=max_tick_return_pct,
                require_depth=require_depth,
            )
            for rej in result.rejects:
                quality_buffer.append(
                    {
                        "event_type": "quality_reject",
                        "trade_date": date_str,
                        "symbol": symbol,
                        "reason": rej.reason,
                        "timestamp": rej.timestamp,
                        "details": rej.details,
                    }
                )
            for freeze in result.halt_flags:
                quality_buffer.append(
                    {
                        "event_type": "stock_quote_freeze",
                        "trade_date": date_str,
                        "symbol": symbol,
                        "reason": freeze.reason,
                        "timestamp": freeze.timestamp,
                        "details": freeze.details,
                    }
                )
                ingest_halt_buffer.append(
                    {
                        "event_type": "stock_quote_freeze",
                        "message": freeze.reason,
                        "symbol": symbol,
                        "details": freeze.details,
                    }
                )
            return result.clean

        for symbol in symbols:
            info = meta.get(symbol)
            if not info or info.get("bucket_kind") != "equity":
                continue
            mode = str(info.get("subscribe_mode", "quote"))
            clean = apply_quality(
                store.read_ticks(date_str, symbol),
                symbol,
                mode,
                mode == "full",
                max_jump,
            )
            feature_rows.extend(
                compute_equity_features(
                    clean,
                    symbol=symbol,
                    subscribe_mode=mode,
                    bucket_minutes=bucket_minutes,
                    lot_size=info.get("lot_size"),
                )
            )

        option_symbols = [
            s for s in symbols if meta.get(s, {}).get("bucket_kind") == "options"
        ]
        option_buckets: dict[str, pd.DataFrame] = {}
        option_meta: dict[str, dict[str, Any]] = {}
        for symbol in option_symbols:
            info = meta[symbol]
            clean = apply_quality(
                store.read_ticks(date_str, symbol),
                symbol,
                "full",
                False,
                options_max_jump,
            )
            option_buckets[symbol] = bucket_option_series(
                clean, bucket_minutes=bucket_minutes
            )
            option_meta[symbol] = info

        pcr_by_underlying = compute_pcr_by_bucket(option_buckets, option_meta)
        for symbol, buckets in option_buckets.items():
            info = option_meta[symbol]
            underlying = str(info.get("underlying") or info.get("name"))
            spot_sym = UNDERLYING_SPOT.get(underlying, "NIFTY 50")
            feature_rows.extend(
                compute_option_features_for_symbol(
                    buckets,
                    symbol=symbol,
                    underlying=underlying,
                    option_type=str(
                        info.get("option_type") or info.get("instrument_type")
                    ),
                    strike=info.get("strike"),
                    expiry=info.get("expiry"),
                    lot_size=info.get("lot_size"),
                    spot_by_bucket=spot_cache.get(spot_sym, {}),
                    pcr_by_bucket=pcr_by_underlying.get(underlying, {}),
                    bucket_minutes=bucket_minutes,
                )
            )

        fut_symbols = [
            s for s in symbols if meta.get(s, {}).get("bucket_kind") == "futures"
        ]
        by_underlying: dict[str, list[str]] = defaultdict(list)
        for symbol in fut_symbols:
            by_underlying[
                str(meta[symbol].get("underlying") or meta[symbol].get("name"))
            ].append(symbol)

        fut_buckets: dict[str, pd.DataFrame] = {}
        for symbol in fut_symbols:
            clean = apply_quality(
                store.read_ticks(date_str, symbol),
                symbol,
                "full",
                False,
                max_jump,
            )
            fut_buckets[symbol] = bucket_futures_series(
                clean, bucket_minutes=bucket_minutes
            )

        for underlying, syms in by_underlying.items():
            ordered = sorted(syms, key=lambda s: str(meta[s].get("expiry") or ""))
            near_sym = ordered[0] if ordered else None
            next_sym = ordered[1] if len(ordered) > 1 else None
            near_map = (
                {
                    pd.Timestamp(r["bucket"]): float(r["ltp"])
                    for _, r in fut_buckets[near_sym].iterrows()
                    if pd.notna(r["ltp"])
                }
                if near_sym
                else None
            )
            next_map = (
                {
                    pd.Timestamp(r["bucket"]): float(r["ltp"])
                    for _, r in fut_buckets[next_sym].iterrows()
                    if pd.notna(r["ltp"])
                }
                if next_sym
                else None
            )
            spot_sym = UNDERLYING_SPOT.get(underlying, "NIFTY 50")
            for symbol in ordered:
                feature_rows.extend(
                    compute_futures_features(
                        fut_buckets[symbol],
                        symbol=symbol,
                        underlying=underlying,
                        expiry=meta[symbol].get("expiry"),
                        lot_size=meta[symbol].get("lot_size"),
                        spot_by_bucket=spot_cache.get(spot_sym, {}),
                        near_ltp_by_bucket=near_map,
                        next_ltp_by_bucket=next_map,
                        is_near=(symbol == near_sym),
                        bucket_minutes=bucket_minutes,
                        basis_days_per_year=settings.features.basis_days_per_year,
                    )
                )

    store_sql.delete_feature_logs_for_trade_date(date_str)
    store_sql.delete_quality_logs_for_trade_date(date_str)

    for event in quality_buffer:
        store_sql.log_quality_event(**event)
    for event in ingest_halt_buffer:
        store_sql.log_ingestion_event(**event)

    inserted = store_sql.insert_feature_logs(feature_rows)
    by_track: dict[str, int] = {}
    for row in feature_rows:
        by_track[row["track"]] = by_track.get(row["track"], 0) + 1

    return {
        "trade_date": date_str,
        "feature_rows": inserted,
        "by_track": by_track,
        "quality_rejects": sum(
            1 for e in quality_buffer if e["event_type"] == "quality_reject"
        ),
        "stock_quote_freeze_flags": sum(
            1 for e in quality_buffer if e["event_type"] == "stock_quote_freeze"
        ),
        "symbols_seen": len(symbols),
    }
