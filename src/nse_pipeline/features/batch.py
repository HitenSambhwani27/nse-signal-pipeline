"""
Stage 2E — batch feature job over compacted Parquet via DuckDB.

Idempotent per trade_date: deletes existing feature_log / quality_log rows for
that date before inserting fresh ones.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
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
from nse_pipeline.features.quality import (
    assess_overnight_gap,
    filter_bad_ticks,
    session_open_price,
)
from nse_pipeline.session_coverage import (
    assess_session_coverage,
    format_coverage_banner,
    log_session_coverage,
)
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)

def symbols_for_tracks(cache: dict[str, Any], tracks: tuple[str, ...] | None) -> set[str] | None:
    """None means the full confirmed universe; otherwise names for the given tracks."""
    if not tracks:
        return None
    wanted = set(tracks)
    out: set[str] = set()
    if "equity_depth" in wanted:
        out.update(cache.get("equity_depth", {}))
    if "equity_quote" in wanted:
        out.update(cache.get("equity_quote", {}))
    if "options" in wanted:
        out.update(str(i["tradingsymbol"]) for i in cache.get("options", []))
    if "futures" in wanted:
        out.update(str(i["tradingsymbol"]) for i in cache.get("futures", []))
    return out

UNDERLYING_SPOT = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
}

DEPTH_FEATURES_SKIPPED = (
    "depth_ratio_bid_ask",
    "spread_bps",
    "spread_abs",
    "ofi_bucket_sum",
)


def _infer_source(ticks: pd.DataFrame) -> str:
    if ticks is None or ticks.empty or "source" not in ticks.columns:
        return "live"
    vals = {str(s) for s in ticks["source"].dropna().unique()}
    if vals == {"historical"}:
        return "historical"
    return "live"


def _stamp_source(rows: list[dict[str, Any]], source: str) -> None:
    completeness = "historical_partial" if source == "historical" else "live_full"
    for row in rows:
        row["source"] = source
        row["feature_completeness"] = completeness
        feats = row.get("features") or {}
        feats["source"] = source
        feats["feature_completeness"] = completeness
        if source == "historical":
            feats["depth_features_skipped"] = list(DEPTH_FEATURES_SKIPPED)
            for key in DEPTH_FEATURES_SKIPPED:
                feats.pop(key, None)
        row["features"] = feats


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


def _split_chunks(items: list[str], n_chunks: int) -> list[list[str]]:
    if not items:
        return []
    n_chunks = max(1, min(n_chunks, len(items)))
    size = (len(items) + n_chunks - 1) // n_chunks
    return [items[i : i + size] for i in range(0, len(items), size)]


def _apply_quality(
    ticks: pd.DataFrame,
    *,
    symbol: str,
    date_str: str,
    mode: str,
    require_depth: bool,
    max_tick_return_pct: float,
    source: str = "live",
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    result = filter_bad_ticks(
        ticks,
        symbol=symbol,
        subscribe_mode=mode,
        max_tick_return_pct=max_tick_return_pct,
        require_depth=require_depth and source != "historical",
        source=source,
    )
    quality_buffer: list[dict[str, Any]] = []
    ingest_halt_buffer: list[dict[str, Any]] = []
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
    return result.clean, quality_buffer, ingest_halt_buffer


def _compute_equity_symbols_on_store(
    store: DuckDBTickStore,
    settings: Settings,
    date_str: str,
    symbols: list[str],
    meta: dict[str, dict[str, Any]],
    index_prev_close: float | None,
    index_today_open: float | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Read parquet + compute equity features for one symbol list. No sqlite writes."""
    bucket_minutes = settings.features.oi_bucket_minutes
    max_jump = settings.features.max_tick_return_pct
    feature_rows: list[dict[str, Any]] = []
    quality_buffer: list[dict[str, Any]] = []
    ingest_halt_buffer: list[dict[str, Any]] = []
    for symbol in symbols:
        info = meta.get(symbol)
        if not info or info.get("bucket_kind") != "equity":
            continue
        ticks = store.read_ticks(date_str, symbol)
        source = _infer_source(ticks)
        mode = str(info.get("subscribe_mode", "quote"))
        clean, q_events, h_events = _apply_quality(
            ticks,
            symbol=symbol,
            date_str=date_str,
            mode=mode,
            require_depth=mode == "full",
            max_tick_return_pct=max_jump,
            source=source,
        )
        quality_buffer.extend(q_events)
        ingest_halt_buffer.extend(h_events)
        ca_flag = None
        if source == "historical" and not clean.empty:
            prev_close = store.last_close_before(date_str, symbol)
            today_open = session_open_price(clean)
            if prev_close is not None and today_open is not None:
                ca_flag = assess_overnight_gap(
                    prev_close=prev_close,
                    today_open=today_open,
                    index_prev_close=index_prev_close,
                    index_today_open=index_today_open,
                    gap_pct=settings.features.corporate_action_gap_pct,
                    index_wide_pct=settings.features.corporate_action_index_wide_pct,
                )
            if ca_flag:
                first_ts = pd.Timestamp(clean["timestamp"].iloc[0]).isoformat()
                quality_buffer.append(
                    {
                        "event_type": "corporate_action_suspect",
                        "trade_date": date_str,
                        "symbol": symbol,
                        "reason": "overnight_gap_vs_index",
                        "timestamp": first_ts,
                        "details": ca_flag,
                    }
                )
        eq_rows = compute_equity_features(
            clean,
            symbol=symbol,
            subscribe_mode=mode,
            bucket_minutes=bucket_minutes,
            lot_size=info.get("lot_size"),
            source=source,
        )
        if ca_flag:
            for row in eq_rows:
                row["features"]["corporate_action_suspect"] = True
                row["features"]["corporate_action_gap_pct"] = ca_flag["stock_gap_pct"]
        feature_rows.extend(eq_rows)
    return feature_rows, quality_buffer, ingest_halt_buffer


def _compute_equity_chunk(
    job: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Process-pool entry: own DuckDB connection, parquet reads only."""
    settings: Settings = job["settings"]
    with DuckDBTickStore(settings) as store:
        return _compute_equity_symbols_on_store(
            store,
            settings,
            job["date_str"],
            job["symbols"],
            job["meta"],
            job["index_prev_close"],
            job["index_today_open"],
        )


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


def run_feature_batch(
    settings: Settings,
    date_str: str,
    *,
    tracks: tuple[str, ...] | None = None,
    workers: int = 1,
    executor: ProcessPoolExecutor | None = None,
) -> dict[str, Any]:
    """Compute equity / options / futures features for one compacted trade date.

    ``workers`` > 1 splits the equity symbol list across processes for parquet
    reads and CPU work only. SQLite delete/insert still happens once per date
    in this process. Do not run multiple date-range feature jobs against the
    same sqlite file while other writers are active.
    """
    cache = load_instrument_cache(settings.paths.instruments_cache)
    store_sql = SQLiteStore(settings.paths.sqlite_db)
    bucket_minutes = settings.features.oi_bucket_minutes
    max_jump = settings.features.max_tick_return_pct
    options_max_jump = settings.features.options_max_tick_return_pct
    meta = _build_meta(cache)
    allow = symbols_for_tracks(cache, tracks)
    want_options = tracks is None or "options" in tracks
    want_futures = tracks is None or "futures" in tracks

    feature_rows: list[dict[str, Any]] = []
    quality_buffer: list[dict[str, Any]] = []
    ingest_halt_buffer: list[dict[str, Any]] = []

    with DuckDBTickStore(settings) as store:
        symbols = store.list_tick_symbols(date_str)
        if allow is not None:
            symbols = [s for s in symbols if s in allow]
        coverage = assess_session_coverage(
            settings, date_str, store=store, symbols=symbols
        )
        log_session_coverage(settings, coverage)
        if coverage.status != "full":
            logger.warning("PARTIAL SESSION: %s", format_coverage_banner(coverage))

        if not symbols:
            return {
                "trade_date": date_str,
                "skipped": True,
                "reason": "no_ticks",
                "coverage": coverage.to_dict(),
                "coverage_banner": format_coverage_banner(coverage),
                "feature_rows": 0,
                "by_track": {},
                "by_source": {},
                "by_completeness": {},
                "quality_rejects": 0,
                "stock_quote_freeze_flags": 0,
                "corporate_action_flags": 0,
                "symbols_seen": 0,
            }

        index_symbol = (
            settings.index_symbols[0] if settings.index_symbols else "NIFTY 50"
        )
        index_prev_close = store.last_close_before(date_str, index_symbol)
        index_today_open = store.open_on(date_str, index_symbol)

        spot_cache = (
            {
                spot: _spot_series_by_bucket(store, date_str, spot, bucket_minutes)
                for spot in UNDERLYING_SPOT.values()
            }
            if want_options or want_futures
            else {}
        )

        equity_symbols = [
            s for s in symbols if meta.get(s, {}).get("bucket_kind") == "equity"
        ]
        use_pool = workers > 1 and len(equity_symbols) > 1
        if use_pool:
            chunk_n = min(workers, len(equity_symbols))
            chunks = _split_chunks(equity_symbols, chunk_n)
            jobs = [
                {
                    "settings": settings,
                    "date_str": date_str,
                    "symbols": chunk,
                    "meta": {s: meta[s] for s in chunk if s in meta},
                    "index_prev_close": index_prev_close,
                    "index_today_open": index_today_open,
                }
                for chunk in chunks
            ]
            own_pool = executor is None
            pool = executor or ProcessPoolExecutor(max_workers=chunk_n)
            logger.info(
                "equity features: %s symbols across %s processes on %s",
                len(equity_symbols),
                chunk_n,
                date_str,
            )
            try:
                chunk_results = list(pool.map(_compute_equity_chunk, jobs))
            finally:
                if own_pool:
                    pool.shutdown(wait=True)
            for rows, q_events, h_events in chunk_results:
                feature_rows.extend(rows)
                quality_buffer.extend(q_events)
                ingest_halt_buffer.extend(h_events)
        else:
            rows, q_events, h_events = _compute_equity_symbols_on_store(
                store,
                settings,
                date_str,
                equity_symbols,
                meta,
                index_prev_close,
                index_today_open,
            )
            feature_rows.extend(rows)
            quality_buffer.extend(q_events)
            ingest_halt_buffer.extend(h_events)

        option_symbols = [
            s
            for s in symbols
            if want_options and meta.get(s, {}).get("bucket_kind") == "options"
        ]
        option_buckets: dict[str, pd.DataFrame] = {}
        option_meta: dict[str, dict[str, Any]] = {}
        for symbol in option_symbols:
            info = meta[symbol]
            ticks = store.read_ticks(date_str, symbol)
            source = _infer_source(ticks)
            clean, q_events, h_events = _apply_quality(
                ticks,
                symbol=symbol,
                date_str=date_str,
                mode="full",
                require_depth=False,
                max_tick_return_pct=options_max_jump,
                source=source,
            )
            quality_buffer.extend(q_events)
            ingest_halt_buffer.extend(h_events)
            option_buckets[symbol] = bucket_option_series(
                clean, bucket_minutes=bucket_minutes
            )
            option_meta[symbol] = {**info, "_source": source}

        pcr_by_underlying = compute_pcr_by_bucket(option_buckets, option_meta)
        for symbol, buckets in option_buckets.items():
            info = option_meta[symbol]
            underlying = str(info.get("underlying") or info.get("name"))
            spot_sym = UNDERLYING_SPOT.get(underlying, "NIFTY 50")
            opt_rows = compute_option_features_for_symbol(
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
            _stamp_source(opt_rows, str(info.get("_source") or "live"))
            feature_rows.extend(opt_rows)

        fut_symbols = [
            s
            for s in symbols
            if want_futures and meta.get(s, {}).get("bucket_kind") == "futures"
        ]
        by_underlying: dict[str, list[str]] = defaultdict(list)
        for symbol in fut_symbols:
            by_underlying[
                str(meta[symbol].get("underlying") or meta[symbol].get("name"))
            ].append(symbol)

        fut_buckets: dict[str, pd.DataFrame] = {}
        fut_source: dict[str, str] = {}
        for symbol in fut_symbols:
            ticks = store.read_ticks(date_str, symbol)
            source = _infer_source(ticks)
            fut_source[symbol] = source
            clean, q_events, h_events = _apply_quality(
                ticks,
                symbol=symbol,
                date_str=date_str,
                mode="full",
                require_depth=False,
                max_tick_return_pct=max_jump,
                source=source,
            )
            quality_buffer.extend(q_events)
            ingest_halt_buffer.extend(h_events)
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
                fut_rows = compute_futures_features(
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
                _stamp_source(fut_rows, fut_source.get(symbol, "live"))
                feature_rows.extend(fut_rows)

    store_sql.delete_feature_logs_for_trade_date(
        date_str, symbols=symbols if tracks else None
    )
    store_sql.delete_quality_logs_for_trade_date(
        date_str, symbols=symbols if tracks else None
    )

    for event in quality_buffer:
        store_sql.log_quality_event(**event)
    for event in ingest_halt_buffer:
        store_sql.log_ingestion_event(**event)

    inserted = store_sql.insert_feature_logs(feature_rows)
    by_track: dict[str, int] = {}
    by_completeness: dict[str, int] = {}
    by_source: dict[str, int] = {}
    for row in feature_rows:
        by_track[row["track"]] = by_track.get(row["track"], 0) + 1
        comp = str(row.get("feature_completeness") or "unknown")
        by_completeness[comp] = by_completeness.get(comp, 0) + 1
        src = str(row.get("source") or "unknown")
        by_source[src] = by_source.get(src, 0) + 1

    ca_flags = sum(
        1 for e in quality_buffer if e["event_type"] == "corporate_action_suspect"
    )
    return {
        "trade_date": date_str,
        "coverage": coverage.to_dict(),
        "coverage_banner": format_coverage_banner(coverage),
        "feature_rows": inserted,
        "by_track": by_track,
        "by_source": by_source,
        "by_completeness": by_completeness,
        "quality_rejects": sum(
            1 for e in quality_buffer if e["event_type"] == "quality_reject"
        ),
        "stock_quote_freeze_flags": sum(
            1 for e in quality_buffer if e["event_type"] == "stock_quote_freeze"
        ),
        "corporate_action_flags": ca_flags,
        "symbols_seen": len(symbols),
    }
