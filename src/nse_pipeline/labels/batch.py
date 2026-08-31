"""Stage 3A — apply labels to feature_log and produce comparison audits."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

from nse_pipeline.broker.instruments import load_instrument_cache
from nse_pipeline.config import Settings
from nse_pipeline.labels.triple_barrier import (
    LabelResult,
    build_five_min_closes,
    label_series_legacy,
    label_series_triple_barrier,
    summarize_labels,
)
from nse_pipeline.session_coverage import (
    assess_session_coverage,
    format_coverage_banner,
    log_session_coverage,
)
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore, _ts_iso

import pandas as pd


logger = logging.getLogger(__name__)


class CloseSeriesCache:
    """Reuse 5-minute close series across consecutive label dates.

    Each date warms vol from the prior 10 sessions. Re-reading those parquet
    files every day is fine while they stay in the OS cache, but a large
    sqlite working set evicts them and wall time jumps by an order of magnitude.
    """

    def __init__(self) -> None:
        self._closes: dict[tuple[str, str], pd.Series] = {}
        self._absent: set[tuple[str, str]] = set()

    def try_get(self, date_str: str, symbol: str) -> tuple[bool, pd.Series | None]:
        key = (date_str, symbol)
        if key in self._absent:
            return True, None
        series = self._closes.get(key)
        if series is not None:
            return True, series
        return False, None

    def put(self, date_str: str, symbol: str, closes: pd.Series | None) -> None:
        key = (date_str, symbol)
        if closes is None or closes.empty:
            self._absent.add(key)
            self._closes.pop(key, None)
            return
        self._closes[key] = closes
        self._absent.discard(key)

    def retain_dates(self, keep: set[str]) -> None:
        self._closes = {k: v for k, v in self._closes.items() if k[0] in keep}
        self._absent = {k for k in self._absent if k[0] in keep}


def _prior_trade_dates(
    store: DuckDBTickStore, trade_date: str, *, n_days: int
) -> list[str]:
    """Compacted dates strictly before trade_date, oldest first, last n_days."""
    prior = [d for d in store.list_compacted_dates() if d < trade_date]
    return prior[-n_days:]


def _load_closes(
    store: DuckDBTickStore,
    date_str: str,
    symbol: str,
    bucket_minutes: int,
    cache: CloseSeriesCache | None,
) -> pd.Series | None:
    if cache is not None:
        hit, series = cache.try_get(date_str, symbol)
        if hit:
            return series
    if not store.has_ticks(date_str, symbol):
        if cache is not None:
            cache.put(date_str, symbol, None)
        return None
    try:
        ticks = store.read_ticks(date_str, symbol)
    except FileNotFoundError:
        if cache is not None:
            cache.put(date_str, symbol, None)
        return None
    closes = build_five_min_closes(ticks, bucket_minutes)
    if closes.empty:
        if cache is not None:
            cache.put(date_str, symbol, None)
        return None
    if cache is not None:
        cache.put(date_str, symbol, closes)
    return closes


def _track_for_symbol(cache: dict[str, Any], symbol: str) -> str:
    if symbol in cache.get("equity_depth", {}):
        return "equity_depth"
    if symbol in cache.get("equity_quote", {}):
        return "equity_quote"
    if symbol in cache.get("index", {}):
        return "index"
    for row in cache.get("options", []):
        if row.get("tradingsymbol") == symbol:
            return "options"
    for row in cache.get("futures", []):
        if row.get("tradingsymbol") == symbol:
            return "futures"
    return "other"


def run_labeling(
    settings: Settings,
    trade_date: str,
    *,
    write_outcomes: bool = True,
    compare_legacy: bool = True,
    tracks: tuple[str, ...] | None = None,
    closes_cache: CloseSeriesCache | None = None,
) -> dict[str, Any]:
    """
    Label feature_log rows for trade_date using compacted tick closes.

    Primary mode from settings.signal.label_mode (triple_barrier).
    If compare_legacy, also compute flat threshold_pct labels for the report.
    Hourly legacy vs TBM comparison is only emitted for full-coverage sessions.
    """
    store_sql = SQLiteStore(settings.paths.sqlite_db)
    cache = load_instrument_cache(settings.paths.instruments_cache)
    horizon_minutes = settings.signal.candle_interval_minutes
    horizon_bars = max(1, horizon_minutes // settings.features.oi_bucket_minutes)

    feature_rows = store_sql.fetch_feature_logs(
        trade_date, tracks=tracks, include_features=False
    )
    if not feature_rows:
        return {
            "trade_date": trade_date,
            "skipped": True,
            "reason": "no_feature_rows_for_tracks",
            "feature_rows": 0,
            "feature_rows_matched": 0,
            "coverage_banner": "coverage=skipped",
            "hourly_compare_status": "skipped",
            "hourly_compare_eligible": False,
            "tbm_labels": {"n": 0},
            "tbm_by_track": {},
        }

    symbols = sorted({r["symbol"] for r in feature_rows})
    tbm_results: list[LabelResult] = []
    legacy_results: list[LabelResult] = []

    with DuckDBTickStore(settings) as store:
        coverage = assess_session_coverage(
            settings,
            trade_date,
            store=store,
            symbols=sorted({r["symbol"] for r in feature_rows}),
        )
        warmup_dates = _prior_trade_dates(store, trade_date, n_days=10)
        if closes_cache is not None:
            closes_cache.retain_dates(set(warmup_dates) | {trade_date})
        bucket_minutes = settings.features.oi_bucket_minutes
        for symbol in symbols:
            closes = _load_closes(
                store, trade_date, symbol, bucket_minutes, closes_cache
            )
            if closes is None or len(closes) < 2:
                if closes is None:
                    logger.warning(
                        "No compacted ticks for %s on %s — skip labels",
                        symbol,
                        trade_date,
                    )
                continue
            prior_series: list[pd.Series] = []
            for prior in warmup_dates:
                prior_closes = _load_closes(
                    store, prior, symbol, bucket_minutes, closes_cache
                )
                if prior_closes is not None and not prior_closes.empty:
                    prior_series.append(prior_closes)
            if prior_series:
                warmed = pd.concat(prior_series + [closes])
                warmed = warmed[~warmed.index.duplicated(keep="last")].sort_index()
            else:
                warmed = closes
            track = _track_for_symbol(cache, symbol)
            emit_from = pd.Timestamp(closes.index.min())

            tbm_results.extend(
                label_series_triple_barrier(
                    warmed,
                    symbol=symbol,
                    track=track,
                    horizon_bars=horizon_bars,
                    settings=settings,
                    emit_from=emit_from,
                )
            )
            if compare_legacy:
                legacy_results.extend(
                    label_series_legacy(
                        closes,
                        symbol=symbol,
                        track=track,
                        horizon_bars=horizon_bars,
                        threshold_pct=settings.signal.threshold_pct,
                    )
                )

    # Hourly up/down/flat compare is the midday-flat test — only valid on full sessions.
    hourly_compare_eligible = coverage.status == "full"
    hourly_compare_status = (
        "ready" if hourly_compare_eligible else "pending_full_session"
    )

    tbm_by_key = {(r.symbol, _ts_iso(r.timestamp)): r for r in tbm_results}
    updates: list[tuple[float | None, int]] = []
    matched = 0
    for row in feature_rows:
        key = (row["symbol"], _ts_iso(row["timestamp"]))
        hit = tbm_by_key.get(key)
        if hit is None:
            continue
        matched += 1
        updates.append((hit.label_code, row["id"]))

    if write_outcomes:
        store_sql.clear_label_audit(trade_date, tracks=tracks)
        store_sql.insert_label_audit(tbm_results, trade_date=trade_date)
        if compare_legacy:
            store_sql.insert_label_audit(legacy_results, trade_date=trade_date)
        store_sql.update_feature_outcomes(updates)
        log_session_coverage(settings, coverage)

    report: dict[str, Any] = {
        "trade_date": trade_date,
        "coverage": coverage.to_dict(),
        "coverage_banner": format_coverage_banner(coverage),
        "hourly_compare_status": hourly_compare_status,
        "hourly_compare_eligible": hourly_compare_eligible,
        "feature_rows": len(feature_rows),
        "symbols": len(symbols),
        "tbm_labels": summarize_labels(tbm_results),
        "legacy_labels": summarize_labels(legacy_results) if compare_legacy else None,
        "feature_rows_matched": matched,
        "horizon_bars": horizon_bars,
        "settings": {
            "label_mode": settings.signal.label_mode,
            "threshold_pct": settings.signal.threshold_pct,
            "min_threshold_pct": settings.signal.triple_barrier.min_threshold_pct,
            "max_threshold_pct": settings.signal.triple_barrier.max_threshold_pct,
            "vol_method": settings.signal.triple_barrier.vol_method,
            "vol_window": settings.signal.triple_barrier.vol_window,
            "barrier_multiplier": settings.signal.triple_barrier.barrier_multiplier,
            "vol_min_periods": settings.signal.vol_min_periods,
        },
    }

    by_track: dict[str, list[LabelResult]] = defaultdict(list)
    for r in tbm_results:
        by_track[r.track].append(r)
    report["tbm_by_track"] = {t: summarize_labels(rs) for t, rs in by_track.items()}

    out_path = settings.paths.features_dir / f"label_comparison_{trade_date}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    report["report_path"] = str(out_path)
    return report


def format_hourly_table(summary: dict[str, Any], title: str) -> str:
    lines = [
        title,
        f"{'hour_ist':>8} {'n':>6} {'up%':>7} {'down%':>7} {'flat%':>7}",
    ]
    by_hour = summary.get("by_hour") or {}
    for hour in sorted(by_hour):
        b = by_hour[hour]
        n = max(b["n"], 1)
        lines.append(
            f"{hour:>8} {b['n']:>6} {100.0 * b['up'] / n:>6.1f}% "
            f"{100.0 * b['down'] / n:>6.1f}% {100.0 * b['flat'] / n:>6.1f}%"
        )
    lines.append(
        f"{'TOTAL':>8} {summary.get('n', 0):>6} "
        f"{summary.get('up_pct', 0) or 0:>6.1f}% "
        f"{summary.get('down_pct', 0) or 0:>6.1f}% "
        f"{summary.get('flat_pct', 0) or 0:>6.1f}%"
    )
    return "\n".join(lines)


def format_binding_table(summary: dict[str, Any], title: str) -> str:
    lines = [
        title,
        f"{'hour_ist':>8} {'n':>6} {'floor%':>8} {'cap%':>7} {'dynamic%':>9}",
    ]
    by_hour = summary.get("by_hour") or {}
    for hour in sorted(by_hour):
        b = by_hour[hour]
        n = max(b["n"], 1)
        lines.append(
            f"{hour:>8} {b['n']:>6} "
            f"{100.0 * b.get('floor_bound', 0) / n:>7.1f}% "
            f"{100.0 * b.get('cap_bound', 0) / n:>6.1f}% "
            f"{100.0 * b.get('dynamic', 0) / n:>8.1f}%"
        )
    lines.append(
        f"{'TOTAL':>8} {summary.get('n', 0):>6} "
        f"{summary.get('floor_bound_pct', 0) or 0:>7.1f}% "
        f"{summary.get('cap_bound_pct', 0) or 0:>6.1f}% "
        f"{summary.get('dynamic_used_pct', 0) or 0:>8.1f}%"
    )
    return "\n".join(lines)
