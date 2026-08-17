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


def _prior_trade_dates(
    store: DuckDBTickStore, trade_date: str, *, n_days: int
) -> list[str]:
    """Compacted dates strictly before trade_date, oldest first, last n_days."""
    prior = [d for d in store.list_compacted_dates() if d < trade_date]
    return prior[-n_days:]


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

    feature_rows = store_sql.fetch_feature_logs(trade_date)
    if tracks:
        wanted = set(tracks)
        feature_rows = [r for r in feature_rows if str(r.get("track")) in wanted]
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
        available = set(store.list_tick_symbols(trade_date))
        warmup_dates = _prior_trade_dates(store, trade_date, n_days=10)
        for symbol in symbols:
            if symbol not in available:
                logger.warning(
                    "No compacted ticks for %s on %s — skip labels", symbol, trade_date
                )
                continue
            ticks = store.read_ticks(trade_date, symbol)
            closes = build_five_min_closes(ticks, settings.features.oi_bucket_minutes)
            if len(closes) < 2:
                continue
            prior_series: list[pd.Series] = []
            for prior in warmup_dates:
                if symbol not in set(store.list_tick_symbols(prior)):
                    continue
                try:
                    prior_closes = build_five_min_closes(
                        store.read_ticks(prior, symbol),
                        settings.features.oi_bucket_minutes,
                    )
                except FileNotFoundError:
                    continue
                if not prior_closes.empty:
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
