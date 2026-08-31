"""
Reusable, model-agnostic walk-forward harness.

Any scorer that maps a feature row → signed score (positive = long) can be
validated here. Stage 3B baseline and Stage 5 coarse/fine models share this
path. A model's own train/test JSON is not sufficient to ship — this harness
must pass first.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from nse_pipeline.backtest.costs import cost_breakdown, round_trip_cost_pct
from nse_pipeline.backtest.metrics import summarize_trades
from nse_pipeline.config import Settings
from nse_pipeline.scoring.baseline import thin_features_for_scoring
from nse_pipeline.session_coverage import scoring_skip_trade_dates
from nse_pipeline.storage.sqlite_store import SQLiteStore

Scorer = Callable[[dict[str, Any]], float]


def _parse(d: str | date) -> date:
    if isinstance(d, date):
        return d
    return date.fromisoformat(str(d))


def iter_walk_forward_windows(
    dates: list[str],
    *,
    train_days: int,
    test_days: int,
    step_days: int,
    min_train_days: int,
) -> list[tuple[list[str], list[str]]]:
    """Strict temporal split: train is always strictly before test. No shuffle."""
    if not dates:
        return []
    ordered = sorted(dates)
    windows: list[tuple[list[str], list[str]]] = []
    i = 0
    while i < len(ordered):
        train = ordered[i : i + train_days]
        test = ordered[i + train_days : i + train_days + test_days]
        if len(train) < min_train_days or not test:
            break
        if train[-1] >= test[0]:
            raise RuntimeError("Walk-forward leakage: train end overlaps test start")
        windows.append((train, test))
        i += step_days
    return windows


def _side_from_score(score: float, threshold: float) -> int:
    if score > threshold:
        return 1
    if score < -threshold:
        return -1
    return 0


def _trades_for_rows(
    rows: list[dict[str, Any]],
    scorer: Scorer,
    settings: Settings,
) -> pd.DataFrame:
    recs: list[dict[str, Any]] = []
    threshold = settings.backtest.signal_threshold
    for row in rows:
        outcome = row.get("actual_outcome")
        if outcome is None:
            continue
        score = float(scorer(row))
        side = _side_from_score(score, threshold)
        if side == 0:
            continue
        price = float((row.get("features") or {}).get("ltp") or 0.0) or 1.0
        costs = round_trip_cost_pct(settings, track=str(row["track"]), price=price)
        breakdown = cost_breakdown(settings, track=str(row["track"]), price=price)
        # Label is +1/0/-1 on the instrument's own return. Side * label * |fwd|
        # approximated by side * outcome (unit payoff) minus costs.
        pnl = side * float(outcome) - costs
        recs.append(
            {
                "trade_date": row.get("trade_date"),
                "timestamp": row.get("timestamp"),
                "symbol": row.get("symbol"),
                "track": row.get("track"),
                "score": score,
                "side": side,
                "outcome": float(outcome),
                "pnl": pnl,
                "cost_total_pct": costs,
                "cost_spread_crossing_pct": breakdown["spread_crossing_pct"],
                "cost_slippage_pct": breakdown["slippage_pct"],
                "cost_brokerage_pct": breakdown["brokerage_pct"],
                "cost_stt_pct": breakdown["stt_pct"],
            }
        )
    return pd.DataFrame(recs)


def run_walk_forward(
    settings: Settings,
    scorer: Scorer,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    rows: list[dict[str, Any]] | None = None,
    model_id: str = "anonymous",
    output_dir: Path | None = None,
    tracks: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """
    Rolling train/test evaluation. The scorer is applied out-of-sample on each
    test window; in-sample scores on the matching train window are compared
    to flag IS/OOS hit-rate divergence.
    """
    store = SQLiteStore(settings.paths.sqlite_db)
    skipped_dates: set[str] = set()
    trades_by_date: dict[str, pd.DataFrame] = {}
    if rows is None:
        dates = store.list_feature_trade_dates()
        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]
        if not dates:
            raise FileNotFoundError("No feature_log dates in the requested window.")
        for d in dates:
            day_rows = store.fetch_feature_logs(d, tracks=tracks)
            if not day_rows:
                continue
            if d in scoring_skip_trade_dates(day_rows):
                skipped_dates.add(d)
                continue
            for row in day_rows:
                row["features"] = thin_features_for_scoring(row.get("features") or {})
            trades_by_date[d] = _trades_for_rows(day_rows, scorer, settings)
    else:
        if tracks:
            wanted = set(tracks)
            rows = [r for r in rows if str(r.get("track")) in wanted]
        skipped_dates = scoring_skip_trade_dates(rows)
        if skipped_dates:
            rows = [r for r in rows if str(r.get("trade_date")) not in skipped_dates]
        by_date: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            d = str(row.get("trade_date") or "")
            if d:
                by_date.setdefault(d, []).append(row)
        for d, day_rows in by_date.items():
            trades_by_date[d] = _trades_for_rows(day_rows, scorer, settings)
    dates = sorted(trades_by_date)

    windows = iter_walk_forward_windows(
        dates,
        train_days=settings.backtest.train_days,
        test_days=settings.backtest.test_days,
        step_days=settings.backtest.step_days,
        min_train_days=settings.backtest.min_train_days,
    )
    annual = settings.backtest.annualization_days
    folds: list[dict[str, Any]] = []
    all_is: list[pd.DataFrame] = []
    all_oos: list[pd.DataFrame] = []

    def _concat_dates(day_list: list[str]) -> pd.DataFrame:
        parts = [
            trades_by_date[d]
            for d in day_list
            if d in trades_by_date and not trades_by_date[d].empty
        ]
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    for train_dates, test_dates in windows:
        is_trades = _concat_dates(train_dates)
        oos_trades = _concat_dates(test_dates)
        is_m = summarize_trades(is_trades, annualization_days=annual)
        oos_m = summarize_trades(oos_trades, annualization_days=annual)
        gap = None
        if is_m["hit_rate"] is not None and oos_m["hit_rate"] is not None:
            gap = float(is_m["hit_rate"] - oos_m["hit_rate"])
        overfit = bool(
            gap is not None and gap > settings.backtest.overfit_hit_rate_gap
        )
        folds.append(
            {
                "train": [train_dates[0], train_dates[-1]],
                "test": [test_dates[0], test_dates[-1]],
                "is": is_m,
                "oos": oos_m,
                "hit_rate_gap": gap,
                "overfit_flag": overfit,
            }
        )
        if not is_trades.empty:
            all_is.append(is_trades)
        if not oos_trades.empty:
            all_oos.append(oos_trades)

    is_all = pd.concat(all_is, ignore_index=True) if all_is else pd.DataFrame()
    oos_all = pd.concat(all_oos, ignore_index=True) if all_oos else pd.DataFrame()
    is_summary = summarize_trades(is_all, annualization_days=annual)
    oos_summary = summarize_trades(oos_all, annualization_days=annual)
    overall_gap = None
    if is_summary["hit_rate"] is not None and oos_summary["hit_rate"] is not None:
        overall_gap = float(is_summary["hit_rate"] - oos_summary["hit_rate"])
    passed = True
    reasons: list[str] = []
    if any(f["overfit_flag"] for f in folds) or (
        overall_gap is not None and overall_gap > settings.backtest.overfit_hit_rate_gap
    ):
        passed = False
        reasons.append("is_oos_hit_rate_gap")
    if (oos_summary.get("n") or 0) == 0:
        passed = False
        reasons.append("no_oos_trades")

    report = {
        "model_id": model_id,
        "n_folds": len(folds),
        "dates": [dates[0] if dates else None, dates[-1] if dates else None],
        "is": is_summary,
        "oos": oos_summary,
        "hit_rate_gap": overall_gap,
        "overfit_threshold": settings.backtest.overfit_hit_rate_gap,
        "harness_passed": passed,
        "fail_reasons": reasons,
        "folds": folds,
        "self_contained": True,
        "tracks": list(tracks) if tracks else None,
        "skipped_dates": sorted(skipped_dates),
        "note": "No Stage 7-9 trade records are produced.",
    }

    out_root = output_dir or (settings.paths.features_dir / "backtest")
    out_root.mkdir(parents=True, exist_ok=True)
    report_path = out_root / f"walkforward_{model_id}.json"
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    if not oos_all.empty:
        oos_all.to_csv(out_root / f"walkforward_{model_id}_oos.csv", index=False)
    report["report_path"] = str(report_path)
    return report
