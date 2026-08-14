"""Part 8 — sliding-window retrain. New models never overwrite; never auto-promote."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from nse_pipeline.config import Settings
from nse_pipeline.features.batch import run_feature_batch
from nse_pipeline.labels.batch import run_labeling
from nse_pipeline.models.registry import load_latest_pair
from nse_pipeline.models.train import train_all
from nse_pipeline.signals.maturity import maturity_snapshot
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore


def _window(settings: Settings, as_of: date | None = None) -> tuple[str, str]:
    end = as_of or date.today()
    start = end - timedelta(days=settings.retrain.window_days - 1)
    return start.isoformat(), end.isoformat()


def source_composition(settings: Settings, start: str, end: str) -> dict[str, int]:
    store = SQLiteStore(settings.paths.sqlite_db)
    rows = store.fetch_feature_logs_range(start, end)
    out: dict[str, int] = {}
    for row in rows:
        key = str(row.get("source") or "unknown")
        out[key] = out.get(key, 0) + 1
    return out


def _oos_hit(pair: dict[str, Any] | None) -> float | None:
    if not pair:
        return None
    meta = pair["coarse"]["metadata"]
    # harness numbers live on disk; metadata may only have harness_passed.
    return meta.get("oos_hit_rate")


def run_retrain(
    settings: Settings,
    *,
    as_of: date | None = None,
    rebuild_features: bool = False,
    run_harness: bool = True,
) -> dict[str, Any]:
    start, end = _window(settings, as_of)
    store = SQLiteStore(settings.paths.sqlite_db)
    composition = source_composition(settings, start, end)
    store.log_retrain_event(
        "retrain_start",
        window_start=start,
        window_end=end,
        source_composition=composition,
    )

    if rebuild_features:
        with DuckDBTickStore(settings) as duck:
            dates = [d for d in duck.list_compacted_dates() if start <= d <= end]
        for d in dates:
            try:
                run_feature_batch(settings, d)
                run_labeling(settings, d)
            except FileNotFoundError:
                continue

    current: dict[str, Any] = {}
    for track in ("equity", "options", "futures"):
        try:
            current[track] = load_latest_pair(
                settings, track, require_harness_passed=True
            )
        except FileNotFoundError:
            current[track] = None

    trained = train_all(settings, start_date=start, end_date=end, run_harness=run_harness)
    comparison: dict[str, Any] = {}
    for class_key, payload in trained.get("classes", {}).items():
        coarse = (payload.get("roles") or {}).get("coarse") or {}
        comparison[class_key] = {
            "new_harness_passed": coarse.get("harness_passed"),
            "new_oos": (coarse.get("harness") or {}).get("oos"),
            "new_path": coarse.get("path"),
            "current_loaded": current.get(class_key) is not None,
        }

    promoted = False
    if settings.retrain.auto_promote:
        # Explicitly off by default. Even if enabled, only harness-passed models
        # enter the registry as live-eligible; promotion is still a recorded event.
        promoted = all(
            (comparison.get(k) or {}).get("new_harness_passed")
            for k in ("equity", "options", "futures")
            if k in comparison
        )

    maturity = maturity_snapshot(settings)
    details = {
        "trained": trained,
        "maturity": maturity,
        "auto_promote_requested": settings.retrain.auto_promote,
    }
    store.log_retrain_event(
        "retrain_complete",
        window_start=start,
        window_end=end,
        source_composition=composition,
        comparison=comparison,
        promoted=promoted,
        details=details,
    )
    return {
        "window": [start, end],
        "source_composition": composition,
        "comparison": comparison,
        "promoted": promoted,
        "maturity": maturity,
        "trained": trained,
        "note": (
            "New versioned files were written; previous models were not overwritten. "
            "Live engine loads only harness-passed pairs. auto_promote is "
            f"{settings.retrain.auto_promote}."
        ),
    }
