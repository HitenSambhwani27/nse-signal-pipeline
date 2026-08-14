"""Train coarse + fine logistic models and gate them through the Part 5 harness."""

from __future__ import annotations

import json
from typing import Any

from nse_pipeline.backtest.harness import run_walk_forward
from nse_pipeline.config import Settings
from nse_pipeline.models.logistic import (
    CLASS_FROM_TRACK,
    fit_logistic,
    signed_score,
)
from nse_pipeline.models.markov import estimate_transition_matrices
from nse_pipeline.models.panel import build_options_panels, panel_summary
from nse_pipeline.models.registry import save_model_bundle
from nse_pipeline.storage.sqlite_store import SQLiteStore


def _class_key(track: str) -> str:
    return CLASS_FROM_TRACK.get(track, "equity")


def _filter_for_role(
    rows: list[dict[str, Any]],
    *,
    class_key: str,
    role: str,
    settings: Settings,
) -> list[dict[str, Any]]:
    include_hist = settings.training.include_historical_partial.get(class_key, True)
    out: list[dict[str, Any]] = []
    for row in rows:
        if _class_key(str(row.get("track"))) != class_key:
            continue
        completeness = str(row.get("feature_completeness") or "")
        source = str(row.get("source") or "")
        if role == "fine":
            if source == "historical" or completeness == "historical_partial":
                continue
        elif completeness == "historical_partial" and not include_hist:
            continue
        if row.get("actual_outcome") is None:
            continue
        out.append(row)
    return out


def _feature_names(settings: Settings, class_key: str, role: str) -> list[str]:
    block = (
        settings.training.fine_features
        if role == "fine"
        else settings.training.coarse_features
    )
    names = list(block.get(class_key) or [])
    if class_key == "options":
        extra = ["moneyness", "days_to_expiry"]
        names = names + [n for n in extra if n not in names]
    return names


def train_class_models(
    settings: Settings,
    rows: list[dict[str, Any]],
    *,
    class_key: str,
    run_harness: bool = True,
) -> dict[str, Any]:
    report: dict[str, Any] = {"class": class_key, "roles": {}}
    if class_key == "options":
        panels = build_options_panels(rows)
        report["options_panels"] = panel_summary(panels)
        markov = estimate_transition_matrices(rows)
        markov_dir = settings.paths.models_dir / "options" / "markov"
        markov_dir.mkdir(parents=True, exist_ok=True)
        (markov_dir / "latest.json").write_text(
            json.dumps(markov, indent=2), encoding="utf-8"
        )
        report["markov"] = {
            k: v.get("n_transitions") for k, v in markov.get("matrices", {}).items()
        }

    for role in ("coarse", "fine"):
        subset = _filter_for_role(rows, class_key=class_key, role=role, settings=settings)
        names = _feature_names(settings, class_key, role)
        entry: dict[str, Any] = {
            "n_rows": len(subset),
            "feature_names": names,
        }
        try:
            pipe, meta = fit_logistic(subset, names)
        except ValueError as exc:
            entry["error"] = str(exc)
            report["roles"][role] = entry
            continue

        harness_report = None
        if run_harness:
            def _scorer(row: dict[str, Any], _pipe=pipe, _names=names) -> float:
                return signed_score(_pipe, row, _names)

            harness_report = run_walk_forward(
                settings,
                _scorer,
                rows=subset,
                model_id=f"{class_key}_{role}",
            )
            entry["harness"] = {
                "passed": harness_report.get("harness_passed"),
                "is": harness_report.get("is"),
                "oos": harness_report.get("oos"),
                "hit_rate_gap": harness_report.get("hit_rate_gap"),
                "report_path": harness_report.get("report_path"),
            }
        path = save_model_bundle(
            settings,
            track=class_key,
            role=role,
            estimator=pipe,
            metadata={
                **meta,
                "source_composition": _source_composition(subset),
                "sample_size": len(subset),
            },
            harness_report=harness_report,
        )
        entry["path"] = str(path)
        entry["harness_passed"] = bool(
            harness_report and harness_report.get("harness_passed")
        )
        report["roles"][role] = entry
    return report


def _source_composition(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        key = str(row.get("source") or "unknown")
        out[key] = out.get(key, 0) + 1
    return out


def train_all(
    settings: Settings,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    run_harness: bool = True,
) -> dict[str, Any]:
    store = SQLiteStore(settings.paths.sqlite_db)
    dates = store.list_feature_trade_dates()
    if start_date:
        dates = [d for d in dates if d >= start_date]
    if end_date:
        dates = [d for d in dates if d <= end_date]
    if not dates:
        raise FileNotFoundError("No labeled feature_log rows to train on.")
    rows = store.fetch_feature_logs_range(dates[0], dates[-1])
    summary: dict[str, Any] = {
        "window": [dates[0], dates[-1]],
        "n_rows": len(rows),
        "source_composition": _source_composition(rows),
        "classes": {},
    }
    for class_key in ("equity", "options", "futures"):
        summary["classes"][class_key] = train_class_models(
            settings, rows, class_key=class_key, run_harness=run_harness
        )
    return summary
