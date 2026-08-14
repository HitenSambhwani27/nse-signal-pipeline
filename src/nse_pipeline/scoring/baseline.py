"""Stage 3B — YAML-driven linear baseline scorer (placeholder weights)."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import yaml

from nse_pipeline.config import PROJECT_ROOT, Settings
from nse_pipeline.storage.sqlite_store import SQLiteStore

OI_LONG = {"long_buildup", "short_covering"}
OI_SHORT = {"short_buildup", "long_unwinding"}


def load_baseline_weights(path: Path | None = None) -> dict[str, Any]:
    weights_path = path or (PROJECT_ROOT / "config" / "baseline_weights.yaml")
    with weights_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _flatten_features(features: dict[str, Any]) -> dict[str, float | None]:
    """Pull nested greeks onto the top level; skip missing depth keys entirely."""
    flat: dict[str, float | None] = {}
    skipped = set(features.get("depth_features_skipped") or [])
    for key, value in features.items():
        if key in skipped:
            continue
        if key == "greeks" and isinstance(value, dict):
            for gk, gv in value.items():
                if isinstance(gv, (int, float)):
                    flat[gk] = float(gv)
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            flat[key] = float(value)
    state = str(features.get("oi_buildup_state") or "")
    flat["oi_buildup_long"] = 1.0 if state in OI_LONG else 0.0
    flat["oi_buildup_short"] = 1.0 if state in OI_SHORT else 0.0
    if "vwap_deviation_bps" in features and features["vwap_deviation_bps"] is not None:
        flat["vwap_deviation"] = float(features["vwap_deviation_bps"]) / 10000.0
    if features.get("depth_ratio_bid_ask") is not None:
        flat["depth_ratio"] = float(features["depth_ratio_bid_ask"])
    if features.get("ofi_bucket_sum") is not None:
        flat["ofi"] = float(features["ofi_bucket_sum"])
    if "calendar_spread_near_minus_next" in features:
        val = features.get("calendar_spread_near_minus_next")
        flat["calendar_spread"] = float(val) if val is not None else None
    return flat


def score_feature_row(
    row: dict[str, Any], weights_cfg: dict[str, Any]
) -> dict[str, Any]:
    """
    Linear score using YAML weights. Missing depth features are skipped
    (contribution 0) so historical_partial and live_quote rows still score.
    """
    track = str(row.get("track") or "")
    if track.startswith("equity"):
        bucket = "equity"
    elif track == "options":
        bucket = "options"
    elif track == "futures":
        bucket = "futures"
    else:
        bucket = "equity"

    weights: dict[str, float] = dict(weights_cfg.get(bucket) or {})
    intercept = float(weights.pop("intercept", 0.0))
    flat = _flatten_features(row.get("features") or {})

    score = intercept
    used: dict[str, float] = {}
    skipped: list[str] = []
    for name, weight in weights.items():
        value = flat.get(name)
        if value is None:
            skipped.append(name)
            continue
        contrib = float(weight) * float(value)
        score += contrib
        used[name] = contrib

    probability = _sigmoid(score)
    return {
        "timestamp": row["timestamp"],
        "trade_date": row.get("trade_date"),
        "symbol": row["symbol"],
        "track": track,
        "model_version": "baseline_yaml_v1",
        "score": score,
        "probability": probability,
        "features": row.get("features") or {},
        "source": row.get("source") or (row.get("features") or {}).get("source"),
        "attribution": {
            "terms": used,
            "skipped_missing": skipped,
            "intercept": intercept,
            "score": score,
            "probability": probability,
        },
        "maturity_tier": None,
    }


def run_baseline_scorer(
    settings: Settings,
    start_date: str,
    end_date: str,
    *,
    weights_path: Path | None = None,
) -> dict[str, Any]:
    """Idempotent per date: deletes existing baseline signal_log rows in range, then writes."""
    store = SQLiteStore(settings.paths.sqlite_db)
    weights = load_baseline_weights(weights_path)
    rows = store.fetch_feature_logs_range(start_date, end_date)
    dates = sorted({r.get("trade_date") for r in rows if r.get("trade_date")})
    for d in dates:
        store.delete_signal_logs_for_trade_date(str(d))

    payload = [score_feature_row(r, weights) for r in rows]
    inserted = store.insert_signal_logs(payload)
    by_track: dict[str, int] = {}
    by_source: dict[str, int] = {}
    by_completeness: dict[str, int] = {}
    for row, scored in zip(rows, payload):
        by_track[scored["track"]] = by_track.get(scored["track"], 0) + 1
        src = str(scored.get("source") or "unknown")
        by_source[src] = by_source.get(src, 0) + 1
        comp = str((row.get("feature_completeness") or "unknown"))
        by_completeness[comp] = by_completeness.get(comp, 0) + 1
    return {
        "start_date": start_date,
        "end_date": end_date,
        "scored": inserted,
        "by_track": by_track,
        "by_source": by_source,
        "by_completeness": by_completeness,
    }
