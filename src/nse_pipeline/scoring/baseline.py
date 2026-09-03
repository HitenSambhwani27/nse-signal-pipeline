"""Stage 3B — YAML-driven linear baseline scorer (placeholder weights)."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import yaml

from nse_pipeline.config import PROJECT_ROOT, Settings
from nse_pipeline.session_coverage import scoring_skip_trade_dates
from nse_pipeline.storage.sqlite_store import SQLiteStore

OI_LONG = {"long_buildup", "short_covering"}
OI_SHORT = {"short_buildup", "long_unwinding"}
# Never applied to equity_quote, even if copied into that YAML block by mistake.
DEPTH_WEIGHT_KEYS = frozenset({"depth_ratio", "spread_bps", "ofi"})


def load_baseline_weights(path: Path | None = None) -> dict[str, Any]:
    weights_path = path or (PROJECT_ROOT / "config" / "baseline_weights.yaml")
    with weights_path.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle) or {}
    # Legacy YAML used a single "equity" block for Nifty 100 depth.
    if "equity_depth" not in cfg and "equity" in cfg:
        cfg["equity_depth"] = dict(cfg["equity"])
    return cfg


def _weight_bucket(track: str, weights_cfg: dict[str, Any]) -> str:
    """Map a feature track to a YAML weight block. Quote never shares depth weights."""
    if track == "equity_quote":
        if "equity_quote" not in weights_cfg:
            raise KeyError(
                "equity_quote rows require a dedicated equity_quote weight set; "
                "refusing to score them with equity/equity_depth (depth) weights"
            )
        return "equity_quote"
    if track == "equity_depth":
        if "equity_depth" in weights_cfg:
            return "equity_depth"
        if "equity" in weights_cfg:
            return "equity"
        raise KeyError("No equity_depth (or legacy equity) weights in baseline YAML")
    if track in weights_cfg:
        return track
    raise KeyError(f"No baseline weights for track={track!r}")


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
    Linear score using YAML weights for this row's track.

    equity_quote uses a depth-free weight set. Depth terms are stripped, not
    zero-filled: a missing or 0.0 book feature must not enter the quote score.
    equity_depth may still skip missing L2 terms on historical_partial rows.
    """
    track = str(row.get("track") or "")
    bucket = _weight_bucket(track, weights_cfg)
    weights: dict[str, float] = dict(weights_cfg.get(bucket) or {})
    if track == "equity_quote":
        for key in DEPTH_WEIGHT_KEYS:
            weights.pop(key, None)
    intercept = float(weights.pop("intercept", 0.0))
    flat = _flatten_features(row.get("features") or {})
    if track == "equity_quote":
        for key in DEPTH_WEIGHT_KEYS:
            flat.pop(key, None)

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
        "model_version": "baseline_yaml_v2",
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
            "weight_bucket": bucket,
        },
        "maturity_tier": None,
    }


def thin_features_for_scoring(features: dict[str, Any]) -> dict[str, Any]:
    """Keep only keys the YAML scorer and cost model read."""
    keep_names = {
        "ltp",
        "depth_ratio_bid_ask",
        "spread_bps",
        "ofi_bucket_sum",
        "vwap_deviation_bps",
        "pcr",
        "oi_buildup_state",
        "delta",
        "gamma",
        "basis_bps",
        "calendar_spread_near_minus_next",
        "greeks",
        "depth_features_skipped",
        "source",
    }
    return {k: features[k] for k in keep_names if k in features}


def run_baseline_scorer(
    settings: Settings,
    start_date: str,
    end_date: str,
    *,
    weights_path: Path | None = None,
    tracks: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Idempotent per date/track: deletes existing baseline signal_log rows, then writes."""
    store = SQLiteStore(settings.paths.sqlite_db)
    weights = load_baseline_weights(weights_path)
    dates = [
        d
        for d in store.list_feature_trade_dates()
        if start_date <= d <= end_date
    ]
    skipped_dates: set[str] = set()
    by_track: dict[str, int] = {}
    by_source: dict[str, int] = {}
    by_completeness: dict[str, int] = {}
    inserted = 0
    for trade_date in dates:
        rows = store.fetch_feature_logs(trade_date, tracks=tracks)
        if not rows:
            continue
        date_skip = scoring_skip_trade_dates(rows)
        if trade_date in date_skip:
            skipped_dates.add(trade_date)
            store.delete_signal_logs_for_trade_date(trade_date, tracks=tracks)
            continue
        store.delete_signal_logs_for_trade_date(trade_date, tracks=tracks)
        payload = [score_feature_row(r, weights) for r in rows]
        inserted += store.insert_signal_logs(payload)
        for row, scored in zip(rows, payload):
            by_track[scored["track"]] = by_track.get(scored["track"], 0) + 1
            src = str(scored.get("source") or "unknown")
            by_source[src] = by_source.get(src, 0) + 1
            comp = str(row.get("feature_completeness") or "unknown")
            by_completeness[comp] = by_completeness.get(comp, 0) + 1
    return {
        "start_date": start_date,
        "end_date": end_date,
        "scored": inserted,
        "by_track": by_track,
        "by_source": by_source,
        "by_completeness": by_completeness,
        "skipped_dates": sorted(skipped_dates),
    }
