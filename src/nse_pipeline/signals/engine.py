"""
Stage 6 live signal engine — incremental scoring, decoupled from Streamlit.

Loads only harness-passed coarse+fine pairs. Blends them; never substitutes
one for the other. Every probability includes a human-readable attribution.
"""

from __future__ import annotations

import logging
from typing import Any

from nse_pipeline.config import Settings
from nse_pipeline.models.logistic import CLASS_FROM_TRACK, predict_proba_up
from nse_pipeline.models.registry import load_latest_pair
from nse_pipeline.signals.attribution import attribution_breakdown
from nse_pipeline.signals.maturity import classify_tier, pooled_live_days
from nse_pipeline.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


def _underlying(row: dict[str, Any]) -> str | None:
    if str(row.get("track")) != "options":
        return None
    feats = row.get("features") or {}
    name = str(feats.get("underlying") or "")
    if name:
        return name.upper()
    symbol = str(row.get("symbol") or "").upper()
    return "BANKNIFTY" if symbol.startswith("BANKNIFTY") else "NIFTY"


class LiveSignalEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = SQLiteStore(settings.paths.sqlite_db)
        self.pairs: dict[str, dict[str, Any]] = {}
        for track in ("equity", "options", "futures"):
            try:
                self.pairs[track] = load_latest_pair(
                    settings, track, require_harness_passed=True
                )
            except FileNotFoundError as exc:
                logger.warning("No live-eligible model for %s: %s", track, exc)

    def score_row(self, row: dict[str, Any]) -> dict[str, Any] | None:
        class_key = CLASS_FROM_TRACK.get(str(row.get("track")), "equity")
        pair = self.pairs.get(class_key)
        if not pair:
            return None
        underlying = _underlying(row)
        days = pooled_live_days(
            self.settings, class_key=class_key, underlying=underlying
        )
        tier = classify_tier(days, self.settings)
        if tier == "suppressed":
            return {
                "suppressed": True,
                "maturity_tier": tier,
                "pooled_live_days": days,
                "symbol": row.get("symbol"),
            }

        coarse = pair["coarse"]
        coarse_names = list(coarse["metadata"].get("feature_names") or [])
        p_coarse = predict_proba_up(coarse["model"], row, coarse_names)
        p_fine = None
        fine = pair.get("fine")
        completeness = str(row.get("feature_completeness") or "")
        if fine and completeness == "live_full":
            fine_names = list(fine["metadata"].get("feature_names") or [])
            p_fine = predict_proba_up(fine["model"], row, fine_names)

        w_c = self.settings.training.blend_coarse_weight
        w_f = self.settings.training.blend_fine_weight
        if p_fine is None:
            probability = p_coarse
            blend_note = "coarse_only"
        else:
            probability = w_c * p_coarse + w_f * p_fine
            blend_note = f"blend coarse={w_c} fine={w_f}"

        maturity_note = (
            f"provisional — {days} days of pooled training data"
            if tier == "provisional"
            else f"full confidence — {days} pooled live days"
        )
        text = attribution_breakdown(
            feature_names=coarse_names,
            coefficients=coarse["metadata"].get("coefficients") or {},
            intercept=float(coarse["metadata"].get("intercept") or 0.0),
            row=row,
            probability=probability,
            sample_size=coarse["metadata"].get("sample_size")
            or coarse["metadata"].get("n"),
            maturity_note=maturity_note,
        )
        return {
            "timestamp": row["timestamp"],
            "trade_date": row.get("trade_date"),
            "symbol": row["symbol"],
            "track": row["track"],
            "model_version": coarse["metadata"].get("version"),
            "score": 2.0 * probability - 1.0,
            "probability": probability,
            "features": row.get("features") or {},
            "source": row.get("source"),
            "maturity_tier": tier,
            "attribution": {
                "text": text,
                "p_coarse": p_coarse,
                "p_fine": p_fine,
                "blend": blend_note,
                "pooled_live_days": days,
                "maturity_note": maturity_note,
            },
        }

    def score_and_log(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        payload: list[dict[str, Any]] = []
        suppressed = 0
        for row in rows:
            scored = self.score_row(row)
            if scored is None:
                continue
            if scored.get("suppressed"):
                suppressed += 1
                continue
            payload.append(scored)
        inserted = self.store.insert_signal_logs(payload)
        return {"scored": inserted, "suppressed": suppressed}
