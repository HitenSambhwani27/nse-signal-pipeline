"""Adapter around the existing logistic pair. Not a new algorithm."""

from __future__ import annotations

import logging
from typing import Any

from nse_pipeline.config import Settings
from nse_pipeline.contracts.algorithms import AlgorithmResult
from nse_pipeline.models.logistic import predict_proba_up
from nse_pipeline.models.registry import load_latest_pair
from nse_pipeline.signals.attribution import attribution_breakdown

logger = logging.getLogger(__name__)


class LogisticAlgorithm:
    """Loads harness-passed coarse+fine pairs if they exist; otherwise unavailable."""

    name = "logistic"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pairs: dict[str, dict[str, Any]] = {}
        for track in ("equity", "options", "futures"):
            try:
                self.pairs[track] = load_latest_pair(
                    settings, track, require_harness_passed=True
                )
            except FileNotFoundError as exc:
                logger.warning("No live-eligible model for %s: %s", track, exc)

    def available(self, class_key: str) -> bool:
        return class_key in self.pairs and self.pairs[class_key] is not None

    def score(self, row: dict[str, Any], *, class_key: str) -> AlgorithmResult:
        pair = self.pairs.get(class_key)
        if pair is None:
            return AlgorithmResult(
                available=False,
                probability=None,
                score=None,
                version="no_harness_passed_model",
                details={"reason": "no_harness_passed_model", "class_key": class_key},
            )
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

        text = attribution_breakdown(
            feature_names=coarse_names,
            coefficients=coarse["metadata"].get("coefficients") or {},
            intercept=float(coarse["metadata"].get("intercept") or 0.0),
            row=row,
            probability=probability,
            sample_size=coarse["metadata"].get("sample_size")
            or coarse["metadata"].get("n"),
            maturity_note="",
        )
        return AlgorithmResult(
            available=True,
            probability=probability,
            score=2.0 * probability - 1.0,
            version=str(coarse["metadata"].get("version") or "live_provisional"),
            details={
                "text": text,
                "p_coarse": p_coarse,
                "p_fine": p_fine,
                "blend": blend_note,
                "research_probability": probability,
                "research_score": 2.0 * probability - 1.0,
            },
        )
