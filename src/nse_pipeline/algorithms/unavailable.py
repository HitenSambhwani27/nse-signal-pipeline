"""Explicit empty algorithm. Never invents a probability."""

from __future__ import annotations

from typing import Any

from nse_pipeline.contracts.algorithms import AlgorithmResult

REASON = "algorithm_not_implemented"


class UnavailableAlgorithm:
    """Skeleton stand-in until a real Algorithm is plugged in."""

    name = "unavailable"

    def available(self, class_key: str) -> bool:
        return False

    def score(self, row: dict[str, Any], *, class_key: str) -> AlgorithmResult:
        return AlgorithmResult(
            available=False,
            probability=None,
            score=None,
            version=REASON,
            details={"reason": REASON, "class_key": class_key},
        )
