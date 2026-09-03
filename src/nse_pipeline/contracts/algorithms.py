"""Algorithm plug-in contract. Implementations must not bypass the maturity gate."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class AlgorithmResult:
    """Raw model output. Public probability is applied only after the gate."""

    available: bool
    probability: float | None = None
    score: float | None = None
    version: str | None = None
    details: dict[str, Any] | None = None


class Algorithm(Protocol):
    """feature row → optional raw score. Never writes SQLite. Never formats N/60."""

    name: str

    def available(self, class_key: str) -> bool: ...

    def score(self, row: dict[str, Any], *, class_key: str) -> AlgorithmResult: ...
