"""Maturity gate contract. The only producer of the public N/60 view."""

from __future__ import annotations

from typing import Any, Protocol

from nse_pipeline.config import Settings


class MaturityGate(Protocol):
    """pooled live days → public view. Must not invent probabilities."""

    def pooled_live_days(
        self,
        settings: Settings,
        *,
        class_key: str,
        underlying: str | None = None,
    ) -> int: ...

    def public_view(
        self,
        days: int,
        settings: Settings,
        *,
        class_key: str,
        underlying: str | None = None,
        has_model: bool = True,
        reason: str | None = None,
    ) -> dict[str, Any]: ...

    def snapshot(
        self, settings: Settings, *, has_model: dict[str, bool] | None = None
    ) -> dict[str, Any]: ...
