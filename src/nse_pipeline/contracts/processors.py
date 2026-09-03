"""Processor contracts — no scoring, no Kite, no dashboard I/O."""

from __future__ import annotations

from typing import Any, Protocol

from nse_pipeline.config import Settings
from nse_pipeline.storage.sqlite_store import SQLiteStore


class FeatureProcessor(Protocol):
    """ticks/day → feature_log rows. Must not score or call Kite."""

    def process_date(self, settings: Settings, trade_date: str) -> dict[str, Any]: ...


class LabelProcessor(Protocol):
    """feature rows → actual_outcome. Must not train models."""

    def process_date(self, settings: Settings, trade_date: str) -> dict[str, Any]: ...


class SignalEngine(Protocol):
    """Algorithm outputs + maturity gate → persistable signals. Must not place orders."""

    def score_and_log(self, rows: list[dict[str, Any]]) -> dict[str, Any]: ...


class AccountCapture(Protocol):
    """REST poll → account tables. Must not change signals."""

    def capture_once(self, store: SQLiteStore, kite: Any) -> dict[str, int]: ...


class DecisionTracker(Protocol):
    """Mint/link trade_id. Must not score decision quality."""

    def log_decision(self, store: SQLiteStore, **kwargs: Any) -> str: ...


class OutcomeEvaluator(Protocol):
    """Closed trades → 2×2. Must not use hindsight-best entry."""

    def score_closed_trade(self, store: SQLiteStore, *, trade_id: str, **kwargs: Any) -> dict[str, Any]: ...


class RetrainEngine(Protocol):
    """Versioned train; never overwrite; never auto-promote by default."""

    def run(self, settings: Settings, **kwargs: Any) -> dict[str, Any]: ...
