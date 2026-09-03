"""Stable plug-in contracts. Implementations may change; API/dashboard must not."""

from nse_pipeline.contracts.algorithms import Algorithm, AlgorithmResult
from nse_pipeline.contracts.gate import MaturityGate
from nse_pipeline.contracts.processors import (
    AccountCapture,
    DecisionTracker,
    FeatureProcessor,
    LabelProcessor,
    OutcomeEvaluator,
    RetrainEngine,
    SignalEngine,
)
from nse_pipeline.contracts.read_models import UiReadModel

__all__ = [
    "FeatureProcessor",
    "LabelProcessor",
    "MaturityGate",
    "Algorithm",
    "AlgorithmResult",
    "SignalEngine",
    "AccountCapture",
    "DecisionTracker",
    "OutcomeEvaluator",
    "RetrainEngine",
    "UiReadModel",
]
