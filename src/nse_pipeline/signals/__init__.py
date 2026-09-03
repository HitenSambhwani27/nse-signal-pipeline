"""Stage 6 live signal engine."""

from nse_pipeline.signals.maturity import maturity_public_view, maturity_snapshot

__all__ = ["LiveSignalEngine", "maturity_snapshot", "maturity_public_view"]


def __getattr__(name: str):
    if name == "LiveSignalEngine":
        from nse_pipeline.signals.engine import LiveSignalEngine

        return LiveSignalEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
