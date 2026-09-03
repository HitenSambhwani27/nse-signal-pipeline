from nse_pipeline.algorithms.unavailable import UnavailableAlgorithm

__all__ = ["UnavailableAlgorithm", "LogisticAlgorithm"]


def __getattr__(name: str):
    if name == "LogisticAlgorithm":
        from nse_pipeline.algorithms.logistic import LogisticAlgorithm

        return LogisticAlgorithm
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
