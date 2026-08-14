"""Human-readable feature attribution from fitted logistic weights."""

from __future__ import annotations

from typing import Any

from nse_pipeline.models.logistic import feature_vector


def attribution_breakdown(
    *,
    feature_names: list[str],
    coefficients: dict[str, float],
    intercept: float,
    row: dict[str, Any],
    probability: float,
    sample_size: int | None,
    maturity_note: str,
) -> str:
    values = feature_vector(row, feature_names)
    parts: list[str] = []
    total = intercept
    for name, value in zip(feature_names, values):
        coef = float(coefficients.get(name, 0.0))
        contrib = coef * value
        total += contrib
        parts.append(f"{name}: {contrib:+.3f}")
    joined = ", ".join(parts) if parts else "(no features)"
    sample_bit = f"n={sample_size}" if sample_size is not None else "n=?"
    return (
        f"{joined} → combined score {total:.3f} → probability {probability:.3f} "
        f"({sample_bit}; {maturity_note})"
    )
