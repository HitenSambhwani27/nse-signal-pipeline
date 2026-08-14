"""Stage 5B — 4×4 OI-state Markov model (regime-fragile; see TRADE_OFFS.md)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

STATES = (
    "long_buildup",
    "short_buildup",
    "long_unwinding",
    "short_covering",
)
STATE_INDEX = {s: i for i, s in enumerate(STATES)}


def _is_expiry_week(row: dict[str, Any]) -> bool:
    feats = row.get("features") or {}
    dte = feats.get("days_to_expiry")
    if dte is None:
        return False
    return int(dte) <= 7


def _underlying(row: dict[str, Any]) -> str:
    feats = row.get("features") or {}
    name = str(feats.get("underlying") or "")
    if name:
        return name.upper()
    symbol = str(row.get("symbol") or "").upper()
    return "BANKNIFTY" if symbol.startswith("BANKNIFTY") else "NIFTY"


def estimate_transition_matrices(
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Per-underlying, expiry-week vs non-expiry-week 4×4 matrices.

    Nifty weekly vs Bank Nifty monthly are never pooled. Stationarity is
    assumed only within a regime — flagged as fragile in TRADE_OFFS.md.
    """
    sequences: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    # key: (underlying, regime, symbol) -> state sequence in time order
    ordered = sorted(
        [r for r in rows if str(r.get("track")) == "options"],
        key=lambda r: (str(r.get("symbol")), str(r.get("timestamp"))),
    )
    for row in ordered:
        state = str((row.get("features") or {}).get("oi_buildup_state") or "")
        if state not in STATE_INDEX:
            continue
        regime = "expiry_week" if _is_expiry_week(row) else "non_expiry_week"
        sequences[(_underlying(row), regime, str(row.get("symbol")))].append(state)

    counts: dict[tuple[str, str], np.ndarray] = {}
    for (underlying, regime, _symbol), states in sequences.items():
        mat = counts.setdefault(
            (underlying, regime), np.zeros((4, 4), dtype=float)
        )
        for a, b in zip(states, states[1:]):
            mat[STATE_INDEX[a], STATE_INDEX[b]] += 1.0

    result: dict[str, Any] = {"states": list(STATES), "matrices": {}, "note": (
        "Regime-fragile: do not treat as unconditionally stationary. "
        "Expiry-week means a weekly event for Nifty and a monthly event for Bank Nifty."
    )}
    for (underlying, regime), mat in counts.items():
        row_sums = mat.sum(axis=1, keepdims=True)
        with np.errstate(invalid="ignore", divide="ignore"):
            trans = np.divide(mat, row_sums, where=row_sums > 0)
        trans = np.nan_to_num(trans)
        result["matrices"][f"{underlying}:{regime}"] = {
            "counts": mat.tolist(),
            "transition": trans.tolist(),
            "n_transitions": float(mat.sum()),
        }
    return result


def next_state_probs(matrix: list[list[float]], current: str) -> dict[str, float]:
    if current not in STATE_INDEX:
        return {s: 0.0 for s in STATES}
    row = matrix[STATE_INDEX[current]]
    return {s: float(row[i]) for i, s in enumerate(STATES)}
