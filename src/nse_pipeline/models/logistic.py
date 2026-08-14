"""Stage 5A — logistic regression (coarse + fine) per instrument class."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from nse_pipeline.scoring.baseline import _flatten_features

CLASS_FROM_TRACK = {
    "equity_depth": "equity",
    "equity_quote": "equity",
    "index": "equity",
    "options": "options",
    "futures": "futures",
}


def feature_vector(
    row: dict[str, Any], names: list[str]
) -> list[float]:
    flat = dict(_flatten_features(row.get("features") or {}))
    feats = row.get("features") or {}
    if "moneyness" in names and "moneyness" not in flat:
        spot, strike = feats.get("spot"), feats.get("strike")
        if spot not in (None, 0) and strike is not None:
            flat["moneyness"] = (float(strike) - float(spot)) / float(spot)
    if "days_to_expiry" in names and feats.get("days_to_expiry") is not None:
        flat["days_to_expiry"] = float(feats["days_to_expiry"])
    vec: list[float] = []
    for name in names:
        val = flat.get(name)
        vec.append(float(val) if val is not None and np.isfinite(val) else 0.0)
    return vec


def fit_logistic(
    rows: list[dict[str, Any]],
    feature_names: list[str],
) -> tuple[Pipeline, dict[str, Any]]:
    """Binary up-vs-rest logistic regression. Returns (pipeline, metadata)."""
    X: list[list[float]] = []
    y: list[int] = []
    for row in rows:
        outcome = row.get("actual_outcome")
        if outcome is None:
            continue
        X.append(feature_vector(row, feature_names))
        y.append(1 if float(outcome) > 0 else 0)
    if len(set(y)) < 2 or len(y) < 10:
        raise ValueError(
            f"Not enough labeled diversity to fit logistic (n={len(y)}, classes={set(y)})"
        )
    pipe = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=500,
                    solver="lbfgs",
                    class_weight="balanced",
                ),
            ),
        ]
    )
    pipe.fit(np.asarray(X, dtype=float), np.asarray(y, dtype=int))
    clf: LogisticRegression = pipe.named_steps["clf"]
    coefs = {
        name: float(w) for name, w in zip(feature_names, clf.coef_[0].tolist())
    }
    meta = {
        "n": int(len(y)),
        "n_up": int(sum(y)),
        "feature_names": feature_names,
        "coefficients": coefs,
        "intercept": float(clf.intercept_[0]),
    }
    return pipe, meta


def predict_proba_up(pipe: Pipeline, row: dict[str, Any], feature_names: list[str]) -> float:
    vec = np.asarray([feature_vector(row, feature_names)], dtype=float)
    proba = pipe.predict_proba(vec)[0]
    classes = list(pipe.named_steps["clf"].classes_)
    if 1 in classes:
        return float(proba[classes.index(1)])
    return float(proba[-1])


def signed_score(pipe: Pipeline, row: dict[str, Any], feature_names: list[str]) -> float:
    """Map P(up) to a signed score in [-1, 1] for the walk-forward harness."""
    p = predict_proba_up(pipe, row, feature_names)
    return 2.0 * p - 1.0
