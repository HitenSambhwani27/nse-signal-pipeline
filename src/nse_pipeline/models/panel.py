"""
Pooled options training panel — moneyness × days-to-expiry, not contract identity.

Nifty (weekly roll) and Bank Nifty (monthly roll) are built as SEPARATE panels.
Mixing them would blend two different data-generating processes.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from nse_pipeline.scoring.baseline import _flatten_features


def _parse_expiry(value: Any) -> date | None:
    if not value:
        return None
    text = str(value)
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(text[:10], fmt).date()
            except ValueError:
                continue
    return None


def _underlying_from_row(row: dict[str, Any]) -> str:
    feats = row.get("features") or {}
    name = str(feats.get("underlying") or "")
    if name:
        return name.upper()
    symbol = str(row.get("symbol") or "").upper()
    if symbol.startswith("BANKNIFTY"):
        return "BANKNIFTY"
    return "NIFTY"


def _moneyness(features: dict[str, Any]) -> float | None:
    spot = features.get("spot")
    strike = features.get("strike")
    if spot in (None, 0) or strike is None:
        return None
    return (float(strike) - float(spot)) / float(spot)


def _dte(row: dict[str, Any]) -> int | None:
    feats = row.get("features") or {}
    if feats.get("days_to_expiry") is not None:
        return int(feats["days_to_expiry"])
    expiry = _parse_expiry(feats.get("expiry"))
    ts = row.get("timestamp")
    if expiry is None or ts is None:
        return None
    day = pd.Timestamp(ts).date()
    return max((expiry - day).days, 0)


def build_options_panels(
    rows: list[dict[str, Any]],
) -> dict[str, pd.DataFrame]:
    """
    Return {'NIFTY': df, 'BANKNIFTY': df} with normalized columns.

    Each observation is recharacterized by moneyness and DTE so rolled
    weekly/monthly contracts can be pooled within an index — never across.
    """
    buckets: dict[str, list[dict[str, Any]]] = {"NIFTY": [], "BANKNIFTY": []}
    for row in rows:
        if str(row.get("track")) != "options":
            continue
        underlying = _underlying_from_row(row)
        if underlying not in buckets:
            continue
        feats = row.get("features") or {}
        mny = _moneyness(feats)
        dte = _dte(row)
        rec = {
            "trade_date": row.get("trade_date"),
            "timestamp": row.get("timestamp"),
            "symbol": row.get("symbol"),
            "underlying": underlying,
            "moneyness": mny,
            "days_to_expiry": dte,
            "actual_outcome": row.get("actual_outcome"),
            "source": row.get("source"),
            "feature_completeness": row.get("feature_completeness"),
            "features": feats,
            "flat": _flatten_features(feats),
        }
        buckets[underlying].append(rec)

    return {k: pd.DataFrame(v) for k, v in buckets.items()}


def panel_summary(panels: dict[str, pd.DataFrame]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, df in panels.items():
        if df.empty:
            out[name] = {"n": 0, "labeled": 0, "unique_symbols": 0, "date_span": None}
            continue
        labeled = df["actual_outcome"].notna().sum() if "actual_outcome" in df else 0
        dates = sorted({str(d) for d in df["trade_date"].dropna().unique()})
        out[name] = {
            "n": int(len(df)),
            "labeled": int(labeled),
            "unique_symbols": int(df["symbol"].nunique()) if "symbol" in df else 0,
            "date_span": [dates[0], dates[-1]] if dates else None,
            "mean_dte": float(df["days_to_expiry"].mean())
            if df["days_to_expiry"].notna().any()
            else None,
            "mean_abs_moneyness": float(df["moneyness"].abs().mean())
            if df["moneyness"].notna().any()
            else None,
        }
    return out
