"""Walk-forward metrics (hit rate, P&L, Sharpe, max drawdown)."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = equity / peak.replace(0, np.nan) - 1.0
    return float(dd.min()) if len(dd) else 0.0


def sharpe_ratio(returns: pd.Series, *, periods_per_year: int) -> float:
    if returns.empty or returns.std() == 0 or not np.isfinite(returns.std()):
        return 0.0
    return float(returns.mean() / returns.std() * np.sqrt(periods_per_year))


def summarize_trades(trades: pd.DataFrame, *, annualization_days: int) -> dict[str, Any]:
    if trades.empty:
        return {
            "n": 0,
            "hit_rate": None,
            "avg_pnl": None,
            "sharpe": None,
            "max_drawdown": None,
        }
    hits = trades["pnl"] > 0
    equity = (1.0 + trades["pnl"]).cumprod()
    return {
        "n": int(len(trades)),
        "hit_rate": float(hits.mean()),
        "avg_pnl": float(trades["pnl"].mean()),
        "sharpe": sharpe_ratio(trades["pnl"], periods_per_year=annualization_days),
        "max_drawdown": max_drawdown(equity),
        "total_pnl": float(trades["pnl"].sum()),
    }
