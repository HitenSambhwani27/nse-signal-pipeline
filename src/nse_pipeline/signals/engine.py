"""
Stage 6 live signal engine — incremental scoring, decoupled from the dashboard.

Depends on the Algorithm protocol. Default plug-in is UnavailableAlgorithm
(no invented numbers). A LogisticAlgorithm adapter is used only when a
harness-passed pair already exists on disk.
"""

from __future__ import annotations

from typing import Any

from nse_pipeline.algorithms.logistic import LogisticAlgorithm
from nse_pipeline.algorithms.unavailable import UnavailableAlgorithm
from nse_pipeline.config import Settings
from nse_pipeline.contracts.algorithms import Algorithm, AlgorithmResult
from nse_pipeline.signals.maturity import (
    CLASS_FROM_TRACK,
    maturity_public_view,
    pooled_live_days,
    signal_public_view,
)
from nse_pipeline.storage.sqlite_store import SQLiteStore


def _underlying(row: dict[str, Any]) -> str | None:
    if str(row.get("track")) != "options":
        return None
    feats = row.get("features") or {}
    name = str(feats.get("underlying") or "")
    if name:
        return name.upper()
    symbol = str(row.get("symbol") or "").upper()
    return "BANKNIFTY" if symbol.startswith("BANKNIFTY") else "NIFTY"


def _default_algorithms(settings: Settings) -> dict[str, Algorithm]:
    logistic = LogisticAlgorithm(settings)
    out: dict[str, Algorithm] = {}
    for key in ("equity", "options", "futures"):
        out[key] = logistic if logistic.available(key) else UnavailableAlgorithm()
    return out


def _gate_row(
    row: dict[str, Any],
    *,
    view: dict[str, Any],
    model_version: str,
    extra_attribution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attribution = {
        "display": view["display"],
        "pooled_live_days": view["pooled_live_days"],
        "maturity_note": view["display"],
        "reason": view.get("reason"),
        "emitter": "live_engine",
        "maturity": view,
    }
    if extra_attribution:
        attribution.update(extra_attribution)
    return {
        "timestamp": row["timestamp"],
        "trade_date": row.get("trade_date"),
        "symbol": row["symbol"],
        "track": row["track"],
        "model_version": model_version,
        "score": None,
        "probability": None,
        "features": row.get("features") or {},
        "source": row.get("source"),
        "maturity_tier": view["tier"],
        "attribution": attribution,
        "suppressed": not view["probability_permitted"],
        "maturity": view,
    }


class LiveSignalEngine:
    def __init__(
        self,
        settings: Settings,
        algorithms: dict[str, Algorithm] | None = None,
    ) -> None:
        self.settings = settings
        self.store = SQLiteStore(settings.paths.sqlite_db)
        self.algorithms = algorithms or _default_algorithms(settings)
        self._days_cache: dict[tuple[str, str | None], int] = {}

    def has_model(self, class_key: str) -> bool:
        algo = self.algorithms.get(class_key)
        return bool(algo and algo.available(class_key))

    def _algorithm(self, class_key: str) -> Algorithm:
        return self.algorithms.get(class_key) or UnavailableAlgorithm()

    def _days(self, class_key: str, underlying: str | None) -> int:
        key = (class_key, underlying)
        if key not in self._days_cache:
            self._days_cache[key] = pooled_live_days(
                self.settings, class_key=class_key, underlying=underlying
            )
        return self._days_cache[key]

    def score_row(self, row: dict[str, Any]) -> dict[str, Any]:
        class_key = CLASS_FROM_TRACK.get(str(row.get("track")), "equity")
        underlying = _underlying(row)
        days = self._days(class_key, underlying)
        algo = self._algorithm(class_key)
        has_model = algo.available(class_key)
        raw: AlgorithmResult = algo.score(row, class_key=class_key)
        reason = None
        if not has_model or not raw.available:
            reason = (raw.details or {}).get("reason") or (
                "algorithm_not_implemented"
                if isinstance(algo, UnavailableAlgorithm)
                else "no_harness_passed_model"
            )
        view = maturity_public_view(
            days,
            self.settings,
            class_key=class_key,
            underlying=underlying,
            has_model=has_model and raw.available,
            reason=reason,
        )

        if not raw.available or not has_model:
            return _gate_row(
                row,
                view=view,
                model_version=raw.version or "gate_no_model",
            )

        if view["tier"] == "suppressed":
            return _gate_row(row, view=view, model_version="gate_suppressed")

        research = dict(raw.details or {})
        if not view["probability_permitted"]:
            return _gate_row(
                row,
                view=view,
                model_version=raw.version or "live_provisional",
                extra_attribution=research,
            )
        return {
            "timestamp": row["timestamp"],
            "trade_date": row.get("trade_date"),
            "symbol": row["symbol"],
            "track": row["track"],
            "model_version": raw.version,
            "score": raw.score,
            "probability": raw.probability,
            "features": row.get("features") or {},
            "source": row.get("source"),
            "maturity_tier": view["tier"],
            "attribution": {
                **research,
                "display": view["display"],
                "pooled_live_days": view["pooled_live_days"],
                "maturity_note": view["display"],
                "emitter": "live_engine",
                "maturity": view,
            },
            "suppressed": False,
            "maturity": view,
        }

    def public_row(self, scored: dict[str, Any]) -> dict[str, Any]:
        view = scored.get("maturity") or (scored.get("attribution") or {}).get("maturity")
        if not isinstance(view, dict):
            view = {"probability_permitted": False, "display": "insufficient data"}
        return signal_public_view(scored, view)

    def score_and_log(
        self,
        rows: list[dict[str, Any]],
        *,
        replace_dates: bool = True,
    ) -> dict[str, Any]:
        self._days_cache.clear()
        payload: list[dict[str, Any]] = []
        suppressed = 0
        dates: set[str] = set()
        last_ts: str | None = None
        last_date: str | None = None
        for row in rows:
            scored = self.score_row(row)
            trade_date = str(scored.get("trade_date") or row.get("trade_date") or "")
            dates.add(trade_date)
            ts = str(scored.get("timestamp") or row.get("timestamp") or "")
            if ts and (last_ts is None or ts > last_ts):
                last_ts = ts
                last_date = trade_date or last_date
            if scored.get("suppressed"):
                suppressed += 1
            payload.append(
                {k: v for k, v in scored.items() if k not in {"suppressed", "maturity"}}
            )
        if not payload:
            return {
                "scored": 0,
                "suppressed": 0,
                "public_probability_null": 0,
            }
        if replace_dates:
            for trade_date in dates:
                if trade_date:
                    self.store.delete_live_engine_signals(trade_date)
        inserted = self.store.insert_signal_logs(payload)
        self.store.upsert_processing_status(
            "signals",
            status="ok",
            last_trade_date=last_date,
            last_timestamp=last_ts,
            rows_written=inserted,
        )
        return {
            "scored": inserted,
            "suppressed": suppressed,
            "public_probability_null": suppressed,
        }
