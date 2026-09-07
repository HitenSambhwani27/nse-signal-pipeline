"""Thin job wrappers that record processing_status. No new feature math."""

from __future__ import annotations

from typing import Any

from nse_pipeline.account.capture import capture_once
from nse_pipeline.broker.session import TokenState, classify_exception
from nse_pipeline.config import Settings
from nse_pipeline.decisions.link import log_decision
from nse_pipeline.features.batch import run_feature_batch
from nse_pipeline.labels.batch import run_labeling
from nse_pipeline.quality.review import score_closed_trade
from nse_pipeline.retrain.loop import run_retrain
from nse_pipeline.storage.sqlite_store import SQLiteStore


class ExistingFeatureProcessor:
    def process_date(self, settings: Settings, trade_date: str, **kwargs: Any) -> dict[str, Any]:
        store = SQLiteStore(settings.paths.sqlite_db)
        store.upsert_processing_status(
            "features", status="running", last_trade_date=trade_date
        )
        try:
            summary = run_feature_batch(settings, trade_date, **kwargs)
            rows = int(summary.get("feature_rows") or 0)
            store.upsert_processing_status(
                "features",
                status="ok" if not summary.get("skipped") else "idle",
                last_trade_date=trade_date,
                rows_written=rows,
                details={"skipped": bool(summary.get("skipped"))},
            )
            return summary
        except Exception as exc:
            store.upsert_processing_status(
                "features",
                status="error",
                last_trade_date=trade_date,
                details={"error": str(exc)},
            )
            raise


class ExistingLabelProcessor:
    def process_date(self, settings: Settings, trade_date: str, **kwargs: Any) -> dict[str, Any]:
        store = SQLiteStore(settings.paths.sqlite_db)
        store.upsert_processing_status(
            "labels", status="running", last_trade_date=trade_date
        )
        try:
            report = run_labeling(settings, trade_date, **kwargs)
            rows = int(report.get("feature_rows_matched") or report.get("feature_rows") or 0)
            store.upsert_processing_status(
                "labels",
                status="ok" if not report.get("skipped") else "idle",
                last_trade_date=trade_date,
                rows_written=rows,
                details={"skipped": bool(report.get("skipped"))},
            )
            return report
        except Exception as exc:
            store.upsert_processing_status(
                "labels",
                status="error",
                last_trade_date=trade_date,
                details={"error": str(exc)},
            )
            raise


class ExistingAccountCapture:
    def capture_once(self, store: SQLiteStore, kite: Any, **kwargs: Any) -> dict[str, int]:
        store.upsert_processing_status("account", status="running")
        try:
            result = capture_once(store, kite, **kwargs)
            store.upsert_processing_status(
                "account",
                status="ok",
                rows_written=int(result.get("trade_fills") or 0),
                details=dict(result),
            )
            return result
        except Exception as exc:
            token_state = classify_exception(exc)
            status = "auth_invalid" if token_state == TokenState.INVALID else "error"
            store.upsert_processing_status(
                "account",
                status=status,
                details={
                    "error": str(exc),
                    "token_state": token_state.value if token_state else None,
                },
            )
            raise


class ExistingDecisionTracker:
    def log_decision(self, store: SQLiteStore, **kwargs: Any) -> str:
        return log_decision(store, **kwargs)


class ExistingOutcomeEvaluator:
    def score_closed_trade(self, store: SQLiteStore, *, trade_id: str, **kwargs: Any) -> dict[str, Any]:
        return score_closed_trade(store, trade_id=trade_id, **kwargs)


class ExistingRetrainEngine:
    def run(self, settings: Settings, **kwargs: Any) -> dict[str, Any]:
        return run_retrain(settings, **kwargs)
