"""Retrain must not auto-promote; 2x2 counts attach when outcome_log has rows."""

from __future__ import annotations

from pathlib import Path

from nse_pipeline.quality.review import CLASS_GB
from nse_pipeline.retrain.loop import run_retrain
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings


def test_retrain_2x2_hook_no_promote(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    settings.retrain.auto_promote = False
    store = SQLiteStore(settings.paths.sqlite_db)
    store.upsert_outcome(
        {
            "trade_id": "t-gb",
            "classification": CLASS_GB,
            "decision_good": 1,
            "outcome_good": 0,
            "pnl": -10.0,
            "checkpoints": {},
            "details": {"actor": "manual"},
        }
    )

    monkeypatch.setattr(
        "nse_pipeline.retrain.loop.train_all",
        lambda *a, **k: {"classes": {}},
    )
    result = run_retrain(settings, run_harness=False)
    assert result["promoted"] is False
    assert result["comparison"]["decision_quality"]["class_counts"][CLASS_GB] == 1
    assert "raw P&L" in result["comparison"]["decision_quality"]["note"]
    assert result["maturity"]["equity"]["probability_permitted"] is False
