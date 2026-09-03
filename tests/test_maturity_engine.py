"""Stage 6 maturity gate: SQL counts, public N/60 view, persist suppressed rows."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from nse_pipeline.signals.engine import LiveSignalEngine
from nse_pipeline.signals.maturity import (
    classify_tier,
    maturity_public_view,
    maturity_snapshot,
    pooled_live_days,
    signal_public_view,
)
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings


def _insert_live(
    store: SQLiteStore,
    *,
    trade_date: str,
    symbol: str,
    track: str,
    hour: int = 4,
    minute: int = 0,
    underlying: str | None = None,
) -> None:
    ts = datetime(
        int(trade_date[:4]),
        int(trade_date[5:7]),
        int(trade_date[8:10]),
        hour,
        minute,
        tzinfo=timezone.utc,
    )
    feats: dict = {"ltp": 100.0}
    if underlying:
        feats["underlying"] = underlying
    store.insert_feature_logs(
        [
            {
                "timestamp": ts.isoformat(),
                "trade_date": trade_date,
                "symbol": symbol,
                "track": track,
                "features": feats,
                "source": "live",
                "feature_completeness": "live_full",
            }
        ]
    )


def test_public_view_never_permits_probability_before_60() -> None:
    class _S:
        class maturity_gate:
            suppress_below_days = 10
            provisional_below_days = 60

    settings = _S()  # type: ignore[assignment]
    for days, tier, permitted in (
        (0, "suppressed", False),
        (2, "suppressed", False),
        (9, "suppressed", False),
        (10, "provisional", False),
        (59, "provisional", False),
        (60, "full", True),
    ):
        view = maturity_public_view(days, settings, class_key="equity")  # type: ignore[arg-type]
        assert view["tier"] == tier
        assert view["probability_permitted"] is permitted
        assert view["threshold_days"] == 60
        if days < 60:
            assert view["display"] == f"insufficient data, {days}/60 pooled days"
            assert "0.62" not in view["display"]
        else:
            assert view["display"] == "full — 60/60 pooled days"
    no_model = maturity_public_view(
        60, settings, class_key="equity", has_model=False  # type: ignore[arg-type]
    )
    assert no_model["probability_permitted"] is False
    assert no_model["reason"] == "no_harness_passed_model"


def test_signal_public_view_nulls_probability_when_not_permitted() -> None:
    maturity = {
        "tier": "suppressed",
        "display": "insufficient data, 2/60 pooled days",
        "probability_permitted": False,
    }
    public = signal_public_view(
        {
            "symbol": "RELIANCE",
            "probability": 0.62,
            "score": 0.24,
            "track": "equity_depth",
        },
        maturity,
    )
    assert public["probability"] is None
    assert public["score"] is None
    assert public["display"] == "insufficient data, 2/60 pooled days"


def test_pooled_live_days_sql_skips_pilot_and_short_slice(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _insert_live(
        store,
        trade_date="2026-08-13",
        symbol="RELIANCE",
        track="equity_depth",
        hour=6,
        minute=30,
    )
    _insert_live(
        store,
        trade_date="2026-08-13",
        symbol="RELIANCE",
        track="equity_depth",
        hour=6,
        minute=40,
    )
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="RELIANCE",
        track="equity_depth",
        hour=3,
        minute=45,
    )
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="RELIANCE",
        track="equity_depth",
        hour=10,
        minute=0,
    )
    _insert_live(
        store,
        trade_date="2026-09-02",
        symbol="RELIANCE",
        track="equity_depth",
        hour=3,
        minute=45,
    )
    _insert_live(
        store,
        trade_date="2026-09-02",
        symbol="RELIANCE",
        track="equity_depth",
        hour=10,
        minute=0,
    )
    days = pooled_live_days(settings, class_key="equity")
    assert days == 2


def test_options_nifty_and_banknifty_counted_separately(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="NIFTY26SEP25000CE",
        track="options",
        underlying="NIFTY",
    )
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="BANKNIFTY26SEP55000CE",
        track="options",
        underlying="BANKNIFTY",
    )
    _insert_live(
        store,
        trade_date="2026-09-02",
        symbol="NIFTY26SEP25000CE",
        track="options",
        underlying="NIFTY",
    )
    assert pooled_live_days(settings, class_key="options", underlying="NIFTY") == 2
    assert pooled_live_days(settings, class_key="options", underlying="BANKNIFTY") == 1


def test_engine_persists_suppressed_rows_with_null_probability(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="RELIANCE",
        track="equity_depth",
        hour=3,
        minute=45,
    )
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="RELIANCE",
        track="equity_depth",
        hour=10,
        minute=0,
    )
    _insert_live(
        store,
        trade_date="2026-09-02",
        symbol="RELIANCE",
        track="equity_depth",
        hour=3,
        minute=45,
    )
    _insert_live(
        store,
        trade_date="2026-09-02",
        symbol="RELIANCE",
        track="equity_depth",
        hour=10,
        minute=0,
    )
    engine = LiveSignalEngine(settings)
    rows = store.fetch_feature_logs_range("2026-09-01", "2026-09-02")
    result = engine.score_and_log(rows)
    assert result["scored"] == 4
    assert result["suppressed"] == 4
    logged = store.fetch_latest_signal_logs(limit=10)
    assert len(logged) == 4
    for row in logged:
        assert row["probability"] is None
        assert row["score"] is None
        assert row["maturity_tier"] == "suppressed"
        assert row["attribution"]["display"] == "insufficient data, 2/60 pooled days"
        public = engine.public_row({**row, "maturity": row["attribution"]["maturity"]})
        assert public["probability"] is None
        assert "insufficient data" in public["display"]


def test_maturity_snapshot_shape(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    snap = maturity_snapshot(
        settings, has_model={"equity": False, "options": False, "futures": False}
    )
    assert snap["equity"]["display"] == "insufficient data, 0/60 pooled days"
    assert snap["equity"]["probability_permitted"] is False
    assert snap["options_nifty"]["tier"] == "suppressed"
    assert snap["options_banknifty"]["tier"] == "suppressed"
    assert classify_tier(2, settings) == "suppressed"
