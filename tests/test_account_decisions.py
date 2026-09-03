"""Stage 7 account capture + Stage 8 linkage + Stage 9 quality on synthetic data."""

from __future__ import annotations

from pathlib import Path

from nse_pipeline.account.capture import capture_once
from nse_pipeline.decisions.link import link_fill, log_decision, match_unlinked_fills
from nse_pipeline.quality.review import (
    CLASS_BG,
    CLASS_GB,
    CLASS_GG,
    CLASS_INSUFFICIENT,
    behavior_rollup,
    classify_2x2,
    score_closed_trade,
)
from nse_pipeline.signals.maturity import maturity_public_view
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings


class FakeKite:
    def orders(self):
        return [
            {
                "order_id": "O1",
                "status": "COMPLETE",
                "tradingsymbol": "RELIANCE",
                "exchange": "NSE",
                "transaction_type": "BUY",
                "quantity": 1,
                "price": 100.0,
                "filled_quantity": 1,
                "average_price": 100.1,
                "order_timestamp": "2026-09-02 10:00:00",
            }
        ]

    def trades(self):
        return [
            {
                "trade_id": "KT1",
                "order_id": "O1",
                "tradingsymbol": "RELIANCE",
                "exchange": "NSE",
                "transaction_type": "BUY",
                "quantity": 1,
                "average_price": 100.1,
                "fill_timestamp": "2026-09-02 10:00:01",
            }
        ]

    def positions(self):
        return {"net": [{"tradingsymbol": "RELIANCE", "quantity": 1}], "day": []}

    def margins(self):
        return {"equity": {"available": {"live_balance": 100000}, "utilised": {"debits": 0}}}

    def holdings(self):
        return []

    def profile(self):
        return {"user_id": "TEST"}


def _view(settings, days: int, *, permitted: bool | None = None) -> dict:
    view = maturity_public_view(days, settings, class_key="equity", has_model=True)
    if permitted is False:
        view["probability_permitted"] = False
    return view


def test_account_capture_fake_kite(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    result = capture_once(
        store, FakeKite(), include_holdings=True, include_session_audit=True
    )
    assert result["order_events"] == 1
    assert result["trade_fills"] == 1
    # Independent of the gate — capture works at 0 live days.
    assert store.latest_json_row("positions_snapshot") is not None
    assert store.latest_json_row("margin_snapshot") is not None
    again = capture_once(store, FakeKite())
    assert again["order_events"] == 0
    assert again["trade_fills"] == 0


def test_system_decision_refused_when_gate_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    view = _view(settings, 2)
    assert view["probability_permitted"] is False
    try:
        log_decision(
            store,
            symbol="RELIANCE",
            actor="system",
            maturity_view=view,
            side="BUY",
        )
        raise AssertionError("system decision must be refused")
    except PermissionError as exc:
        assert "insufficient data" in str(exc)


def test_decision_fill_outcome_round_trip(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    view = _view(settings, 2)
    trade_id = log_decision(
        store,
        symbol="RELIANCE",
        actor="manual",
        maturity_view=view,
        side="BUY",
        suggested_qty=1,
        suggested_price=100.0,
        timestamp="2026-09-02T04:30:00+00:00",
        extras={"stated_probability": 0.7, "model_probability_at_entry": 0.7, "model_side": "BUY"},
    )
    store.insert_trade_fills(
        [
            {
                "trade_id": "KT-ENTRY",
                "tradingsymbol": "RELIANCE",
                "transaction_type": "BUY",
                "quantity": 1,
                "average_price": 100.2,
                "fill_timestamp": "2026-09-02T04:30:05+00:00",
            }
        ]
    )
    fills = store.fetch_fills(unlinked_only=True)
    assert len(fills) == 1
    link_fill(store, fill_pk=int(fills[0]["id"]), trade_id=trade_id)
    linked = store.fetch_fills_for_trade(trade_id)
    assert len(linked) == 1
    assert linked[0]["trade_id"] == trade_id
    assert json_div(linked[0])["price_diff"] is not None

    prices = {
        "2026-09-02T04:35:00+00:00": 101.0,
        "2026-09-02T04:45:00+00:00": 101.5,
        "2026-09-02T05:00:00+00:00": 102.0,
    }

    def price_at(ts):
        return {"price": 101.0, "iv": None, "oi": None}

    scored = score_closed_trade(store, trade_id=trade_id, price_at=price_at)
    assert scored["classification"] == CLASS_INSUFFICIENT
    assert scored["entry_quality"] is None
    assert scored["details"]["entry"]["display"].startswith("insufficient data")


def json_div(fill: dict) -> dict:
    import json

    raw = fill.get("fill_vs_decision_json")
    return json.loads(raw) if raw else {}


def test_fallback_symbol_side_match(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    view = _view(settings, 2)
    trade_id = log_decision(
        store,
        symbol="INFY",
        actor="manual",
        maturity_view=view,
        side="SELL",
        timestamp="2026-09-02T04:00:00+00:00",
    )
    store.insert_trade_fills(
        [
            {
                "trade_id": "KT-INFY",
                "tradingsymbol": "INFY",
                "transaction_type": "SELL",
                "quantity": 2,
                "average_price": 1500.0,
                "fill_timestamp": "2026-09-02T04:01:00+00:00",
            }
        ]
    )
    n = match_unlinked_fills(store)
    assert n == 1
    assert store.fetch_fills_for_trade(trade_id)


def test_quality_2x2_and_anti_hindsight(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    full = maturity_public_view(60, settings, class_key="equity", has_model=True)
    assert full["probability_permitted"] is True

    def _closed(trade_key: str, stated: float, model_p: float, exit_px: float) -> dict:
        tid = log_decision(
            store,
            symbol="RELIANCE",
            actor="system",
            maturity_view=full,
            side="BUY",
            suggested_qty=1,
            suggested_price=100.0,
            timestamp="2026-09-02T04:00:00+00:00",
            extras={
                "stated_probability": stated,
                "model_probability_at_entry": model_p,
                "model_side": "BUY",
            },
            trade_id=trade_key,
        )
        store.insert_trade_fills(
            [
                {
                    "trade_id": f"{trade_key}-e",
                    "linked_trade_id": tid,
                    "tradingsymbol": "RELIANCE",
                    "transaction_type": "BUY",
                    "quantity": 1,
                    "average_price": 100.0,
                    "fill_timestamp": "2026-09-02T04:00:01+00:00",
                },
                {
                    "trade_id": f"{trade_key}-x",
                    "linked_trade_id": tid,
                    "tradingsymbol": "RELIANCE",
                    "transaction_type": "SELL",
                    "quantity": 1,
                    "average_price": exit_px,
                    "fill_timestamp": "2026-09-02T04:20:00+00:00",
                },
            ]
        )
        def price_at(ts):
            # Post-exit continuation higher — would look like a "better" entry
            # in hindsight. Entry quality must ignore this.
            return {"price": 110.0}
        return score_closed_trade(store, trade_id=tid, price_at=price_at)

    good_good = _closed("gg", 0.70, 0.72, 105.0)
    good_bad = _closed("gb", 0.70, 0.71, 90.0)
    # Stated 0.95 vs model 0.55 is inconsistent even if the trade made money.
    bad_good = _closed("bg", 0.95, 0.55, 105.0)

    assert good_good["classification"] == CLASS_GG
    assert good_bad["classification"] == CLASS_GB
    assert bad_good["classification"] == CLASS_BG
    # Hindsight 110 price must not flip the good entry into a "should have waited" penalty.
    assert good_good["decision_good"] == 1
    assert classify_2x2(decision_good=True, outcome_good=False) == CLASS_GB
    roll = behavior_rollup(store.fetch_outcomes())
    assert roll["system"]["unlucky"] == 1
    assert roll["system"]["lucky"] == 1
