"""Deterministic Run 2 analytics tests. No live Kite."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.market.activity import (
    classify_by_percentile,
    classify_ratio,
    trade_notional,
    trade_size_lots,
)
from nse_pipeline.market.flow import aggressive_side_proxy, displayed_depth_changes, liquidity_events
from nse_pipeline.market.futures_analytics import (
    basis,
    basis_freshness,
    basis_pct,
    futures_snapshot,
    price_oi_interpretation,
)
from nse_pipeline.market.options_analytics import build_option_chain, chain_coverage, max_pain_strike, pcr
from nse_pipeline.market.quality import dedupe_persisted_observations
from nse_pipeline.market.universe import (
    allocate_stock_option_slots,
    atm_strike,
    collect_stock_option_slots,
    nifty100_fo_eligible,
    stock_option_subscribe_mode,
)
from nse_pipeline.broker.instruments import build_stock_futures
from nse_pipeline.market.unusual import unusual_activity_score
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings


def test_atm_nearest_listed() -> None:
    assert atm_strike(23940, [23900, 23950, 24000], interval=50) == 23950
    assert atm_strike(None, [100]) is None
    assert atm_strike(100, []) is None


def test_pcr_and_zero_denominator() -> None:
    assert pcr(200, 100) == 2.0
    assert pcr(10, 0) is None
    assert pcr(0, 10) == 0.0
    assert pcr(None, 10) is None


def test_max_pain_insufficient_chain() -> None:
    tiny = max_pain_strike(
        [{"strike": 100, "ce": {"oi": 1}, "pe": {"oi": 1}}],
        min_strikes=11,
        min_completeness=0.7,
    )
    assert tiny["max_pain_strike"] is None
    assert tiny["status"] == "insufficient_chain"


def test_max_pain_complete_chain() -> None:
    rows = []
    for strike in range(90, 120, 5):
        rows.append(
            {
                "strike": float(strike),
                "ce": {"oi": 10 if strike >= 105 else 2},
                "pe": {"oi": 10 if strike <= 100 else 2},
            }
        )
    out = max_pain_strike(rows, min_strikes=5, min_completeness=0.5)
    assert out["status"] == "ok"
    assert out["max_pain_strike"] is not None


def test_option_chain_atm_oi_pcr_multistrike() -> None:
    contracts = []
    quotes = {}
    for strike, ce_oi, pe_oi in ((100, 10, 20), (110, 40, 5), (120, 8, 12)):
        ce_sym = f"X{strike}CE"
        pe_sym = f"X{strike}PE"
        contracts.append(
            {
                "tradingsymbol": ce_sym,
                "instrument_type": "CE",
                "strike": strike,
                "lot_size": 50,
                "tick_size": 0.05,
            }
        )
        contracts.append(
            {
                "tradingsymbol": pe_sym,
                "instrument_type": "PE",
                "strike": strike,
                "lot_size": 50,
                "tick_size": 0.05,
            }
        )
        quotes[ce_sym] = {
            "symbol": ce_sym,
            "last_price": 12,
            "oi": ce_oi,
            "oi_delta": 1,
            "volume": 100,
            "volume_delta": 5,
            "bid_depth_5": 10,
            "ask_depth_5": 8,
            "depth_imbalance": 0.1,
        }
        quotes[pe_sym] = {
            "symbol": pe_sym,
            "last_price": 11,
            "oi": pe_oi,
            "oi_delta": -2,
            "volume": 80,
            "volume_delta": 4,
            "bid_depth_5": 7,
            "ask_depth_5": 9,
            "depth_imbalance": -0.1,
        }
    chain = build_option_chain(
        underlying="NIFTY",
        expiry="2026-09-08",
        contracts=contracts,
        quotes_by_symbol=quotes,
        spot=110,
        interval=10,
        atm_method="nearest_listed",
        pcr_window=1,
        max_pain_min_strikes=3,
        max_pain_min_completeness=0.5,
    )
    assert chain["atm"] == 110
    assert chain["pcr_oi"] == (20 + 5 + 12) / (10 + 40 + 8)
    assert chain["highest_ce_oi"]["strike"] == 110
    assert chain["chain_completeness"] == 1.0
    assert chain["complete"] is True
    assert chain["chain_status"] == "complete"
    assert len(chain["multi_strike"]) >= 1
    assert chain["max_pain"]["status"] == "ok"


def test_partial_option_chain_completeness() -> None:
    chain = build_option_chain(
        underlying="NIFTY",
        expiry="2026-09-08",
        contracts=[
            {"tradingsymbol": "A", "instrument_type": "CE", "strike": 100},
            {"tradingsymbol": "B", "instrument_type": "PE", "strike": 110},
        ],
        quotes_by_symbol={"A": {"symbol": "A", "oi": 1, "last_price": 2}},
        spot=100,
        interval=10,
        atm_method="nearest_listed",
        pcr_window=5,
        max_pain_min_strikes=11,
        max_pain_min_completeness=0.9,
    )
    assert chain["number_of_strikes"] == 2
    assert chain["max_pain"]["max_pain_strike"] is None


def test_futures_basis_and_price_oi() -> None:
    assert basis(100, 98) == 2
    assert basis_pct(100, 80) == 25
    assert basis(None, 100) is None
    assert basis(100, None) is None
    assert price_oi_interpretation(1, 1)["label"] == "long_buildup"
    assert price_oi_interpretation(1, -1)["label"] == "short_covering"
    assert price_oi_interpretation(-1, 1)["label"] == "short_buildup"
    assert price_oi_interpretation(-1, -1)["label"] == "long_unwinding"
    assert price_oi_interpretation(None, 1)["label"] is None


def test_trade_notional_lots_and_percentiles() -> None:
    assert trade_notional(100, 2) == 200
    assert trade_size_lots(150, 75) == 2
    assert trade_size_lots(10, None) is None
    assert classify_by_percentile(None, large=95, very_large=99, extreme=99.9, baseline_available=False) is None
    assert classify_by_percentile(99.5, large=95, very_large=99, extreme=99.9, baseline_available=True) == "very_large"
    assert classify_ratio(4.1, elevated=2, burst=3, extreme=5, baseline_available=True) == "burst"
    assert classify_ratio(4.1, elevated=2, burst=3, extreme=5, baseline_available=False) is None


def test_aggressive_proxy_and_depth_changes() -> None:
    buy = aggressive_side_proxy(101, 99, 100, tolerance_bps=2)
    sell = aggressive_side_proxy(99, 99, 101, tolerance_bps=2)
    mid = aggressive_side_proxy(100, 99, 101, tolerance_bps=0)
    assert buy["label"] == "aggressive_buy_proxy"
    assert sell["label"] == "aggressive_sell_proxy"
    assert mid["label"] == "unknown"
    assert "executed" not in buy["note"].lower()
    prev = {"bid_depth_5": 100, "ask_depth_5": 100, "spread": 1, "depth_imbalance": 0, "best_bid": 10, "best_ask": 11}
    now = {"bid_depth_5": 140, "ask_depth_5": 40, "spread": 2, "depth_imbalance": 0.5, "best_bid": 10.5, "best_ask": 11}
    ch = displayed_depth_changes(now, prev)
    assert ch["bid_quantity_added"] == 40
    assert ch["ask_quantity_removed"] == 60
    events = liquidity_events(ch, depth_shock_pct=40, spread_widen_pct=50, previous=prev)
    assert "ask_depth_shock" in events
    assert "spread_widening" in events


def test_unusual_score_is_deterministic_not_hft() -> None:
    a = unusual_activity_score(
        large_trade="very_large",
        trade_notional_percentile=99,
        volume_level="burst",
        volume_ratio=4.1,
        activity_level="elevated",
        oi_delta=10,
        liquidity_events=["ask_depth_shock"],
        aggressor="aggressive_buy_proxy",
        high_score=70,
    )
    b = unusual_activity_score(
        large_trade="very_large",
        trade_notional_percentile=99,
        volume_level="burst",
        volume_ratio=4.1,
        activity_level="elevated",
        oi_delta=10,
        liquidity_events=["ask_depth_shock"],
        aggressor="aggressive_buy_proxy",
        high_score=70,
    )
    assert a == b
    assert a["activity_score"] >= 70
    assert "HFT" not in a["note"]
    assert any("4.1x" in r for r in a["reasons"])


def test_dedupe_keeps_legitimate_repeats() -> None:
    a = {
        "instrument_token": 1,
        "timestamp": "t1",
        "last_price": 10,
        "last_quantity": 5,
        "volume": 100,
        "oi": 1,
        "bid_prices": [9],
        "bid_quantities": [1],
        "ask_prices": [11],
        "ask_quantities": [1],
    }
    same = dict(a)
    depth_change = {**a, "bid_quantities": [9]}
    kept = dedupe_persisted_observations([a, same, depth_change])
    assert len(kept) == 2
    assert kept[1]["bid_quantities"] == [9]


def test_dedupe_collapses_consecutive_reflush_rows() -> None:
    row = {
        "instrument_token": 7,
        "timestamp": "2026-09-04T03:45:01+00:00",
        "last_price": 10.0,
        "last_quantity": 1,
        "volume": 50,
        "oi": 3,
        "bid_prices": [9.5],
        "bid_quantities": [2],
        "ask_prices": [10.5],
        "ask_quantities": [2],
    }
    kept = dedupe_persisted_observations([row, dict(row), dict(row)])
    assert len(kept) == 1


def test_dedupe_preserves_identical_payload_at_different_timestamps() -> None:
    first = {
        "instrument_token": 7,
        "timestamp": "2026-09-04T03:45:01+00:00",
        "last_price": 10.0,
        "last_quantity": 1,
        "volume": 50,
        "oi": 3,
        "bid_prices": [9.5],
        "bid_quantities": [2],
        "ask_prices": [10.5],
        "ask_quantities": [2],
    }
    later = {**first, "timestamp": "2026-09-04T03:45:02+00:00"}
    kept = dedupe_persisted_observations([first, later])
    assert len(kept) == 2


def test_dedupe_does_not_globally_collapse_nonadjacent_fingerprints() -> None:
    row = {
        "instrument_token": 7,
        "timestamp": "2026-09-04T03:45:01+00:00",
        "last_price": 10.0,
        "last_quantity": 1,
        "volume": 50,
        "oi": 3,
        "bid_prices": [9.5],
        "bid_quantities": [2],
        "ask_prices": [10.5],
        "ask_quantities": [2],
    }
    other = {**row, "volume": 51}
    kept = dedupe_persisted_observations([row, other, dict(row)])
    assert len(kept) == 3


def test_nifty100_fo_intersection() -> None:
    assert nifty100_fo_eligible(["RELIANCE", "XYZ"], {"RELIANCE", "NIFTY"}) == ["RELIANCE"]


def test_futures_basis_freshness_tolerance() -> None:
    spot_ts = "2026-09-04T03:45:01.120000+00:00"
    one_second = "2026-09-04T03:45:01.880000+00:00"
    within = "2026-09-04T03:45:10.120000+00:00"
    outside = "2026-09-04T03:45:12.120000+00:00"

    one = basis_freshness(
        future_price=101.0,
        spot_price=100.0,
        futures_as_of=one_second,
        spot_as_of=spot_ts,
        max_age_seconds=10,
    )
    assert one["basis"] == 1.0
    assert one["basis_status"] == "fresh"

    snap = futures_snapshot(
        {"last_price": 101.0, "timestamp": within},
        meta={"tradingsymbol": "NIFTY26SEPFUT", "name": "NIFTY"},
        spot=100.0,
        spot_as_of=spot_ts,
        spot_symbol="NIFTY 50",
        max_age_seconds=10,
    )
    assert snap["basis"] == 1.0
    assert snap["basis_status"] == "fresh"
    assert snap["spot_as_of"] == spot_ts
    assert snap["futures_as_of"] == within
    assert snap["data_age_seconds"] == 9.0

    stale = futures_snapshot(
        {"last_price": 101.0, "timestamp": outside},
        meta={"name": "NIFTY"},
        spot=100.0,
        spot_as_of=spot_ts,
        spot_symbol="NIFTY 50",
        max_age_seconds=10,
    )
    assert stale["basis"] is None
    assert stale["basis_status"] == "stale"

    missing_spot = futures_snapshot(
        {"last_price": 101.0, "timestamp": one_second},
        meta={"name": "NIFTY"},
        spot=None,
        spot_as_of=None,
        spot_symbol="NIFTY 50",
    )
    assert missing_spot["basis"] is None
    assert missing_spot["basis_status"] == "missing"

    missing_fut = futures_snapshot(
        {"timestamp": one_second},
        meta={"name": "NIFTY"},
        spot=100.0,
        spot_as_of=spot_ts,
        spot_symbol="NIFTY 50",
    )
    assert missing_fut["basis"] is None
    assert missing_fut["basis_status"] == "missing"
    assert missing_fut["ltp"] is None


def _option_row(name: str, strike: float, kind: str, token: int, expiry: date) -> dict:
    return {
        "instrument_token": token,
        "tradingsymbol": f"{name}{int(strike)}{kind}",
        "exchange": "NFO",
        "name": name,
        "segment": "NFO-OPT",
        "instrument_type": kind,
        "strike": strike,
        "expiry": expiry.isoformat(),
        "_expiry_date": expiry,
        "lot_size": 1,
        "tick_size": 0.05,
    }


def _windowed_stock_option_universe():
    expiry = date(2026, 9, 24)
    names = ["AAA", "BBB", "CCC"]
    by_name: dict[str, list[dict]] = {}
    token = 1000
    for name in names:
        rows = []
        for strike in (50.0, 90.0, 100.0, 110.0, 200.0):
            for kind in ("CE", "PE"):
                rows.append(_option_row(name, strike, kind, token, expiry))
                token += 1
        by_name[name] = rows
    spots = {name: 100.0 for name in names}
    slots, listed, uncovered = collect_stock_option_slots(
        names,
        by_name,
        spots,
        today=date(2026, 9, 4),
        expiry_count=1,
        strikes_each_side=1,
    )
    return slots, listed, uncovered, names, expiry


def test_stock_option_breadth_first_atm_full_and_caps() -> None:
    slots, listed, uncovered, names, expiry = _windowed_stock_option_universe()
    assert uncovered == []
    assert listed == names
    assert all(s.strike in {90.0, 100.0, 110.0} for s in slots)
    assert not any(s.strike in {50.0, 200.0} for s in slots)

    selected, truncated = allocate_stock_option_slots(slots, max_contracts=6)
    assert truncated is True
    assert len(selected) == 6
    covered = {(s.underlying, s.option_type, s.distance_from_atm) for s in selected}
    for name in names:
        assert (name, "CE", 0) in covered
        assert (name, "PE", 0) in covered
        assert stock_option_subscribe_mode(0, atm_mode="full", wing_mode="quote") == "full"

    for slot in selected:
        assert slot.distance_from_atm == 0
        assert (
            stock_option_subscribe_mode(
                slot.distance_from_atm, atm_mode="full", wing_mode="quote"
            )
            == "full"
        )

    selected_all, truncated_all = allocate_stock_option_slots(slots, max_contracts=600)
    assert truncated_all is False
    assert len(selected_all) == len(slots)
    assert len(selected_all) <= 600
    wing_modes = {
        stock_option_subscribe_mode(s.distance_from_atm, atm_mode="full", wing_mode="quote")
        for s in selected_all
        if s.distance_from_atm > 0
    }
    assert wing_modes == {"quote"}
    atm_modes = {
        stock_option_subscribe_mode(s.distance_from_atm, atm_mode="full", wing_mode="quote")
        for s in selected_all
        if s.distance_from_atm == 0
    }
    assert atm_modes == {"full"}

    capped, truncated_cap = allocate_stock_option_slots(slots, max_contracts=4)
    assert truncated_cap is True
    assert len(capped) == 4
    assert {s.underlying for s in capped} == {"AAA", "BBB"}
    assert not any(s.underlying == "CCC" for s in capped)


def test_stock_futures_remain_full(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    expiry = date(2026, 9, 24)
    nfo = [
        {
            "instrument_token": 55,
            "tradingsymbol": "AAA26SEPFUT",
            "exchange": "NFO",
            "name": "AAA",
            "segment": "NFO-FUT",
            "instrument_type": "FUT",
            "strike": 0,
            "expiry": expiry.isoformat(),
            "lot_size": 1,
            "tick_size": 0.05,
        }
    ]
    futures = build_stock_futures(settings, ["AAA", "ZZZ"], nfo, token_budget=10)
    assert len(futures) == 1
    assert futures[0].subscribe_mode == "full"
    assert futures[0].tradingsymbol == "AAA26SEPFUT"


def test_chain_coverage_semantics() -> None:
    complete = chain_coverage(
        eligible_contract_count=6, selected_contract_count=6, truncated=False
    )
    assert complete["chain_status"] == "complete"
    assert complete["complete"] is True
    assert complete["chain_completeness"] == 1.0

    partial = chain_coverage(
        eligible_contract_count=6, selected_contract_count=4, truncated=False
    )
    assert partial["chain_status"] == "partial"
    assert partial["complete"] is False
    assert partial["missing_contract_count"] == 2

    truncated = chain_coverage(
        eligible_contract_count=6, selected_contract_count=4, truncated=True
    )
    assert truncated["chain_status"] == "truncated"
    assert truncated["complete"] is False

    empty = chain_coverage(
        eligible_contract_count=0, selected_contract_count=0, truncated=False
    )
    assert empty["chain_status"] == "empty"
    assert empty["complete"] is False

    chain = build_option_chain(
        underlying="AAA",
        expiry="2026-09-24",
        contracts=[
            {"tradingsymbol": "A", "instrument_type": "CE", "strike": 100},
            {"tradingsymbol": "B", "instrument_type": "PE", "strike": 100},
        ],
        quotes_by_symbol={"A": {"symbol": "A", "oi": 1, "last_price": 2}},
        spot=100,
        interval=10,
        atm_method="nearest_listed",
        pcr_window=5,
        max_pain_min_strikes=11,
        max_pain_min_completeness=0.9,
        eligible_contract_count=12,
        selected_contract_count=2,
        truncated=True,
    )
    assert chain["complete"] is False
    assert chain["chain_status"] == "truncated"
    assert chain["eligible_contract_count"] == 12
    assert chain["selected_contract_count"] == 2
    assert chain["chain_completeness"] < 1.0


def test_analytics_api_nulls_and_envelope(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    settings.paths.instruments_cache.write_text(
        json.dumps(
            {
                "updated_at": "2026-09-04T00:00:00Z",
                "index": {"NIFTY 50": {"tradingsymbol": "NIFTY 50", "name": "NIFTY"}},
                "options": [
                    {
                        "tradingsymbol": "NIFTY26908100CE",
                        "name": "NIFTY",
                        "instrument_type": "CE",
                        "strike": 100,
                        "expiry": "2026-09-08",
                        "lot_size": 65,
                        "tick_size": 0.05,
                    }
                ],
                "futures": [
                    {
                        "tradingsymbol": "NIFTY26SEPFUT",
                        "name": "NIFTY",
                        "instrument_type": "FUT",
                        "expiry": "2026-09-29",
                        "lot_size": 65,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    client = TestClient(create_app(settings))
    missing = client.get("/api/v1/quotes/NIFTY 50").json()
    assert missing["found"] is False
    assert missing["quote"] is None
    chain = client.get("/api/v1/options/NIFTY").json()
    by_exp = client.get("/api/v1/options/NIFTY/2026-09-08").json()
    oi = client.get("/api/v1/options/NIFTY/2026-09-08/oi").json()
    activity = client.get("/api/v1/options/NIFTY/2026-09-08/activity").json()
    assert "maturity" in chain and "as_of" in chain
    assert "maturity" in by_exp and "as_of" in by_exp
    assert by_exp["chain"]["pcr_oi"] is None
    assert by_exp["chain"]["max_pain"]["max_pain_strike"] is None
    assert by_exp["chain"]["chain_status"] in {"complete", "partial", "truncated", "empty"}
    assert "chain_status" in oi["oi"]
    assert "maturity" in oi and "as_of" in oi
    assert "maturity" in activity and "as_of" in activity
    fut = client.get("/api/v1/futures/NIFTY").json()
    assert "maturity" in fut and "as_of" in fut
    assert fut["futures"]["contracts"][0]["basis"] is None
    assert fut["futures"]["contracts"][0]["basis_status"] == "missing"
    assert "spot_as_of" in fut["futures"]["contracts"][0]
    assert "futures_as_of" in fut["futures"]["contracts"][0]
    assert "data_age_seconds" in fut["futures"]["contracts"][0]
    act = client.get("/api/v1/market-activity/NIFTY 50").json()
    assert act["found"] is False
    assert "maturity" in act
    unusual = client.get("/api/v1/unusual-activity").json()
    assert unusual["unusual_activity"] == []
    assert "maturity" in unusual
    charts = client.get("/api/v1/charts/NIFTY 50").json()
    assert charts["chart"]["points"] == []
    assert "maturity" in charts
    cross = client.get("/api/v1/cross-market/NIFTY").json()
    assert "maturity" in cross and "as_of" in cross
    watch = client.get("/api/v1/watchlists").json()
    assert watch["watchlists"]
    assert client.post("/api/v1/options/NIFTY").status_code == 405
    blob = str(chain) + str(fut) + str(unusual) + str(oi) + str(cross)
    assert "features_json" not in blob
    assert "HFT" not in blob
