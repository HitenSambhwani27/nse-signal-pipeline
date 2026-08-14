"""Tests for Nifty weekly vs monthly option symbol classification."""

from nse_pipeline.broker.option_series import (
    classify_nifty_option_symbol,
    filter_rows_by_series,
)


def test_classify_weekly_and_monthly_symbols() -> None:
    assert classify_nifty_option_symbol("NIFTY25AUG1924500CE") == "weekly"
    assert classify_nifty_option_symbol("NIFTY25AUG24500PE") == "monthly"
    assert classify_nifty_option_symbol("BANKNIFTY25AUG55000CE") is None


def test_nifty_weekly_filter_drops_monthly() -> None:
    rows = [
        {"tradingsymbol": "NIFTY25AUG1924500CE", "name": "NIFTY"},
        {"tradingsymbol": "NIFTY25AUG24500CE", "name": "NIFTY"},
    ]
    kept, note = filter_rows_by_series(rows, underlying="NIFTY", series="weekly")
    assert note == "nifty_weekly"
    assert [r["tradingsymbol"] for r in kept] == ["NIFTY25AUG1924500CE"]


def test_banknifty_unfiltered() -> None:
    rows = [{"tradingsymbol": "BANKNIFTY25AUG55000CE", "name": "BANKNIFTY"}]
    kept, note = filter_rows_by_series(rows, underlying="BANKNIFTY", series="monthly")
    assert note == "banknifty_monthly_only"
    assert kept == rows


def test_weekly_coincide_fallback_when_no_weekly_pattern() -> None:
    rows = [{"tradingsymbol": "NIFTY25AUG24500CE", "name": "NIFTY"}]
    kept, note = filter_rows_by_series(rows, underlying="NIFTY", series="weekly")
    assert note == "nifty_weekly_monthly_coincide_fallback"
    assert kept == rows
