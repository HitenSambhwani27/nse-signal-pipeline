"""Tests for NSE membership quote-only set = Nifty500 \\ Nifty100."""

from __future__ import annotations

from nse_pipeline.broker.nse_membership import _parse_symbols


def test_parse_symbols_and_quote_only_math() -> None:
    csv100 = "Company Name,Industry,Symbol,Series,ISIN Code\nA,X,AAA,EQ,1\nB,Y,BBB,EQ,2\n"
    csv500 = (
        "Company Name,Industry,Symbol,Series,ISIN Code\n"
        "A,X,AAA,EQ,1\nB,Y,BBB,EQ,2\nC,Z,CCC,EQ,3\nD,Z,DDD,EQ,4\n"
    )
    nifty100 = _parse_symbols(csv100)
    nifty500 = _parse_symbols(csv500)
    quote_only = [s for s in nifty500 if s not in set(nifty100)]
    assert nifty100 == ["AAA", "BBB"]
    assert quote_only == ["CCC", "DDD"]
    assert set(quote_only).isdisjoint(set(nifty100))
