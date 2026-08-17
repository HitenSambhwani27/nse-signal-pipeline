"""Historical chunking, writer idempotency, and estimate math."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from nse_pipeline.historical.chunks import chunk_range
from nse_pipeline.historical.fetch import (
    HistoricalAuthError,
    HistoricalRateLimiter,
    _is_auth_error,
    _is_timeout,
    _is_transient,
    attempt_backoff,
    attempt_timeout,
    fetch_candles_with_retry,
)
from nse_pipeline.historical.writer import (
    SOURCE_HISTORICAL,
    SOURCE_LIVE,
    peek_ticks_source,
    write_historical_partition,
)
from nse_pipeline.storage.schemas import CandleRecord


def test_chunk_range_respects_cap() -> None:
    chunks = chunk_range(date(2022, 1, 1), date(2022, 4, 30), max_days=60)
    assert chunks[0] == (date(2022, 1, 1), date(2022, 3, 1))
    assert all((b - a).days + 1 <= 60 for a, b in chunks)
    assert chunks[-1][1] == date(2022, 4, 30)


def _candle(ts: datetime, close: float = 100.0) -> CandleRecord:
    return CandleRecord(
        timestamp=ts,
        instrument_token=1,
        symbol="RELIANCE",
        exchange="NSE",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=10,
        oi=None,
    )


def test_historical_writer_tags_source_and_is_idempotent(tmp_path: Path) -> None:
    ts = datetime(2022, 1, 3, 4, 0, tzinfo=timezone.utc)
    candles = [_candle(ts), _candle(ts.replace(minute=1), 101.0)]
    stats = write_historical_partition(tmp_path, candles, interval="minute")
    assert stats["rows"] == 2
    path = tmp_path / "2022-01-03" / "RELIANCE" / "ticks.parquet"
    df = pd.read_parquet(path)
    assert set(df["source"].unique()) == {SOURCE_HISTORICAL}
    stats2 = write_historical_partition(tmp_path, candles, interval="minute")
    assert stats2["rows"] == 2
    assert len(pd.read_parquet(path)) == 2


def test_historical_writer_does_not_overwrite_live(tmp_path: Path) -> None:
    day = tmp_path / "2022-01-03" / "RELIANCE"
    day.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": [datetime(2022, 1, 3, 4, 0, tzinfo=timezone.utc)],
            "instrument_token": [1],
            "symbol": ["RELIANCE"],
            "last_price": [100.0],
            "source": [SOURCE_LIVE],
        }
    ).to_parquet(day / "ticks.parquet", index=False)
    candles = [_candle(datetime(2022, 1, 3, 4, 0, tzinfo=timezone.utc))]
    stats = write_historical_partition(tmp_path, candles, interval="minute")
    assert stats["skipped_live"] == 1
    assert peek_ticks_source(day / "ticks.parquet") == SOURCE_LIVE
    assert (day / "candles.parquet").exists()


def test_timeout_and_backoff_increase_across_attempts() -> None:
    from nse_pipeline.config import HistoricalSettings

    hist = HistoricalSettings(
        interval="minute",
        lookback_days=1,
        http_timeout_seconds=45.0,
        http_timeout_ceiling_seconds=90.0,
        retry_backoff_seconds=2.0,
        retry_backoff_multiplier=2.0,
    )
    assert attempt_timeout(hist, 1) == 45.0
    assert attempt_timeout(hist, 2) == 67.5
    assert attempt_timeout(hist, 3) == 90.0
    assert attempt_timeout(hist, 5) == 90.0
    assert attempt_backoff(hist, 1) == 2.0
    assert attempt_backoff(hist, 2) == 4.0
    assert attempt_backoff(hist, 3) == 8.0
    assert _is_timeout(TimeoutError("Read timed out. (read timeout=7)"))
    assert _is_transient(TimeoutError("Read timed out"))
    assert not _is_timeout(ValueError("bad token"))


def test_parse_warning_windows_dedupes() -> None:
    from nse_pipeline.historical.coverage_audit import parse_warning_windows

    log = (
        "historical_data failed IOC minute 2026-06-11→2026-08-09: timed out\n"
        "historical_data failed IOC minute 2026-06-11→2026-08-09: timed out\n"
        "historical_data failed M&M minute 2026-02-11→2026-04-11: timed out\n"
        "historical_data failed HINDZINC minute 2026-04-12\\u21922026-06-10: timed out\n"
    )
    rows = parse_warning_windows(log)
    assert len(rows) == 3
    assert rows[0]["symbol"] == "IOC"
    assert rows[0]["start"] == "2026-06-11"
    assert rows[1]["symbol"] == "M&M"
    assert rows[2]["symbol"] == "HINDZINC"
    assert rows[2]["start"] == "2026-04-12"


class _TokenException(Exception):
    def __init__(self, message: str = "Incorrect `api_key` or `access_token`.") -> None:
        super().__init__(message)
        self.code = 403


def test_auth_errors_are_not_retryable() -> None:
    err = _TokenException()
    assert _is_auth_error(err)
    assert not _is_transient(err)
    assert not _is_auth_error(TimeoutError("Read timed out. (read timeout=7)"))
    assert not _is_auth_error(ConnectionError("HTTPSConnectionPool read timed out"))


def test_fetch_auth_failure_raises_without_retry() -> None:
    from nse_pipeline.config import HistoricalSettings
    from nse_pipeline.storage.schemas import InstrumentInfo

    calls = {"n": 0}

    class FakeKite:
        timeout = 7.0

        def historical_data(self, **kwargs):
            calls["n"] += 1
            raise _TokenException()

    hist = HistoricalSettings(
        interval="minute",
        lookback_days=1,
        retry_max=5,
        http_timeout_seconds=45.0,
        http_timeout_ceiling_seconds=90.0,
        retry_backoff_seconds=0.0,
        retry_backoff_multiplier=2.0,
        rate_limit_per_second=100.0,
    )
    instrument = InstrumentInfo(
        instrument_token=1,
        tradingsymbol="PAYTM",
        exchange="NSE",
        name="PAYTM",
        segment="NSE",
        instrument_type="EQ",
    )
    try:
        fetch_candles_with_retry(
            FakeKite(),
            instrument,
            date(2026, 6, 11),
            date(2026, 8, 9),
            "minute",
            hist,
            HistoricalRateLimiter(100.0),
        )
        raise AssertionError("expected HistoricalAuthError")
    except HistoricalAuthError as exc:
        assert "PAYTM" in str(exc)
    assert calls["n"] == 1


def test_skip_completed_keys_ignore_probe_and_keep_full_execute(tmp_path: Path) -> None:
    from nse_pipeline.historical.backfill import load_completed_backfill_keys
    from nse_pipeline.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "test.db")
    store.log_ingestion_event(
        event_type="historical_backfill_start",
        message="probe",
        details={"counts": {"equity_depth": 2, "index": 1}},
    )
    store.log_ingestion_event(
        event_type="historical_backfill_symbol",
        message="symbol complete",
        symbol="NIFTY 50",
        details={"class": "index", "symbol": "NIFTY 50", "expiry": None},
    )
    store.log_ingestion_event(
        event_type="historical_backfill_start",
        message="full",
        details={"counts": {"equity_depth": 100, "equity_quote": 400, "index": 2}},
    )
    store.log_ingestion_event(
        event_type="historical_backfill_symbol",
        message="symbol complete",
        symbol="PAYTM",
        details={"class": "equity_quote", "symbol": "PAYTM", "expiry": None},
    )
    keys = load_completed_backfill_keys(store)
    assert ("equity_quote", "PAYTM", "") in keys
    assert ("index", "NIFTY 50", "") not in keys
