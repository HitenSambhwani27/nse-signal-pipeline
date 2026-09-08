"""Timestamp provenance: Kite naive IST vs UTC ingest receive time."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from nse_pipeline.market.data_state import DataState, observation_age_seconds, resolve
from nse_pipeline.market.freshness import DEGRADED, FRESH_LIVE, NO_DATA, describe_freshness
from nse_pipeline.market.normalize import normalize_tick
from nse_pipeline.market.observability import median_clock_skew_seconds
from nse_pipeline.market.timestamps import (
    AMBIGUOUS_UTC_HOUR,
    IST,
    REPAIR_NAIVE_IST_LABELED_UTC,
    canonical_observation,
    parse_kite_datetime,
)
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_market_foundation import EXCH, MAP, TOKEN, _full_tick

UTC = timezone.utc


def test_aware_timestamp_keeps_offset_and_positive_age() -> None:
    now = datetime(2026, 9, 7, 10, 32, 8, tzinfo=IST)
    ts = (now - timedelta(seconds=8)).isoformat()
    age = observation_age_seconds(ts, now=now)
    assert age == 8
    assert age >= 0
    fresh = describe_freshness(
        DataState(
            market_state="open",
            data_status="live",
            as_of=ts,
            session_date="2026-09-07",
            source="latest_quotes",
        ),
        {"timestamp": ts, "ingested_at": now.astimezone(UTC).isoformat()},
        now=now,
        max_age_seconds=120.0,
    )
    assert fresh["status"] == FRESH_LIVE
    assert fresh["freshness_age_seconds"] == 8
    assert fresh["ingested_at"] is not None
    assert fresh["observed_at"] == ts


def test_kite_naive_ist_is_localized_not_labeled_utc() -> None:
    naive = datetime(2026, 9, 7, 15, 53, 59)
    parsed = parse_kite_datetime(naive)
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(hours=5, minutes=30)
    assert parsed.astimezone(UTC) == datetime(2026, 9, 7, 10, 23, 59, tzinfo=UTC)
    ingested = datetime(2026, 9, 7, 10, 23, 59, 400000, tzinfo=UTC)
    tick = normalize_tick(
        _full_tick(
            timestamp=naive,
            exchange_timestamp=naive,
            last_trade_time=datetime(2026, 9, 7, 15, 53, 50),
        ),
        MAP,
        EXCH,
        ingested_at=ingested,
    )
    assert tick is not None
    assert tick.exchange_timestamp is not None
    assert tick.exchange_timestamp.utcoffset() == timedelta(hours=5, minutes=30)
    assert tick.ingested_at == ingested
    assert tick.timestamp.utcoffset() != tick.ingested_at.utcoffset()
    assert tick.timestamp != tick.ingested_at


def test_stored_ist_labeled_as_utc_is_repaired_with_ingested_at() -> None:
    stored = "2026-09-07T15:53:59+00:00"
    ingested = "2026-09-07T10:23:59+00:00"
    observed, repair = canonical_observation(stored, ingested_at=ingested)
    assert repair == REPAIR_NAIVE_IST_LABELED_UTC
    assert observed is not None
    assert observed.tzinfo == IST
    assert observed.hour == 15
    now = datetime(2026, 9, 7, 10, 24, 10, tzinfo=UTC)
    age = observation_age_seconds(stored, now=now, ingested_at=ingested)
    assert age is not None
    assert age == 11
    assert age >= 0


def test_no_negative_age_from_known_mislabeled_ist() -> None:
    stored = "2026-09-07T15:53:59+00:00"
    ingested = "2026-09-07T10:23:59+00:00"
    now = datetime(2026, 9, 7, 16, 6, 0, tzinfo=IST)
    age = observation_age_seconds(stored, now=now, ingested_at=ingested)
    assert age is not None
    assert age > 0


def test_observed_and_ingested_remain_distinct() -> None:
    stored = "2026-09-07T15:53:59+00:00"
    ingested = "2026-09-07T10:23:59.400000+00:00"
    observed, _ = canonical_observation(stored, ingested_at=ingested)
    recv, _ = canonical_observation(ingested, ingested_at=None)
    assert observed is not None and recv is not None
    assert observed != recv
    skew = median_clock_skew_seconds([(stored, ingested)])
    assert skew is not None
    assert abs(skew) < 2.0


def test_missing_observation_is_not_fabricated() -> None:
    observed, repair = canonical_observation(None, ingested_at="2026-09-07T10:23:59+00:00")
    assert observed is None
    assert repair is None
    from nse_pipeline.market.data_state import DataState, STATUS_NO_DATA

    payload = describe_freshness(
        DataState(market_state="open", data_status=STATUS_NO_DATA, as_of=None),
        None,
        now=datetime(2026, 9, 7, 10, 32, tzinfo=IST),
    )
    assert payload["status"] == NO_DATA
    assert payload["freshness_age_seconds"] is None
    assert payload["observed_at"] is None


def test_ambiguous_utc_hour_without_ingested_at_is_degraded() -> None:
    stored = "2026-09-07T09:30:00+00:00"
    observed, repair = canonical_observation(stored, ingested_at=None)
    assert repair == AMBIGUOUS_UTC_HOUR
    assert observed is not None
    from nse_pipeline.market.data_state import DataState, STATUS_LIVE

    payload = describe_freshness(
        DataState(
            market_state="open",
            data_status=STATUS_LIVE,
            as_of=stored,
            session_date="2026-09-07",
            source="latest_quotes",
        ),
        {"timestamp": stored},
        now=datetime(2026, 9, 7, 10, 32, tzinfo=IST),
        max_age_seconds=120.0,
    )
    assert payload["status"] == DEGRADED
    assert payload["freshness_age_seconds"] is None


def test_sqlite_read_repairs_latest_quotes(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "q.db")
    store.upsert_latest_quotes(
        [
            {
                "instrument_token": TOKEN,
                "symbol": "NIFTY 50",
                "exchange": "NSE",
                "timestamp": "2026-09-07T15:53:59+00:00",
                "ingested_at": "2026-09-07T10:23:59+00:00",
                "last_price": 23779.15,
            }
        ]
    )
    row = store.fetch_latest_quote("NIFTY 50")
    assert row is not None
    assert row["timestamp"].endswith("+05:30")
    assert "15:53:59" in row["timestamp"]
    stats = store.latest_quotes_observation_stats()
    assert stats["clock_skew_clocks"] == "ingested_at - observed_at"
    assert stats["clock_skew_seconds"] is not None
    assert abs(stats["clock_skew_seconds"]) < 2.0


def test_last_session_as_of_is_observation_not_now(tmp_path) -> None:
    session = _settings(tmp_path).session
    stored = "2026-09-07T15:53:59+00:00"
    ingested = "2026-09-07T10:23:59+00:00"
    now = datetime(2026, 9, 7, 16, 20, tzinfo=IST)
    _choice, state = resolve(
        session=session,
        live_timestamp=stored,
        live_ingested_at=ingested,
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=now,
    )
    assert state.data_status == "last_session"
    assert state.as_of is not None
    assert "16:20" not in state.as_of
    assert state.as_of.endswith("+05:30")
    assert "15:53:59" in state.as_of
