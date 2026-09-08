"""Observation freshness. Uses market timestamps, never envelope.as_of."""

from __future__ import annotations

from typing import Any

from nse_pipeline.market.data_state import (
    STATUS_LAST_SESSION,
    STATUS_LIVE,
    STATUS_NO_DATA,
    DataState,
)
from nse_pipeline.market.timestamps import (
    AMBIGUOUS_UTC_HOUR,
    FUTURE_AGE_SLACK_SECONDS,
    observation_age_seconds,
)

FRESH_LIVE = "fresh_live"
STALE_LIVE = "stale_live"
LAST_SESSION = "last_session"
NO_DATA = "no_data"
DEGRADED = "degraded"
PARTIAL = "partial"

QUALITY_FRESH = "FRESH"
QUALITY_STALE = "STALE"
QUALITY_DELAYED = "DELAYED"


def describe_freshness(
    state: DataState,
    row: dict[str, Any] | None,
    *,
    now=None,
    max_age_seconds: float = 120.0,
    coverage: str | None = None,
) -> dict[str, Any]:
    observed = None if row is None else row.get("timestamp") or row.get("exchange_timestamp")
    ingested = None if row is None else row.get("ingested_at")
    row_repair = None if row is None else row.get("timestamp_repair")
    age, repair = observation_age_seconds(observed, now=now, ingested_at=ingested)
    if age is not None and -FUTURE_AGE_SLACK_SECONDS <= age < 0:
        age = 0.0
    if state.data_status == STATUS_NO_DATA:
        status = NO_DATA
        quality = None
    elif repair == AMBIGUOUS_UTC_HOUR or (age is not None and age < -FUTURE_AGE_SLACK_SECONDS):
        status = DEGRADED
        quality = QUALITY_STALE
    elif state.data_status == STATUS_LIVE:
        if age is None:
            status = DEGRADED
            quality = QUALITY_STALE
        elif age <= float(max_age_seconds):
            status = FRESH_LIVE
            quality = QUALITY_FRESH
        else:
            status = STALE_LIVE
            quality = QUALITY_STALE
    elif coverage and str(coverage).lower().startswith("partial"):
        status = PARTIAL
        quality = QUALITY_DELAYED
    else:
        status = LAST_SESSION
        quality = QUALITY_DELAYED
    return {
        "status": status,
        "quality": quality,
        "observed_at": state.as_of,
        "freshness_age_seconds": None if age is None else round(float(age), 3),
        "source": state.source,
        "session_date": state.session_date,
        "coverage": coverage,
        "data_status": state.data_status,
        "ingested_at": ingested,
        "timezone_repair": repair or row_repair,
    }
