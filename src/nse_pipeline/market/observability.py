"""Lightweight subsystem health. No historical-table scans, no secrets."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from nse_pipeline.market.calendar import cash_session_open
from nse_pipeline.market.data_state import observation_age_seconds
from nse_pipeline.market.timestamps import AMBIGUOUS_UTC_HOUR, canonical_observation

COMPONENT_STATUSES = (
    "healthy",
    "degraded",
    "unavailable",
    "stale",
    "not_applicable",
)

_COMPONENT_KEYS = (
    "ingestion",
    "latest_quotes",
    "activity_samples",
    "instrument_cache",
    "market_calendar",
    "account",
    "api",
    "disk",
    "maturity",
    "tick_stream_counters",
)


def _component(status: str, *, reason: str | None = None, **extra: Any) -> dict[str, Any]:
    if status not in COMPONENT_STATUSES:
        raise ValueError(f"unknown component status: {status}")
    payload: dict[str, Any] = {"status": status, "reason": reason}
    payload.update(extra)
    return payload


def _age(value: Any, *, now: datetime) -> float | None:
    return observation_age_seconds(value, now=now)


def _freshness_status(
    *,
    has_rows: bool,
    age: float | None,
    live_expected: bool,
    max_age_seconds: float,
) -> str:
    if not has_rows:
        return "unavailable"
    if not live_expected:
        return "healthy"
    if age is None:
        return "degraded"
    if age <= float(max_age_seconds):
        return "healthy"
    return "stale"


def median_clock_skew_seconds(pairs: list[tuple[Any, Any]]) -> float | None:
    """Median ingested_at − canonical observed_at.

    Clocks compared: ingest receive time vs exchange observation time.
    """
    deltas: list[float] = []
    for timestamp, ingested_at in pairs:
        observed, repair = canonical_observation(timestamp, ingested_at=ingested_at)
        ingested, _ = canonical_observation(ingested_at, ingested_at=None)
        if observed is None or ingested is None or repair == AMBIGUOUS_UTC_HOUR:
            continue
        deltas.append((ingested - observed).total_seconds())
    if not deltas:
        return None
    ordered = sorted(deltas)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[mid], 3)
    return round((ordered[mid - 1] + ordered[mid]) / 2.0, 3)


def cache_component(cache_health: dict[str, Any] | None) -> dict[str, Any]:
    if not cache_health:
        return _component("unavailable", reason="cache_file_missing")
    status = str(cache_health.get("status") or "unknown")
    if status == "unknown":
        return _component("unavailable", reason=cache_health.get("reason") or "cache_file_missing")
    if status != "ok" or cache_health.get("needs_refresh"):
        return _component("degraded", reason=status)
    return _component("healthy", reason=None)


def maturity_component(maturity: dict[str, Any] | None) -> dict[str, Any]:
    snap = maturity or {}
    permitted = False
    for key, block in snap.items():
        if key in {"as_of", "pooled"}:
            continue
        if isinstance(block, dict) and block.get("probability_permitted"):
            permitted = True
            break
    if permitted:
        return _component("healthy", reason=None, probability_permitted=True)
    return _component(
        "healthy",
        reason="maturity_gate",
        probability_permitted=False,
    )


def build_health_components(
    *,
    clock: str,
    now: datetime,
    max_age_seconds: float,
    quotes_count: int,
    quotes_max_timestamp: str | None,
    quotes_max_ingested_at: str | None,
    clock_skew_seconds: float | None,
    activity_max_timestamp: str | None,
    cache_health: dict[str, Any] | None,
    calendar_rows: int,
    holiday_list_seeded: bool,
    account_status: str | None,
    disk: dict[str, Any],
    disk_warning: bool,
    maturity: dict[str, Any] | None,
    ingest_fresh: bool | None,
) -> dict[str, dict[str, Any]]:
    """Distinct subsystem states. Callers must not collapse these into one flag."""
    live_expected = cash_session_open(clock)
    quote_age = observation_age_seconds(
        quotes_max_timestamp, now=now, ingested_at=quotes_max_ingested_at
    )
    ingest_age = _age(quotes_max_ingested_at, now=now)
    activity_age = _age(activity_max_timestamp, now=now)
    quotes_status = _freshness_status(
        has_rows=quotes_count > 0,
        age=quote_age,
        live_expected=live_expected,
        max_age_seconds=max_age_seconds,
    )
    ingest_status = _freshness_status(
        has_rows=quotes_count > 0 or ingest_fresh is True,
        age=ingest_age if ingest_age is not None else quote_age,
        live_expected=live_expected,
        max_age_seconds=max_age_seconds,
    )
    if quotes_count <= 0 and ingest_fresh is not True:
        ingest_status = "unavailable"
    activity_status = _freshness_status(
        has_rows=activity_max_timestamp is not None,
        age=activity_age,
        live_expected=live_expected,
        max_age_seconds=max_age_seconds,
    )
    account = str(account_status or "unknown")
    if account in {"ok", "healthy"}:
        account_comp = _component("healthy", reason=None)
    elif account in {"auth_invalid", "error", "failed"}:
        account_comp = _component("unavailable", reason=account)
    elif account == "unknown":
        account_comp = _component("unavailable", reason="account_capture_unknown")
    else:
        account_comp = _component("degraded", reason=account)
    if disk.get("disk_free_gb") is None:
        disk_comp = _component("unavailable", reason="disk_stat_unreadable")
    elif disk_warning:
        disk_comp = _component("degraded", reason="low_session_headroom")
    else:
        disk_comp = _component("healthy", reason=None)
    return {
        "ingestion": _component(
            ingest_status,
            reason=None if ingest_status == "healthy" else ingest_status,
            live_data_expected=live_expected,
        ),
        "latest_quotes": _component(
            quotes_status,
            reason=None if quotes_status == "healthy" else quotes_status,
            count=quotes_count,
            max_timestamp=quotes_max_timestamp,
            max_ingested_at=quotes_max_ingested_at,
            freshness_age_seconds=None if quote_age is None else round(quote_age, 3),
            clock_skew_seconds=clock_skew_seconds,
            clock_skew_clocks="ingested_at - observed_at",
        ),
        "activity_samples": _component(
            activity_status,
            reason=None if activity_status == "healthy" else activity_status,
            max_timestamp=activity_max_timestamp,
            freshness_age_seconds=None if activity_age is None else round(activity_age, 3),
        ),
        "instrument_cache": cache_component(cache_health),
        "market_calendar": _component(
            "healthy",
            reason=None if holiday_list_seeded else "holiday_list_not_seeded",
            rows=calendar_rows,
            holiday_list_seeded=holiday_list_seeded,
            source="weekday_clock",
        ),
        "account": account_comp,
        "api": _component("healthy", reason=None),
        "disk": disk_comp,
        "maturity": maturity_component(maturity),
        "tick_stream_counters": _component(
            "not_applicable",
            reason="no_in_process_ingest_ring",
        ),
    }