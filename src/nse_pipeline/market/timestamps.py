"""Single timestamp normalization boundary (architecture E.7).

KiteConnect ticker.py uses ``datetime.fromtimestamp(epoch)``, which yields a
*naive local wall-clock* datetime. On the production ingest host that local
zone is Asia/Kolkata, so naive Kite times are IST, not UTC.

This module is the only place that attaches a timezone to those naive values.
It does not subtract a hardcoded 5.5 hours from every timestamp.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc
IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60  # 19800
REPAIR_SKEW_TOLERANCE_SECONDS = 120.0
FUTURE_AGE_SLACK_SECONDS = 2.0

REPAIR_NAIVE_IST_LABELED_UTC = "naive_ist_labeled_utc"
REPAIR_NONE = None
AMBIGUOUS_UTC_HOUR = "ambiguous_utc_hour"

_AMBIGUOUS_UTC_HOURS = frozenset(range(9, 11))  # 09:00-10:59 UTC overlaps both cases


def parse_instant(value: Any) -> datetime | None:
    """Parse a stored or in-memory timestamp. Does not invent a time.

    Timezone-aware values keep their offset. Naive values are *not* localized
    here — callers must use :func:`parse_kite_datetime` at the ingest boundary.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return None
    return dt


def parse_kite_datetime(value: Any, *, naive_tz: ZoneInfo = IST) -> datetime | None:
    """Ingest-boundary parse for Kite tick datetimes.

    Naive → ``naive_tz`` (Asia/Kolkata on this system). Aware → keep offset.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=naive_tz)
    return dt


def _utc_offset_seconds(dt: datetime) -> float | None:
    offset = dt.utcoffset()
    if offset is None:
        return None
    return offset.total_seconds()


def canonical_observation(
    value: Any,
    *,
    ingested_at: Any = None,
) -> tuple[datetime | None, str | None]:
    """Return (canonical observation instant, repair reason).

    Repair of stored ``+00:00`` IST-wall-clock values happens only when
    ``ingested_at`` proves the ~19800s mislabel, or when the UTC hour cannot
    be a real NSE cash-session UTC hour (11:00-18:00 UTC).
    Hours 09-10 UTC without ``ingested_at`` are ``ambiguous_utc_hour``.
    """
    observed = parse_instant(value)
    if observed is None:
        kite = parse_kite_datetime(value) if isinstance(value, datetime) and value.tzinfo is None else None
        if kite is not None:
            return kite, REPAIR_NONE
        return None, None
    ingested = parse_instant(ingested_at)
    if ingested is not None:
        skew = (observed - ingested).total_seconds()
        if _utc_offset_seconds(observed) == 0.0 and abs(skew - IST_OFFSET_SECONDS) <= REPAIR_SKEW_TOLERANCE_SECONDS:
            wall = observed.replace(tzinfo=None)
            return wall.replace(tzinfo=IST), REPAIR_NAIVE_IST_LABELED_UTC
        return observed, REPAIR_NONE
    if _utc_offset_seconds(observed) == 0.0:
        hour = observed.hour
        if 11 <= hour <= 18:
            wall = observed.replace(tzinfo=None)
            return wall.replace(tzinfo=IST), REPAIR_NAIVE_IST_LABELED_UTC
        if hour in _AMBIGUOUS_UTC_HOURS:
            return observed, AMBIGUOUS_UTC_HOUR
    return observed, REPAIR_NONE


def observation_age_seconds(
    value: Any,
    *,
    now: datetime | None = None,
    ingested_at: Any = None,
) -> tuple[float | None, str | None]:
    """Age of canonical observation vs ``now``. Never substitutes envelope time."""
    observed, repair = canonical_observation(value, ingested_at=ingested_at)
    if observed is None:
        return None, repair
    if repair == AMBIGUOUS_UTC_HOUR:
        return None, repair
    moment = now or datetime.now(UTC)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    age = (moment - observed).total_seconds()
    return age, repair


def is_fresh_age(age: float | None, *, max_age_seconds: float) -> bool:
    if age is None:
        return False
    if age < -FUTURE_AGE_SLACK_SECONDS:
        return False
    return age <= float(max_age_seconds)


def canonicalize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return a shallow copy with canonical ``timestamp``. Does not write back."""
    if row is None:
        return None
    out = dict(row)
    observed, repair = canonical_observation(out.get("timestamp"), ingested_at=out.get("ingested_at"))
    if observed is not None and repair != AMBIGUOUS_UTC_HOUR:
        out["timestamp"] = observed.isoformat()
    if repair:
        out["timestamp_repair"] = repair
    ltt, _ = canonical_observation(out.get("last_trade_time"), ingested_at=out.get("ingested_at"))
    if ltt is not None:
        out["last_trade_time"] = ltt.isoformat()
    return out
