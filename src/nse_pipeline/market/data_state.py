"""Read-state vocabulary: is a payload live, or the last completed session?

Three states are exposed to the API:

live          current session has a sufficiently fresh latest_quotes snapshot
last_session  market closed, or no fresh current-session snapshot, and a real
              historical observation exists
no_data       neither a live snapshot nor a historical observation exists

A row existing in latest_quotes is never on its own enough to claim "live";
freshness against the session clock decides. Historical observations keep their
own timestamp — wall-clock time is never substituted for as_of.

Session bounds come from the existing SessionSettings / session_coverage logic.
NSE trading holidays are not modelled anywhere in this repository, so on a
holiday the clock still reports "open" during session hours; the freshness check
then downgrades data_status to last_session, which is the honest answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from nse_pipeline.config import SessionSettings
from nse_pipeline.market.futures_analytics import parse_observation_ts
from nse_pipeline.session_coverage import expected_bounds

MARKET_OPEN = "open"
MARKET_CLOSED = "closed"

STATUS_LIVE = "live"
STATUS_LAST_SESSION = "last_session"
STATUS_NO_DATA = "no_data"

SOURCE_LATEST_QUOTES = "latest_quotes"
SOURCE_COMPACTED_TICKS = "compacted_ticks"
SOURCE_COMPACTED_DAILY = "compacted_daily"

# Which candidate the caller should read from.
CHOICE_LIVE = "live"
CHOICE_HISTORICAL = "historical"
CHOICE_NONE = "none"

_STATUS_RANK = {STATUS_NO_DATA: 0, STATUS_LAST_SESSION: 1, STATUS_LIVE: 2}


@dataclass(frozen=True)
class DataState:
    """Explicit backend decision about which session a payload came from."""

    market_state: str
    data_status: str
    as_of: str | None
    session_date: str | None = None
    source: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_state": self.market_state,
            "data_status": self.data_status,
            "as_of": self.as_of,
            "session_date": self.session_date,
            "source": self.source,
            "reason": self.reason,
        }


def market_state(session: SessionSettings, *, now: datetime | None = None) -> str:
    """"open" inside the weekday cash session in the configured timezone."""
    tz = ZoneInfo(session.timezone)
    moment = (now or datetime.now(timezone.utc)).astimezone(tz)
    if moment.weekday() >= 5:
        return MARKET_CLOSED
    open_dt, _open_deadline, _close_earliest, close_dt = expected_bounds(
        moment.date().isoformat(), session
    )
    return MARKET_OPEN if open_dt <= moment <= close_dt else MARKET_CLOSED


def observation_age_seconds(value: Any, *, now: datetime | None = None) -> float | None:
    """Age of a real observation. Returns None when the stamp is unusable."""
    stamp = parse_observation_ts(value)
    if stamp is None:
        return None
    moment = now or datetime.now(timezone.utc)
    return (moment - stamp).total_seconds()


def is_fresh(value: Any, *, max_age_seconds: float, now: datetime | None = None) -> bool:
    age = observation_age_seconds(value, now=now)
    return age is not None and age <= float(max_age_seconds)


def session_iso(value: Any, session: SessionSettings) -> str | None:
    """Same instant, rendered in the session timezone. Never rewritten to now."""
    stamp = parse_observation_ts(value)
    if stamp is None:
        return None
    return stamp.astimezone(ZoneInfo(session.timezone)).isoformat()


def session_date_of(value: Any, session: SessionSettings) -> str | None:
    stamp = parse_observation_ts(value)
    if stamp is None:
        return None
    return stamp.astimezone(ZoneInfo(session.timezone)).date().isoformat()


def no_data_state(session: SessionSettings, *, now: datetime | None = None, reason: str) -> DataState:
    return DataState(
        market_state=market_state(session, now=now),
        data_status=STATUS_NO_DATA,
        as_of=None,
        session_date=None,
        source=None,
        reason=reason,
    )


def resolve(
    *,
    session: SessionSettings,
    live_timestamp: Any = None,
    historical_timestamp: Any = None,
    historical_source: str | None = None,
    max_age_seconds: float,
    now: datetime | None = None,
) -> tuple[str, DataState]:
    """Pick between a live snapshot and a historical observation.

    Callers may pass ``historical_timestamp=None`` when they short-circuited the
    historical read because live data was already fresh.
    """
    moment = now or datetime.now(timezone.utc)
    clock = market_state(session, now=moment)
    live_ts = parse_observation_ts(live_timestamp)
    hist_ts = parse_observation_ts(historical_timestamp)
    live_fresh = live_ts is not None and (moment - live_ts).total_seconds() <= float(max_age_seconds)

    if live_fresh and clock == MARKET_OPEN:
        return CHOICE_LIVE, DataState(
            market_state=clock,
            data_status=STATUS_LIVE,
            as_of=session_iso(live_ts, session),
            session_date=session_date_of(live_ts, session),
            source=SOURCE_LATEST_QUOTES,
        )

    if live_ts is None and hist_ts is None:
        return CHOICE_NONE, DataState(
            market_state=clock,
            data_status=STATUS_NO_DATA,
            as_of=None,
            session_date=None,
            source=None,
            reason="no_live_or_historical_observation",
        )

    # Serve the most recent real observation, and do not relabel it as live.
    if hist_ts is not None and (live_ts is None or hist_ts > live_ts):
        choice = CHOICE_HISTORICAL
        stamp = hist_ts
        source = historical_source or SOURCE_COMPACTED_TICKS
    else:
        choice = CHOICE_LIVE
        stamp = live_ts
        source = SOURCE_LATEST_QUOTES

    if clock == MARKET_CLOSED:
        reason = "market_closed"
    elif live_ts is None:
        reason = "live_snapshot_missing"
    else:
        reason = "live_snapshot_stale"

    return choice, DataState(
        market_state=clock,
        data_status=STATUS_LAST_SESSION,
        as_of=session_iso(stamp, session),
        session_date=session_date_of(stamp, session),
        source=source,
        reason=reason,
    )


def merge_states(states: list[DataState], *, fallback: DataState) -> DataState:
    """Best state across several symbols: live beats last_session beats no_data."""
    best = fallback
    for state in states:
        if state is None:
            continue
        rank = _STATUS_RANK.get(state.data_status, 0)
        best_rank = _STATUS_RANK.get(best.data_status, 0)
        if rank > best_rank:
            best = state
        elif rank == best_rank and (state.as_of or "") > (best.as_of or ""):
            best = state
    return best
