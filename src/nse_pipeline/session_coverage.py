"""
Session coverage checks — flag partial days (ingestion not near open / incomplete close).

Partial days still get compaction → features → labeling, but must be marked so later
analysis does not treat them as full open-to-close sessions.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd

from nse_pipeline.config import SessionSettings, Settings
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore


CoverageStatus = Literal["full", "partial", "unknown"]


@dataclass
class SessionCoverage:
    trade_date: str
    status: CoverageStatus
    reasons: list[str]
    first_tick_ist: str | None
    last_tick_ist: str | None
    market_open_ist: str
    market_close_ist: str
    open_grace_minutes: int
    close_grace_minutes: int
    symbols_sampled: int

    @property
    def is_partial(self) -> bool:
        return self.status == "partial"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":")
    return time(int(hour), int(minute))


def expected_bounds(
    trade_date: str, session: SessionSettings
) -> tuple[datetime, datetime, datetime, datetime]:
    """Return (open, open_deadline, close_earliest, close) as tz-aware IST datetimes."""
    tz = ZoneInfo(session.timezone)
    day = datetime.strptime(trade_date, "%Y-%m-%d").date()
    open_dt = datetime.combine(day, _parse_hhmm(session.market_open), tzinfo=tz)
    close_dt = datetime.combine(day, _parse_hhmm(session.market_close), tzinfo=tz)
    open_deadline = open_dt + timedelta(minutes=session.open_grace_minutes)
    close_earliest = close_dt - timedelta(minutes=session.close_grace_minutes)
    return open_dt, open_deadline, close_earliest, close_dt


def assess_session_coverage(
    settings: Settings,
    trade_date: str,
    *,
    store: DuckDBTickStore | None = None,
) -> SessionCoverage:
    """
    Classify trade_date as full vs partial from compacted tick span.

    late_start: first tick after market_open + open_grace
    early_end: last tick before market_close - close_grace
    """
    session = settings.session
    open_dt, open_deadline, close_earliest, close_dt = expected_bounds(
        trade_date, session
    )

    owns_store = store is None
    duck = store or DuckDBTickStore(settings)
    try:
        symbols = duck.list_symbols(trade_date)
        if not symbols:
            return SessionCoverage(
                trade_date=trade_date,
                status="unknown",
                reasons=["no_compacted_ticks"],
                first_tick_ist=None,
                last_tick_ist=None,
                market_open_ist=open_dt.isoformat(),
                market_close_ist=close_dt.isoformat(),
                open_grace_minutes=session.open_grace_minutes,
                close_grace_minutes=session.close_grace_minutes,
                symbols_sampled=0,
            )
        first_ts, last_ts = duck.tick_time_span(trade_date)
    finally:
        if owns_store:
            duck.close()

    if first_ts is None or last_ts is None:
        return SessionCoverage(
            trade_date=trade_date,
            status="unknown",
            reasons=["empty_tick_span"],
            first_tick_ist=None,
            last_tick_ist=None,
            market_open_ist=open_dt.isoformat(),
            market_close_ist=close_dt.isoformat(),
            open_grace_minutes=session.open_grace_minutes,
            close_grace_minutes=session.close_grace_minutes,
            symbols_sampled=len(symbols),
        )

    first_ist = pd.Timestamp(first_ts).tz_convert(session.timezone).to_pydatetime()
    last_ist = pd.Timestamp(last_ts).tz_convert(session.timezone).to_pydatetime()

    reasons: list[str] = []
    if first_ist > open_deadline:
        reasons.append(
            f"late_start: first_tick={first_ist.isoformat()} > "
            f"open+grace={open_deadline.isoformat()}"
        )
    if last_ist < close_earliest:
        reasons.append(
            f"early_end: last_tick={last_ist.isoformat()} < "
            f"close-grace={close_earliest.isoformat()}"
        )

    status: CoverageStatus = "partial" if reasons else "full"
    return SessionCoverage(
        trade_date=trade_date,
        status=status,
        reasons=reasons,
        first_tick_ist=first_ist.isoformat(),
        last_tick_ist=last_ist.isoformat(),
        market_open_ist=open_dt.isoformat(),
        market_close_ist=close_dt.isoformat(),
        open_grace_minutes=session.open_grace_minutes,
        close_grace_minutes=session.close_grace_minutes,
        symbols_sampled=len(symbols),
    )


def log_session_coverage(settings: Settings, coverage: SessionCoverage) -> None:
    """Persist coverage flag to ingestion_meta (visible in ops log)."""
    store = SQLiteStore(settings.paths.sqlite_db)
    store.log_ingestion_event(
        event_type="session_coverage",
        message=f"{coverage.trade_date}: {coverage.status}",
        symbol=None,
        rows_written=coverage.symbols_sampled,
        details=coverage.to_dict(),
    )


def format_coverage_banner(coverage: SessionCoverage) -> str:
    if coverage.status == "full":
        return (
            f"coverage=full  first={coverage.first_tick_ist}  "
            f"last={coverage.last_tick_ist}"
        )
    reason = "; ".join(coverage.reasons) if coverage.reasons else coverage.status
    return (
        f"coverage={coverage.status.upper()}  "
        f"first={coverage.first_tick_ist}  last={coverage.last_tick_ist}  "
        f"reasons=[{reason}]"
    )
