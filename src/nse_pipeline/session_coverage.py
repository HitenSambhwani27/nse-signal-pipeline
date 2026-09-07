"""
Session coverage checks — flag partial days (ingestion not near open / incomplete close).

Partial days still get compaction → features → labeling, but must be marked so later
analysis does not treat them as full open-to-close sessions.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta
from collections.abc import Iterable
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd

from nse_pipeline.config import SessionSettings, Settings
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore

# Live-pilot capture from the start of this project — always excluded from
# baseline and walk-forward, regardless of which job last wrote session_coverage.
LIVE_PILOT_TRADE_DATES = frozenset({"2026-08-13"})
# Equity source=live days shorter than this are treated as incomplete live slices,
# not full live sessions. Muhurat (historical, ~1h) is not live and is kept.
_LIVE_PARTIAL_MAX_HOURS = 2.0


def _warm_start_reason(settings: Settings, trade_date: str) -> str | None:
    """A mid-session ingest start makes the day PARTIAL even if ticks later span the session."""
    path = settings.paths.sqlite_db
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    try:
        rows = conn.execute(
            "SELECT details_json, timestamp FROM ingestion_meta WHERE event_type = 'warm_start'"
        ).fetchall()
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    for details_json, timestamp in rows:
        session_date = None
        if details_json:
            try:
                details = json.loads(details_json)
            except json.JSONDecodeError:
                details = {}
            session_date = details.get("session_date")
        if session_date == trade_date:
            return f"warm_start:{timestamp or session_date}"
        if session_date is None and str(timestamp or "").startswith(trade_date):
            return f"warm_start:{timestamp}"
    return None


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


def _cash_session_symbols(settings: Settings, listed: list[str]) -> list[str]:
    """Equity + index names present that day. F&O contracts are ignored.

    Coverage is a cash-session property. Using whichever symbols a job sampled
    (e.g. two option names with a full historical span) made 2026-08-13 look
    full during FO labeling while quote features correctly marked it partial.
    """
    cache_path = settings.paths.instruments_cache
    if not cache_path.exists():
        return []
    from nse_pipeline.broker.instruments import load_instrument_cache

    cache = load_instrument_cache(cache_path)
    cash = set(cache.get("equity_depth") or {})
    cash.update(cache.get("equity_quote") or {})
    cash.update(cache.get("index") or {})
    return [s for s in listed if s in cash]


def scoring_skip_trade_dates(rows: Iterable[dict[str, Any]]) -> set[str]:
    """Dates baseline and walk-forward must not treat as normal full sessions.

    Always skips 2026-08-13 (live-pilot). Also skips any date whose equity
    rows are source=live and span less than two hours. Historical partials
    such as Muhurat 2025-10-21 are kept.
    """
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        d = str(row.get("trade_date") or "")
        if d:
            by_date.setdefault(d, []).append(row)
    skip = {d for d in by_date if d in LIVE_PILOT_TRADE_DATES}
    for trade_date, date_rows in by_date.items():
        if trade_date in skip:
            continue
        equity = [
            r
            for r in date_rows
            if str(r.get("track") or "").startswith("equity")
            and str(r.get("source") or "") == "live"
        ]
        if not equity:
            continue
        stamps: list[pd.Timestamp] = []
        for r in equity:
            ts = r.get("timestamp")
            if ts is None:
                continue
            t = pd.Timestamp(ts)
            stamps.append(t)
        if len(stamps) < 2:
            skip.add(trade_date)
            continue
        span_h = (max(stamps) - min(stamps)).total_seconds() / 3600.0
        if span_h < _LIVE_PARTIAL_MAX_HOURS:
            skip.add(trade_date)
    return skip


def assess_session_coverage(
    settings: Settings,
    trade_date: str,
    *,
    store: DuckDBTickStore | None = None,
    symbols: list[str] | None = None,
) -> SessionCoverage:
    """
    Classify trade_date as full vs partial from compacted tick span.

    Uses cash-session (equity + index) symbols when the instrument cache
    exists, so FO vs equity jobs cannot disagree. If the cache is absent
    (unit tests), falls back to ``symbols`` or all compacted names.

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
        listed = duck.list_tick_symbols(trade_date)
        cash = _cash_session_symbols(settings, listed)
        if cash:
            listed = cash
        elif symbols is not None:
            allow = set(symbols)
            listed = [s for s in listed if s in allow]
        if not listed:
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
        first_ts, last_ts = duck.tick_time_span(trade_date, symbols=listed)
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
            symbols_sampled=len(listed),
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
    warm = _warm_start_reason(settings, trade_date)
    if warm:
        reasons.append(warm)

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
        symbols_sampled=len(listed),
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
