"""Backend market calendar and session clock (ADR-14 / RM-5).

Holiday list is NOT seeded. Weekends are calendar facts. Weekdays are treated as
trading days with holiday_list_seeded=false so we never invent holidays.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from nse_pipeline.config import SessionSettings

PRE_OPEN = "pre_open"
OPEN = "open"
POST_CLOSE = "post_close"
CLOSED = "closed"
WEEKEND = "weekend"
HOLIDAY = "holiday"
UNKNOWN = "unknown"

# NSE cash windows from the architecture (D.6). Open/close still come from SessionSettings.
PRE_OPEN_START = "09:00"
POST_CLOSE_END = "16:00"

CALENDAR_SOURCE = "weekday_clock"
CALENDAR_SOURCE_VERSION = "phase1_no_nse_holiday_list"


@dataclass(frozen=True)
class CalendarDay:
    session_date: str
    is_trading_day: bool
    segment: str
    session_type: str
    pre_open_start: str
    pre_open_end: str
    open_time: str
    close_time: str
    post_close_end: str
    is_expiry: bool
    holiday_reason: str | None
    source: str
    source_version: str
    holiday_list_seeded: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_date": self.session_date,
            "is_trading_day": self.is_trading_day,
            "segment": self.segment,
            "session_type": self.session_type,
            "windows": {
                "pre_open_start": self.pre_open_start,
                "pre_open_end": self.pre_open_end,
                "open": self.open_time,
                "close": self.close_time,
                "post_close_end": self.post_close_end,
            },
            "is_expiry": self.is_expiry,
            "holiday_reason": self.holiday_reason,
            "source": self.source,
            "source_version": self.source_version,
            "holiday_list_seeded": self.holiday_list_seeded,
        }

    def to_row(self) -> dict[str, Any]:
        return {
            "session_date": self.session_date,
            "segment": self.segment,
            "is_trading_day": 1 if self.is_trading_day else 0,
            "session_type": self.session_type,
            "pre_open_start": self.pre_open_start,
            "pre_open_end": self.pre_open_end,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "post_close_end": self.post_close_end,
            "is_expiry": 1 if self.is_expiry else 0,
            "holiday_reason": self.holiday_reason,
            "source": self.source,
            "source_version": self.source_version,
        }


def _hhmm(text: str) -> tuple[int, int]:
    parts = str(text).split(":")
    return int(parts[0]), int(parts[1] if len(parts) > 1 else 0)


def _at(day: datetime, hhmm: str) -> datetime:
    hour, minute = _hhmm(hhmm)
    return day.replace(hour=hour, minute=minute, second=0, microsecond=0)


def derive_calendar_day(session_date: date, session: SessionSettings, *, segment: str = "CASH") -> CalendarDay:
    iso = session_date.isoformat()
    weekend = session_date.weekday() >= 5
    return CalendarDay(
        session_date=iso,
        is_trading_day=not weekend,
        segment=segment,
        session_type="WEEKEND" if weekend else "NORMAL",
        pre_open_start=PRE_OPEN_START,
        pre_open_end=session.market_open,
        open_time=session.market_open,
        close_time=session.market_close,
        post_close_end=POST_CLOSE_END,
        is_expiry=False,
        holiday_reason=None,
        source=CALENDAR_SOURCE,
        source_version=CALENDAR_SOURCE_VERSION,
        holiday_list_seeded=False,
    )


def calendar_day_from_row(row: dict[str, Any], session: SessionSettings) -> CalendarDay:
    return CalendarDay(
        session_date=str(row["session_date"]),
        is_trading_day=bool(row["is_trading_day"]),
        segment=str(row.get("segment") or "CASH"),
        session_type=str(row.get("session_type") or "NORMAL"),
        pre_open_start=str(row.get("pre_open_start") or PRE_OPEN_START),
        pre_open_end=str(row.get("pre_open_end") or session.market_open),
        open_time=str(row.get("open_time") or session.market_open),
        close_time=str(row.get("close_time") or session.market_close),
        post_close_end=str(row.get("post_close_end") or POST_CLOSE_END),
        is_expiry=bool(row.get("is_expiry")),
        holiday_reason=row.get("holiday_reason"),
        source=str(row.get("source") or CALENDAR_SOURCE),
        source_version=str(row.get("source_version") or CALENDAR_SOURCE_VERSION),
        holiday_list_seeded=str(row.get("source") or "") == "nse_published",
    )


def session_clock(
    session: SessionSettings,
    *,
    now: datetime | None = None,
    day: CalendarDay | None = None,
) -> str:
    """What the exchange clock says. Does not invent holidays."""
    tz = ZoneInfo(session.timezone)
    moment = (now or datetime.now(timezone.utc)).astimezone(tz)
    info = day or derive_calendar_day(moment.date(), session)
    if not info.is_trading_day:
        if info.session_type == "WEEKEND" or moment.weekday() >= 5:
            return WEEKEND
        if info.session_type == "HOLIDAY":
            return HOLIDAY
        return CLOSED
    pre = _at(moment, info.pre_open_start)
    open_t = _at(moment, info.open_time)
    close_t = _at(moment, info.close_time)
    post = _at(moment, info.post_close_end)
    if pre <= moment < open_t:
        return PRE_OPEN
    if open_t <= moment <= close_t:
        return OPEN
    if close_t < moment <= post:
        return POST_CLOSE
    return CLOSED


def cash_session_open(clock: str) -> bool:
    """LIVE market data is only expected during the continuous cash session."""
    return clock == OPEN


def iter_weekday_clock_days(
    session: SessionSettings,
    *,
    start: date,
    end: date,
    segment: str = "CASH",
) -> list[CalendarDay]:
    """Weekday/weekend rows only. Never infers a holiday from missing data."""
    days: list[CalendarDay] = []
    cursor = start
    while cursor <= end:
        days.append(derive_calendar_day(cursor, session, segment=segment))
        cursor = cursor + timedelta(days=1)
    return days


def previous_trading_day(
    session: SessionSettings,
    *,
    as_of: date,
    lookup=None,
    max_lookback: int = 14,
) -> tuple[str | None, bool]:
    """Skip weekends (and seeded holidays). complete=False until an NSE holiday list exists."""
    cursor = as_of
    for _ in range(max_lookback):
        cursor = cursor - timedelta(days=1)
        info = None
        if lookup is not None:
            info = lookup(cursor.isoformat())
        if info is None:
            info = derive_calendar_day(cursor, session)
        if info.is_trading_day:
            return info.session_date, False
    return None, False


def calendar_lookup(store: Any, session: SessionSettings, *, segment: str = "CASH", conn=None):
    """RM-5 table lookup. Missing rows fall through to weekday_clock derivation."""

    def lookup(iso: str) -> CalendarDay | None:
        row = store.fetch_market_calendar_day(iso, segment=segment, conn=conn)
        if row is None:
            return None
        return calendar_day_from_row(row, session)

    return lookup


def load_calendar_day(
    store: Any,
    session: SessionSettings,
    *,
    session_date: str | None = None,
    now: datetime | None = None,
    segment: str = "CASH",
    conn=None,
) -> CalendarDay:
    tz = ZoneInfo(session.timezone)
    moment = (now or datetime.now(timezone.utc)).astimezone(tz)
    iso = session_date or moment.date().isoformat()
    row = store.fetch_market_calendar_day(iso, segment=segment, conn=conn)
    if row is not None:
        return calendar_day_from_row(row, session)
    return derive_calendar_day(date.fromisoformat(iso), session, segment=segment)


def ensure_weekday_calendar(
    store: Any,
    session: SessionSettings,
    *,
    today: date,
    lookback_days: int = 21,
    lookahead_days: int = 7,
    segment: str = "CASH",
    conn=None,
) -> int:
    """Seed weekday/weekend rows only when the table is empty. Never invents holidays."""
    count = store.market_calendar_count(conn=conn)
    if count > 0:
        return count
    days = iter_weekday_clock_days(
        session,
        start=today - timedelta(days=lookback_days),
        end=today + timedelta(days=lookahead_days),
        segment=segment,
    )
    return store.upsert_market_calendar([day.to_row() for day in days], conn=conn)


def build_session_view(
    session: SessionSettings,
    *,
    now: datetime | None = None,
    data_status: str | None = None,
    coverage: str | None = None,
    clock_skew_seconds: float | None = None,
    day: CalendarDay | None = None,
    live_expected: bool | None = None,
    lookup=None,
) -> dict[str, Any]:
    tz = ZoneInfo(session.timezone)
    moment = (now or datetime.now(timezone.utc)).astimezone(tz)
    info = day or derive_calendar_day(moment.date(), session)
    clock = session_clock(session, now=moment, day=info)
    prev, prev_complete = previous_trading_day(session, as_of=moment.date(), lookup=lookup)
    expected = cash_session_open(clock) if live_expected is None else live_expected
    return {
        "market_state": clock,
        "session_date": info.session_date,
        "windows": info.to_dict()["windows"],
        "is_expiry": info.is_expiry,
        "session_type": info.session_type,
        "is_trading_day": info.is_trading_day,
        "live_data_expected": expected,
        "previous_trading_day": prev,
        "previous_trading_day_complete": prev_complete,
        "holiday_list_seeded": info.holiday_list_seeded,
        "calendar_source": info.source,
        "coverage": coverage,
        "data_status": data_status,
        "clock_skew_seconds": clock_skew_seconds,
        "as_of": moment.isoformat(),
        "reason": None
        if info.holiday_list_seeded
        else "holiday_list_not_seeded",
    }
