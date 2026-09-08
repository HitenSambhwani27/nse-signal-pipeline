"""RM-2 session_reference lifecycle. Populated from observed data only."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from nse_pipeline.config import SessionSettings
from nse_pipeline.market.calendar import (
    OPEN,
    PRE_OPEN,
    calendar_lookup,
    ensure_weekday_calendar,
    load_calendar_day,
    previous_trading_day,
    session_clock,
)
from nse_pipeline.market.data_state import STATUS_LIVE, DataState
from nse_pipeline.market.reference import (
    REF_OFFICIAL_CLOSE,
    REF_PREVIOUS_CLOSE,
    REF_SETTLEMENT,
    REF_TODAY_OPEN,
    SOURCE_SESSION_REFERENCE,
    _usable_price,
    day_reference_from_row,
    reference_type_for_instrument,
)
from nse_pipeline.storage.sqlite_store import SQLiteStore

_DERIVATIVES = {"FUT", "CE", "PE"}


def _token(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _instrument_kind(meta: Mapping[str, Any] | None, row: Mapping[str, Any]) -> str:
    raw = None if meta is None else meta.get("instrument_type")
    if raw in (None, ""):
        raw = row.get("instrument_type")
    return str(raw or "EQ").upper()


def _observation_session_closed(
    *,
    session_date: str,
    wall_session_date: str,
    clock: str,
    data_status: str | None,
) -> bool:
    if session_date != wall_session_date:
        return True
    if data_status == STATUS_LIVE or clock in {OPEN, PRE_OPEN}:
        return False
    return True


def _previous_close_seed(
    *,
    kind: str,
    prior: Mapping[str, Any] | None,
    ohlc_close: Any,
    previous_session_date: str | None,
) -> tuple[float | None, str | None, str]:
    if prior:
        prior_settle = _usable_price(prior.get("settlement_price"))
        prior_official = _usable_price(prior.get("official_close"))
        if kind in _DERIVATIVES and prior_settle is not None:
            return prior_settle, REF_SETTLEMENT, "verified_settlement"
        if prior_official is not None:
            rtype = REF_OFFICIAL_CLOSE if kind in _DERIVATIVES else REF_PREVIOUS_CLOSE
            reason = (
                "settlement_unavailable" if kind in _DERIVATIVES else "previous_session_official_close"
            )
            return prior_official, rtype, reason
    observed = _usable_price(ohlc_close)
    if observed is not None:
        rtype = REF_OFFICIAL_CLOSE if kind in _DERIVATIVES else REF_PREVIOUS_CLOSE
        reason = (
            "previous_session_official_close_unavailable"
            if previous_session_date
            else "previous_trading_day_unavailable"
        )
        return observed, rtype, reason
    return None, None, "previous_close_unavailable"


def _first_valid_open(ohlc_open: Any, last_price: Any) -> float | None:
    return _usable_price(ohlc_open) or _usable_price(last_price)


def maintain_session_references(
    store: SQLiteStore,
    session: SessionSettings,
    items: Sequence[tuple[Mapping[str, Any], DataState, Mapping[str, Any] | None]],
    *,
    now: datetime | None = None,
) -> dict[tuple[str, int], dict[str, Any]]:
    """Create/update RM-2 rows for observation sessions. Idempotent upsert."""
    moment = now or datetime.now(timezone.utc)
    tz = ZoneInfo(session.timezone)
    local = moment.astimezone(tz)
    with store.connection() as conn:
        return _maintain_session_references_on_conn(
            store, session, items, now=moment, local=local, conn=conn
        )


def _maintain_session_references_on_conn(
    store: SQLiteStore,
    session: SessionSettings,
    items: Sequence[tuple[Mapping[str, Any], DataState, Mapping[str, Any] | None]],
    *,
    now: datetime,
    local,
    conn,
) -> dict[tuple[str, int], dict[str, Any]]:
    ensure_weekday_calendar(store, session, today=local.date(), conn=conn)
    wall_day = load_calendar_day(store, session, now=now, conn=conn)
    lookup = calendar_lookup(store, session, conn=conn)
    clock = session_clock(session, now=now, day=wall_day)
    wall_session_date = wall_day.session_date

    grouped: dict[str, list[tuple[Mapping[str, Any], DataState, Mapping[str, Any] | None, int]]] = {}
    for row, state, meta in items:
        token = _token(row.get("instrument_token"))
        session_date = state.session_date
        if token is None or not session_date:
            continue
        grouped.setdefault(session_date, []).append((row, state, meta, token))

    out: dict[tuple[str, int], dict[str, Any]] = {}
    for session_date, rows in grouped.items():
        as_of = date.fromisoformat(session_date)
        prev_date, _prev_complete = previous_trading_day(session, as_of=as_of, lookup=lookup)
        tokens = [token for _row, _state, _meta, token in rows]
        existing_map = store.fetch_session_reference_map(session_date, tokens, conn=conn)
        prior_map = (
            store.fetch_session_reference_map(prev_date, tokens, conn=conn) if prev_date else {}
        )
        payload: list[dict[str, Any]] = []
        for row, state, meta, token in rows:
            existing = existing_map.get(token) or {}
            kind = _instrument_kind(meta, row)
            official_locked = _usable_price(existing.get("official_close")) is not None
            if (
                official_locked
                and _usable_price(existing.get("previous_close")) is not None
                and _usable_price(existing.get("today_open")) is not None
            ):
                out[(session_date, token)] = dict(existing)
                continue
            closed = _observation_session_closed(
                session_date=session_date,
                wall_session_date=wall_session_date,
                clock=clock,
                data_status=state.data_status,
            )
            official_locked = _usable_price(existing.get("official_close")) is not None
            previous_close = _usable_price(existing.get("previous_close"))
            previous_type = existing.get("reference_type")
            previous_reason = existing.get("reference_reason")
            if previous_close is None:
                previous_close, previous_type, previous_reason = _previous_close_seed(
                    kind=kind,
                    prior=prior_map.get(token),
                    ohlc_close=row.get("ohlc_close"),
                    previous_session_date=prev_date,
                )
            today_open = _usable_price(existing.get("today_open"))
            if today_open is None:
                if official_locked:
                    today_open = _usable_price(row.get("ohlc_open"))
                else:
                    today_open = _first_valid_open(row.get("ohlc_open"), row.get("last_price"))
            official_close = _usable_price(existing.get("official_close"))
            if official_close is None and closed and not official_locked:
                official_close = _usable_price(row.get("last_price"))
            draft = {
                "session_date": session_date,
                "instrument_token": token,
                "previous_close": previous_close,
                "today_open": today_open,
                "official_close": official_close,
                "settlement_price": existing.get("settlement_price"),
                "reference_type": previous_type,
                "reference_reason": previous_reason,
                "instrument_type": kind,
            }
            reference_price, reference_type, reference_reason = day_reference_from_row(
                draft, instrument_type=kind
            )
            if reference_price is None and today_open is not None:
                reference_price = today_open
                reference_type = REF_TODAY_OPEN
                reference_reason = "previous_reference_unavailable"
            payload.append(
                {
                    "session_date": session_date,
                    "instrument_token": token,
                    "symbol": row.get("symbol") or (None if meta is None else meta.get("tradingsymbol")),
                    "previous_close": previous_close,
                    "today_open": today_open,
                    "official_close": official_close,
                    "settlement_price": None,
                    "reference_price": reference_price,
                    "reference_type": reference_type or reference_type_for_instrument(kind),
                    "source": SOURCE_SESSION_REFERENCE,
                    "reference_reason": reference_reason,
                }
            )
        store.upsert_session_references(payload, conn=conn)
        fetched = store.fetch_session_reference_map(session_date, tokens, conn=conn)
        for token, stored in fetched.items():
            out[(session_date, token)] = stored
    return out


def stored_for_snapshot(
    refs: Mapping[tuple[str, int], dict[str, Any]],
    row: Mapping[str, Any] | None,
    state: DataState,
) -> dict[str, Any] | None:
    if row is None or not state.session_date:
        return None
    token = _token(row.get("instrument_token"))
    if token is None:
        return None
    return refs.get((state.session_date, token))
