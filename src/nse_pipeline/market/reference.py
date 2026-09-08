"""Canonical day-change (ADR-15). Frontend must not recompute these fields.

Day change is resolved from a matching RM-2 session_reference row only.
Tick OHLC is a bootstrap source for RM-2, never the quote-path calculator.
"""

from __future__ import annotations

from typing import Any

REF_PREVIOUS_CLOSE = "PREVIOUS_CLOSE"
REF_TODAY_OPEN = "TODAY_OPEN"
REF_OFFICIAL_CLOSE = "OFFICIAL_CLOSE"
REF_SETTLEMENT = "SETTLEMENT"

_NO_REF = "no_reference"
SOURCE_SESSION_REFERENCE = "session_reference"


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _usable_price(value: Any) -> float | None:
    number = _num(value)
    if number is None or number <= 0:
        return None
    return number


def reference_type_for_instrument(instrument_type: str | None) -> str:
    kind = (instrument_type or "EQ").upper()
    if kind in {"FUT"}:
        return REF_OFFICIAL_CLOSE
    if kind in {"CE", "PE"}:
        return REF_OFFICIAL_CLOSE
    return REF_PREVIOUS_CLOSE


def matching_session_row(
    stored: dict[str, Any] | None,
    *,
    session_date: str | None = None,
    instrument_token: Any = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Refuse cross-session or cross-instrument joins."""
    if not stored:
        return None, _NO_REF
    stored_date = stored.get("session_date")
    if (
        session_date is not None
        and stored_date is not None
        and str(stored_date) != str(session_date)
    ):
        return None, "session_mismatch"
    stored_token = stored.get("instrument_token")
    if instrument_token is not None and stored_token is not None:
        try:
            if int(stored_token) != int(instrument_token):
                return None, "session_mismatch"
        except (TypeError, ValueError):
            return None, "session_mismatch"
    return stored, None


def day_reference_from_row(
    stored: dict[str, Any] | None,
    *,
    instrument_type: str | None = None,
) -> tuple[float | None, str | None, str | None]:
    """Pick the day-change reference from one RM-2 row. Never invents settlement."""
    stored = stored or {}
    wanted = reference_type_for_instrument(instrument_type or stored.get("instrument_type"))
    previous_close = _usable_price(stored.get("previous_close"))
    today_open = _usable_price(stored.get("today_open"))
    stored_type = str(stored.get("reference_type") or "") or None
    stored_reason = stored.get("reference_reason")

    if wanted == REF_OFFICIAL_CLOSE:
        if stored_type == REF_SETTLEMENT and previous_close is not None:
            return previous_close, REF_SETTLEMENT, stored_reason or "verified_settlement"
        if previous_close is not None:
            return (
                previous_close,
                REF_OFFICIAL_CLOSE,
                stored_reason or "settlement_unavailable",
            )
        if today_open is not None:
            return today_open, REF_TODAY_OPEN, "previous_reference_unavailable"
        return None, None, stored_reason or _NO_REF

    if previous_close is not None:
        return previous_close, REF_PREVIOUS_CLOSE, stored_reason
    if today_open is not None:
        return today_open, REF_TODAY_OPEN, "previous_reference_unavailable"
    return None, None, stored_reason or _NO_REF


def resolve_reference(
    row: dict[str, Any] | None,
    *,
    instrument_type: str | None = None,
    stored: dict[str, Any] | None = None,
    session_date: str | None = None,
) -> dict[str, Any]:
    """Pick an observed RM-2 reference. Never invent settlement or a zero change."""
    row = row or {}
    last = _num(row.get("last_price"))
    kind = instrument_type or row.get("instrument_type")
    token = row.get("instrument_token")
    expected_date = session_date or (None if stored is None else stored.get("session_date"))
    matched, fail_reason = matching_session_row(
        stored,
        session_date=expected_date,
        instrument_token=token,
    )
    if matched is None:
        return _change(last, None, None, source=None, reason=fail_reason or _NO_REF)
    reference, reference_type, reason = day_reference_from_row(matched, instrument_type=kind)
    if reference is None:
        return _change(
            last,
            None,
            None,
            source=SOURCE_SESSION_REFERENCE,
            reason=reason or _NO_REF,
        )
    return _change(
        last,
        reference,
        reference_type,
        source=SOURCE_SESSION_REFERENCE,
        reason=reason,
    )


def _change(
    last: float | None,
    reference: float | None,
    reference_type: str | None,
    *,
    source: str | None,
    reason: str | None = None,
) -> dict[str, Any]:
    if reference is None:
        return {
            "reference_price": None,
            "reference_type": reference_type,
            "change_absolute": None,
            "change_percent": None,
            "reference_source": source,
            "reference_reason": reason or _NO_REF,
        }
    if last is None:
        return {
            "reference_price": reference,
            "reference_type": reference_type,
            "change_absolute": None,
            "change_percent": None,
            "reference_source": source,
            "reference_reason": reason or _NO_REF,
        }
    absolute = last - reference
    percent = (absolute / reference) * 100.0
    return {
        "reference_price": reference,
        "reference_type": reference_type,
        "change_absolute": absolute,
        "change_percent": percent,
        "reference_source": source,
        "reference_reason": reason,
    }
