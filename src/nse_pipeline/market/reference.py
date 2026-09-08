"""Canonical day-change (ADR-15). Frontend must not recompute these fields."""

from __future__ import annotations

from typing import Any

REF_PREVIOUS_CLOSE = "PREVIOUS_CLOSE"
REF_TODAY_OPEN = "TODAY_OPEN"
REF_OFFICIAL_CLOSE = "OFFICIAL_CLOSE"
REF_SETTLEMENT = "SETTLEMENT"

_NO_REF = "no_reference"


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


def resolve_reference(
    row: dict[str, Any] | None,
    *,
    instrument_type: str | None = None,
    stored: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pick an observed reference. Never invent settlement or a zero change."""
    row = row or {}
    stored = stored or {}
    last = _num(row.get("last_price"))
    wanted = reference_type_for_instrument(instrument_type or row.get("instrument_type"))

    stored_price = _usable_price(stored.get("reference_price") or stored.get("previous_close"))
    stored_type = stored.get("reference_type")
    if stored_price is not None and stored.get("settlement_price") not in (None, "") and wanted == REF_OFFICIAL_CLOSE:
        # Only claim SETTLEMENT when a real settlement value was stored.
        settle = _usable_price(stored.get("settlement_price"))
        if settle is not None:
            return _change(last, settle, REF_SETTLEMENT, source=str(stored.get("source") or "session_reference"))
    if stored_price is not None:
        rtype = str(stored_type or wanted)
        return _change(last, stored_price, rtype, source=str(stored.get("source") or "session_reference"))

    prev_close = _usable_price(row.get("ohlc_close"))
    today_open = _usable_price(row.get("ohlc_open"))
    if prev_close is not None:
        return _change(
            last,
            prev_close,
            wanted if wanted in {REF_PREVIOUS_CLOSE, REF_OFFICIAL_CLOSE} else REF_PREVIOUS_CLOSE,
            source="kite_tick_ohlc_close",
        )
    if today_open is not None:
        return _change(last, today_open, REF_TODAY_OPEN, source="kite_tick_ohlc_open")
    return _change(last, None, None, source=None, reason=_NO_REF)


def _change(
    last: float | None,
    reference: float | None,
    reference_type: str | None,
    *,
    source: str | None,
    reason: str | None = None,
) -> dict[str, Any]:
    if reference is None or last is None:
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
        "reference_reason": None,
    }
