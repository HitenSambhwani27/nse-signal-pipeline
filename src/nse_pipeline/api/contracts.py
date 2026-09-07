"""Phase 0 frozen API contracts. Additive; does not invent market data."""

from __future__ import annotations

import json
from typing import Any, Mapping

SUBSYSTEM_STATUSES = (
    "OK",
    "PARTIAL",
    "UNAVAILABLE",
    "NOT_APPLICABLE",
    "NOT_MATURE",
)

SUBSYSTEM_KEYS = (
    "options",
    "futures",
    "activity",
    "intelligence",
    "charts",
)

# Names frozen in Phase 0. Full field shapes ship with the domain that owns them.
CANONICAL_TYPES = (
    "CanonicalQuote",
    "ReferenceType",
    "Provenance",
    "DataState",
    "SessionState",
    "Quality",
    "Coverage",
    "InstrumentCapabilities",
    "GroupFreshness",
    "Coherence",
    "AlgorithmOutput",
    "AlgorithmDescriptor",
    "DurableInstrumentKey",
    "OrderRequest",
)

REFERENCE_TYPES = (
    "PREVIOUS_CLOSE",
    "OFFICIAL_CLOSE",
    "SETTLEMENT",
    "UNAVAILABLE",
)

MATURITY_CONTRACT = {
    "suppress_below_days": 10,
    "provisional_below_days": 60,
    "no_fabricated_probability": True,
}

# Hard budgets (bytes / rows). Collection endpoints must declare one.
RESPONSE_SIZE_LIMITS: dict[str, dict[str, int]] = {
    "health": {"max_bytes": 32_768},
    "quotes": {"max_bytes": 16_384},
    "quotes_bulk": {"max_bytes": 65_536, "max_rows": 250},
    "instruments": {"max_bytes": 65_536, "max_rows": 50},
    "candles": {"max_bytes": 262_144, "max_rows": 500},
    "options": {"max_bytes": 262_144},
    "futures": {"max_bytes": 65_536},
    "unusual_activity": {"max_bytes": 262_144, "max_rows": 100},
    "default": {"max_bytes": 262_144},
}

ABSOLUTE_MAX_BYTES = 262_144


def default_subsystems() -> dict[str, dict[str, str | None]]:
    return {key: {"status": "OK", "reason": None} for key in SUBSYSTEM_KEYS}


def intelligence_status(maturity: Mapping[str, Any] | None) -> dict[str, str | None]:
    """NOT_MATURE until any class permits a probability. Never invent one."""
    snap = maturity or {}
    permitted = False
    for key, block in snap.items():
        if key in {"as_of", "pooled"}:
            continue
        if isinstance(block, dict) and block.get("probability_permitted"):
            permitted = True
            break
    if permitted:
        return {"status": "OK", "reason": None}
    return {"status": "NOT_MATURE", "reason": "maturity_gate"}


def payload_bytes(payload: Mapping[str, Any] | list[Any] | dict[str, Any]) -> int:
    return len(json.dumps(payload, default=str).encode("utf-8"))


def over_budget(payload: Mapping[str, Any], *, endpoint: str) -> bool:
    limit = RESPONSE_SIZE_LIMITS.get(endpoint) or RESPONSE_SIZE_LIMITS["default"]
    return payload_bytes(payload) > int(limit["max_bytes"])
