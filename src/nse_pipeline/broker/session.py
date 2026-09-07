"""Kite access-token lifecycle. Never logs or returns the token itself."""

from __future__ import annotations

from enum import Enum


AUTH_BACKOFF_SECONDS = 300


class TokenState(str, Enum):
    VALID = "valid"
    EXPIRING = "expiring"
    INVALID = "invalid"
    REAUTHENTICATED = "reauthenticated"
    MISSING = "missing"
    UNKNOWN = "unknown"


def classify_exception(exc: BaseException) -> TokenState | None:
    """Return TokenState.INVALID for Kite auth failures, else None."""
    name = type(exc).__name__.lower()
    if "tokenexception" in name or name == "tokenerror":
        return TokenState.INVALID
    code = getattr(exc, "code", None)
    if code in {401, 403}:
        return TokenState.INVALID
    text = str(exc).lower()
    needles = (
        "incorrect `api_key`",
        "incorrect api_key",
        "invalid token",
        "token expired",
        "access_token` is invalid",
        "access token is invalid",
        "tokenexception",
    )
    if any(n in text for n in needles):
        return TokenState.INVALID
    if "forbidden" in text and "token" in text:
        return TokenState.INVALID
    return None


def token_present(access_token: str | None) -> bool:
    return bool((access_token or "").strip())


def public_token_status(
    *,
    access_token_present: bool,
    last_account_status: str | None = None,
    last_account_error: str | None = None,
    last_ingest_event: str | None = None,
    ingest_fresh: bool = False,
) -> dict[str, str | None]:
    """Infer token state from already-observed process status. Does not call Kite."""
    if not access_token_present:
        state = TokenState.MISSING
    elif last_account_status == "auth_invalid" or (
        last_account_error and classify_exception(RuntimeError(last_account_error))
    ):
        # Account REST failed auth. Market-data WS may still be alive on the
        # same token — callers must not collapse the two without ingest_fresh.
        state = TokenState.INVALID if not ingest_fresh else TokenState.VALID
    elif ingest_fresh or last_ingest_event in {"connect", "flush", "startup", "warm_start"}:
        state = TokenState.VALID
    elif last_account_status == "ok":
        state = TokenState.VALID
    else:
        state = TokenState.UNKNOWN
    return {
        "kite_token_state": state.value,
        "account_status": last_account_status,
        "ingest_event": last_ingest_event,
    }
