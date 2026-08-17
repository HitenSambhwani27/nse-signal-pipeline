"""Rate-limited Kite historical_data fetch with 429/timeout backoff."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone

from kiteconnect import KiteConnect

from nse_pipeline.config import HistoricalSettings
from nse_pipeline.storage.schemas import CandleRecord, InstrumentInfo

logger = logging.getLogger(__name__)


class HistoricalAuthError(RuntimeError):
    """Kite token/auth failure — not retryable; abort the backfill."""


class HistoricalRateLimiter:
    """Stay at or below Kite's historical candle cap (3 req/s)."""

    def __init__(self, per_second: float) -> None:
        self.min_interval = 1.0 / max(per_second, 0.1)
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        gap = self.min_interval - (now - self._last)
        if gap > 0:
            time.sleep(gap)
        self._last = time.monotonic()


def _to_utc_datetime(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)


def _is_rate_limit(exc: BaseException) -> bool:
    text = str(exc).lower()
    code = getattr(exc, "code", None)
    if code == 429:
        return True
    return "too many" in text or "429" in text


def _is_timeout(exc: BaseException) -> bool:
    text = str(exc).lower()
    name = type(exc).__name__.lower()
    if "timeout" in name or "timed out" in text or "read timed out" in text:
        return True
    if "504" in text or "gateway time" in text:
        return True
    return False


def _is_auth_error(exc: BaseException) -> bool:
    """Kite TokenException / 401 / 403 / invalid access_token — do not retry."""
    if isinstance(exc, HistoricalAuthError):
        return True
    name = type(exc).__name__.lower()
    if "tokenexception" in name or name == "tokenerror":
        return True
    code = getattr(exc, "code", None)
    if code in {401, 403}:
        return True
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
        return True
    if "forbidden" in text and "token" in text:
        return True
    return False


def _is_transient(exc: BaseException) -> bool:
    if _is_rate_limit(exc) or _is_timeout(exc):
        return True
    text = str(exc).lower()
    code = getattr(exc, "code", None)
    if code in {502, 503, 504}:
        return True
    return any(
        token in text
        for token in ("connection", "temporarily", "unavailable", "502", "503")
    )


def attempt_timeout(historical: HistoricalSettings, attempt: int) -> float:
    """Raise HTTP timeout toward the ceiling on later attempts."""
    base = float(historical.http_timeout_seconds)
    ceiling = float(historical.http_timeout_ceiling_seconds)
    # attempt 1 → base, 2 → base*1.5, 3 → base*2, ... capped
    scaled = base * (1.0 + 0.5 * (attempt - 1))
    return min(max(scaled, base), ceiling)


def attempt_backoff(historical: HistoricalSettings, attempt: int) -> float:
    """Exponential backoff *after* a failed attempt, before the next try."""
    base = float(historical.retry_backoff_seconds)
    mult = float(historical.retry_backoff_multiplier)
    return base * (mult ** (attempt - 1))


def fetch_candles_with_retry(
    kite: KiteConnect,
    instrument: InstrumentInfo,
    from_date: date,
    to_date: date,
    interval: str,
    historical: HistoricalSettings,
    limiter: HistoricalRateLimiter,
) -> list[CandleRecord]:
    """
    One Kite historical_data call with retry.

    Timeouts, 429s, and other transient errors retry with exponential backoff
    and a raised HTTP timeout so later attempts are not identical resends.
    Empty returns are logged honestly — never padded with synthetic bars.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, historical.retry_max + 1):
        timeout_s = attempt_timeout(historical, attempt)
        kite.timeout = timeout_s
        limiter.wait()
        try:
            raw = kite.historical_data(
                instrument_token=instrument.instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=interval,
                continuous=False,
                oi=True,
            )
        except Exception as exc:
            last_exc = exc
            if _is_auth_error(exc):
                logger.error(
                    "Kite AUTH FAILURE %s %s %s→%s attempt %s/%s: %s "
                    "(not retrying; abort the backfill and re-auth with "
                    "scripts/00_kite_auth.py)",
                    instrument.tradingsymbol,
                    interval,
                    from_date,
                    to_date,
                    attempt,
                    historical.retry_max,
                    exc,
                )
                raise HistoricalAuthError(
                    f"Kite auth/token failure for {instrument.tradingsymbol} "
                    f"{interval} {from_date}→{to_date}: {exc}"
                ) from exc
            retryable = _is_transient(exc)
            if attempt >= historical.retry_max or not retryable:
                logger.warning(
                    "historical_data failed %s %s %s→%s attempt %s/%s "
                    "timeout=%.1fs retryable=%s: %s",
                    instrument.tradingsymbol,
                    interval,
                    from_date,
                    to_date,
                    attempt,
                    historical.retry_max,
                    timeout_s,
                    retryable,
                    exc,
                )
                if retryable:
                    break
                raise

            if _is_rate_limit(exc):
                backoff = historical.retry_429_cooldown_seconds * attempt
            else:
                backoff = attempt_backoff(historical, attempt)
            logger.warning(
                "historical_data retry %s %s %s→%s attempt %s/%s "
                "timeout=%.1fs backoff=%.1fs kind=%s: %s",
                instrument.tradingsymbol,
                interval,
                from_date,
                to_date,
                attempt,
                historical.retry_max,
                timeout_s,
                backoff,
                "429" if _is_rate_limit(exc) else "timeout" if _is_timeout(exc) else "transient",
                exc,
            )
            time.sleep(backoff)
            continue

        candles: list[CandleRecord] = []
        for row in raw or []:
            ts = row.get("date")
            if not isinstance(ts, datetime):
                continue
            candles.append(
                CandleRecord(
                    timestamp=_to_utc_datetime(ts),
                    instrument_token=instrument.instrument_token,
                    symbol=instrument.tradingsymbol,
                    exchange=instrument.exchange,
                    open=float(row.get("open", 0.0)),
                    high=float(row.get("high", 0.0)),
                    low=float(row.get("low", 0.0)),
                    close=float(row.get("close", 0.0)),
                    volume=int(row.get("volume", 0) or 0),
                    oi=int(row["oi"]) if row.get("oi") is not None else None,
                )
            )
        return candles

    raise RuntimeError(
        f"historical_data exhausted retries for {instrument.tradingsymbol}: {last_exc}"
    )
