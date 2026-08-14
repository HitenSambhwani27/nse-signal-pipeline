"""Rate-limited Kite historical_data fetch with 429 backoff."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone

from kiteconnect import KiteConnect

from nse_pipeline.config import HistoricalSettings
from nse_pipeline.storage.schemas import CandleRecord, InstrumentInfo

logger = logging.getLogger(__name__)


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

    Logs empty returns honestly — never pads with synthetic bars.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, historical.retry_max + 1):
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
        except Exception as exc:  # kiteconnect raises typed exceptions; keep broad
            last_exc = exc
            if _is_rate_limit(exc):
                cooldown = historical.retry_429_cooldown_seconds * attempt
                logger.warning(
                    "429 on %s %s %s→%s attempt %s; cooldown %.1fs",
                    instrument.tradingsymbol,
                    interval,
                    from_date,
                    to_date,
                    attempt,
                    cooldown,
                )
                time.sleep(cooldown)
                continue
            logger.warning(
                "historical_data failed %s %s %s→%s: %s",
                instrument.tradingsymbol,
                interval,
                from_date,
                to_date,
                exc,
            )
            if attempt < historical.retry_max:
                time.sleep(historical.sleep_seconds * attempt)
                continue
            raise

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
