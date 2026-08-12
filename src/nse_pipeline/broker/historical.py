"""
Historical OHLCV + OI candle backfill via Kite Connect REST API.

Kite limits historical requests; this module batches by date range and sleeps
between calls to stay conservative.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from kiteconnect import KiteConnect

from nse_pipeline.broker.instruments import all_subscribed_instruments, load_instrument_cache
from nse_pipeline.config import Settings
from nse_pipeline.storage.parquet_writer import BufferedParquetWriter
from nse_pipeline.storage.schemas import CandleRecord, InstrumentInfo
from nse_pipeline.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


def _to_utc_datetime(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)


def fetch_historical_candles(
    kite: KiteConnect,
    instrument: InstrumentInfo,
    from_date: date,
    to_date: date,
    interval: str,
) -> list[CandleRecord]:
    """
    Fetch historical candles for one instrument.

    kite.historical_data returns list of dicts with date/open/high/low/close/volume/oi.
    """
    raw = kite.historical_data(
        instrument_token=instrument.instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=interval,
        continuous=False,
        oi=True,
    )

    candles: list[CandleRecord] = []
    for row in raw:
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


def backfill_instruments(
    kite: KiteConnect,
    settings: Settings,
    instruments: Iterable[InstrumentInfo],
    lookback_days: int | None = None,
    sleep_seconds: float = 0.35,
) -> dict[str, int]:
    """
    Backfill all instruments and write candles.parquet per symbol/day.

    Returns mapping symbol -> candle count written.
    """
    store = SQLiteStore(settings.paths.sqlite_db)
    writer = BufferedParquetWriter(raw_dir=settings.paths.raw_dir)
    lookback = lookback_days or settings.historical.lookback_days
    interval = settings.historical.interval

    to_date = date.today()
    from_date = to_date - timedelta(days=lookback)

    counts: dict[str, int] = {}
    instrument_list = list(instruments)

    store.log_ingestion_event(
        event_type="backfill_start",
        message="Historical backfill started",
        details={
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "instrument_count": len(instrument_list),
            "interval": interval,
        },
    )

    for index, instrument in enumerate(instrument_list, start=1):
        logger.info(
            "Backfilling %s (%s/%s)...",
            instrument.tradingsymbol,
            index,
            len(instrument_list),
        )
        try:
            candles = fetch_historical_candles(
                kite=kite,
                instrument=instrument,
                from_date=from_date,
                to_date=to_date,
                interval=interval,
            )
            if candles:
                writer.write_candles(instrument.tradingsymbol, candles)
            counts[instrument.tradingsymbol] = len(candles)
            store.log_ingestion_event(
                event_type="backfill_symbol",
                message="Backfill complete for symbol",
                symbol=instrument.tradingsymbol,
                rows_written=len(candles),
            )
        except Exception as exc:
            logger.exception("Backfill failed for %s: %s", instrument.tradingsymbol, exc)
            store.log_ingestion_event(
                event_type="backfill_error",
                message=str(exc),
                symbol=instrument.tradingsymbol,
            )
            counts[instrument.tradingsymbol] = 0

        time.sleep(sleep_seconds)

    store.log_ingestion_event(
        event_type="backfill_complete",
        message="Historical backfill finished",
        details={"counts": counts},
    )
    return counts


def backfill_from_cache(kite: KiteConnect, settings: Settings) -> dict[str, int]:
    cache = load_instrument_cache(settings.paths.instruments_cache)
    instruments = all_subscribed_instruments(cache)
    return backfill_instruments(kite, settings, instruments)
