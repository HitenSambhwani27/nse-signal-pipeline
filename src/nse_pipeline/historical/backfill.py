"""Orchestrate Part 1 historical backfill (idempotent, coverage-logged)."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from kiteconnect import KiteConnect

from nse_pipeline.config import Settings
from nse_pipeline.historical.chunks import chunk_range, max_days_for_interval
from nse_pipeline.historical.fetch import (
    HistoricalAuthError,
    HistoricalRateLimiter,
    fetch_candles_with_retry,
)
from nse_pipeline.historical.universe import (
    InstrumentClass,
    flatten_universe,
)
from nse_pipeline.historical.writer import daterange_bounds, write_historical_partition
from nse_pipeline.storage.schemas import CandleRecord, InstrumentInfo
from nse_pipeline.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

EQUITY_CLASSES: tuple[InstrumentClass, ...] = (
    "equity_depth",
    "equity_quote",
    "index",
)
FO_CLASSES: tuple[InstrumentClass, ...] = ("options", "futures")


def _instrument_key(cls: str, symbol: str, expiry: str | None) -> tuple[str, str, str]:
    return (cls, symbol, expiry or "")


def load_completed_backfill_keys(store: SQLiteStore) -> set[tuple[str, str, str]]:
    """
    Instruments already finished in a full-universe execute.

    A full execute is identified by historical_backfill_start whose counts
    include equity_quote (Nifty 500 \\ 100). Probe runs do not skip index/F&O.
    """
    with store.connection() as conn:
        starts = conn.execute(
            "SELECT timestamp, details_json FROM ingestion_meta "
            "WHERE event_type = 'historical_backfill_start' ORDER BY id"
        ).fetchall()
        full_start = None
        for ts, details_json in starts:
            payload = json.loads(details_json) if details_json else {}
            counts = payload.get("counts") or {}
            if int(counts.get("equity_quote") or 0) > 0:
                full_start = ts
                break
        if full_start is None:
            return set()
        rows = conn.execute(
            "SELECT symbol, details_json FROM ingestion_meta "
            "WHERE event_type = 'historical_backfill_symbol' AND timestamp >= ?",
            (full_start,),
        ).fetchall()
    keys: set[tuple[str, str, str]] = set()
    for symbol, details_json in rows:
        payload = json.loads(details_json) if details_json else {}
        cls = str(payload.get("class") or "")
        if not cls:
            continue
        keys.add(_instrument_key(cls, str(payload.get("symbol") or symbol), payload.get("expiry")))
    return keys


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def run_backfill(
    kite: KiteConnect | None,
    settings: Settings,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    universe_only: bool = False,
    probe_symbols: list[str] | None = None,
    include_minute: bool = True,
    include_daily: bool = True,
    skip_completed: bool = False,
) -> dict[str, Any]:
    """
    Pull Kite historical_data for the confirmed universe.

    Options/futures ignore the equity start date: we request the same window
    but log whatever the live contract actually returns (lifetime ceiling).
    """
    hist = settings.historical
    end = _parse_date(end_date or date.today())
    start = _parse_date(start_date or hist.equity_start_date)
    store = SQLiteStore(settings.paths.sqlite_db)
    limiter = HistoricalRateLimiter(hist.rate_limit_per_second)

    pairs = flatten_universe(settings)
    if probe_symbols:
        wanted = {s.upper() for s in probe_symbols}
        pairs = [(c, i) for c, i in pairs if i.tradingsymbol.upper() in wanted]
    skipped: list[dict[str, Any]] = []
    if skip_completed and not universe_only:
        done = load_completed_backfill_keys(store)
        remaining: list[tuple[InstrumentClass, InstrumentInfo]] = []
        for cls, info in pairs:
            key = _instrument_key(cls, info.tradingsymbol, info.expiry)
            if key in done:
                skipped.append(
                    {
                        "class": cls,
                        "symbol": info.tradingsymbol,
                        "expiry": info.expiry,
                    }
                )
                continue
            remaining.append((cls, info))
        logger.info(
            "skip-completed: %s already done, %s remaining",
            len(skipped),
            len(remaining),
        )
        pairs = remaining
    if universe_only:
        return {
            "mode": "universe_only",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "instruments": [
                {
                    "class": cls,
                    "symbol": info.tradingsymbol,
                    "token": info.instrument_token,
                    "expiry": info.expiry,
                    "series": getattr(info, "instrument_type", None),
                }
                for cls, info in pairs
            ],
            "counts": _count_by_class(pairs),
        }

    if kite is None:
        raise ValueError("Authenticated Kite client is required unless --universe-only")

    store.log_ingestion_event(
        event_type="historical_backfill_start",
        message="Part 1 historical backfill started",
        details={
            "start": start.isoformat(),
            "end": end.isoformat(),
            "include_minute": include_minute,
            "include_daily": include_daily,
            "counts": _count_by_class(pairs),
            "skipped_completed": len(skipped),
            "minute_lookback_days": hist.minute_lookback_days,
        },
    )

    per_symbol: list[dict[str, Any]] = []
    minute_start = start
    if hist.minute_lookback_days is not None:
        minute_start = max(start, end - timedelta(days=hist.minute_lookback_days - 1))

    for cls, info in pairs:
        is_fo = cls in FO_CLASSES
        symbol_report: dict[str, Any] = {
            "class": cls,
            "symbol": info.tradingsymbol,
            "expiry": info.expiry,
            "daily_rows": 0,
            "minute_rows": 0,
            "daily_range": None,
            "minute_range": None,
            "errors": [],
            "failed_chunks": [],
            "zero_data": False,
            "ceiling": "contract_lifetime" if is_fo else "equity_start",
        }
        try:
            if include_daily:
                daily, failed_d = _pull_interval(
                    kite,
                    settings,
                    info,
                    start=start,
                    end=end,
                    interval=hist.daily_interval,
                    limiter=limiter,
                )
                symbol_report["failed_chunks"].extend(failed_d)
                symbol_report["daily_rows"] = len(daily)
                symbol_report["daily_range"] = _range_tuple(daily)
                write_historical_partition(
                    settings.paths.compacted_dir, daily, interval=hist.daily_interval
                )
            if include_minute:
                m_start = start if is_fo else minute_start
                minute, failed_m = _pull_interval(
                    kite,
                    settings,
                    info,
                    start=m_start,
                    end=end,
                    interval=hist.minute_interval,
                    limiter=limiter,
                )
                symbol_report["failed_chunks"].extend(failed_m)
                symbol_report["minute_rows"] = len(minute)
                symbol_report["minute_range"] = _range_tuple(minute)
                write_historical_partition(
                    settings.paths.compacted_dir, minute, interval=hist.minute_interval
                )
        except HistoricalAuthError as exc:
            logger.error(
                "AUTH ABORT after %s: %s — remaining instruments will not be attempted. "
                "Re-run scripts/00_kite_auth.py then resume with --skip-completed.",
                info.tradingsymbol,
                exc,
            )
            store.log_ingestion_event(
                event_type="historical_backfill_auth_failed",
                message=str(exc),
                symbol=info.tradingsymbol,
                details={
                    "class": cls,
                    "symbol": info.tradingsymbol,
                    "expiry": info.expiry,
                    "completed_before_abort": len(per_symbol),
                    "skipped_completed": len(skipped),
                },
            )
            raise
        except Exception as exc:
            logger.exception("Backfill failed for %s: %s", info.tradingsymbol, exc)
            symbol_report["errors"].append(str(exc))
            store.log_ingestion_event(
                event_type="historical_backfill_error",
                message=str(exc),
                symbol=info.tradingsymbol,
            )
        symbol_report["zero_data"] = (
            symbol_report["daily_rows"] == 0 and symbol_report["minute_rows"] == 0
        )
        per_symbol.append(symbol_report)
        store.log_ingestion_event(
            event_type="historical_backfill_symbol",
            message="symbol complete",
            symbol=info.tradingsymbol,
            rows_written=int(symbol_report["daily_rows"]) + int(symbol_report["minute_rows"]),
            details=symbol_report,
        )

    summary = summarize_coverage(per_symbol, start=start, end=end)
    summary["skipped_completed"] = len(skipped)
    store.log_ingestion_event(
        event_type="historical_backfill_complete",
        message="Part 1 historical backfill finished",
        details=summary,
    )
    return {"per_symbol": per_symbol, "summary": summary, "skipped": skipped}


def _pull_interval(
    kite: KiteConnect,
    settings: Settings,
    info: InstrumentInfo,
    *,
    start: date,
    end: date,
    interval: str,
    limiter: HistoricalRateLimiter,
) -> tuple[list[CandleRecord], list[dict[str, Any]]]:
    hist = settings.historical
    cap = max_days_for_interval(hist, interval)
    out: list[CandleRecord] = []
    failed: list[dict[str, Any]] = []
    for chunk_start, chunk_end in chunk_range(start, end, max_days=cap):
        try:
            part = fetch_candles_with_retry(
                kite,
                info,
                chunk_start,
                chunk_end,
                interval,
                hist,
                limiter,
            )
        except HistoricalAuthError:
            raise
        except Exception as exc:
            logger.warning(
                "chunk failed %s %s %s→%s: %s (continuing)",
                info.tradingsymbol,
                interval,
                chunk_start,
                chunk_end,
                exc,
            )
            failed.append(
                {
                    "symbol": info.tradingsymbol,
                    "interval": interval,
                    "from": chunk_start.isoformat(),
                    "to": chunk_end.isoformat(),
                    "error": str(exc),
                }
            )
            continue
        if not part:
            logger.info(
                "Kite returned 0 bars for %s %s %s→%s (not padded)",
                info.tradingsymbol,
                interval,
                chunk_start,
                chunk_end,
            )
        out.extend(part)
    return out, failed


def _range_tuple(candles: list[CandleRecord]) -> list[str] | None:
    first, last = daterange_bounds(candles)
    if first is None or last is None:
        return None
    return [first.isoformat(), last.isoformat()]


def _count_by_class(
    pairs: list[tuple[InstrumentClass, InstrumentInfo]],
) -> dict[str, int]:
    out: dict[str, int] = {}
    for cls, _ in pairs:
        out[cls] = out.get(cls, 0) + 1
    return out


def summarize_coverage(
    per_symbol: list[dict[str, Any]],
    *,
    start: date,
    end: date,
) -> dict[str, Any]:
    by_class: dict[str, list[dict[str, Any]]] = {}
    for row in per_symbol:
        by_class.setdefault(str(row["class"]), []).append(row)

    def _class_summary(rows: list[dict[str, Any]], *, fo: bool) -> dict[str, Any]:
        n = len(rows)
        zero = [r["symbol"] for r in rows if r.get("zero_data")]
        errors = [r["symbol"] for r in rows if r.get("errors")]
        with_minute = sum(1 for r in rows if int(r.get("minute_rows") or 0) > 0)
        minute_rows = sum(int(r.get("minute_rows") or 0) for r in rows)
        daily_rows = sum(int(r.get("daily_rows") or 0) for r in rows)
        ranges = [r.get("minute_range") for r in rows if r.get("minute_range")]
        return {
            "n_instruments": n,
            "with_minute_data": with_minute,
            "pct_with_minute": round(100.0 * with_minute / n, 1) if n else 0.0,
            "zero_data_symbols": zero,
            "error_symbols": errors,
            "minute_rows": minute_rows,
            "daily_rows": daily_rows,
            "minute_span_samples": ranges[:8],
            "failed_chunks_n": sum(len(r.get("failed_chunks") or []) for r in rows),
            "coverage_benchmark": (
                "contract_lifetime" if fo else f"{start.isoformat()}→{end.isoformat()}"
            ),
        }

    nifty_opt = [
        r
        for r in by_class.get("options", [])
        if str(r["symbol"]).upper().startswith("NIFTY")
        and not str(r["symbol"]).upper().startswith("NIFTYNXT")
        and "BANKNIFTY" not in str(r["symbol"]).upper()
    ]
    bn_opt = [
        r for r in by_class.get("options", []) if "BANKNIFTY" in str(r["symbol"]).upper()
    ]

    return {
        "requested_equity_window": [start.isoformat(), end.isoformat()],
        "by_class": {
            cls: _class_summary(rows, fo=cls in {"options", "futures"})
            for cls, rows in by_class.items()
        },
        "options_nifty_weekly": _class_summary(nifty_opt, fo=True) if nifty_opt else None,
        "options_banknifty_monthly": _class_summary(bn_opt, fo=True) if bn_opt else None,
        "options_strike_retrieval_pct": (
            round(
                100.0
                * sum(
                    1
                    for r in by_class.get("options", [])
                    if int(r.get("minute_rows") or 0) > 0
                )
                / max(len(by_class.get("options", [])), 1),
                1,
            )
        ),
    }
