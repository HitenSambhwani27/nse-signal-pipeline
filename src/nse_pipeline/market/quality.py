"""Analytics-level observation dedup. Raw Parquet is never rewritten."""

from __future__ import annotations

from typing import Any, Iterable, Sequence


_FINGERPRINT_FIELDS = (
    "instrument_token",
    "timestamp",
    "exchange_timestamp",
    "last_trade_time",
    "last_price",
    "last_quantity",
    "volume",
    "oi",
    "bid_prices",
    "bid_quantities",
    "ask_prices",
    "ask_quantities",
)


def _norm(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_norm(v) for v in value)
    if isinstance(value, tuple):
        return tuple(_norm(v) for v in value)
    # Parquet depth columns arrive as numpy arrays, not Python lists.
    # `array == array` is element-wise and cannot be used as a boolean.
    shape = getattr(value, "shape", None)
    tolist = getattr(value, "tolist", None)
    if shape is not None and callable(tolist) and not isinstance(value, (str, bytes, dict)):
        try:
            return _norm(tolist())
        except Exception:
            return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    try:
        import math

        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:
        pass
    return value


def observation_fingerprint(row: dict[str, Any]) -> tuple[Any, ...]:
    """Identity of one persisted snapshot. Volume/OI/depth changes produce a new key."""
    return tuple(_norm(row.get(field)) for field in _FINGERPRINT_FIELDS)


def dedupe_persisted_observations(
    rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Collapse consecutive exact-duplicate observations.

    This matches Parquet re-flush / minute-file rewrite artifacts, where the
    same packet is written twice back-to-back. It does NOT keep a session-wide
    fingerprint set, which would drop legitimate later repeats.

    Raw files are untouched. This is analytics/compaction-read only.
    """
    out: list[dict[str, Any]] = []
    previous: tuple[Any, ...] | None = None
    for row in rows:
        key = observation_fingerprint(row)
        if previous is not None and key == previous:
            continue
        out.append(row)
        previous = key
    return out


def dedupe_tick_dataframe(df: Any) -> Any:
    """Apply consecutive fingerprint dedup to a ticks DataFrame. Empty/short frames pass through."""
    if df is None or getattr(df, "empty", True) or len(df) < 2:
        return df
    records = df.to_dict("records")
    kept = dedupe_persisted_observations(records)
    if len(kept) == len(records):
        return df
    import pandas as pd

    out = pd.DataFrame.from_records(kept)
    return out.reindex(columns=list(df.columns))


def consecutive_exact_duplicates(rows: Iterable[dict[str, Any]]) -> int:
    previous = None
    dupes = 0
    for row in rows:
        key = observation_fingerprint(row)
        if previous is not None and key == previous:
            dupes += 1
        previous = key
    return dupes
