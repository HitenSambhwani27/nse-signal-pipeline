"""Confirmed-universe flattening for historical backfill (no expansion)."""

from __future__ import annotations

from typing import Any, Literal

from nse_pipeline.broker.instruments import load_instrument_cache
from nse_pipeline.config import Settings
from nse_pipeline.storage.schemas import InstrumentInfo

InstrumentClass = Literal["equity_depth", "equity_quote", "index", "options", "futures"]


def instruments_by_class(settings: Settings) -> dict[InstrumentClass, list[InstrumentInfo]]:
    cache = load_instrument_cache(settings.paths.instruments_cache)
    return {
        "equity_depth": [
            InstrumentInfo.from_cache_dict(d)
            for d in cache.get("equity_depth", {}).values()
        ],
        "equity_quote": [
            InstrumentInfo.from_cache_dict(d)
            for d in cache.get("equity_quote", {}).values()
        ],
        "index": [
            InstrumentInfo.from_cache_dict(d) for d in cache.get("index", {}).values()
        ],
        "options": [
            InstrumentInfo.from_cache_dict(d) for d in cache.get("options", [])
        ],
        "futures": [
            InstrumentInfo.from_cache_dict(d) for d in cache.get("futures", [])
        ],
    }


def flatten_universe(
    settings: Settings,
    *,
    classes: tuple[InstrumentClass, ...] | None = None,
) -> list[tuple[InstrumentClass, InstrumentInfo]]:
    grouped = instruments_by_class(settings)
    wanted = classes or tuple(grouped.keys())
    out: list[tuple[InstrumentClass, InstrumentInfo]] = []
    for cls in wanted:
        for info in grouped[cls]:
            out.append((cls, info))
    return out


def class_for_track(track: str) -> str:
    if track.startswith("equity") or track == "index":
        return "equity"
    if track == "options":
        return "options"
    if track == "futures":
        return "futures"
    return "equity"


def cache_counts(settings: Settings) -> dict[str, Any]:
    grouped = instruments_by_class(settings)
    return {k: len(v) for k, v in grouped.items()}
