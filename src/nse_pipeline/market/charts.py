"""Downsampled chart series. Never dump raw ticks to the API."""

from __future__ import annotations

from typing import Any, Sequence


def downsample_points(rows: Sequence[dict[str, Any]], *, max_points: int) -> list[dict[str, Any]]:
    if max_points <= 0 or len(rows) <= max_points:
        return list(rows)
    step = max(1, len(rows) / float(max_points))
    out: list[dict[str, Any]] = []
    cursor = 0.0
    last_idx = -1
    while len(out) < max_points and int(cursor) < len(rows):
        idx = int(cursor)
        if idx != last_idx:
            out.append(rows[idx])
            last_idx = idx
        cursor += step
    if rows and out[-1] is not rows[-1]:
        out[-1] = rows[-1]
    return out


CHART_POINT_FIELDS = (
    "last_price",
    "volume",
    "oi",
    "oi_delta",
    "volume_delta",
    "trade_notional",
    "depth_imbalance",
    "spread",
)


def merge_chart_rows(
    historical: Sequence[dict[str, Any]],
    samples: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Union by timestamp. Activity samples win on overlap (more derived fields)."""
    by_ts: dict[str, dict[str, Any]] = {}
    for row in historical:
        ts = row.get("timestamp")
        if not ts:
            continue
        by_ts[str(ts)] = dict(row)
    for row in samples:
        ts = row.get("timestamp")
        if not ts:
            continue
        key = str(ts)
        prev = by_ts.get(key, {})
        by_ts[key] = {**prev, **dict(row)}
    return sorted(by_ts.values(), key=lambda r: str(r.get("timestamp") or ""))


def chart_payload(
    rows: Sequence[dict[str, Any]],
    *,
    max_points: int,
    fields: Sequence[str],
) -> dict[str, Any]:
    kept = downsample_points(rows, max_points=max_points)
    series = []
    for row in kept:
        point = {"timestamp": row.get("timestamp")}
        for field in fields:
            point[field] = row.get(field)
        series.append(point)
    return {
        "points": series,
        "observation_count": len(rows),
        "returned_points": len(series),
        "downsampled": len(rows) > len(series),
        "kind": "derived",
    }
