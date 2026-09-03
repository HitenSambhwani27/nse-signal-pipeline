"""Progress / completion status for the in-flight 07 --execute run."""

from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings
from nse_pipeline.historical.universe import flatten_universe, cache_counts


def main() -> int:
    settings = load_settings()
    counts = cache_counts(settings)
    pairs = flatten_universe(settings)
    total = len(pairs)
    by_class_symbols = defaultdict(list)
    for cls, info in pairs:
        by_class_symbols[cls].append(info.tradingsymbol)

    con = sqlite3.connect(settings.paths.sqlite_db)
    starts = con.execute(
        "select timestamp, details_json from ingestion_meta "
        "where event_type='historical_backfill_start' order by id"
    ).fetchall()
    completes = con.execute(
        "select timestamp, details_json from ingestion_meta "
        "where event_type='historical_backfill_complete' order by id"
    ).fetchall()
    errors = con.execute(
        "select timestamp, symbol, message from ingestion_meta "
        "where event_type='historical_backfill_error' order by id"
    ).fetchall()

    execute_start = "2026-08-14T11:42:00"
    rows = con.execute(
        "select timestamp, symbol, rows_written, details_json from ingestion_meta "
        "where event_type='historical_backfill_symbol' and timestamp >= ? "
        "order by id",
        (execute_start,),
    ).fetchall()

    done_by_class: Counter[str] = Counter()
    seen: set[str] = set()
    last = None
    for ts, symbol, rows_written, details_json in rows:
        payload = json.loads(details_json) if details_json else {}
        cls = payload.get("class") or "?"
        key = f"{cls}:{symbol}:{payload.get('expiry')}"
        if key in seen:
            continue
        seen.add(key)
        done_by_class[cls] += 1
        last = (ts, cls, symbol, rows_written, payload.get("zero_data"))

    report_path = settings.paths.logs_dir / "historical_backfill_last.json"
    print(json.dumps({
        "execute_finished": bool(completes) and report_path.exists(),
        "complete_events": [
            {"timestamp": ts, "summary_keys": list(json.loads(d).keys()) if d else []}
            for ts, d in completes
        ],
        "start_events": [
            {"timestamp": ts, "counts": (json.loads(d) or {}).get("counts") if d else None}
            for ts, d in starts[-3:]
        ],
        "report_exists": report_path.exists(),
        "universe_total": total,
        "universe_by_class": counts,
        "symbols_completed_this_execute": len(seen),
        "completed_by_class": dict(done_by_class),
        "remaining": total - len(seen),
        "errors_n": len(errors),
        "last": last,
        "progress_fraction": round(len(seen) / total, 4) if total else None,
        "ordered_classes": [
            {
                "class": cls,
                "universe": counts.get(cls, 0),
                "done": done_by_class.get(cls, 0),
                "remaining": counts.get(cls, 0) - done_by_class.get(cls, 0),
            }
            for cls in ("equity_depth", "equity_quote", "index", "futures", "options")
        ],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
