"""Audit timeout WARNING windows vs on-disk minute coverage."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings
from nse_pipeline.historical.coverage_audit import analyze_log


def main() -> int:
    settings = load_settings()
    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if log_path is None:
        print("Usage: python scripts/_audit_timeout_gaps.py <terminal-log>")
        return 1
    report = analyze_log(settings, log_path)
    print(json.dumps(report, indent=2, default=str))
    print("\nwindow coverage (vs RELIANCE weekdays):")
    for row in report.get("window_details") or []:
        flag = "GAP" if not row["recovered"] else "ok"
        print(
            f"  [{flag}] {row['symbol']:12} {row['window'][0]}->{row['window'][1]} "
            f"present={row['present_days']} missing={row['missing_n']} "
            f"recent_missing={row['recent_missing_n']}"
        )
    recent = report.get("recent_gaps_2026_04_onward") or []
    if recent:
        print("\n*** RECENT GAPS (2026-04 onward) — costly, re-pull required ***")
        for row in recent:
            print(
                f"  {row['symbol']} {row['window']}: "
                f"missing {len(row['recent_missing_vs_reference'])} days vs RELIANCE"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
