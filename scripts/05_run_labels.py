#!/usr/bin/env python3
"""
Stage 3A — Triple-Barrier labeling + legacy comparison.

Usage:
  python scripts/05_run_labels.py --date 2026-08-13
  python scripts/05_run_labels.py --date 2026-08-13 --compare-legacy
  python scripts/05_run_labels.py --date 2026-08-13 --no-write

Partial-coverage days still run labeling and always print floor/cap binding.
Hourly legacy vs TBM comparison is deferred until a full open-to-close session.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.labels.batch import (  # noqa: E402
    format_binding_table,
    format_hourly_table,
    run_labeling,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Stage 3A labeling for a date")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD trade date")
    parser.add_argument(
        "--compare-legacy",
        action="store_true",
        default=True,
        help="Also compute flat +/-threshold_pct labels for comparison (default: on)",
    )
    parser.add_argument(
        "--no-compare-legacy",
        action="store_true",
        help="Skip legacy flat comparison",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Compute report only; do not update feature_log / label_audit",
    )
    parser.add_argument(
        "--force-hourly",
        action="store_true",
        help="Print hourly legacy vs TBM tables even on partial-coverage days",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = load_settings()
    compare = not args.no_compare_legacy

    try:
        report = run_labeling(
            settings,
            args.date,
            write_outcomes=not args.no_write,
            compare_legacy=compare,
        )
    except Exception as exc:
        print(f"Labeling failed: {exc}")
        return 1

    tbm = report["tbm_labels"]
    print(f"trade_date={report['trade_date']}")
    print(report["coverage_banner"])
    print(f"feature_rows={report['feature_rows']} matched={report['feature_rows_matched']}")
    print(f"horizon_bars={report['horizon_bars']}")
    print(f"hourly_compare_status={report['hourly_compare_status']}")
    print(f"report_path={report['report_path']}")
    print()

    # (1) Floor/cap binding — always shown, including partial days.
    print("=== TBM threshold binding (floor/cap vs dynamic) ===")
    print(
        f"n={tbm['n']}  floor={tbm['floor_bound_n']} ({tbm['floor_bound_pct']:.1f}%)  "
        f"cap={tbm['cap_bound_n']} ({tbm['cap_bound_pct']:.1f}%)  "
        f"dynamic={tbm['dynamic_used_n']} ({tbm['dynamic_used_pct']:.1f}%)"
    )
    print(
        f"  floor breakdown: missing_vol={tbm['floor_missing_vol_n']} "
        f"({tbm['floor_missing_vol_pct']:.1f}%)  "
        f"below_min={tbm['floor_below_min_n']} ({tbm['floor_below_min_pct']:.1f}%)"
    )
    print(format_binding_table(tbm, "TBM binding by hour (IST)"))
    print()
    if report.get("tbm_by_track"):
        print("=== TBM floor binding by track ===")
        for track, s in sorted(report["tbm_by_track"].items()):
            print(
                f"  {track}: n={s['n']} floor={s['floor_bound_pct']:.1f}% "
                f"cap={s['cap_bound_pct']:.1f}% dynamic={s['dynamic_used_pct']:.1f}% "
                f"(missing_vol={s['floor_missing_vol_pct']:.1f}% "
                f"below_min={s['floor_below_min_pct']:.1f}%)"
            )
        print()

    # (2) Hourly compare — full sessions only (unless --force-hourly).
    show_hourly = report["hourly_compare_eligible"] or args.force_hourly
    if not show_hourly:
        print(
            "=== Hourly legacy vs TBM comparison: PENDING ===\n"
            "Skipped for this date (partial coverage). Re-run after a full "
            "open-to-close session is captured; results are flagged so they "
            "are not treated as full-day equivalents."
        )
        return 0

    if report["coverage"]["status"] != "full":
        print(
            "WARNING: forcing hourly tables on a non-full session "
            f"(coverage={report['coverage']['status']})"
        )
        print()

    print(format_hourly_table(tbm, "=== TBM labels by hour (IST) ==="))
    print()
    if compare and report.get("legacy_labels"):
        leg = report["legacy_labels"]
        print(
            format_hourly_table(
                leg, "=== Legacy flat +/-0.15% labels by hour (IST) ==="
            )
        )
        print()
        print("=== Overall label mix ===")
        print(
            f"TBM:    up={tbm['up_pct']:.1f}% down={tbm['down_pct']:.1f}% "
            f"flat={tbm['flat_pct']:.1f}% (n={tbm['n']})"
        )
        print(
            f"Legacy: up={leg['up_pct']:.1f}% down={leg['down_pct']:.1f}% "
            f"flat={leg['flat_pct']:.1f}% (n={leg['n']})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
