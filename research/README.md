# Research vs production

This tree holds **preserved** work that is not part of the live VM path
(`ingest → compact → features → labels → SQLite → API`).

Do not import these modules from `nse_pipeline` production jobs. Run scripts
from the repo root with `PYTHONPATH=src`.

| Path | What lives here |
|---|---|
| [INVENTORY.md](INVENTORY.md) | Classification of every algorithm, result, and ops helper |
| `ops/` | One-off SQLite/backfill diagnostics (formerly `scripts/_*.py`) |
| `checkpoints/` | Session state snapshots (not a live runbook) |

Production entrypoints remain `scripts/00_*.py` … `scripts/14_*.py`.
