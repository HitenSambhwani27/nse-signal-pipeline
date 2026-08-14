# NSE Order-Book / OI Signal Pipeline

Research pipeline for NSE equity order-book and Nifty options OI signals. **Does not place orders.**

## Quick start

### 1. Prerequisites

- Python 3.11+
- Zerodha account with **Kite Connect API** subscription ([kite.trade](https://kite.trade/))
- **Market depth** subscription for NSE cash + F&O (required for 5-level book)

### 2. Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
copy .env.example .env
```

Edit `.env` with your `KITE_API_KEY` and `KITE_API_SECRET`.

### 3. Kite Connect setup (first time)

1. Register a Kite Connect app at [kite.trade](https://kite.trade/) and note API key/secret.
2. Set redirect URL in the app (e.g. `http://127.0.0.1/` — you only need the `request_token` from the redirect).
3. Enable **market depth** on your Zerodha account for symbols you will track.
4. Run daily auth:

```powershell
python scripts/00_kite_auth.py
```

This opens a login URL, exchanges `request_token` for `access_token`, saves it to `.env`, runs a smoke test, and refreshes `config/instruments_cache.json`.

**Note:** Kite access tokens expire daily (~6 AM IST next day). Re-run auth each trading day.

### 4. Run ingestion (market hours)

```powershell
python scripts/01_run_ingestion.py
```

Writes ticks to `data/raw/{date}/{symbol}/ticks_*.parquet` and logs events to SQLite `ingestion_meta`.

Press `Ctrl+C` to stop gracefully (flushes buffers).

### 5. Historical backfill (Part 1 — confirm estimate first)

Stage 1 short lookback (unchanged, do not use for the 2022- range):

```powershell
python scripts/02_run_backfill.py
```

Long historical pull into the compacted layout (`source=historical`):

```powershell
python scripts/07_backfill_historical.py --estimate
# small test:
python scripts/07_backfill_historical.py --probe RELIANCE,INFY,"NIFTY 50"
# only after confirming scenario (a) vs (b):
python scripts/07_backfill_historical.py --execute --start-date 2022-01-01
```

Does not overwrite live `ticks.parquet`. Daily bars go to `daily.parquet`.

### 6. After-close compaction (Stage 1H)

Merge per-minute tick parts into one Parquet file per symbol/day, then query via DuckDB:

```powershell
python scripts/03_run_compaction.py --date 2026-08-12 --verify
# or all dates with minute parts:
python scripts/03_run_compaction.py --verify
```

Layout:

- Raw (ingestion): `data/raw/{YYYY-MM-DD}/{symbol}/ticks_HHmm.parquet`
- Compacted: `data/compacted/{YYYY-MM-DD}/{symbol}/ticks.parquet`

Example DuckDB access from Python:

```python
from nse_pipeline.config import load_settings
from nse_pipeline.storage.duckdb_store import DuckDBTickStore

settings = load_settings()
with DuckDBTickStore(settings) as store:
    df = store.read_ticks("2026-08-12", "RELIANCE")
    counts = store.tick_row_counts("2026-08-12")
```

### 7. Inspect logged data

```powershell
python scripts/inspect_data.py
```

### 8. Run tests

```powershell
pytest
```

## Project layout

See [PROJECT_PLAN.md](PROJECT_PLAN.md) for staged milestones. Stage 1G expands the universe; Stage 1H adds compaction + DuckDB before feature engineering.

## Configuration

- `config/settings.yaml` — universe, paths, flush intervals, signal horizon, compaction
- `config/baseline_weights.yaml` — Phase 1 placeholder weights (Stage 3+)
- `.env` — Kite credentials (never commit)

Universe (Stage 1G): Nifty 100 full depth, Nifty 500\\Nifty 100 quote-only,
Nifty **weekly** options + Bank Nifty monthly options, Nifty + Bank Nifty
futures, index spots. Nifty monthly options are out of scope ([TRADE_OFFS.md](TRADE_OFFS.md)).

Every compacted / feature / signal row is tagged `source=historical` or
`source=live`. A last-N-days query reads both; historical does not grow after
the one-time backfill.

## Daily runbook (market days)

1. `python scripts/00_kite_auth.py` — refresh access token + instrument cache (+ NSE membership)
2. `python scripts/01_run_ingestion.py` — run during market hours
3. After close: `python scripts/03_run_compaction.py --date YYYY-MM-DD --verify`
4. `python scripts/04_run_features.py --date YYYY-MM-DD`
5. `python scripts/05_run_labels.py --date YYYY-MM-DD`
6. Optional: `python scripts/06_run_baseline.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`

One-time / incremental: `scripts/07_backfill_historical.py` (confirm `--estimate` first).
Weekly: `scripts/11_run_retrain.py` after models exist.

## Windows Task Scheduler (local PC — now)

Create these weekday tasks (all run from the repo root with the venv Python).
Set "Start in" to the repo path. Tokens expire ~6 AM IST — auth must run first.

| Task | Trigger (IST) | Command |
|---|---|---|
| NSE_Auth | 08:50 weekdays | `.\.venv\Scripts\python.exe scripts\00_kite_auth.py` |
| NSE_Ingest | 09:10 weekdays (manual start still OK) | `.\.venv\Scripts\python.exe scripts\01_run_ingestion.py` |
| NSE_Compact | 15:45 weekdays | `.\.venv\Scripts\python.exe scripts\03_run_compaction.py --verify` |
| NSE_Features | 16:00 weekdays | `.\.venv\Scripts\python.exe scripts\04_run_features.py --date` *today* |
| NSE_Labels | 16:30 weekdays | `.\.venv\Scripts\python.exe scripts\05_run_labels.py --date` *today* |
| NSE_Retrain | Sunday 18:00 | `.\.venv\Scripts\python.exe scripts\11_run_retrain.py` |

For `--date` tasks, wrap in a one-liner that injects today's IST date, e.g.:

```powershell
$d = (Get-Date).ToString("yyyy-MM-dd")
.\.venv\Scripts\python.exe scripts\04_run_features.py --date $d
```

Cloud VM (Mumbai) + cron is documented as a later migration in TRADE_OFFS.md — not built now.



