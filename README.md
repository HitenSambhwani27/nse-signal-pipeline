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

### 5. Historical backfill

```powershell
python scripts/02_run_backfill.py
```

Writes 5-minute OHLCV+OI candles to `data/raw/{date}/{symbol}/candles.parquet`.

### 6. Inspect logged data

```powershell
python scripts/inspect_data.py
```

### 7. Run tests

```powershell
pytest
```

## Project layout

See [PROJECT_PLAN.md](PROJECT_PLAN.md) for staged milestones. Stage 1 covers ingestion + storage only.

## Configuration

- `config/settings.yaml` — symbols, paths, flush intervals, option chain width
- `config/baseline_weights.yaml` — Phase 1 placeholder weights (Stage 3+)
- `.env` — Kite credentials (never commit)

Default equity symbols: `RELIANCE`, `HDFCBANK`, `INFY`

Default options: NIFTY weekly expiry, ATM ± 10 strikes (CE + PE)

## Daily runbook (market days)

1. `python scripts/00_kite_auth.py` — refresh access token + instrument cache
2. `python scripts/01_run_ingestion.py` — run during market hours
3. After close: `python scripts/02_run_backfill.py` (optional catch-up)
4. `python scripts/inspect_data.py` — verify files and ingestion health

## Windows Task Scheduler (later)

Stage 8 will add weekly retrain scheduling. For ingestion, create a task that runs `01_run_ingestion.py` at 9:10 IST on weekdays after auth.


