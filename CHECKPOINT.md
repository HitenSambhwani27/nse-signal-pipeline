# NSE pipeline checkpoint

Written: 2026-08-16 ~18:22 IST (session still open; depth labels still running).

## Will depth labels survive closing this session?

**No.** The depth-label job is a Cursor-child process (PowerShell PID 13224 → python 12676, started 12:03 IST). Closing this chat/IDE/session will kill it the same way the overnight historical backfill died. It is not detached (not Task Scheduler, not a user-owned `Start-Process` with a log file).

To keep it alive across close, you would have to start it yourself in a standalone PowerShell window that is not a Cursor child, with sleep disabled.

## Pipeline state

### Historical backfill — DONE

- `scripts/07_backfill_historical.py --execute --start-date 2022-01-01`
- 590/590 symbols. Last: `NIFTY26SEPFUT` at 2026-08-15T13:17:46Z.
- Do **not** re-run `--execute`.

### Features

| Track | Dates | Span | Notes |
|---|---:|---|---|
| equity_depth | 246 | 2025-08-18 → 2026-08-14 | Complete. Keep. |
| equity_quote | 246 | 2025-08-18 → 2026-08-14 | Complete (`--workers 4`, exit 0 at 15:48 IST). Same date set as depth. |
| options | 56 | 2026-05-27 → 2026-08-14 | Complete (live-contract lifetime). |
| futures | 56 | 2026-05-27 → 2026-08-14 | Complete. |
| index spot | — | — | No Stage 2 feature track. Exclude from labels/baseline/harness. |

Last **date** is 2026-08-14 on every feature track. FO starts later (contract lifetime), which is expected.

### Labels

| Track | Status | Dates | Span |
|---|---|---:|---|
| options + futures | **DONE** (exit 0, ~61 min) | 56/56 | 2026-05-27 → 2026-08-14 |
| equity_depth | **IN PROGRESS** (Cursor child) | ~174+/246 as of 18:21 IST | log last printed `2026-05-05`; sqlite still also holds leftover `2026-08-13` stub until the job rewrites that date |
| equity_quote | **NOT STARTED** (held until depth labels finish) | 1 leftover live date only | `2026-08-13`, 800 TBM rows |

Depth labels command currently running:

```text
.\.venv\Scripts\python.exe scripts\05_run_labels.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks equity_depth --no-compare-legacy
```

Pace after quote features exited: ~47 s/day immediately, ~105 s/day over the post-quote span, last-10 intervals ~223 s/day. Remaining after 2026-04-17 was 82 dates; the job has since moved into May 2026.

### Baseline and walk-forward — NOT STARTED

- No `06_run_baseline.py` / `08_run_backtest.py` process.
- `signal_log` is still only the live 2026-08-13 slice.
- Blocked on: depth labels finishing, then quote labels, then these.

Stop before `09_train_models.py` (Part 6) unless explicitly requested.

## Partial-day resolutions

### 2026-08-13 — live-pilot, **excluded from scoring/harness**

This is the original ~10-minute live capture (equity timestamps 12:00–12:10 IST / 06:30–06:40 UTC). Feature rows:

- equity_depth: `source=live`, `live_full`, 300 rows
- equity_quote: `source=live`, `live_quote`, 1200 rows
- futures/options: mostly live 10-minute slice; 2 option names also have historical span (that is what made FO labels log `session_coverage=full`)

**Exclusion (code, 2026-08-16):** `scoring_skip_trade_dates()` in `src/nse_pipeline/session_coverage.py`.

- Always skips `2026-08-13`.
- Also skips any date whose **equity** rows are `source=live` and span **< 2 hours**.
- Wired into `run_baseline_scorer` (`06_run_baseline.py`) and `run_walk_forward` (`08_run_backtest.py`). Skipped dates are dropped before scoring; baseline also deletes any existing `signal_log` rows for those dates.

**Coverage tagging (code, 2026-08-16):** `assess_session_coverage` now classifies from **cash-session symbols** (equity_depth + equity_quote + index in the instrument cache), not whichever symbols the current job sampled. FO jobs can no longer mark this day `full` because two option contracts have a long tick span.

Existing `ingestion_meta` rows for 2026-08-13 still mix old `full` and `partial` events. New jobs will write `partial` from the cash-session span. Scoring does **not** trust `ingestion_meta`; it uses the skip helper above.

### 2025-10-21 — Muhurat half-day, **keep**

NSE Muhurat session 13:45–14:45 IST. Quote 4,692 rows = 391 symbols × 12 five-minute buckets. Tagged `session_coverage=partial`, `hourly_compare_status=pending_full_session`. Historical `source`, not live-pilot. **Not** in the scoring skip set.

## Resume commands (if this session closes)

Labeling is **idempotent per date** (delete+insert that date/track). It does **not** skip completed dates by itself. You do **not** need to restart from 2025-08-18.

1. After close, check last fully written depth-label date (ignore `2026-08-13` until the job reaches the end):

```powershell
$env:PYTHONPATH = "C:\Users\sambh\nse-signal-pipeline\src"
.\.venv\Scripts\python.exe -c "import sqlite3; from nse_pipeline.config import load_settings; c=sqlite3.connect(load_settings().paths.sqlite_db); print(list(c.execute(\"select trade_date, count(*) from label_audit where track='equity_depth' and mode='triple_barrier' and trade_date!='2026-08-13' group by trade_date order by trade_date desc limit 5\")))"
```

2. Resume depth labels from that date **inclusive** (rewrites one already-complete day, then continues). Example if sqlite last full day is `2026-05-04`:

```powershell
cd C:\Users\sambh\nse-signal-pipeline
$env:PYTHONUNBUFFERED = "1"
.\.venv\Scripts\python.exe scripts\05_run_labels.py --start-date 2026-05-04 --end-date 2026-08-14 --tracks equity_depth --no-compare-legacy
```

As of this file, the log last printed `2026-05-05`. Prefer the sqlite query above over the log if the process was killed mid-date.

**Run this in a standalone PowerShell window** (not Cursor) if you need it to survive another close.

### After depth labels reach 246 dates through 2026-08-14

Quote labels (held until depth is done):

```powershell
.\.venv\Scripts\python.exe scripts\05_run_labels.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks equity_quote --no-compare-legacy
```

Then baseline and harness per track (2026-08-13 will be skipped automatically):

```powershell
.\.venv\Scripts\python.exe scripts\06_run_baseline.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks options
.\.venv\Scripts\python.exe scripts\06_run_baseline.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks futures
.\.venv\Scripts\python.exe scripts\06_run_baseline.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks equity_depth
.\.venv\Scripts\python.exe scripts\06_run_baseline.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks equity_quote

.\.venv\Scripts\python.exe scripts\08_run_backtest.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks options --model-id baseline_options
.\.venv\Scripts\python.exe scripts\08_run_backtest.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks futures --model-id baseline_futures
.\.venv\Scripts\python.exe scripts\08_run_backtest.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks equity_depth --model-id baseline_depth
.\.venv\Scripts\python.exe scripts\08_run_backtest.py --start-date 2025-08-15 --end-date 2026-08-14 --tracks equity_quote --model-id baseline_quote
```

Options/futures labels are already complete, so those two baseline/harness commands can run as soon as you want (they do not need to wait on depth/quote labels). Depth/quote baseline still wait on their label jobs.

## Do not

- Re-run historical `--execute`
- Start quote labels while depth labels are still writing sqlite
- Start Stage 6+ (`09_train_models.py`) from backtest output
- Treat `2026-08-13` live rows as a full session
