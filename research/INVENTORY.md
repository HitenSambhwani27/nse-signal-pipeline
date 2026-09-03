# Research and algorithm inventory

Written 2026-09-03 as part of the architecture cleanup. **No historical
Parquet, SQLite, or model files were deleted.** Local `data/` and VM
`data/` stay gitignored.

Classification: **ACTIVE** (live or after-close path) · **USEFUL BUT NOT
CONNECTED** (keep, not on the VM runtime) · **EXPERIMENTAL** (draft math,
do not promote) · **OBSOLETE** · **UNKNOWN**.

Status **EXPERIMENTAL** and **USEFUL BUT NOT CONNECTED** were not deleted.

---

## Algorithms and models

| Name | Purpose | Inputs | Features | Output | Status | Implementation | Reproducible | Deps | Artifacts | Connected to live |
|---|---|---|---|---|---|---|---|---|---|---|
| UnavailableAlgorithm | Skeleton stand-in; never invents a probability | feature row | none | `available=False`, null score | ACTIVE | `src/nse_pipeline/algorithms/unavailable.py` | yes | none | n/a | yes (default when no harness-passed pair) |
| LogisticAlgorithm (adapter) | Wraps existing logistic pair; does not rewrite fit/score math | feature row + harness-passed joblib | coarse/fine names in settings | p_up, signed score, research attribution | ACTIVE adapter / EXPERIMENTAL model | `src/nse_pipeline/algorithms/logistic.py` → `models/logistic.py` | yes if labeled `feature_log` + `09_train_models.py` | sklearn, joblib | `data/models/{class}/` (gitignored) | only if harness-passed files exist on disk (none on VM as of 2026-09-03) |
| Logistic fit (`fit_logistic`) | Coarse/fine binary up-vs-rest | labeled feature rows | `training.coarse_features` / `fine_features` | Pipeline + coefficients | EXPERIMENTAL | `src/nse_pipeline/models/logistic.py` | yes | sklearn | versioned joblib via registry | no auto-load without harness |
| Markov OI 4×4 | Regime-fragile transition matrices | options feature rows | OI state, DTE | JSON matrices per underlying × expiry-week | EXPERIMENTAL | `src/nse_pipeline/models/markov.py` (TRADE_OFFS) | yes | numpy | `data/models/options/markov/latest.json` | written by `train.py`, **not** used by live engine |
| Options panels | Moneyness × DTE pooling | options rows | moneyness, DTE | panel tables | EXPERIMENTAL | `src/nse_pipeline/models/panel.py` | yes | pandas | inside train report | training only |
| Baseline weighted scorer | Placeholder Stage 3B scores | feature_log | `config/baseline_weights.yaml` | signed score + attribution | EXPERIMENTAL | `src/nse_pipeline/scoring/baseline.py` | yes | yaml | sqlite `signal_log` if `06` is run | **not** the live engine |
| Walk-forward harness | Four-way OOS gate | models + features | class-specific | harness_passed flag | USEFUL BUT NOT CONNECTED to API | `src/nse_pipeline/backtest/harness.py` | yes | sklearn | metadata on model files | live engine requires `harness_passed` |
| Decision-quality 2×2 | Anti-hindsight entry/exit | decision_log + fills | frozen p at entry | GG/GB/BG/BB | ACTIVE scaffold | `src/nse_pipeline/quality/review.py` | synthetic tests yes | none | `outcome_log` | not used for promotion (`auto_promote: false`) |

## Feature engineering

| Name | Purpose | Status | Where |
|---|---|---|---|
| Equity depth/quote features | OFI, depth ratio, spread, VWAP deviation | ACTIVE | `src/nse_pipeline/features/equity.py`, `tick_stats.py` |
| Options features | PCR, Greeks (Black-Scholes), DTE, VWAP | ACTIVE | `src/nse_pipeline/features/options.py`, `greeks.py` |
| Futures features | basis, VWAP, live L2 | ACTIVE | `src/nse_pipeline/features/futures.py` |
| Quality gates | freeze, overnight gap, CA flag | ACTIVE | `src/nse_pipeline/features/quality.py` |
| Date-scoped batch | DuckDB ticks → `feature_log` | ACTIVE | `src/nse_pipeline/features/batch.py` + `ExistingFeatureProcessor` |

Do not rewrite these formulas to “fit architecture”; processors wrap them.

## Labels, signals, backtest

| Name | Purpose | Status | Where |
|---|---|---|---|
| Triple-barrier labels | TBM vs legacy flat | ACTIVE | `src/nse_pipeline/labels/triple_barrier.py`, `labels/batch.py` |
| Maturity gate | pooled live days, N/60 public view | ACTIVE | `src/nse_pipeline/signals/maturity.py` |
| Live engine | Algorithm + gate → `signal_log` | ACTIVE | `src/nse_pipeline/signals/engine.py` |
| Attribution text | coefficient breakdown (research JSON only) | USEFUL BUT NOT CONNECTED to API | `src/nse_pipeline/signals/attribution.py` |
| Backtest costs/metrics | transaction costs, summary stats | USEFUL BUT NOT CONNECTED | `src/nse_pipeline/backtest/costs.py`, `metrics.py` |
| `08_run_backtest.py` | historical walk of baseline | EXPERIMENTAL | `scripts/08_run_backtest.py` |

## Data and reproducibility

| Item | Kind | Status | Location | Notes |
|---|---|---|---|---|
| Live Parquet ticks | live data | ACTIVE | VM `data/raw/{date}/` | gitignored; do not copy off VM |
| Compacted ticks | live/training precursor | ACTIVE when compact runs | `data/compacted/` | empty on VM as of 2026-09-03 |
| Historical candles | training / backtest | USEFUL | `data/` via `07_backfill_historical.py` | do not re-run `--execute` (CHECKPOINT) |
| SQLite WAL | live + labels + signals | ACTIVE | `data/nse_pipeline.db` | gitignored; PC copy is not production |
| Membership CSVs | universe config | ACTIVE | `config/membership/` | needed to reproduce universe |
| `settings.yaml` | all knobs | ACTIVE | `config/settings.yaml` | maturity 10/60, auto_promote false |
| `baseline_weights.yaml` | baseline experiment | EXPERIMENTAL | `config/baseline_weights.yaml` | required to reproduce `06`/`08` |
| Instruments cache | runtime | ACTIVE | `config/instruments_cache.json` | gitignored; refresh via auth |
| Trained joblib | model artifacts | EXPERIMENTAL / absent on VM | `data/models/` | gitignored |
| Feature–outcome correlation script | research | USEFUL BUT NOT CONNECTED | `research/ops/_feature_outcome_corr.py` | Pearson vs `actual_outcome`; not a live signal |

## Production scripts (keep in `scripts/`)

`00_kite_auth.py`, `01_run_ingestion.py`, `02_run_backfill.py` (short Stage 1 lookback), `03_run_compaction.py`, `04_run_features.py`, `05_run_labels.py`, `06_run_baseline.py` (experimental scorer CLI), `07_backfill_historical.py`, `08_run_backtest.py`, `09_train_models.py` (do not run as trusted live), `10_run_live_signals.py`, `11_run_retrain.py`, `12_run_account_capture.py`, `13_log_decision.py`, `14_run_ui_api.py`, `inspect_data.py`, `verify_stage1.py`.

`02` is **not** superseded by `07` and is **not** archived. `02_run_backfill.py` writes Stage 1 `candles.parquet` via `backfill_from_cache` (short lookback, `settings.historical.interval`). `07_backfill_historical.py` writes compacted tick partitions for the long execute path. Different outputs; both stay.

`tests/test_decision_quality.py` is **permanent**: Stage 9 evidence entrypoint (synthetic 2×2 / anti-hindsight). It imports the implementation test from `test_account_decisions.py`; do not delete the alias.

## Ops diagnostics (moved to `research/ops/`)

Backfill/feature progress SQL helpers. Not on the systemd path. Preserve for resume and coverage audits.

## Documentation

| File | Status |
|---|---|
| `docs/ARCHITECTURE.md` | ACTIVE frozen contracts |
| `README.md` | ACTIVE |
| `TRADE_OFFS.md` | ACTIVE product constraints |
| `MASTER_REFERENCE.md` | USEFUL product intent |
| `PROJECT_PLAN.md` | USEFUL staged plan |
| `research/checkpoints/` | USEFUL session snapshots |

## Dashboard (`nse-signal-dashboard`)

All Streamlit pages, fixtures, and banner tests are **ACTIVE**. No pipeline import. No data files to archive.
