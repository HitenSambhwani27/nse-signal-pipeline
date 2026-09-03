# Architecture

Frozen contracts for the two-repo layout: the VM pipeline is the source of truth;
the dashboard is a read-only HTTP client. Algorithms plug in later behind Protocols.
This document is the skeleton. Do not treat draft logistic/baseline/Markov files as
production models.

## Layers

1. **Data (VM)** — Kite ingest → Parquet → DuckDB compact → date-scoped features/labels → SQLite WAL.
2. **Intelligence (VM)** — `FeatureProcessor` / `LabelProcessor` → `processing_status`; `Algorithm` plugins → `SignalEngine` gated by `MaturityGate`; public view only.
3. **Presentation** — FastAPI `/api/v1` (`UiReadModel`) → `nse-signal-dashboard` (or JSON fixtures).

The dashboard never sees algorithms, `features_json`, or SQLite.

## Plug-in contracts

Module: `src/nse_pipeline/contracts/`.

| Contract | Responsibility | Must not do |
|---|---|---|
| `FeatureProcessor` | ticks/day → feature rows | score, call Kite |
| `LabelProcessor` | feature rows → outcomes | train models |
| `MaturityGate` | pooled live days → public view | invent probabilities |
| `Algorithm` | feature row → optional raw score | bypass the gate |
| `SignalEngine` | Algorithm outputs + gate → persistable signal | place orders |
| `AccountCapture` | REST poll → account tables | change signals |
| `DecisionTracker` | mint/link `trade_id` | score quality |
| `OutcomeEvaluator` | closed trades → 2×2 | use hindsight best-entry |
| `RetrainEngine` | versioned train; never auto-promote | overwrite models |
| `UiReadModel` | compact DTOs for FastAPI | load joblib |

Unfinished algorithms use `UnavailableAlgorithm` (`available=False`, null probability,
`reason=algorithm_not_implemented`). Missing harness-passed models use
`reason=no_harness_passed_model`. Never emit a random number.

## API (`/api/v1`, GET only)

Envelope on every response:

```json
{
  "maturity": {
    "equity": {},
    "futures": {},
    "options_nifty": {},
    "options_banknifty": {}
  },
  "as_of": "ISO-8601"
}
```

Maturity objects are produced only by `maturity_public_view`. FastAPI does not
format N/60. Maturity is cached ~30s in-process.

| Endpoint | Returns |
|---|---|
| `GET /api/v1/overview` | maturity + last ingest/feature/signal timestamps + signal counts |
| `GET /api/v1/maturity` | the four public views |
| `GET /api/v1/signals` | latest public signals (no `features_json`, no `attribution`) |
| `GET /api/v1/account` | latest positions/margins + compact fills |
| `GET /api/v1/decisions` | decisions + 2×2 counts (empty list is valid) |
| `GET /api/v1/health` | ingest/compact/coverage + `processing_status` |

`/v1/*` aliases remain for one release, then drop.

The API process is **read path only**: no `LiveSignalEngine`, no Kite client.

## SQLite (WAL on the VM)

Keep ticks in Parquet. Do not add FKs. `trade_id` is an application key.

`processing_status` records `features|labels|signals|account` checkpoints. Until a
job writes a row, health shows `status=unknown`.

Indexes: `feature_log(source, track, trade_date)`, `feature_log(trade_date)`,
`label_audit(trade_date, track)`, `signal_log(trade_date, maturity_tier)`,
`ingestion_meta(event_type, timestamp)`.

## Dashboard

Repo: `nse-signal-dashboard`. Pages: Overview, Signals, Account, Decisions, Health.
`MaturityBanner` on every page.

- Fixtures: `NSE_API_URL=mock` or `NSE_USE_FIXTURES=1`.
- Default fixture: suppressed, `pooled_live_days=2`, display
  `insufficient data, 2/60 pooled days`, `probability=null`.
- Live: SSH tunnel to VM `127.0.0.1:8080`. No credentials in the dashboard repo.

## Deployment

Example units live in `deploy/vm/*.example`. On the Bangalore VM the **API**,
**live signal watch**, and **account capture** units are installed and enabled
(`nse-api`, `nse-live-signals`, `nse-account-capture`). Ingest stays the
existing VM process — do not start a second copy from the PC.

Weekly retrain timer examples are files only until enabled. `auto_promote` stays false.

## What must not change in this skeleton

- Ingest (`scripts/01_run_ingestion.py`, websocket listener)
- Maturity thresholds 10 / 60 and pooled (not per-contract) options counting
- Historical backfill execute path
- Feature/label formulas (wrap only)
- `retrain.auto_promote=false`; no `09_train_models.py` as a trusted live model
- No order placement, no second dashboard in the pipeline repo
- No Postgres / Redis / Nifty monthly options / IV solver / hedge engine
