# Trade-offs and accepted constraints

Living log of decisions that later stages must not silently reverse.
Authoritative product intent still lives in MASTER_REFERENCE.md; this file
records *why* we accepted a limitation or excluded a product.

---

## Options / futures minute-history ceiling (Kite)

**Constraint:** Once an options or futures contract expires, Kite
`historical_data()` has **no minute-level history** for it. Only the
currently listed (live) contract returns real minute bars. Nifty weekly
options therefore backfill days-to-weeks; Bank Nifty monthly options up to
~one month; index futures similarly bound to the live near/next contracts.

This is an exchange/vendor ceiling, not a bug. Coverage reports must
compare F&O retrieval against **contract lifetime**, not against the equity
2022-01-01 benchmark. Do not pad, interpolate, or synthesize missing
expired-contract bars.

**Trigger to revisit:** Stolo (or similar vendor) integration for expired
contract 1-minute history — *not in this build*. See MASTER_REFERENCE.md
Tier 2.

---

## Nifty monthly options — out of scope

NSE lists **two** Nifty option series: weekly (every Tuesday since Sept 2025)
and monthly (last Tuesday). This system targets **intraday** signals; weekly
Theta/Gamma dynamics match that horizon. Pooling weekly and monthly contracts
into one training panel would recreate the short-vs-long contract mismatch
already forbidden for the coarse-model panel (Nifty weekly vs Bank Nifty
monthly must stay separate).

If monthly Nifty options are added later they need their **own pooling
category**, never merged into the weekly panel.

Bank Nifty options are monthly-only (weekly discontinued Nov 2024) — that
series *is* in scope; it is the only Bank Nifty options series.

Equity options and single-stock futures remain out of scope.

---

## Roll-cadence: Nifty weekly vs Bank Nifty monthly

Nifty options roll every week; Bank Nifty options roll every month. Any
logic that pools observations across a rolling contract sequence must treat
the two indices separately:

- Coarse-model training panel (moneyness × DTE, not contract identity)
- Maturity-gate **pooled cumulative trading days** (not per-contract)

"60 days of Nifty options" means 60 cumulative trading days across many
weekly contracts. The same calendar window covers fewer, longer Bank Nifty
monthly contracts. A uniform 60-day-per-contract rule would leave Nifty
options permanently "provisional" — that would be a bug.

---

## Markov OI model — regime-fragile / stationarity

The 4×4 OI-state transition matrix assumes (weak) stationarity of the
buildup process. Expiry-week flow is a different regime from non-expiry-week
flow, and "expiry week" itself means a different cadence for Nifty (every
week) vs Bank Nifty (once a month). First split: expiry-week vs
non-expiry-week **per underlying**, not one matrix for all index options.
Treat fitted matrices as regime-fragile; do not promote them as unconditionally
stable.

---

## No historical Level-2 depth

Nobody sells cheap historical 5-level books. Historical rows therefore
cannot compute OFI, bid/ask depth ratio, or spread. Those features are
**skipped** (not null-filled, not imputed) and tagged
`feature_completeness=historical_partial`. The live archive we capture
ourselves is the only depth history. Fine models start weak and improve as
live days accumulate.

---

## Stages 7–9 (built, not live-trading)

Account capture, decision/fill/outcome linkage, and decision-quality scoring
are implemented. They do **not** place orders. The live signal engine still
refuses a public probability until 60 pooled live days. Stage 4 backtest
must not mint `decision_log` rows.

---

## Scheduling: VM ingest + after-close jobs

Live ingest runs on the Bangalore droplet. Do **not** dual-run the PC and the
VM on the same session. Cron snippets for the VM (not installed by this
change — copy when you are ready) live in README.

---

## Corporate-action adjustment — deferred (why scenario (b))

Kite historical OHLCV is **not** split/bonus adjusted. A 1:2 split shows up as
an overnight close-to-open gap of ~50%. Full point-in-time corporate-action
adjustment (NSE CA feed → back-adjust or forward-adjust prices, volumes, and
barriers) is **not built**.

What exists now: a cheap quality-gate **flag** on equity historical rows.
Overnight |close→open| ≥ `features.corporate_action_gap_pct` (15%) with **no**
corresponding index-wide move (`features.corporate_action_index_wide_pct`, 5%)
is written to `quality_log` as `corporate_action_suspect` and stamped on that
day's feature rows. Rows are **kept** — not dropped, not auto-corrected.
Manual review only.

This is why Part 1 is running **scenario (b)** (daily 2022→present + 1-minute
last 365 days) instead of (a) (1-minute back to 2022-01-01). Deeper minute
history multiplies unadjusted split/bonus distortions in VWAP, returns, vol
windows, and labels. One year of minute bars is enough to stand up the engine
while the CA problem stays bounded.

**Trigger to revisit:** before any backfill deeper than 365 days of minute
bars. Do not expand `historical.minute_lookback_days` past 365 until a real
CA adjuster exists.

---

## Minute granularity — decided: scenario (b)

`historical.minute_lookback_days: 365`. Daily OHLCV still covers 2022-01-01
forward. See the corporate-action entry above for the reason.
---
