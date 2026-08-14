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

## Stages 7–9 (this build)

Account/order capture, decision-fill-outcome linkage, and decision-quality
scoring are **out of scope**. No real trading is happening. Stage 4 backtest
validates the model only — it must not emit trade records shaped for
Stages 7–9.

---

## Scheduling: local now, cloud later

**Now:** Windows Task Scheduler on the local PC for the after-close
sequence (auth → compaction → features → labels). See README.

**Later (not built now):** migrate the same scripts to a Mumbai-region VM
and cron, once the local pipeline is trusted. Do not dual-run.

---

## Pending confirmation: Part 1 minute granularity

Full-scale `07 --execute` is gated on choosing:

- **(a)** 1-minute bars for equities+spot from 2022-01-01 (~14.6k minute requests,
  ~1.7–3.4 hours), or
- **(b)** daily for the full range plus 1-minute for the last 365 days (~3.5k
  minute requests, ~30–60 minutes).

Set `historical.minute_lookback_days` to `365` for (b), or leave `null` for (a).
---
