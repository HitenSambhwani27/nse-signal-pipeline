# NSE Quant Trading System — Master Reference

This is the single consolidated reference for everything discussed across our
sessions. Feed this to Cursor alongside the existing `PROJECT_PLAN.md` so any
new stage work stays consistent with everything already decided.

---

## 1. Complete list of everything covered so far

### Phase 1 — Foundations (completed, doc delivered)
**Shared:** OI buildup/unwinding (4-state table), Greeks refresher (Delta,
Gamma, Theta, Vega, Rho), Alpha vs Beta, order book (bid/ask/spread/depth,
price-time priority), basic probability, conditional probability & Bayes'
theorem, ratios/weighted averages/linear equations, vectors.
**Equity track:** Order Flow Imbalance (OFI), toy linear prediction model.
**Options track:** OI-across-strikes reading, Put-Call Ratio (used correctly,
paired with buildup direction — not standalone), Greeks in motion intraday,
toy Nifty options edge model.

### Phase 2 — Signals to Probabilities (completed, doc delivered)
**Shared:** probability distributions (Binomial, Normal), Expected Value &
Variance, Markov chains (OI-state transition matrices), linear algebra
(matrices, dot products), linear regression, logistic regression (sigmoid,
replacing guessed weights with fitted ones), PCA, time series basics
(stationarity, autocorrelation), backtesting fundamentals (walk-forward
validation, overfitting detection, transaction costs).
**Equity track:** fitted logistic regression on order-book features, full
walk-forward backtest.
**Options track:** Markov projection on OI states, fitted logistic
regression combining OI + PCR + Greeks, full walk-forward backtest.

### Phase 3 — not yet built (deferred; building Phase 1+2 system first,
learning Phase 3 concepts in parallel per agreed plan)
Covers: calculus for random processes, Brownian motion, Itô's lemma,
volatility modeling, Black-Scholes properly, Kelly criterion / position
sizing, HFT reality check.

### Data sourcing (researched, decided)
- Tier 0 (free): NSE bhavcopy, `nsepython`/`jugaad-data`, free daily
  option-chain snapshot sites — daily-resolution only.
- Tier 1 (chosen starting point): **Kite Connect**, ₹500/month — live
  WebSocket + 1-min historical candles with OI included. Gap: expired
  option contract data gets wiped by the exchange, Kite won't retain it.
- Tier 2 (later, once expiry-wipe becomes a real limitation): **Stolo** —
  4 years of 1-min options+OI history including expired contracts.
- Tier 3 (later, only if genuinely needed): GDFL/TrueData tick-level
  vendors, ~₹1,500-2,500+/month, for true order-book depth history beyond
  what we log ourselves going forward.
- **Known gap, accepted as permanent for now:** no one sells cheap
  historical Level-2 order-book depth. We are building our own archive of
  it going forward, from today, via our own WebSocket logger — this is
  why ingestion (Stage 1) starting as early as possible matters.

### Architecture (agreed, mirrors the diagram already shown)
Data sources (Kite) → Ingestion & storage (WebSocket logger, Parquet/SQLite)
→ Feature engineering (OFI, OI buildup, Greeks, PCR) → Model layer (Phase 1
rules + Phase 2 regression/Markov) → Signal engine + dashboard (Streamlit)
→ weekly retrain loop feeding back into the model layer.

### Cursor project (in progress)
- `PROJECT_PLAN.md` already created by Cursor (Stage 0 → Stage 8, detailed
  milestone breakdown, folder structure under `nse-signal-pipeline/`).
- Currently on **Stage 0/1** (scaffold + Kite auth + ingestion) — Stage 1
  explicitly gated for manual review before Stage 2 begins.
- Decided: build locally (not Cursor's cloud build) because Stage 1 needs a
  live WebSocket holding real broker credentials during market hours.
- Tech stack: Python 3.11+, pandas/numpy/scikit-learn/scipy, Parquet +
  SQLite, Streamlit dashboard, `.NET/C#` reserved for a possible future
  execution/frontend service behind a clean API boundary — not used in the
  core pipeline.
- Cursor subscription: started on **Cursor Start** (₹649/mo, India-only,
  Grok 4.5 + Composer, no Auto Mode/Bugbot/frontier-model choice). Agreed
  upgrade trigger: if Grok 4.5 shows subtle errors specifically on the
  math-heavy stages (Greeks, IV solver, backtest engine, payoff calculator),
  upgrade to Pro ($20/mo) for frontier-model access + Bugbot on those
  stages specifically.

### Strategic pivot — hedge option selling (equity + index)
Reframed the end goal from pure direction prediction to **hedged option
premium selling**, whose real documented edge source is the **volatility
risk premium** (implied vol tends to systematically overstate realized vol),
with the Phase 1/2 direction signals (OFI, OI, PCR) used as a **secondary
filter**, not the primary edge driver. Three new components designed
(written up, not yet built in Cursor):

1. **IV rank/percentile tracking** — new `iv_history` table; IV solved by
   inverting Black-Scholes (Newton-Raphson, reusing existing `greeks.py`
   Vega as the derivative step); tracks IV rank, IV percentile, and
   vol risk premium (IV − realized vol) as the core edge signal.
2. **Decision-logging schema** (`decision_log`) — every trade's strike
   rationale, sizing rationale (incl. Kelly-fraction comparison), timing
   rationale (IV rank + OI/PCR at entry), risk parameters, and a flag for
   system-suggested vs. manual vs. system-suggested-but-overridden.
3. **Hedge-structure payoff engine** — generic multi-leg payoff/Greeks/
   probability-of-profit calculator, starting narrow (credit spreads +
   iron condors only, not every possible structure).

### Honesty checkpoints established along the way (don't relitigate these
without new evidence — they were deliberately reasoned through)
- We will never match true HFT latency/infrastructure — that's structurally
  gated by exchange colocation/membership, not a skill or grind problem.
  The realistic target is mid-frequency systematic edge (seconds-minutes),
  not microsecond HFT.
- SEBI's algo framework (full enforcement April 1, 2026) requires Algo-ID
  registration for anything that places orders without manual confirmation.
  We are deliberately staying signal-only / manual-execution for the
  entire current build scope.
- A realistic, honest target for a well-built system: Sharpe ~1-1.5,
  ~55-58% directional hit rate after costs. That is a genuinely good
  outcome, not a disappointing one — most retail traders lose money.
- Kronos (github.com/shiyu-coder/Kronos) was reviewed: a legitimate
  candlestick-sequence foundation model, but non-overlapping with our
  order-book/OI/Greeks data — bookmarked as a possible future ensemble
  input, explicitly NOT something to integrate now (needs GPU + NSE
  fine-tuning, premature before our own signals are proven).

---

## 2. Kite Connect — full data capture spec

Store essentially everything Kite Connect can give us. Storage is cheap;
re-fetching historical broker/account state later is often impossible.
Cursor should implement capture for all of the following, not just ticks:

| Data type | Kite Connect source | Store as | Priority |
|---|---|---|---|
| Live tick (LTP, OHLC, volume) | WebSocket, `mode=full` | Parquet, partitioned by date/symbol | Stage 1 (already planned) |
| 5-level market depth | WebSocket, `mode=full` | Parquet, partitioned by date/symbol | Stage 1 (already planned) |
| Historical candles (OHLCV+OI) | `historical_data()` | Parquet, separate from tick data | Stage 1 (already planned) |
| Instrument master (tokens, lot size, expiry, strike, tick size) | `instruments()` | SQLite, refreshed daily | Stage 1 (already planned) |
| **Order updates** (placed/modified/executed/cancelled/rejected) | WebSocket order postback, or `orders()` | **New: `order_events` table, SQLite** | Add to Stage 1/6 |
| **Trade fills** (actual executed price/qty/time) | `trades()` | **New: `trade_fills` table, SQLite** — this is the ground truth for point 5 below | Add to Stage 1/6 |
| **Positions** (net/day, live P&L, quantity) | `positions()` | **New: `positions_snapshot` table**, polled periodically during market hours | Add to Stage 6 |
| **Holdings** (equity delivery) | `holdings()` | **New: `holdings_snapshot` table**, daily | Add to Stage 6 |
| **Margins** (available, used, SPAN+exposure) | `margins()` | **New: `margin_snapshot` table**, polled periodically | Add to Stage 6 |
| GTT orders, if used | `get_gtts()` | New: `gtt_log` table | Optional, only if you use GTTs |
| Profile/funds | `profile()`, `margins()` | Logged once per session for audit trail | Low priority |

**Key point to give Cursor explicitly:** `trade_fills` and `order_events`
are not optional extras — they are the ground-truth data source for the
entry/exit review algorithm in section 4 below. Without capturing your
actual fills (not just what the model suggested), there is nothing to
compare the model's recommendation against.

---

## 3. Logging discipline — tying recommendation to actual action to actual outcome

Every trade must be traceable through three linked records:

1. **`decision_log`** — what the system recommended (or what you did
   manually), and why (strike rationale, sizing rationale, IV rank/OI
   context at the time, a system/manual/overridden flag).
2. **`trade_fills`** — what actually got executed on Kite (real fill
   price, real quantity, real timestamp — this can differ from what was
   recommended).
3. **`outcome_log`** (new — see section 4) — what happened to the market
   after entry and after exit, and a decision-quality score independent
   of raw P&L.

All three link via a shared `trade_id`. This three-way link is what makes
the system able to answer "was my judgment good," not just "did I make
money" — those are genuinely different questions, and conflating them is
one of the most common mistakes in self-reviewing trading performance.

---

## 4. Entry/exit "sense" algorithm — decision quality review

This is the new component you asked for: a system that reviews whether an
entry and exit were *good decisions*, separately from whether they made
money — because a good decision can lose money on bad luck, and a bad
decision can win on good luck. Conflating the two is exactly what stops
most traders from actually improving over time.

### Design
For every closed trade (entry+exit pair from `trade_fills`):

1. **Capture market state at fixed checkpoints**, not just at entry/exit:
   +5min, +15min, +30min, and end-of-day, relative to both entry and exit
   timestamps. Store price, IV, and OI state at each checkpoint.
2. **Entry quality score**: compare the model's stated probability/
   confidence at entry time against what a well-calibrated model *should*
   have believed given only the information available at that moment — not
   against the full-hindsight best possible entry. (Important distinction,
   explained below.)
3. **Exit quality score**: did price/IV keep moving favorably after you
   exited (→ exited early) or reverse after you exited (→ exit was well
   timed)? Measured at the same fixed checkpoints, so it's a consistent,
   comparable metric across all trades.
4. **2×2 classification** for every trade: Good decision/Good outcome,
   Good decision/Bad outcome (bad luck — don't punish this in retraining),
   Bad decision/Good outcome (lucky — don't reward this in retraining),
   Bad decision/Bad outcome. This classification, not raw P&L, is what
   should feed back into model retraining (Stage 8).
5. **Personal behavior tracking**: aggregate the system/manual/overridden
   flag from `decision_log` against these quality scores over time — this
   directly answers whether your manual judgment is adding value on top of
   the model, or quietly subtracting from it, with real evidence instead
   of a gut feeling either way.

### One honest caveat, worth flagging clearly before this gets built
There's a real risk of **hindsight bias** if "entry quality" is defined as
"was this the objectively best possible entry, using full knowledge of
what happened afterward." That target is unrealistic and unlearnable — no
model can actually achieve it in real time, so training toward it teaches
the model to chase noise. The score must be built around "was this
consistent with what a well-calibrated model should have believed *using
only information available at that moment*" — i.e., compare against your
own model's probability output at the time, not against a perfect
hindsight benchmark. This distinction should be written directly into the
Cursor prompt when this stage gets built, so it isn't lost in translation.

---

## 5. Immediate next step

When you're ready, this document plus the existing `PROJECT_PLAN.md`
should both be given to Cursor together, with an instruction to extend the
plan with new stages for: (a) the full Kite data capture spec in section 2,
(b) the `decision_log`/`trade_fills`/`outcome_log` linkage in section 3,
and (c) the entry/exit quality algorithm in section 4 — inserted as new
stages after the existing Stage 6 (live signal engine), before the
dashboard and retrain loop stages, since the dashboard should be able to
display decision-quality history once it exists.
