# TERMINAL UI ARCHITECTURE — FINAL AUTHORITATIVE PLAN

**Status:** FINAL. This document supersedes every earlier version of itself and every other
architecture note in this repository. Where this document and any prior note disagree, this
document wins.

**Scope:** two repositories.

| Repo | Path | Role |
| --- | --- | --- |
| Pipeline / backend | `C:\Users\sambh\nse-signal-pipeline` | Kite ingestion, storage, analytics, models, FastAPI read layer |
| Dashboard / frontend | `C:\Users\sambh\nse-signal-dashboard` | Next.js 15 UI |

**Product target:** a professional trading terminal. UX reference is Zerodha Kite Web / Kite
Terminal for *workflow, density, information hierarchy, Marketwatch behaviour, workspaces, chart
interaction, option chain interaction, quick actions and keyboard-first operation*. Not a pixel
copy. On top of that: HNI-grade market-data presentation and our own Market Intelligence and Trade
Intelligence engines.

**Core principle:** the product is INSTRUMENT-CENTRIC, WORKSPACE-CENTRIC and LIVE-DATA-CENTRIC.
It is not page-centric. The selected instrument is shared terminal context, not a route parameter.

**Evidence policy for this document.** Every claim in section A is either (a) a line reference
into one of the two repositories, or (b) a measurement taken on the production VM
(`nse@168.144.66.235`) read-only on 2026-09-07. Claims that are inferences are labelled
INFERRED. Claims that could not be verified are labelled UNVERIFIED with the reason. No claim in
section A is an assumption carried over from an earlier pass; six of them were carried over,
tested, and are corrected in §0.3.

**Read-only run.** No application code was written, modified, refactored, committed or deployed
in producing this document. No service was started, stopped or restarted. The only file changed is
this one.

---

## 0. How to read this document

### 0.1 Structure

| § | Contents |
| --- | --- |
| 0 | Status, decision index, corrections to the previous pass, newly discovered requirements |
| A–B | Verified current state; problems and blockers |
| C | Target terminal architecture |
| D | Data architecture: four layers, reference data, capabilities, calendar, temporal consistency, historical layers, NSE constraints |
| E | Live-data architecture: subscription management, transport, browser store, ordering, session machine, timestamps |
| F | API architecture and server-side read models |
| G–H | Frontend architecture; workspace architecture |
| I–Z | Surface-by-surface architecture (Marketwatch → Notes) |
| AA–AD | Canonical dictionary, change semantics, lineage, live/historical state |
| AE–AI | Error handling, security, performance, observability, resource budgets |
| AJ–AL | Technical debt, backend gaps, migration strategy |
| AM–AO | Testing, acceptance criteria, implementation phases |
| Appendix 1 | Professional-trader workflow completeness check |
| Appendix 2 | Final self-critique: what would force a redesign |
| Appendix 3 | FINAL GAP CHECK |

### 0.2 Architecture decision index

Every foundational choice is decided here, with alternatives considered and reasoning recorded at
the referenced section. There are no open foundational questions.

| ADR | Decision | Alternatives rejected | § |
| --- | --- | --- | --- |
| **ADR-1** | Browser transport = **SSE** (`text/event-stream`) from FastAPI via the existing Next.js server proxy, plus REST for hydration and history | browser WebSocket; coordinated polling; hybrid WS+poll | E.4 |
| **ADR-2** | Ingest→API live handoff = **API polls `latest_quotes` on one shared 1 s timer**, fanned out to all SSE clients. Upgrade path to a UNIX-domain-socket fan-out from the ingest process is designed now, built only if sub-second is required | Redis pub/sub; second WS; per-client SQLite polling; shared memory | E.5 |
| **ADR-3** | Frontend state = **Zustand for selection/workspace/user state + a plain non-React `MarketCache` (Map + per-instrument subscriber registry) for market data**, bridged with `useSyncExternalStore` | all-in-Zustand; Redux Toolkit; Jotai; React Context; custom global store | G.3 |
| **ADR-4** | Snapshot/historical/metadata fetching = **TanStack Query v5** | keep `useApiQuery`; SWR; hand-rolled cache | G.5 |
| **ADR-5** | Chart data = **materialise 1-minute bars at compaction into Parquet; roll up 3m/5m/10m/15m/30m/60m server-side; materialise daily bars separately** | aggregate raw ticks per request (today); materialise every interval; aggregate in the browser | L.3 |
| **ADR-6** | Marketwatch = **bulk query over `latest_quotes` + a `session_reference` join. No new Marketwatch read model.** | precomputed Marketwatch table; per-row requests (today) | I.3, F.6 |
| **ADR-7** | Instrument search = **SQLite `instruments` table with a precomputed `search_rank` column and prefix indexes; server-side, `LIKE 'x%'` + token match** | client-side over watchlists (today); SQLite FTS5; in-memory trie per worker; Elasticsearch | J.3 |
| **ADR-8** | Time & Sales = **observation-delta events, explicitly labelled OBSERVED/DERIVED, with no synthetic trade identifiers** | synthesise per-trade prints; omit the panel; claim exchange trade IDs | N.3 |
| **ADR-9** | Subscriptions = **static core universe + dynamic focus set, both owned by one subscription manager inside the single existing ingest process** | fully static (today); per-viewer dynamic; second Kite WS | E.2 |
| **ADR-10** | Workspace persistence = **`localStorage` with a versioned schema and forward migrations; backend sync added later behind the same interface** | backend-only; IndexedDB; URL-only; cookies | H.5 |
| **ADR-11** | Read-model storage = **bars in Parquet (columnar, DuckDB-native); `session_reference`, `instruments`, `unusual_rank`, `market_calendar` in SQLite (point lookups)** | everything in SQLite; everything in Parquet; DuckDB persistent DB; Postgres; Redis | F.6 |
| **ADR-12** | Live depth ladder = **two JSON text columns (`bid_levels`, `ask_levels`) added to `latest_quotes`** | separate `latest_depth` table; msgpack blob; keep discarding depth (today) | M.3 |
| **ADR-13** | Percent convention = **every field named `*_pct` is a percentage (0–100 scale), corrected at the source** | keep the mixed convention and compensate in the frontend (today); rename to `_ratio`; ship both | AA.3 |
| **ADR-14** | Session/market calendar = **backend-owned `market_calendar` SQLite table, seeded from the NSE holiday list, single source for every session-sensitive calculation** | hardcode holidays in the frontend (partially today); derive from data presence; third-party calendar API | D.6 |
| **ADR-15** | Canonical change = **computed once in the backend from `session_reference`, shipped as `reference_price` + `reference_type` + `change_absolute` + `change_percent`. The frontend never computes it.** | frontend computes from `previous_close`; keep `price_delta` as `change` (today) | AB |
| **ADR-16** | Frontend disposition = **HYBRID: keep and extend the backend; rebuild the frontend shell, routing, state and transport; salvage the domain layer** | keep frontend; refactor frontend; rebuild both; rebuild backend | AL.1 |

### 0.3 Corrections to the previous pass

The previous pass produced a 2 087-line document with 84 sections. Its structure and most of its
content survive into this one. Re-verification against the repositories and the VM invalidated or
materially sharpened ten claims. Each correction below changes the plan, not just the prose.

| # | Previous claim | Verified reality | Consequence for the plan |
| --- | --- | --- | --- |
| **R-1** | "Ingestion is down; Phase 0 item one is to start a supervised `nse-ingest.service`." | Ingestion is down **and the Kite access token is invalid**. `nse-account-capture.service` is in `activating (auto-restart)` right now, failing on `kiteconnect.exceptions.TokenException: Incorrect api_key or access_token` at 13:41:41 IST today, consuming 3.2 s CPU per restart attempt on a 1-vCPU box. | Starting the ingest unit would fail identically. **Phase 0 item one is token lifecycle, not service supervision.** New requirement N-4. The crash loop is also stealing CPU from the API and is a live contributor to the measured chart latency. |
| **R-2** | "The chart is slow because `assemble_charts` reads the same Parquet frames twice." | True but secondary. **There is no candle storage layer at all.** Compaction writes exactly one file per symbol per session — `ticks.parquet` (589 dirs, 589 files, all `ticks.parquet`, verified on 2026-09-04). `duckdb_store.read_symbol_ohlc_frames` looks for `candles` and `daily` paths that **never exist**, so `load_ohlc_candles` always falls through to `session_aggregate_ohlc(ticks, mode="last_price")` — building every candle from raw `last_price` rows on every request. That path also returns `volume=None` for every bar (`ohlc.py:176`). | The fix is not deduplicating a read; it is **adding the missing aggregation layer** (ADR-5, F.6 RM-1). Also explains why candle volume is always null. Re-measured cost today: **32.03 s**, worse than the 22.81 s recorded in the previous pass. |
| **R-3** | "Only near-month futures are subscribed (`futures_contract_count: 1`); gap E-6 is to add Next and Far." | Wrong. `config/settings.yaml:56` sets `futures.contract_count: 2` (near + next) and the compacted data confirms four contracts: `NIFTY26SEPFUT`, `NIFTY26OCTFUT`, `BANKNIFTY26SEPFUT`, `BANKNIFTY26OCTFUT`. The `futures_contract_count: 1` I quoted is `stock_derivatives.futures_contract_count`, a different setting. | Near and Next already work. Gap narrows to **Far month only**, and drops from Phase 9 scope to a config change. |
| **R-4** | "The subscribed universe is ~2 500 tokens; expect ~1 250 updates/s." | The live universe is **589 tokens**: `equity_depth` 100, `equity_quote` 399, `index` 2, `options` 84, `futures` 4. `max_total_tokens: 2500` is a cap that was never approached. Stock derivatives are configured (`stock_derivatives.enabled: true`) but **never materialised** — the cache has no `universe` key at all, and `settings.yaml:60` says so: *"Becomes live only on the next planned ingest restart."* | Every load, memory and bandwidth estimate in the previous pass was ~4× too pessimistic. Recomputed in AI. Also means the option chain is **NIFTY + BANKNIFTY only** — no stock options exist in any stored session, so stock-option UI must render a typed "not subscribed" state, not an empty chain. |
| **R-5** | Not identified. | **Raw data is 9.2 GB for four sessions (~2.3 GB/session) against 32 GB free on a 48 GB disk.** There is no retention policy anywhere in `config/settings.yaml` or in the compaction job. At the current rate the disk fills in roughly **14 trading sessions** after ingestion resumes. | New requirement **N-1**, priority equal to ingestion itself. A terminal that dies of a full disk three weeks after launch is not a terminal. |
| **R-6** | "`SessionSettings` drives market state." | Correct, and **there is no trading-holiday calendar anywhere in either repository.** `market_state()` (`market/data_state.py:72-81`) excludes weekends and nothing else. The module's own docstring admits it: *"NSE trading holidays are not modelled anywhere in this repository, so on a holiday the clock still reports 'open'."* There is no pre-open, no closing session, no post-close, no halt state. | ADR-14 and requirement D.6 are promoted from "nice to have" to a Phase 2 blocker, because reference prices, day change and chart session boundaries all depend on knowing what the previous *trading* day was. |
| **R-7** | Not identified. | **No freeze quantity, no price bands, no circuit limits, no settlement price, and no lot-size enforcement exist anywhere.** The only hit for "circuit" is a comment in `features/quality.py:148-161` explaining that a per-symbol freeze *suspicion* is deliberately not treated as a circuit breaker. `tick_size` and `lot_size` are present in the instrument cache but are never used for display rounding or quantity validation. | New requirements N-21, N-24 and the NSE-constraints audit in D.8. Blocks any future order ticket, and affects price rendering today. |
| **R-8** | "Account data exists via `/account`." | It exists but is **opaque and stale**. `positions_snapshot` has 3 826 rows whose latest payload is an **empty list**; `margin_snapshot` 1 913 rows; `holdings_snapshot` **1** row; `order_events` **0** rows; `trade_fills` **0** rows. All are `payload_json` blobs with no typed columns, so realised/unrealised/day/net P&L cannot be queried, only re-parsed. And the capture service is the one crash-looping on the bad token. | Portfolio work (W, X) needs a typed projection layer, not just an endpoint. Moves from "wire up the existing endpoint" to a real read model. |
| **R-9** | "The instrument cache refreshes on auth or ingest start (gap E-2)." | Worse. The VM cache is stamped `2026-09-04T05:13:00Z` and was written by a build that predates the `universe` / `fo_eligible_nifty100` keys entirely — the deployed code writes them, the on-disk cache does not have them. The cache is both stale and **schema-stale**. | E-2 becomes a Phase 0 item with a schema-version check, not just a refresh timer. |
| **R-10** | "`/unusual-activity` is 2.93 s." | Re-measured today: **1.03 s warm**. The endpoint is behind `analytics.last_session_cache_ttl_seconds: 120`, so 2.93 s was a cold measurement and 11.55 s was the pre-optimisation cold path. Meanwhile `/options/NIFTY` measures **1.29 s / 33.5 KB**, which the previous pass had recorded as fast. | The precomputed-rank read model (RM-3) drops from Phase 3 to Phase 10. **`/options/{underlying}` replaces it as the second-worst latency problem** and moves into Phase 9. |

Additionally, one previous recommendation is rejected outright: see §F.6 on why **"five new read models" was the wrong answer**, and what the correct set is.

### 0.4 Newly discovered requirements

These came out of the independent gap-discovery pass. None of them appear in the requirement
checklist this pass was given, and none were in the previous document. They are numbered `N-*` and
are referenced from the phase plan in AO.

---

#### N-1 — Raw-data retention and disk lifecycle

- **REQUIREMENT.** A retention policy, enforced by a scheduled job, covering `data/raw/{date}/`,
  `data/compacted/{date}/`, the new bar store, and the SQLite operational tables. Default:
  raw retained 7 sessions, compacted ticks 90 sessions, 1m bars 400 sessions, daily bars forever,
  `feature_log`/`quality_log` 180 days, `signal_log` forever (it is the model audit trail).
- **WHY IT MATTERS.** Measured: 9.2 GB of raw Parquet for 4 sessions, 32 GB free. ~14 sessions of
  headroom. When the disk fills, ingestion stops writing, SQLite writes fail, and the API starts
  returning `no_data` — a total product outage with no market cause. This is the single most likely
  way the system dies unattended.
- **CURRENT STATE.** No retention configuration exists. `nse-compact.timer` compacts and never
  deletes. Nothing prunes SQLite (367 MB; `feature_log` 134 672 rows, `quality_log` 93 785 rows).
- **TARGET STATE.** `retention:` block in `config/settings.yaml`; a `nse-retention.timer` running
  after `nse-labels.timer`; a dry-run mode; a `/health` field reporting disk free, days of headroom
  at the current burn rate, and the oldest retained partition per tier.
- **DATA SOURCE.** Filesystem `statvfs`; partition directory listings; SQLite `COUNT(*)`.
- **ARCHITECTURAL OWNER.** Backend — a new `nse_pipeline/storage/retention.py`, invoked by a
  systemd timer. Never invoked from an API request.
- **DEPENDENCIES.** None. Can ship before ingestion is restored, and should.
- **IMPLEMENTATION PHASE.** Phase 0.
- **ACCEPTANCE CRITERIA.** (1) A dry run on the VM lists exactly the partitions older than the
  configured windows and no others. (2) After a live run, `du -sh data/raw` is within the
  configured budget. (3) `/health` reports `disk_free_gb` and `sessions_of_headroom`, and
  `sessions_of_headroom < 10` raises a WARNING. (4) Deleting a raw partition never deletes its
  compacted or bar output — asserted by a test that compacts, retains, prunes raw, and still
  serves a chart for the pruned date.

---

#### N-2 — Kite session/token lifecycle as a supervised, observable concern

- **REQUIREMENT.** Treat the Kite access token as a daily-expiring credential with an explicit
  lifecycle: valid → expiring → invalid → re-authenticated. Expose token state in `/health`. Never
  let a component crash-loop on `TokenException`.
- **WHY IT MATTERS.** Verified today: `nse-account-capture.service` is in
  `activating (auto-restart)`, failing with `TokenException: Incorrect api_key or access_token`,
  burning 3.2 s of CPU per attempt on a 1-vCPU machine that also serves the API. Kite access tokens
  expire daily. This means (a) account data is dead, (b) restarting ingestion **would fail the same
  way**, and (c) the restart loop is degrading API latency for every user. The previous pass's
  "just start the ingest service" instruction would have failed on first contact.
- **CURRENT STATE.** Token is read from configuration/secret at process start. No expiry model, no
  health surface, no backoff on `TokenException`, no distinction between "token expired" (expected,
  daily) and "credentials wrong" (a real fault). Services retry forever at high frequency.
- **TARGET STATE.** (1) A single `broker/session.py` owning token validity, with an explicit
  `TokenState` enum. (2) `TokenException` classified as terminal-for-the-day: the service backs off
  to a long interval (≥5 min) and reports state instead of hot-looping. (3) `/health` exposes
  `kite_token_state`, `kite_token_age_seconds`, `kite_last_auth_at`. (4) The terminal status bar
  renders a CRITICAL banner when the token is invalid, because in that state *no* data is or will
  become live, and every other status indicator would otherwise lie.
- **DATA SOURCE.** Kite Connect auth flow; `ingestion_meta` events; a new `broker_session` table
  or `ingestion_meta` event types.
- **ARCHITECTURAL OWNER.** Backend `broker/session.py`; consumed by ingestion, account capture and
  `/health`. Frontend consumes only the `/health` projection — the token itself never leaves the VM
  (AF).
- **DEPENDENCIES.** Blocks N-3 (ingestion supervision), and therefore blocks every real-time
  acceptance test in AN.
- **IMPLEMENTATION PHASE.** Phase 0, item 1. Nothing else in the plan can be validated first.
- **ACCEPTANCE CRITERIA.** (1) With a deliberately invalid token, no service consumes more than 1%
  CPU averaged over 10 minutes. (2) `/health` reports `kite_token_state: "invalid"` within 60 s.
  (3) The terminal shows a CRITICAL token banner and does not show any instrument as LIVE.
  (4) After re-auth, ingestion reaches `data_status: "live"` with no manual restart of the API.

---

#### N-3 — Supervised ingestion with warm-start delta suppression

- **REQUIREMENT.** A managed `nse-ingest.service` with restart-on-failure, plus correct behaviour
  when it starts *mid-session*: the first observation per instrument after a (re)start must not
  produce volume/OI delta events.
- **WHY IT MATTERS.** Two separate problems. First, there is no installed ingest unit at all —
  verified: `/etc/systemd/system` contains `nse-api`, `nse-account-capture`, `nse-compact`,
  `nse-features`, `nse-labels`, `nse-live-signals` and their timers, and **no ingest unit**.
  `deploy/vm/nse-ingest.service.example` was never installed. Second, `volume_delta` is a difference
  between consecutive observations (`market/latest.py:50`), so a mid-session restart loses the
  volume traded while down. If the tracker's first post-restart observation were differenced against
  anything, it would emit an enormous false "volume burst" straight into the unusual-activity engine.
- **CURRENT STATE.** No unit file. The in-process guard is already correct — `LatestQuoteTracker`
  starts empty, `numeric_delta(x, None)` yields `None`, and `snapshot_from_tick` clamps negative
  volume deltas to `None` (`latest.py:51-52`). So the code is safe; the **operational** gap is the
  unit, and the **semantic** gap is that the lost volume is never disclosed.
- **TARGET STATE.** Installed unit with `Restart=on-failure`, `RestartSec` backoff, and a
  dependency on token validity (N-2). An `ingestion_meta` `warm_start` event on every start, and a
  `session_coverage` consequence: any session with a `warm_start` after market open is marked
  `PARTIAL_SESSION` (E.6), which the terminal renders as a data-quality badge rather than silently
  showing a clean day.
- **DATA SOURCE.** systemd; `ingestion_meta`; `session_coverage`.
- **ARCHITECTURAL OWNER.** Backend/deploy. `deploy/vm/nse-ingest.service`; `session_coverage.py`.
- **DEPENDENCIES.** N-2.
- **IMPLEMENTATION PHASE.** Phase 0, item 2.
- **ACCEPTANCE CRITERIA.** (1) `systemctl is-enabled nse-ingest` returns `enabled`. (2) Killing the
  process mid-session brings it back within 30 s. (3) After a mid-session restart, zero unusual
  activity events of kind `volume_burst` are emitted in the first 60 s. (4) The session is labelled
  `PARTIAL_SESSION` and the terminal displays it.

---

#### N-4 — VM clock integrity

- **REQUIREMENT.** The VM clock must be NTP-synchronised, and clock skew relative to exchange
  timestamps must be measured and exposed.
- **WHY IT MATTERS.** Every freshness decision in the system is `now() - exchange_timestamp`
  (`data_state.observation_age_seconds`, `analytics.live_quote_max_age_seconds: 120`). A +3-minute
  VM clock skew silently reclassifies every live quote as stale and flips the entire product into
  `last_session` during a live market, with no error anywhere. A −3-minute skew does the opposite
  and shows stale data as live. This is a silent, total, and completely invisible failure mode of
  the central data-state decision.
- **CURRENT STATE.** Unmodelled. `timedatectl` state UNVERIFIED (not checked; adding it to the
  Phase 0 checklist rather than guessing).
- **TARGET STATE.** `systemd-timesyncd` verified active at deploy time. A rolling estimate of
  `median(receive_time - exchange_timestamp)` over the last 1 000 ticks, exposed as
  `/health.clock_skew_estimate_seconds`. A skew beyond ±30 s raises CRITICAL, because freshness
  cannot be trusted.
- **DATA SOURCE.** `NormalizedTick.exchange_timestamp` vs `ingested_at`; `timedatectl`.
- **ARCHITECTURAL OWNER.** Backend ingestion (measurement) + deploy (NTP) + `/health` (exposure).
- **DEPENDENCIES.** N-3 (needs live ticks to measure).
- **IMPLEMENTATION PHASE.** Phase 0 (NTP check), Phase 1 (skew estimate in `/health`).
- **ACCEPTANCE CRITERIA.** (1) `deploy.sh` fails loudly if NTP sync is inactive. (2) With the clock
  deliberately shifted 5 minutes, `/health` reports skew within 60 s and the terminal shows a
  CRITICAL clock banner rather than a market-wide stale state.

---

#### N-5 — Cross-tab stream coordination

- **REQUIREMENT.** One live transport connection per browser profile, not per tab. Multiple
  terminal tabs share a single SSE connection and a single normalized cache.
- **WHY IT MATTERS.** The acceptance criteria demand "no duplicate request storm". A trader with
  four tabs open produces four SSE connections and four full snapshot hydrations against a 1-vCPU
  VM — a 4× multiplier on the exact resource we have least of. This is invisible in single-tab
  testing and appears immediately in real use.
- **CURRENT STATE.** Not applicable — there is no stream. Today each tab independently runs its own
  `setInterval` polls, which is the same problem in its current form (~113 req/min per tab).
- **TARGET STATE.** A `SharedWorker` owning the single `EventSource` and the normalized cache, with
  tabs attaching over `postMessage`. Fallback for browsers without `SharedWorker`: leader election
  over `BroadcastChannel` + `localStorage` lease, where the leader tab holds the connection and
  broadcasts frames; on leader unload or lease expiry another tab takes over within 2 s.
- **DATA SOURCE.** N/A (transport-layer concern).
- **ARCHITECTURAL OWNER.** Frontend `transport/` (G.4). Invisible to panels — they subscribe to
  `MarketCache` either way.
- **DEPENDENCIES.** ADR-1, ADR-2, ADR-3.
- **IMPLEMENTATION PHASE.** Phase 6 (built into the transport from the start; retrofitting a
  SharedWorker after panels exist means rewriting the subscription layer).
- **ACCEPTANCE CRITERIA.** (1) With 4 tabs open, the VM reports exactly 1 active stream connection.
  (2) Closing the leader tab keeps the other 3 updating, with a gap under 2 s. (3) Instrument
  subscriptions are the union across tabs, reference-counted, and shrink when a tab closes.

---

#### N-6 — Stream admission control and fan-out cap

- **REQUIREMENT.** An explicit maximum concurrent stream-connection count with a defined rejection
  behaviour, plus a per-connection subscription cap.
- **WHY IT MATTERS.** On 1 vCPU with 1 uvicorn worker, unbounded SSE connections are a
  self-inflicted denial of service. The system must degrade predictably (refuse the 21st connection
  with a documented status and a UI message) rather than unpredictably (all 20 users get 8-second
  updates).
- **CURRENT STATE.** No stream exists, therefore no cap. The API has no rate limiting or
  concurrency limiting of any kind.
- **TARGET STATE.** `stream.max_connections` (default 16) and `stream.max_instruments_per_connection`
  (default 250, aligned with I's Marketwatch budget). Over the limit: HTTP 503 with
  `Retry-After` and a typed error body, rendered as "terminal at capacity" in the UI. Metrics for
  current/peak connections in `/health`.
- **DATA SOURCE.** N/A.
- **ARCHITECTURAL OWNER.** Backend `api/stream.py`; surfaced by frontend `ConnectionMonitor`.
- **DEPENDENCIES.** ADR-1.
- **IMPLEMENTATION PHASE.** Phase 4, with the stream itself.
- **ACCEPTANCE CRITERIA.** (1) Connection 17 receives 503 + `Retry-After`, not a hang. (2) Existing
  16 connections show no latency regression when the 17th is refused. (3) Subscribing to 251
  instruments on one connection returns a typed error naming the cap.

---

#### N-7 — Option-chain strike-window drift

- **REQUIREMENT.** The subscribed strike window must follow spot intraday, or the UI must
  explicitly disclose that the window no longer brackets ATM.
- **WHY IT MATTERS.** `options.underlyings[].strikes_each_side: 10` with `strike_interval: 50` gives
  NIFTY a ±500-point subscribed window, fixed at instrument-cache refresh time (~05:13 UTC). A 2%
  intraday move (~±480 points) walks spot to the edge of that window; anything more and **ATM is
  outside the subscribed set**. The chain then renders centred on a strike that is no longer ATM,
  PCR is computed over the wrong strikes (`analytics.pcr_atm_strikes: 5`), and Max Pain is computed
  over a truncated domain — all without any error, because every subscribed contract is present and
  fresh. This is a correctness failure disguised as a healthy chain.
- **CURRENT STATE.** Window is static per session. `chain_status` and `selected/eligible/missing`
  counts report *completeness of the subscribed set*, not *whether the set brackets ATM*. So the
  existing honesty machinery does not catch this.
- **TARGET STATE.** (1) A derived `atm_coverage` on every chain response:
  `{ atm_strike, subscribed_min_strike, subscribed_max_strike, strikes_below_atm,
  strikes_above_atm, coverage_status: BALANCED | SKEWED | ATM_OUTSIDE_WINDOW }`. (2) The dynamic
  focus set (E.2) re-subscribes strikes to keep ≥5 strikes on each side of ATM, within the token
  budget. (3) The chain UI renders `SKEWED`/`ATM_OUTSIDE_WINDOW` as a prominent banner, and PCR and
  Max Pain are suppressed to `null` with a reason when coverage is `ATM_OUTSIDE_WINDOW`, exactly as
  `max_pain_min_completeness: 0.70` already suppresses on completeness.
- **DATA SOURCE.** Spot from `latest_quotes`; subscribed strikes from the instrument cache;
  `analytics.atm_method: nearest_listed`.
- **ARCHITECTURAL OWNER.** Backend `market/options_analytics.py` (coverage), `broker/instruments.py`
  + subscription manager (re-subscription); frontend options panel (disclosure).
- **DEPENDENCIES.** ADR-9 (dynamic focus set) for the fix; nothing for the disclosure.
- **IMPLEMENTATION PHASE.** Disclosure in Phase 9; dynamic re-subscription in Phase 9 after E.2.
- **ACCEPTANCE CRITERIA.** (1) A replayed session with a 3% index move produces
  `coverage_status: ATM_OUTSIDE_WINDOW` and `pcr: null` with `pcr_reason` set. (2) With the dynamic
  focus set enabled, ATM stays within the subscribed window for the same replay, with total tokens
  never exceeding the cap. (3) A balanced window reports `BALANCED` and PCR is populated.

---

#### N-8 — Kite capacity governance

- **REQUIREMENT.** Encode the broker's hard limits as configuration and enforce them in the
  subscription manager: tokens per connection, connections per API key, and mode-change cost.
- **WHY IT MATTERS.** The subscription architecture (E.2) is only safe if the ceiling is explicit.
  `max_total_tokens: 2500` is a self-imposed cap in our config; the broker's own per-connection
  limit is a different number, and exceeding it fails the *whole* connection, not the marginal
  subscription — turning a UI action (open a stock's option chain) into a market-data outage.
- **CURRENT STATE.** `stock_derivatives.max_total_tokens: 2500` and
  `max_stock_option_contracts: 600` exist as our own caps. Broker limits are not encoded anywhere.
  The single connection currently carries 589 tokens, far below any limit, so the risk is latent.
- **TARGET STATE.** A `broker.limits` config block with `max_tokens_per_connection`,
  `max_connections_per_key`, `mode_change_batch_size`, sourced from Kite's published limits and
  version-stamped. The subscription manager treats the cap as a hard admission control: a focus-set
  request that would exceed it evicts the least-recently-used dynamic subscription first, and if it
  still would not fit, it is refused with a typed error the UI renders.
- **DATA SOURCE.** Kite Connect documentation (config-encoded, UNVERIFIED against live limits in
  this read-only pass).
- **ARCHITECTURAL OWNER.** Backend subscription manager in the ingest process.
- **DEPENDENCIES.** ADR-9.
- **IMPLEMENTATION PHASE.** Phase 4 (with the subscription manager).
- **ACCEPTANCE CRITERIA.** (1) A synthetic request for 5 000 tokens is refused without dropping the
  existing connection. (2) LRU eviction is observable in `ingestion_meta`. (3) The static core
  universe is never evicted to satisfy a dynamic request.

---

#### N-9 — SQLite concurrency policy under stream load

- **REQUIREMENT.** An explicit read/write concurrency policy for `nse_pipeline.db`: WAL,
  `busy_timeout`, read-only connections for the API, and a bound on how long any API-held read
  transaction may run.
- **WHY IT MATTERS.** ADR-2 has the API polling `latest_quotes` every second while ingestion writes
  it every ≤2 s and `nse-live-signals` writes `signal_log` continuously. WAL permits concurrent
  readers, but a long-running reader delays checkpointing, and a writer that hits a busy database
  without a timeout raises `SQLITE_BUSY` — which in the ingest process means dropped persistence,
  i.e. data loss that looks like a quiet market.
- **CURRENT STATE.** `PRAGMA journal_mode=WAL` is set (`sqlite_store.py`). `busy_timeout` is not
  configured anywhere. The API opens read-write connections where read-only would do. WAL file is
  currently 0 bytes (checkpointed), so no evidence of pressure — but there is also no live traffic.
- **TARGET STATE.** API connections opened `file:...?mode=ro` with `busy_timeout=5000`; ingest
  writes with `busy_timeout=10000` and a retry counter exported to `/health`; the stream's poll
  query bounded to a single indexed `SELECT` over ≤600 rows with no joins to Parquet; an explicit
  rule that **no request-path query may hold a read transaction longer than 250 ms**.
- **DATA SOURCE.** N/A.
- **ARCHITECTURAL OWNER.** Backend `storage/sqlite_store.py`.
- **DEPENDENCIES.** ADR-2.
- **IMPLEMENTATION PHASE.** Phase 4.
- **ACCEPTANCE CRITERIA.** (1) A 30-minute soak with ingestion writing, 16 stream connections
  polling, and the signal engine running yields zero `SQLITE_BUSY` errors. (2) WAL size stays under
  64 MB. (3) `/health` reports a `sqlite_busy_retries` counter, and it stays at 0.

---

#### N-10 — Instrument token stability and expiry rollover identity

- **REQUIREMENT.** A rule for how instrument identity is preserved across expiry rollovers, symbol
  changes and token reassignment, and what the terminal's persisted state stores.
- **WHY IT MATTERS.** Workspaces, watchlists, alerts and notes are persisted (N-31/§H.5). If they
  store `instrument_token`, they break the moment a contract expires or a token is reassigned. If
  they store `tradingsymbol`, they break on a corporate rename. Getting this wrong means users lose
  their saved state at every monthly expiry — the most visible possible failure of a "professional"
  terminal.
- **CURRENT STATE.** The frontend keys everything by display symbol string
  (`lib/instruments.ts`, route params). Options are keyed by `(underlying, expiry, strike, side)`
  in the API, which is the durable form. `LatestQuoteTracker` keys by `instrument_token`, which is
  correct for live state. No rename or rollover mapping exists (previous gap E-3, still open).
- **TARGET STATE.** Two-tier identity. (1) **Durable identity** for persisted user state:
  equities/indices by `(exchange, tradingsymbol)`; derivatives by
  `(exchange, underlying, instrument_type, expiry_rule, strike, option_type)` where `expiry_rule` is
  `NEAR | NEXT | FAR | ABSOLUTE:<date>`. A watchlist entry of "NIFTY near future" therefore survives
  rollover. (2) **Transient identity** for live data: `instrument_token`, valid only within a
  session. A resolver in the instrument read model maps durable → transient at load time; a durable
  key that resolves to nothing renders as `EXPIRED`/`DELISTED` rather than disappearing.
- **DATA SOURCE.** `instruments` read model (RM-4); a `symbol_alias` table for renames.
- **ARCHITECTURAL OWNER.** Backend RM-4 + resolver; frontend `canonical/instrument.ts`.
- **DEPENDENCIES.** RM-4.
- **IMPLEMENTATION PHASE.** Phase 7 (with search/instruments), before any persistence ships in
  Phase 14.
- **ACCEPTANCE CRITERIA.** (1) A watchlist containing "NIFTY near future" resolves to the new
  contract after a simulated rollover, with no user action. (2) An `ABSOLUTE` expiry that has passed
  renders `EXPIRED` and is not requested from the stream. (3) A renamed symbol resolves through the
  alias table and the chart shows continuous history.

---

#### N-11 — Runtime data-provenance inspector

- **REQUIREMENT.** Any number on screen can be interrogated at runtime for its lineage: source
  layer, observation timestamp, computation, inputs, and freshness.
- **WHY IT MATTERS.** The document mandates written lineage (AC), which serves engineers. Traders
  need it at 10:47 when Marketwatch shows −1.2% and the chart implies −0.4%: without an inspector,
  the answer is a support conversation and a loss of trust in the whole terminal. With four data
  layers, three provenance classes and a live/last-session fallback, "why does this say that" is a
  *frequent* question by design, not an edge case.
- **CURRENT STATE.** Nothing. `envelope.as_of` is displayed, and it is the wrong timestamp
  (response time, not market time) — the exact class of confusion an inspector resolves.
- **TARGET STATE.** Every canonical field carries `{ value, provenance, source, as_of,
  reference_type?, inputs? }` in the store (not necessarily on the wire — the wire ships it once per
  group, see D.5). Shift-click, or a per-panel "inspect" toggle, renders a popover with the field's
  lineage and a link to the AC lineage table entry. Costs nothing when closed.
- **DATA SOURCE.** Existing envelope `data_state` + per-group freshness (D.5) + the canonical
  adapter's own knowledge of what it computed.
- **ARCHITECTURAL OWNER.** Frontend `canonical/` (attaches provenance) + `ui/Inspector`.
- **DEPENDENCIES.** ADR-15, D.4, D.5.
- **IMPLEMENTATION PHASE.** Phase 2 for the data model (provenance on canonical fields), Phase 15
  for the inspector UI. The data model must come first or it cannot be retrofitted.
- **ACCEPTANCE CRITERIA.** (1) Inspecting Marketwatch change% shows `reference_type`,
  `reference_price`, its session date, and the LTP observation time. (2) Inspecting an INFERRED
  field names the inference and its assumption. (3) Inspecting a MODEL field shows algorithm id,
  version, maturity and missing inputs.

---

#### N-12 — Partial-session and data-gap rendering

- **REQUIREMENT.** Sessions with ingestion gaps must render as gaps, never as continuity.
- **WHY IT MATTERS.** If ingestion is down 10:00–11:00, tick-derived candles simply have no rows in
  that hour. `session_aggregate_ohlc` does "no empty-bar fill" (`ohlc.py:147`), so the chart draws a
  line from the 09:59 close to the 11:00 open — a smooth move that never happened. A trader reads
  that as price action. Given N-3 (restarts) and the current 4-session history built during an
  unsupervised period, gaps are the normal case, not the exception.
- **CURRENT STATE.** `session_coverage.py` and `expected_bounds` exist and compute expected session
  bounds for partial-coverage detection — the machinery is there. It is not surfaced through the
  chart API and the frontend has no concept of a gap.
- **TARGET STATE.** The bar read model records, per session per symbol, the expected vs observed bar
  count and a `gaps: [{from, to}]` list. `/candles` returns `coverage: { expected_bars,
  observed_bars, completeness, gaps[] }`. The chart renders gaps as visible discontinuities with a
  hatched region, and the panel header shows `PARTIAL 87%`. Aggregations over a gapped window
  (VWAP, volume totals, relative strength) return `null` with a reason when completeness is below a
  configured floor, reusing the `max_pain_min_completeness` precedent.
- **DATA SOURCE.** RM-1 bar store + `session_coverage` + `market_calendar` (RM-5) for expected bars.
- **ARCHITECTURAL OWNER.** Backend RM-1 + `market/ohlc.py`; frontend chart controller.
- **DEPENDENCIES.** RM-1, ADR-14.
- **IMPLEMENTATION PHASE.** Phase 3 (with RM-1).
- **ACCEPTANCE CRITERIA.** (1) A synthetic session with a 60-minute hole reports
  `completeness ≈ 0.84` and one gap spanning that hour. (2) The chart shows a discontinuity, not an
  interpolated line. (3) Session VWAP over that session is `null` with
  `reason: "insufficient_coverage"`.

---

#### N-13 — Tick-size-aware price rendering and quantity semantics

- **REQUIREMENT.** Prices render at the instrument's tick size; quantities render in the unit the
  trader thinks in (shares for equity, lots and contracts for derivatives).
- **WHY IT MATTERS.** A terminal that shows `1234.5600000001` or an option premium at four decimals
  when the tick is 0.05 reads as amateur, and worse, it obscures whether a price is *at* a tradable
  tick. Lot size is the difference between "500" meaning 500 shares and 500 lots (= 37 500 units for
  a 75-lot NIFTY contract) — a 75× misreading of size on the tape and in depth.
- **CURRENT STATE.** `tick_size` and `lot_size` are present in the instrument cache and are used
  nowhere. `lib/format.ts` formats by generic number rules. `market/unusual.py` computes
  `trade_notional` from `last_quantity × price`, which is correct in units but is displayed without
  a lot context. `OptionChain.tsx` guesses IV units by testing `iv <= 3`, which is the same class of
  problem — inferring units instead of carrying them.
- **TARGET STATE.** `InstrumentMeta` carries `tick_size`, `lot_size`, `price_decimals` (derived from
  tick size), `quantity_unit`. `canonical/units.ts` exposes `formatPrice(value, meta)` and
  `formatQuantity(value, meta, { as: "units" | "lots" })`. Depth, tape and chain show lots with
  units on hover. No component formats a price without instrument metadata.
- **DATA SOURCE.** RM-4 `instruments` (already available in the Kite master).
- **ARCHITECTURAL OWNER.** Frontend `canonical/units.ts` + `lib/format.ts`; backend supplies meta.
- **DEPENDENCIES.** RM-4 (G-12), which is why this cannot land before Phase 7 even though the need
  appears as soon as any price is rendered.
- **IMPLEMENTATION PHASE.** Phase 7, with RM-4. Until then, prices render with a documented default
  of 2 decimals and quantities in units only, with the lots column marked `UNAVAILABLE`.
- **ACCEPTANCE CRITERIA.** (1) A 0.05-tick instrument never renders more than 2 decimals.
  (2) Option quantities show both contracts and lots. (3) A unit test asserts no component imports
  a raw number formatter for prices.

---

#### N-14 — Account-data classification and isolation

- **REQUIREMENT.** Account data (positions, holdings, orders, margins, funds) is a separate data
  domain from market data, with its own storage, its own API namespace, its own authorisation, and
  its own failure isolation.
- **WHY IT MATTERS.** Market data is non-sensitive and cacheable; account data is financially
  sensitive and must never be cached in a shared layer or logged. Today they share one SQLite file,
  one API surface, one envelope and one capture process — and the account process is the one
  crash-looping on a bad token. Mixing them means an account-domain failure (or an account-domain
  exposure) becomes a market-data event.
- **CURRENT STATE.** `positions_snapshot`, `margin_snapshot`, `holdings_snapshot`, `order_events`,
  `trade_fills` live in `nse_pipeline.db` alongside market data, as opaque `payload_json`.
  `/api/v1/account` returns the latest blob. `settings/page.tsx` renders it via `JSON.stringify` —
  i.e. raw account JSON is shipped to the browser and dumped on screen.
- **TARGET STATE.** (1) A typed projection layer parsing Kite payloads into explicit columns
  (W.3), so no raw broker JSON reaches the browser. (2) `/api/v1/account/**` treated as a distinct
  security domain (AF), never included in bulk/market read models, never in the stream's market
  channel, and excluded from any shared cache. (3) A separate error boundary so an account failure
  degrades only account panels (AE). (4) Field-level allow-listing: the API returns only fields the
  UI needs, not the broker's whole response.
- **DATA SOURCE.** Kite REST (orders, positions, holdings, margins) via the existing capture
  service.
- **ARCHITECTURAL OWNER.** Backend: a new `account/` module and `api/account.py`. Frontend:
  `panels/portfolio/*` reading only typed models.
- **DEPENDENCIES.** N-2 (token) to have any data at all.
- **IMPLEMENTATION PHASE.** Phase 13.
- **ACCEPTANCE CRITERIA.** (1) No endpoint outside `/account/**` returns any account field. (2) No
  account field appears in the SSE market channel. (3) `JSON.stringify` of a broker payload appears
  nowhere in the frontend. (4) Killing the account capture leaves Marketwatch, charts, options and
  intelligence fully functional.

---

#### N-15 — Expiry-day and settlement semantics

- **REQUIREMENT.** Explicit handling of expiry-day behaviour: OI reset, final settlement price,
  the last trading hour of an expiring contract, and the roll of "near month".
- **WHY IT MATTERS.** On expiry day, OI on the expiring series collapses to zero by definition —
  which the unusual-activity engine will read as the largest OI event it has ever seen, and the
  futures buildup classifier will read as mass "long unwinding". Weekly NIFTY expiry means this
  happens *every week*. Without expiry awareness the intelligence layer generates its most confident
  garbage on the most-watched day.
- **CURRENT STATE.** `expiry` is known per contract. Nothing keys off proximity to it.
  `futures_analytics.price_oi` classifies buildup with no expiry guard.
  `market/unusual.py` scores OI bursts with no expiry guard. No settlement price exists (R-7), so
  `ohlc_close` stands in as the futures reference — technically wrong every day, and wrong in a
  materially visible way on expiry.
- **TARGET STATE.** (1) `days_to_expiry` and an `expiry_phase` (`NORMAL | EXPIRY_WEEK | EXPIRY_DAY |
  EXPIRED`) on every derivative in RM-4. (2) OI-based inference suppressed or explicitly re-based on
  `EXPIRY_DAY` for the expiring series, with a stated reason rather than a silent skip. (3) A
  `settlement_price` column in `session_reference` (RM-2), populated for derivatives, used as the
  futures/options reference price for day change (AB). Until it can be sourced, `reference_type`
  reports `OFFICIAL_CLOSE` and the UI says so — honest rather than mislabelled. (4) Near/Next/Far
  resolution driven by the calendar, so "near" rolls automatically.
- **DATA SOURCE.** Instrument master (expiry); `market_calendar` (RM-5) for expiry sessions;
  settlement price source UNVERIFIED — not available from the tick stream, so flagged as a sourcing
  task in Phase 2 rather than assumed.
- **ARCHITECTURAL OWNER.** Backend RM-2, RM-4, `futures_analytics.py`, `unusual.py`.
- **DEPENDENCIES.** ADR-14, RM-2, RM-4.
- **IMPLEMENTATION PHASE.** Phase 9.
- **ACCEPTANCE CRITERIA.** (1) A replayed expiry day produces zero `oi_burst` events on the
  expiring series and a stated suppression reason. (2) `days_to_expiry` and `expiry_phase` appear on
  every derivative payload. (3) Futures day change uses `settlement_price` when available and
  reports `reference_type` truthfully when not.

---

#### N-16 — API response-size and pagination governance

- **REQUIREMENT.** Every collection endpoint declares a maximum response size and a pagination or
  bounding strategy; no endpoint can return an unbounded set.
- **WHY IT MATTERS.** RM-4 will hold the full NSE instrument master — tens of thousands of rows
  including every option contract. A naive `/instruments` returns tens of megabytes through a
  1-vCPU box and a Next.js proxy with a 20 s timeout. Measured precedent already exists at small
  scale: `/charts` returns 125 KB and `/options/NIFTY` 33.5 KB for only 84 contracts.
- **CURRENT STATE.** `analytics.chart_max_points: 500` is the only response bound in the system,
  and it is a downsampling cap rather than a declared response contract. `/instruments` does not
  exist yet, which is precisely why the bound should be designed before it does.
- **TARGET STATE.** A documented per-endpoint budget table (AG) with `max_rows`, `max_bytes` and the
  bounding mechanism (`limit`+cursor, or a required filter). `/instruments` requires either a query
  string of ≥2 characters or a segment filter, and returns ≤50 rows. `/candles` is bounded by
  `max_points`. Bulk quotes bounded by an explicit token list of ≤250. A test asserts every
  collection endpoint's p99 payload against its budget.
- **DATA SOURCE.** N/A.
- **ARCHITECTURAL OWNER.** Backend `api/`.
- **DEPENDENCIES.** None.
- **IMPLEMENTATION PHASE.** Phase 0 (as contract), enforced per endpoint as each ships.
- **ACCEPTANCE CRITERIA.** (1) No endpoint returns >256 KB at p99. (2) `/instruments` with an empty
  query returns a typed 400, not the master. (3) Payload budgets are asserted in CI.

---

#### N-17 — Effective-SLA alignment across the request chain

- **REQUIREMENT.** Timeouts must be monotonically increasing from the innermost hop outward, and
  the tightest budget must be the backend's own.
- **WHY IT MATTERS.** Today the chain is: backend unbounded → Next proxy 20 s
  (`api/v1/[...path]/route.ts`) → browser client 15 s (`REQUEST_TIMEOUT_MS`). The browser gives up
  *before* the proxy, so a slow response produces a client abort while the VM keeps burning CPU on a
  request nobody will read — the worst outcome on a 1-vCPU box, and exactly what a 32 s chart request
  does today. Under load this compounds: aborted-but-still-executing requests are pure waste.
- **CURRENT STATE.** Inverted as described. No backend-side timeout at all.
- **TARGET STATE.** Backend per-endpoint deadline (AG budgets, hard cap 5 s) that returns a typed
  `deadline_exceeded` rather than running on; proxy timeout 8 s; browser timeout 10 s. Backend
  cancellation must actually abort the DuckDB query, not just return. Streaming endpoints are exempt
  and use heartbeats instead (E.4).
- **DATA SOURCE.** N/A.
- **ARCHITECTURAL OWNER.** Backend `api/`; frontend `data/queryClient` and the proxy route.
- **DEPENDENCIES.** RM-1 (a chart cannot meet a 5 s cap without it).
- **IMPLEMENTATION PHASE.** Phase 3 (after RM-1 makes the budgets achievable; enforcing them first
  would just convert slow charts into failed charts).
- **ACCEPTANCE CRITERIA.** (1) A deliberately slow query returns `deadline_exceeded` in ≤5 s with
  CPU released. (2) No client-side abort occurs before a server-side deadline. (3) Aborted requests
  release their DuckDB connection, asserted by a connection-count test.

---

#### N-18 — Degraded-mode contract for the intelligence and analytics tiers

- **REQUIREMENT.** Analytics and model tiers declare a machine-readable degradation state that the
  UI consumes, distinct from an HTTP error.
- **WHY IT MATTERS.** The requirement is that "options analytics unavailable must not break spot,
  quote, chart, depth". Achieving that with HTTP status codes alone forces the frontend to guess
  from a 500 what is broken and what is still trustworthy. A typed degradation state makes graceful
  degradation a data property rather than error-handling folklore.
- **CURRENT STATE.** Partially present and well-designed where it exists: `chain_status`,
  `candles_status` + `candles_reason`, `basis_status`, `iv_source`, `maturity` tiers, and
  `data_state.reason` all follow this pattern. It is not uniform, and there is no tier-level
  aggregate.
- **TARGET STATE.** Generalise the existing pattern into one envelope field:
  `subsystems: { options: {status, reason}, futures: {...}, activity: {...}, intelligence: {...},
  charts: {...} }` with statuses `OK | PARTIAL | UNAVAILABLE | NOT_APPLICABLE | NOT_MATURE`. Every
  panel reads its own subsystem status and renders a typed state. `NOT_APPLICABLE` is explicitly not
  `UNAVAILABLE` (D.4).
- **DATA SOURCE.** Existing per-domain status fields, lifted into the envelope.
- **ARCHITECTURAL OWNER.** Backend `api/read_model.py::_envelope`; frontend `canonical/status.ts`.
- **DEPENDENCIES.** None — it formalises what exists.
- **IMPLEMENTATION PHASE.** Phase 0 as a contract; populated per domain as each ships.
- **ACCEPTANCE CRITERIA.** (1) Forcing an options-analytics exception yields
  `subsystems.options.status = UNAVAILABLE` with the quote and chart still `OK`. (2) An unsubscribed
  stock's option chain reports `NOT_APPLICABLE`, not `UNAVAILABLE`. (3) Every panel has a test for
  each of its possible subsystem states.

---

#### N-19 — Bounded, testable frontend fixture corpus from real sessions

- **REQUIREMENT.** A committed corpus of real recorded payloads — live, last-session, no-data,
  partial, degraded, expiry-day — used by frontend tests and by local development without the VM.
- **WHY IT MATTERS.** Ingestion has been down since 2026-09-04 and the token is invalid. Nobody can
  develop or test any live behaviour today. Without fixtures, the entire frontend rebuild is blocked
  on a working broker session, and every state that is *hard* to reproduce (stale, partial, gap,
  ATM-outside-window, not-mature) will be tested by nobody and will therefore be wrong.
- **CURRENT STATE.** Backend has focused tests including fallback tests. The frontend has vitest
  configured with no market-data fixtures. Local development points at `http://127.0.0.1:8080`,
  which requires an SSH tunnel to a VM that is currently serving only last-session data.
- **TARGET STATE.** `fixtures/` in the dashboard repo containing captured real responses for each
  endpoint × each data state, plus a `mock` mode in the API client that serves them. A recorded
  SSE frame sequence for stream tests, including a disconnect, a reconnect, an out-of-order frame
  and a duplicate. Fixtures are captured from the VM by a script, never hand-authored, so they cannot
  drift into fiction.
- **DATA SOURCE.** Real VM responses; real compacted sessions replayed.
- **ARCHITECTURAL OWNER.** Frontend `fixtures/` + `api/mock.ts`; backend a `scripts/capture_fixtures.py`.
- **DEPENDENCIES.** None for last-session/no-data fixtures (capturable today). Live fixtures need N-2.
- **IMPLEMENTATION PHASE.** Phase 0 — it unblocks Phases 6–15 while the broker session is being fixed.
- **ACCEPTANCE CRITERIA.** (1) `npm test` passes with no network and no VM. (2) Every data state in
  AD has at least one fixture. (3) A fixture-staleness test fails when the live API's shape diverges
  from the recorded shape.

---

#### N-20 — Model-input reproducibility (already satisfied — do not regress)

- **REQUIREMENT.** Every model output must be reconstructible from stored inputs.
- **WHY IT MATTERS.** Trade Intelligence makes claims. Without stored inputs, a disputed signal
  cannot be audited and the maturity discipline is unverifiable.
- **CURRENT STATE.** **Already satisfied.** `signal_log` stores `features_json`,
  `attribution_json`, `model_version`, `maturity_tier`, `score`, `probability`, `trade_date` and
  `track` per signal (`sqlite_store.py:47-60`), with indexes on timestamp, symbol and
  `(trade_date, maturity_tier)`. 55 549 rows exist. This is better than most production systems.
- **TARGET STATE.** Unchanged. Recorded here so the rebuild does not lose it: the new
  `/intelligence` endpoint (C-1) must read from this table and expose `model_version` and
  `maturity_tier`, and the retention policy (N-1) must keep `signal_log` **forever**.
- **DATA SOURCE.** `signal_log`.
- **ARCHITECTURAL OWNER.** Backend `signals/`.
- **DEPENDENCIES.** N-1 must not prune it.
- **IMPLEMENTATION PHASE.** Phase 11 (exposure), Phase 0 (retention exemption).
- **ACCEPTANCE CRITERIA.** (1) The retention job never deletes from `signal_log`, asserted by test.
  (2) `/intelligence` responses carry `model_version` and are joinable to `signal_log`.

---

## A. Current verified state

### A.1 Production runtime (measured on the VM, 2026-09-07)

| Fact | Value | How verified |
| --- | --- | --- |
| Host | `nse@168.144.66.235`, `ubuntu-s-1vcpu-2gb-blr1` | hostname in journal |
| CPU / RAM | **1 vCPU / 2 GB** | prior `nproc` / `free`; hostname confirms the droplet class |
| Disk | **48 GB total, 16 GB used, 32 GB available (33%)** | `df -h /` |
| API | `nse-api.service` — active, running, localhost-only, 1 uvicorn worker | `systemctl`, prior inspection |
| Installed units | `nse-api`, `nse-account-capture`, `nse-compact`(+timer), `nse-features`(+timer), `nse-labels`(+timer), `nse-live-signals` | `ls /etc/systemd/system` |
| **No ingest unit** | `nse-ingest.service` is **not installed**; only `deploy/vm/nse-ingest.service.example` exists in the repo | `ls /etc/systemd/system \| grep nse` |
| **Account capture** | **`activating (auto-restart)` — crash-looping** on `kiteconnect.exceptions.TokenException: Incorrect api_key or access_token`, last at 13:41:41 IST, 3.2 s CPU per attempt | `systemctl list-units`, `journalctl -u nse-account-capture` |
| Ingestion last event | `2026-09-04T11:21:51Z` `shutdown` "Ingestion service stopped", preceded by `disconnect` and three `flush` events | `ingestion_meta` |
| `latest_quotes` | **0 rows** | `SELECT COUNT(*)` |
| `activity_samples` | **0 rows** | `SELECT COUNT(*)` |
| SQLite | 367 MB; `-wal` 0 bytes (checkpointed), `-shm` 32 KB | `du -sh` |
| Operational table sizes | `feature_log` 134 672, `quality_log` 93 785, `signal_log` 55 549, `positions_snapshot` 3 826, `margin_snapshot` 1 913, `holdings_snapshot` 1, `order_events` **0**, `trade_fills` **0** | `SELECT COUNT(*)` |
| Raw store | `data/raw/{2026-09-01..04}` = **9.2 GB** (~2.3 GB/session) | `du -sh data/raw` |
| Compacted store | **390 MB**, 4 sessions, 589 symbol directories per session, **one `ticks.parquet` each, nothing else** | `du -sh`, `find -name '*.parquet' \| uniq -c` |

### A.2 API latency (re-measured 2026-09-07, on last-session fallback, with the account service crash-looping)

| Endpoint | Time | Payload | Verdict |
| --- | --- | --- | --- |
| `GET /api/v1/charts/NIFTY%2050?interval=5m` | **32.03 s** | 125 094 B | **Broken.** Exceeds the browser's 15 s timeout and the proxy's 20 s. Worse than the 22.81 s measured earlier in this audit cycle. |
| `GET /api/v1/options/NIFTY` | **1.29 s** | 33 555 B | **Too slow to poll.** 84 contracts. At an 8 s poll this is 16% of the only core, per client. |
| `GET /api/v1/unusual-activity?limit=10` | 1.03 s warm | 7 242 B | Acceptable warm; cold path was 2.93 s after optimisation and 11.55 s before. Behind `last_session_cache_ttl_seconds: 120`. |
| `GET /api/v1/watchlists/quotes` | 0.22 s | 8 203 B | Fine. |
| `GET /api/v1/overview` | 0.22 s | 2 587 B | Fine. |

The chart figure regressed between two measurements in the same audit. Two contributing causes are
identified and both are real: the missing bar layer (R-2) makes the work O(ticks), and the
account-capture crash loop (N-2) is competing for the single core. This is precisely the class of
compounding failure a 1-vCPU deployment produces.

### A.3 Instrument universe (verified, and smaller than previously reported)

`config/instruments_cache.json` on the VM, stamped `2026-09-04T05:13:00Z`:

| Class | Count | Kite mode |
| --- | --- | --- |
| `equity_depth` | 100 | FULL |
| `equity_quote` | 399 | QUOTE |
| `index` | 2 | FULL |
| `options` | 84 | FULL |
| `futures` | 4 | FULL |
| **total** | **589** | — |

Corroborated by the compacted data: 589 symbol directories, containing NIFTY strikes
23450–24450 CE/PE (42), BANKNIFTY 56500–58500 CE/PE (42), `NIFTY26SEPFUT`, `NIFTY26OCTFUT`,
`BANKNIFTY26SEPFUT`, `BANKNIFTY26OCTFUT`, `NIFTY 50`, `NIFTY BANK`, and 499 equities.

Two consequences:

1. **Near + Next futures already work** (R-3). Only Far month is missing, and
   `futures.contract_count: 2` → `3` is the whole change.
2. **Stock derivatives do not exist in any stored session** (R-4). `stock_derivatives.enabled: true`
   is set, and `settings.yaml:60` states it "becomes live only on the next planned ingest restart".
   The cache also lacks the `universe` / `fo_eligible_nifty100` keys the deployed code writes,
   proving it was generated by an older build and never regenerated (R-9). So stock option chains
   and stock futures must render a typed **NOT_SUBSCRIBED** state, not an empty chain (D.4).

### A.4 Backend architecture (verified accurate)

Kite → one WebSocket (`broker/websocket_listener.py`) → `NormalizedTick` → `LatestQuoteTracker`
(in-process, monotonic) → SQLite `latest_quotes` (batched, ≥2 s) + `activity_samples` (≥60 s) → raw
Parquet (`data/raw/{date}/`) → `nse-compact` at 15:45 IST → `data/compacted/{date}/{symbol}/ticks.parquet`
→ `nse-features` 16:00 → `feature_log` → `nse-labels` 16:30 → labels → `nse-live-signals` →
`signal_log` → FastAPI `127.0.0.1:8080` → SSH tunnel → Next.js.

26 routes, **all `GET`**. No WebSocket, no SSE, no POST/PUT/DELETE. CORS allows only
`http://127.0.0.1:8501` and `http://localhost:8501` — the old Streamlit port, not the Next.js port,
which is why the server-side proxy at `src/app/api/v1/[...path]/route.ts` is load-bearing rather than
optional.

**What is genuinely good and must be preserved:**

- `market/read_policy.py` — `MarketDataPolicy` is the single live→last_session→no_data decision
  point, with internal caching. Endpoint-specific fallbacks do not exist.
- `market/data_state.py` — already speaks the vocabulary the terminal needs
  (`market_state`, `data_status`, `as_of`, `session_date`, `source`, `reason`), never substitutes
  wall-clock for observation time (`session_iso`), and refuses to infer "live" from row existence.
  `merge_states` ranks live > last_session > no_data.
- `market/latest.py` — `LatestQuoteTracker.observe` drops out-of-order ticks
  (`latest.py:102-105`), and `snapshot_from_tick` clamps negative volume deltas to `None`.
- `signals/maturity.py` — the 60-pooled-live-day gate. Refuses to emit a probability or score below
  threshold. This is the most valuable discipline in the codebase.
- `futures_analytics.basis_freshness` — returns `null` rather than a basis computed across
  incompatible timestamps, governed by `futures_basis_max_age_seconds: 10`.
- `signal_log` — full feature and attribution capture per signal (N-20).
- `research/` — documented as never imported by production.

**What does not exist (verified by repo-wide search):** browser streaming; instrument search;
Time & Sales; per-level depth in live state; Greeks in the API; bar/candle storage; a trading
calendar; price bands, freeze quantity, circuit limits, settlement price; a retention policy; typed
account projections; `busy_timeout`; response-size governance; a request-level deadline.

### A.5 Frontend architecture (verified accurate)

Next.js 15.5 App Router, React 19.1, `lightweight-charts` 4.2, TypeScript, vitest. **No state
management library, no data-fetching library, no virtualization library.**

- **One data primitive:** `hooks/useApiQuery.ts` — `setInterval` polling, visibility-aware,
  late-response guarded. Everything fetches through it.
- **One transport:** `api/client.ts::apiGet` — 1.5 s TTL cache, in-flight dedupe, 15 s timeout.
- **`AppShell.tsx:20-24` holds five live queries** (NIFTY 5 s, BANKNIFTY 5 s, health 8 s, watchlists
  30 s, watchlist quotes 8 s) and wraps `{children}` at `layout.tsx:30`, so they run on every route.
- **`app/page.tsx` re-requests what `AppShell` already has**, and adds `/unusual-activity` at 10 s
  and `/options` at 8 s. Measured request volume for a single idle tab on the overview:
  **~113 req/min**.
- **Nine page routes**, page-per-domain: `/`, `/markets`, `/options/[underlying]`,
  `/symbol/[symbol]/[tab]`, `/charts/[symbol]` (redirect), `/unusual`, `/activity`, `/settings`.
- **`data_state` is never read.** Repo-wide search: the frontend does not consume `market_state`,
  `data_status`, `session_date`, `source` or `reason` anywhere. The backend's entire fallback
  discipline is invisible to the user.
- **`Badges.tsx::DataFreshnessBadge` computes freshness from `envelope.as_of`**, which
  `read_model._envelope` documents as *the response time*, not the data time. So stale data renders
  "As of 0s". `PriceHeader.tsx` uses `quote.timestamp` and is correct — two components, two answers,
  one of them wrong.
- **`lib/freshness.ts:31-86`** re-derives NSE session hours in the browser from hardcoded 09:15–15:30
  and weekday checks, using the browser's wall clock and timezone.
- **`lib/candles.ts`** correctly refuses to synthesise candles from `last_price` observations — keep
  that rule — but `latestSessionDate`/`pointsOnSessionDate` apply a hardcoded 5.5 h offset for IST.
- **Charts destroy and recreate the series on every data update** (`CandlestickChart.tsx`,
  `HistogramChart` wrappers), i.e. every 8 s poll. `chartTheme.ts::useChartHost` disposes in the
  wrong order (`chart.remove()` before series removal), throwing on unmount.
- **`OptionChain.tsx` guesses IV units** by testing `iv <= 3`.
- **`FinancialTable.tsx`** has client-side sorting and no virtualization; column definitions are
  re-created per render, defeating memoization.
- **`api/envelope.ts`** is the one piece of frontend discipline worth preserving verbatim: it
  requires `maturity` on every envelope and strips probabilities that are not permitted.
- `next.config.ts` sets `reactStrictMode` and configures **no rewrites**, despite UI text implying a
  proxy configuration.

### A.6 Data-capability reality (what the backend actually receives)

| Capability | Index | Equity FULL (100) | Equity QUOTE (399) | Options (84) | Futures (4) | Stock F&O |
| --- | --- | --- | --- | --- | --- | --- |
| LTP | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ not subscribed |
| Session OHLC | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| Volume | ❌ n/a | ✅ | ✅ | ✅ | ✅ | ❌ |
| Last quantity | ❌ n/a | ✅ | ✅ | ✅ | ✅ | ❌ |
| Average price | ❌ n/a | ✅ | ✅ | ✅ | ✅ | ❌ |
| OI | ❌ n/a | ❌ n/a | ❌ n/a | ✅ | ✅ | ❌ |
| Exchange timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| Last trade time | ❌ n/a | ✅ | ❌ | ✅ | ✅ | ❌ |
| Total buy/sell qty | ❌ n/a | ✅ | ❌ | ✅ | ✅ | ❌ |
| **5-level depth** | ❌ n/a | **received, then discarded** | ❌ | **received, then discarded** | **received, then discarded** | ❌ |

The depth row is the sharpest finding of this pass. MODE_FULL delivers five bid and five ask levels
with price, quantity and order count. Raw and compacted Parquet **retain them**
(`duckdb_store.LAST_ROW_DEPTH_COLUMNS`). But `latest_quotes` has **no per-level columns at all** —
its depth footprint is `best_bid_price`, `best_bid_quantity`, `best_ask_price`,
`best_ask_quantity`, `bid_depth_5`, `ask_depth_5` (aggregate quantities), `spread`, `mid_price`,
`depth_imbalance` (`sqlite_store.py:251-283`). So **a live depth ladder is not merely unexposed by
the API, it is not stored** — the ladder is only recoverable from Parquet, i.e. only for historical
sessions. This changes the depth work from "add an API field" to a schema change plus an ingestion
change (ADR-12, M.3), and it must happen before any depth panel is designed.

### A.7 Unit and semantic conventions (verified inconsistent)

| Field | Convention | Evidence |
| --- | --- | --- |
| `quotes.change` | **tick-to-tick delta** | `latest.py:126` — `"change": row.get("price_delta")`, and `price_delta = numeric_delta(tick.last_price, prev.last_price)` (`latest.py:54`) |
| `quotes.change_pct` | percent, computed from that tick delta | derived from the above |
| `futures.basis_pct` | **percent** (`×100`) | `futures_analytics.basis_pct` |
| `futures.price_change_pct` | **ratio** (no `×100`) | `_ratio(...)` |
| `futures.oi_change_pct` | **ratio** | `_ratio(oi_change, prev_oi)` |
| `options.oi_change_pct` | **ratio** | `_side_from_quote` |
| `options.iv` / `iv_pct` | both shipped, unit ambiguous to consumers | `OptionChain.tsx` tests `iv <= 3` to guess |
| candle `volume` | **always `null`** | `ohlc.py:176` — non-bar mode sets `volume = None`, and non-bar mode is the only mode that ever runs (R-2) |

The frontend compensates with `formatPct` vs `formatRatioPct` per call site. That is a convention
held in human memory across two repositories, and it is one careless call away from a 100× error on
a number a trader acts on.

### A.8 Historical/aggregation reality

`analytics.chart_lookback_days: 10`, `chart_max_points: 500`. `CHART_INTERVALS` supports
`1m, 3m, 5m, 10m, 15m, 30m, 60m, 1D`. `duckdb_store.read_symbol_ohlc_frames` returns three frames —
`daily`, `candles`, `ticks` — from three path patterns.

**Only the `ticks` path ever exists.** Verified: every compacted symbol directory contains exactly
`ticks.parquet` and nothing else. Therefore:

- `frame_has_bar_ohlc(ticks)` is false — ticks carry *session* `ohlc_*` columns, which `ohlc.py`
  deliberately distinguishes from *bar* `open/high/low/close`.
- Every request lands on `session_aggregate_ohlc(ticks, interval, mode="last_price")`, grouping raw
  observations into session buckets in pandas, per request, per symbol, per interval.
- That path sets `volume = None` for every bar.

`NIFTY 50` alone is 301 KB of ticks for one session; ten sessions of lookback is ~3 MB read,
parsed and grouped per chart request. This is the 32 s. **There are four sessions of history in
existence**, so no amount of query tuning helps — the missing artefact is the bar store itself.

### A.9 Reference-data reality

| Element | Available? | Source |
| --- | --- | --- |
| `tradingsymbol`, `token`, `exchange`, `segment`, `instrument_type`, `expiry`, `strike`, `option_type` | ✅ | Kite instrument master, cached to `config/instruments_cache.json` (589 entries only) |
| `lot_size`, `tick_size` | ✅ present, **used nowhere** | same |
| Index membership (NIFTY100/500) | ✅ | `config/membership/ind_nifty100list.csv`, `ind_nifty500list.csv` |
| **Sector** | ✅ **available and unused** | the `Industry` column of those same CSVs — verified: `Company Name,Industry,Symbol,Series,ISIN Code` |
| ISIN | ✅ unused | same CSVs |
| Trading calendar / holidays | ❌ **does not exist** | — |
| Pre-open / closing / post-close windows | ❌ | `SessionSettings` has only `market_open`, `market_close`, and two grace windows |
| Price bands / circuit limits | ❌ | — |
| Freeze quantity | ❌ | — |
| Settlement price | ❌ | `ohlc_close` substitutes |
| Corporate actions | ❌ | only a `ca_suspect` quality flag |
| Symbol renames | ❌ | — |

---

## B. Problems and blockers

Ordered by whether they block the product, not by effort.

### B.1 Blockers — the product cannot work until these are fixed

| # | Blocker | Evidence | Why it blocks everything |
| --- | --- | --- | --- |
| **BL-1** | **Kite session invalid.** | `TokenException` at 13:41:41 IST today; account service crash-looping | No live data can exist. Restarting ingestion fails identically. Every real-time acceptance test in AN is unrunnable. Also stealing CPU. → N-2 |
| **BL-2** | **No ingestion service.** | No unit in `/etc/systemd/system`; last event 2026-09-04 | `latest_quotes` = 0 rows. The API has served last-session fallback for three days. → N-3 |
| **BL-3** | **No bar storage layer.** | 589 dirs × `ticks.parquet` only; 32.03 s chart | The chart, the single most important panel, cannot load within any client timeout. Not fixable by tuning. → RM-1 |
| **BL-4** | **`change` is a tick delta.** | `latest.py:126` | The first number a trader reads is wrong on every surface. Building a Marketwatch on it means rebuilding it later. → ADR-15 |
| **BL-5** | **No live depth storage.** | `latest_quotes` schema has no per-level columns | "Depth must update even when LTP does not" is unimplementable as designed. → ADR-12 |
| **BL-6** | **No browser transport.** | 26 GET routes, zero streaming | "Continuously updating" is unachievable with `setInterval` at acceptable cost. → ADR-1 |
| **BL-7** | **Disk fills in ~14 sessions.** | 2.3 GB/session, 32 GB free, no retention | The system will take itself down within a month of ingestion resuming. → N-1 |

BL-1 → BL-2 is a hard chain: the token must be valid before the service can run, and the service
must run before anything real-time can be observed, let alone accepted.

### B.2 Correctness problems

| # | Problem | Evidence | Impact |
| --- | --- | --- | --- |
| CO-1 | `change`/`change_pct` are tick-level, presented as day change | A.7 | Wrong on Marketwatch, ticker, instrument header |
| CO-2 | `_pct` fields mix percent and ratio | A.7 | Latent 100× display errors |
| CO-3 | Freshness computed from response time | `Badges.tsx` vs `_envelope` | Stale data displays as fresh — the worst possible lie in a market terminal |
| CO-4 | Session state derived in the browser from a hardcoded clock | `lib/freshness.ts:31-86` | Diverges from the backend's authoritative `market_state` |
| CO-5 | No holiday calendar | R-6, `data_state.py:14-18` | On a holiday the clock says "open"; "previous close" can reference a non-trading day |
| CO-6 | Candle `volume` always null | `ohlc.py:176` | Volume histograms on charts are empty by construction |
| CO-7 | Option-chain window can stop bracketing ATM | N-7 | PCR and Max Pain computed over the wrong domain, with a healthy-looking `chain_status` |
| CO-8 | Futures reference is `ohlc_close`, not settlement | A.9 | Futures day change is technically wrong daily, visibly wrong on expiry |
| CO-9 | Expiry-day OI collapse is unguarded | N-15 | The intelligence layer produces its most confident wrong output every weekly expiry |
| CO-10 | Ingestion gaps render as price moves | N-12 | Charts show moves that never occurred |
| CO-11 | IV units inferred by magnitude test | `OptionChain.tsx` | Silent wrongness when IV legitimately sits near the threshold |
| CO-12 | Clock skew would silently invert live/stale | N-4 | Invisible, total failure of the data-state decision |

### B.3 Architectural problems

| # | Problem | Evidence |
| --- | --- | --- |
| AR-1 | Page-centric routing; instrument is a route param, not shared context | 9 page routes; `symbol/[symbol]/[tab]` |
| AR-2 | No global state; market data lives in component `useState` via `useApiQuery` | repo-wide |
| AR-3 | `AppShell` owns five live queries and wraps every route | `AppShell.tsx:20-24`, `layout.tsx:30` |
| AR-4 | Duplicate fetching: overview re-requests what the shell has | `app/page.tsx` |
| AR-5 | No subscription model; every widget is its own timer | `useApiQuery` per component |
| AR-6 | No virtualization anywhere | `FinancialTable.tsx`, `OptionChain.tsx` |
| AR-7 | Charts recreate series per update; disposal order bug | `chartTheme.ts`, `CandlestickChart.tsx` |
| AR-8 | Search universe is watchlists + two hardcoded indices | `lib/search.ts`, `lib/instruments.ts` |
| AR-9 | Algorithms hard-wired by class key, not a registry | `signals/engine.py` |
| AR-10 | Account data is opaque JSON, rendered by `JSON.stringify` | `settings/page.tsx`, R-8 |
| AR-11 | Timeouts inverted across the request chain | N-17 |
| AR-12 | No cross-tab coordination | N-5 |

### B.4 Performance problems

| # | Problem | Measured | Target (AG) |
| --- | --- | --- | --- |
| PE-1 | `/charts` | **32.03 s** | p95 < 400 ms warm, < 900 ms cold |
| PE-2 | `/options/{u}` | **1.29 s** | p95 < 250 ms |
| PE-3 | `/unusual-activity` cold | 2.93 s (11.55 s pre-optimisation) | p95 < 300 ms |
| PE-4 | Frontend request volume | ~113 req/min per idle tab | < 10 req/min idle, plus one stream |
| PE-5 | Chart re-render | full series recreation per 8 s poll | `series.update()`, < 2 ms |
| PE-6 | Marketwatch | unvirtualized, columns rebuilt per render | 250 rows at 60 fps |

---

## C. Target terminal architecture

### C.1 What the product is

A single-page terminal shell that owns a persistent live connection and a normalized market cache,
inside which the user arranges panels into workspaces. The selected instrument is terminal-wide
context. Navigation changes *what is in a panel*, not *what page you are on*.

### C.2 The shell

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ NIFTY 24 380.15 +0.42%  BANKNIFTY 57 210.05 −0.18%  │ ⌘K search │ ● LIVE 09:47:12 IST │
│                                        │ 🔔 3 │ P&L ₹— │ Funds ₹— │ [Trade ▾] workspace│
├────────────────┬───────────────────────────────────────────┬─────────────────────────┤
│ MARKETWATCH    │ CHART · HDFCBANK · 5m                     │ SNAPSHOT · HDFCBANK     │
│ (virtualized)  │                                           │ LTP / day chg / OHLC    │
│ NIFTY 50       │                                           │ VWAP / vol / OI         │
│ NIFTY BANK     │                                           ├─────────────────────────┤
│ HDFCBANK    ◀  │                                           │ DEPTH (5×2 ladder)      │
│ RELIANCE       │                                           │ bid/ask/qty/orders      │
│ …              │                                           │ spread · imbalance      │
├────────────────┴───────────────────────────────────────────┴─────────────────────────┤
│ TRADE INTELLIGENCE │ OPTIONS │ FUTURES │ ACTIVITY │ TIME & SALES │ UNUSUAL │ POSITIONS│
│ bias · confidence · maturity N/60 · reasons · contradictions · invalidation           │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ ● stream connected · 589 subscribed · LTP 340ms · depth 900ms · OI 3.2s · clock ok    │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

Persistent regions: top bar (index ticker, market state, search, notifications, P&L, funds,
workspace selector) and a status bar (connection, subscription count, per-group data age, clock
health). Neither ever unmounts. Everything between them is panels.

### C.3 The five architectural moves that define the target

1. **One live connection, one cache, many subscribers.** SSE → `SharedWorker` → `MarketCache` →
   per-instrument subscriptions. A HDFCBANK tick notifies only the components that asked for
   HDFCBANK.
2. **Canonical semantics computed once, in the backend.** `change`, `change_percent`,
   `reference_price`, `reference_type`, freshness and session state are computed server-side and
   consumed verbatim. React performs no market-critical arithmetic.
3. **Four explicit data layers with provenance on every field.** Reference / Observed / Derived /
   Model, with OBSERVED / DERIVED / INFERRED / MODEL labels reaching the UI (D.1, D.4).
4. **Read models replace request-time computation** for exactly the four things that need it, and
   nothing else (F.6).
5. **Honesty is structural, not editorial.** Unavailable, not-applicable, stale, partial and
   not-mature are typed states with dedicated rendering. The existing `chain_status`,
   `candles_status`, `basis_status`, `iv_source` and `maturity` fields are the model; N-18
   generalises them.

### C.4 What is explicitly out of scope

Live order execution (designed, gated off — W.5); a second Kite connection (never); synthetic
trade prints or participant identity (never — ADR-8); fabricated Greeks or probabilities (never —
S, T, and the immutable maturity gate); mobile-first layout (AG.6).

## D. Data architecture

### D.1 The four data layers

Every field in the system belongs to exactly one layer. The layer determines where it is stored,
who may compute it, how it is cached, how it is invalidated, and how the UI is permitted to present
it.

| Layer | Contains | Storage | Mutability | Cache | UI presentation rule |
| --- | --- | --- | --- | --- | --- |
| **1. REFERENCE** | instrument identity, exchange, segment, `instrument_type`, `tradingsymbol`, token, expiry, strike, `option_type`, `lot_size`, `tick_size`, `price_band`, `freeze_quantity`, sector, index membership, ISIN, trading calendar, corporate actions, symbol aliases | SQLite `instruments`, `market_calendar`, `corporate_actions`, `symbol_alias` (RM-4, RM-5) | changes daily at most | long TTL (hours), versioned | stated as fact, no freshness badge, but a `reference_as_of` is available in the inspector |
| **2. OBSERVED** | `last_price`, session `open/high/low/close`, `volume`, `last_quantity`, `average_price`, `oi`, `bid`/`ask` prices and quantities, 5-level depth, `exchange_timestamp`, `last_trade_time`, `total_buy_quantity`, `total_sell_quantity` | `latest_quotes` (live), raw + compacted Parquet (historical) | append-only; live row replaced under monotonic guard | no cache on the live path; 120 s on the last-session path | must carry an observation timestamp and a freshness state. Never displayed without one. |
| **3. DERIVED** | `change_absolute`, `change_percent`, `volume_delta`, `oi_delta`, `oi_change_percent`, `spread`, `mid_price`, `depth_imbalance`, VWAP, `basis`, `basis_percent`, PCR, Max Pain, IV, `trade_notional`, relative strength, activity scores, unusual-activity classification | computed in `market/*_analytics.py`; materialised only where F.6 says so | recomputed from inputs | derives its cache from its inputs | must state its inputs' coherence (D.9) and be labelled DERIVED or INFERRED |
| **4. MODEL** | `score`, `probability`, `confidence`, `signal`, `direction`, `reasons[]`, `contradictions[]`, `maturity`, `model_version` | `signal_log`; served by `/intelligence` | versioned, reproducible from `features_json` | short | subject to the immutable maturity gate (S.6). Never rendered as observation. |

**Enforced boundaries.**

- A layer-2 field is never computed. If it is not observed, it is `null` — never inferred from a
  neighbouring field.
- A layer-3 field never overwrites a layer-2 field. `change` does not replace `last_price`;
  `mid_price` does not stand in for `last_price` when LTP is missing.
- A layer-4 field never feeds back into layers 1–3. Model output is a leaf.
- Layer 1 is not duplicated inside layer-2 responses beyond an instrument key plus the display
  minimum (D.3).
- Frontend directories mirror this: `canonical/` may compute layer 3 for *presentation only*
  (formatting, not semantics); it may never compute a market-critical layer-3 value that the backend
  also computes. `change` and `change_percent` are backend-only (ADR-15).

**Current violations to fix:** `change` is layer 3 computed from a layer-2 delta and presented as a
day change (BL-4); the frontend computes session state (layer 1, calendar-dependent) in
`lib/freshness.ts` (CO-4); `iv` units are inferred in the UI (CO-11); `ohlc_close` substitutes for
settlement price, a layer-1/2 confusion (CO-8).

### D.2 Reference data vs market data — ownership

| Element | Owning layer | Owning module | Delivered to the UI how |
| --- | --- | --- | --- |
| Instrument identity (`exchange`, `tradingsymbol`, `token`, `segment`, `instrument_type`) | REFERENCE | RM-4 `instruments` | once per instrument, cached in `MarketCache.meta`, hours-long TTL |
| Contract specification (`expiry`, `strike`, `option_type`, `lot_size`, `tick_size`) | REFERENCE | RM-4 | with instrument meta, not with each quote |
| Sector | REFERENCE | RM-4, from the `Industry` column of the membership CSVs | with instrument meta |
| Index membership | REFERENCE | RM-4, from the membership CSVs | with instrument meta |
| Trading calendar, holidays, session windows | REFERENCE | RM-5 `market_calendar` | once per app load + on date change; drives `SessionState` |
| Price band, freeze quantity | REFERENCE | RM-4 (future — D.8) | with instrument meta |
| Corporate actions | REFERENCE | `corporate_actions` (future — Y) | on demand per instrument |
| LTP, OHLC, volume, OI, depth, timestamps | OBSERVED | `latest_quotes` / Parquet | stream + snapshot |
| `previous_close`, `today_open`, `settlement_price` | OBSERVED, materialised as a session artefact | RM-2 `session_reference` | joined into quote/bulk responses |
| change, change%, basis, PCR, VWAP, imbalance | DERIVED | `market/*_analytics.py` | with the payload that needs them |
| score, probability, maturity | MODEL | `signals/`, `/intelligence` | separate endpoint, separate panel, separate failure boundary |

**Rule:** market-data responses carry `instrument_token` + `tradingsymbol` and nothing else from
layer 1. Everything else about the instrument is fetched once from RM-4 and cached. Rationale: the
bulk-quote path serves up to 250 instruments and must stay under 64 KB (N-16); repeating
`lot_size`, `tick_size`, `sector` and `expiry` on every quote in every frame would multiply the
stream payload for data that changes daily at most.

**One deliberate exception:** `tick_size` and `lot_size` are also included in the *snapshot* (not
the stream frame) for the active instrument, because N-13 requires them to render the very first
price, and waiting on a second round trip would flash an unformatted number.

### D.3 What travels on which channel

| Channel | Layers | Frequency | Payload rule |
| --- | --- | --- | --- |
| REST metadata (`/instruments`, `/calendar`) | 1 | on demand, hours TTL | ≤50 rows, ≤64 KB (N-16) |
| REST snapshot (`/quotes`, `/quotes/bulk`, `/options`, `/futures`, `/activity`) | 2 + 3 | on selection / on hydration | ≤250 instruments, ≤64 KB |
| REST historical (`/candles`, `/series`, `/tape`) | 2 + 3 | on selection, on timeframe change | ≤`chart_max_points` |
| **SSE stream** | 2 + minimal 3 | 1 s coalesced | **delta only**: token + changed fields + `exchange_timestamp`. Full replacement never streams. |
| REST analytics (`/unusual-activity`, `/screener`, `/breadth`) | 3 | 5–30 s | ≤`limit`, capped |
| REST model (`/intelligence`) | 4 | 15–60 s | per instrument or per watchlist |
| REST account (`/account/**`) | separate domain (N-14) | 5–30 s | typed, allow-listed |

### D.4 Data capability negotiation

The terminal must know, per instrument, what data *can* exist — before it renders a placeholder.

**Capability states.** Five, and they are not interchangeable:

| State | Meaning | Rendering |
| --- | --- | --- |
| `AVAILABLE` | subscribed, observed, and fresh within the field group's tolerance | the value |
| `STALE` | subscribed and observed, but older than tolerance | value in a muted style + age |
| `PARTIAL` | the group is partly observed (e.g. 3 of 5 depth levels) | what exists, plus an explicit partial marker |
| `UNAVAILABLE` | *should* exist for this instrument but is not currently obtainable (not subscribed, ingestion down, analytics failed) | a typed empty state naming the reason |
| `NOT_APPLICABLE` | cannot exist for this instrument by nature | the field is **absent or dashed with a tooltip** — never an error, never a zero, never "no data" |

**`NOT_APPLICABLE` must never render as `UNAVAILABLE`.** Concrete cases in this system today:
`NIFTY 50` has no volume, no OI, no depth and no last quantity — it is an index. Rendering "Volume:
no data" for an index is a bug that makes the terminal look broken; rendering the row as absent is
correct. Equity has no OI, ever. `equity_quote`-mode instruments (399 of them) have no depth and no
`last_trade_time` **by subscription mode**, which is `UNAVAILABLE` (a capacity decision we could
change), not `NOT_APPLICABLE`. That distinction is exactly why five states are needed rather than
three.

**Capability contract.** RM-4 supplies, per instrument:

```
InstrumentCapabilities {
  ltp:                 SUPPORTED | NOT_APPLICABLE
  session_ohlc:        SUPPORTED | NOT_APPLICABLE
  volume:              SUPPORTED | NOT_APPLICABLE
  last_quantity:       SUPPORTED | NOT_APPLICABLE
  average_price:       SUPPORTED | NOT_APPLICABLE
  oi:                  SUPPORTED | NOT_APPLICABLE
  exchange_timestamp:  SUPPORTED | NOT_APPLICABLE
  last_trade_time:     SUPPORTED | NOT_APPLICABLE
  depth_5:             SUPPORTED | NOT_APPLICABLE
  subscription:        { mode: FULL | QUOTE | LTP | NONE, kind: STATIC | DYNAMIC | NONE }
}
```

`SUPPORTED` is a statement about the instrument *and* its subscription mode; it is derived from
`(instrument_type, subscription.mode)` by one function so the matrix in A.6 exists in exactly one
place. The runtime state (`AVAILABLE`/`STALE`/`PARTIAL`/`UNAVAILABLE`) is then computed by the
frontend from `SUPPORTED` + observation presence + group freshness (D.5). A field that is
`NOT_APPLICABLE` is never requested, never subscribed and never rendered.

**Frontend rule:** no panel may write `value ?? "—"`. Every optional market field goes through
`capabilityOf(instrument, field)` and renders one of the five states. This is enforceable by lint
against a `MarketField` component.

### D.5 Field-group freshness and temporal consistency

**The problem.** A single `/quotes/{symbol}` response is not a simultaneous snapshot. `last_price`
may come from an observation at 09:47:12.340, the depth aggregate from the same observation, `oi`
from an option contract's last update at 09:47:09, and `previous_close` from a session artefact
dated 2026-09-04. Presenting one `as_of` for all of it is a lie of composition. The current
`DataFreshnessBadge` makes it worse by using the response time (CO-3).

**Field groups.** Freshness is tracked per group, not per field (too costly) and not per instrument
(too coarse):

| Group | Fields | Tolerance (live) | Source of its timestamp |
| --- | --- | --- | --- |
| `price` | `last_price`, `last_quantity`, `last_trade_time`, session `ohlc_*` | 5 s | `exchange_timestamp` of the observation |
| `depth` | 5-level ladder, `best_bid/ask`, `spread`, `mid_price`, `depth_imbalance`, `bid/ask_depth_5` | 5 s | `exchange_timestamp` of the depth-bearing observation |
| `volume` | `volume`, `volume_delta`, `average_price` | 10 s | as `price` |
| `oi` | `oi`, `oi_delta`, `oi_change_percent` | 60 s | `exchange_timestamp` of the OI-bearing observation |
| `reference` | `previous_close`, `today_open`, `settlement_price`, `reference_price` | session-scoped | `session_reference.session_date` |
| `derived` | change, basis, PCR, Max Pain, VWAP, imbalance, activity scores | inherits the worst of its inputs | `min(inputs.as_of)` |
| `model` | score, probability, confidence | 300 s | `signal_log.timestamp` |

Wire format — one entry per group, not per field:

```
freshness: {
  price:  { as_of: "2026-09-07T09:47:12.340+05:30", age_ms: 340,  state: "AVAILABLE" },
  depth:  { as_of: "2026-09-07T09:47:11.780+05:30", age_ms: 900,  state: "AVAILABLE" },
  oi:     { as_of: "2026-09-07T09:47:09.100+05:30", age_ms: 3200, state: "AVAILABLE" },
  reference: { as_of: "2026-09-04T15:30:00+05:30", session_date: "2026-09-04", state: "AVAILABLE" }
}
```

The status bar renders exactly this: `LTP 340ms · depth 900ms · OI 3.2s`. That display is the
requirement's own example, and it falls out of the model rather than being special-cased.

**Temporal tolerance for derived metrics.** Every derived metric declares the maximum acceptable
timestamp spread between its inputs, and returns `null` with a reason when exceeded. This
generalises `futures_basis_max_age_seconds: 10`, which already does exactly this and is the
precedent:

| Metric | Inputs | Max spread | On violation |
| --- | --- | --- | --- |
| `basis`, `basis_percent` | future LTP, spot LTP | **10 s** (existing config) | `null`, `basis_status: "stale_spot"` / `"stale_future"` — already implemented |
| `change_absolute`, `change_percent` | LTP, `reference_price` | reference is session-scoped; only requires same-instrument, and that `reference.session_date` is the *previous trading day* per RM-5 | `null`, `reason: "no_reference"` |
| `depth_imbalance`, `spread`, `mid_price` | bid and ask of the **same observation** | **0 — must be same observation** | `null`. Never combine a bid from one tick with an ask from another. |
| PCR (OI and volume) | all chain contracts' OI/volume | **30 s** across contracts, plus `atm_coverage != ATM_OUTSIDE_WINDOW` (N-7) | `null` + reason |
| Max Pain | all chain contracts' OI | **30 s** + `completeness ≥ 0.70` (existing `max_pain_min_completeness`) | `null` + reason |
| IV | option LTP, spot LTP, time to expiry, rate | **10 s** between option and spot | `null`, `iv_source: "unavailable"` |
| Relative strength | instrument series, benchmark series | same bar bucket | `null` + reason |
| Volume/OI delta | consecutive observations of one instrument | ≤120 s between them, else the delta spans an unknown gap | `null` + `reason: "observation_gap"` |
| `trade_notional` | `last_quantity`, `last_price` of the same observation | 0 — same observation | `null` |
| Unusual-activity score | its declared inputs | worst input's tolerance | omit the event rather than score it partially |

**Cross-instrument rule:** any metric combining two instruments (basis, relative strength, PCR
against spot, option vs underlying) must record both timestamps and the spread in its output, so the
UI and the inspector (N-11) can show *why* a metric is null. `basis_freshness` already returns
`spot_as_of`, `futures_as_of` and `data_age_seconds`; that shape becomes the standard.

### D.6 Market calendar — canonical source and ownership (ADR-14)

**Decision: a backend-owned `market_calendar` SQLite table is the single source of truth for every
session-sensitive calculation. No frontend session arithmetic. No inference from data presence.**

Alternatives considered:

| Option | Verdict |
| --- | --- |
| Hardcode session hours and weekday checks (today, both repos) | **Rejected.** Already wrong: no holidays (CO-5), duplicated in `lib/freshness.ts` and `config.py`, and it makes "previous trading day" unanswerable — which breaks `previous_close`, day change, and chart session boundaries. |
| Infer trading days from data presence | **Rejected.** Circular. A day with ingestion down looks identical to a holiday, so a real outage would be laundered into "market was closed" — the opposite of the honesty this system is built on. |
| Third-party calendar API | **Rejected.** A network dependency in the freshness path, on a 1-vCPU box, for data that changes a few times a year. |
| **Backend `market_calendar` table, seeded from the published NSE holiday list, version-stamped** | **Chosen.** Auditable, offline, cheap, and answers every question below with a point lookup. |

Schema (RM-5):

```
market_calendar(
  session_date      TEXT PRIMARY KEY,   -- IST date
  is_trading_day    INTEGER NOT NULL,
  segment           TEXT NOT NULL,      -- CASH | FO  (they can differ)
  pre_open_start    TEXT,               -- 09:00
  pre_open_end      TEXT,               -- 09:08 (+ random close 09:08–09:12)
  open_time         TEXT,               -- 09:15
  close_time        TEXT,               -- 15:30
  post_close_end    TEXT,               -- 16:00
  session_type      TEXT NOT NULL,      -- NORMAL | MUHURAT | SPECIAL | HOLIDAY | WEEKEND
  is_expiry         INTEGER NOT NULL,
  expiry_kind       TEXT,               -- WEEKLY | MONTHLY | QUARTERLY
  holiday_reason    TEXT,
  source            TEXT NOT NULL,      -- nse_published | manual
  source_version    TEXT NOT NULL
)
```

Questions it must answer, all as point lookups: is `D` a trading day for segment `S`; what is the
previous/next trading day before/after `D`; is `D` an expiry session and of what kind; what are the
session windows on `D` (Muhurat sessions have different ones); what is the current
`MarketState` right now.

**Consumers, all of which currently do this wrong or not at all:**
`data_state.market_state()` (weekends only today); `session_coverage.expected_bounds` (fixed hours);
RM-2 `session_reference` (needs "previous *trading* day", not "yesterday"); the bar store's expected
bar count for gap detection (N-12); `days_to_expiry` and `expiry_phase` (N-15); Near/Next/Far
resolution (N-10); alert scheduling (V); the frontend `SessionState` (E.6), which consumes it and
computes nothing.

**Maintenance:** the NSE holiday list is published annually. A `scripts/seed_calendar.py` loads it;
`source_version` records which publication. A missing future year raises a WARNING in `/health` in
December rather than failing silently in January. An unknown `session_date` is treated as
**not a trading day** and the data state falls to `last_session` — the conservative direction.

### D.7 Market state vs data state vs data quality — three separate concepts

Never collapsed into one enum. They answer three different questions and change independently.

**MARKET STATE** — what the exchange is doing. Owned by RM-5 + the clock.

```
PRE_OPEN | OPEN | CLOSING | POST_CLOSE | CLOSED | HOLIDAY | WEEKEND | HALTED
```

Today only `open`/`closed` exist, from weekday + fixed hours. `HALTED` is UNVERIFIED as observable
from the tick stream — we cannot detect a market-wide halt from Kite market data alone, and
`features/quality.py:148-161` already deliberately refuses to infer a circuit breaker from a
per-symbol freeze. So `HALTED` is defined in the enum, sourced as `manual | inferred_unavailable`,
and never inferred from silence. Silence is `data quality: STALE`, which is the honest read.

**DATA STATE** — which session the payload came from. Owned by `MarketDataPolicy`. **Unchanged,
already correct:**

```
LIVE | LAST_SESSION | NO_DATA
```

**DATA QUALITY** — how good the data we have is. New; partially implicit today.

```
FRESH | DELAYED | STALE | PARTIAL | DEGRADED | RECOVERING
```

| Quality | Definition |
| --- | --- |
| `FRESH` | within the field group's tolerance (D.5) |
| `DELAYED` | beyond tolerance but under `live_quote_max_age_seconds` (120 s); still current-session |
| `STALE` | beyond 120 s while `MARKET_STATE = OPEN` — we should be receiving data and are not |
| `PARTIAL` | the session has coverage gaps (N-12) or a group is incomplete |
| `DEGRADED` | a subsystem is `UNAVAILABLE` (N-18) or the stream is reconnecting with buffered gaps |
| `RECOVERING` | reconnected, resynchronising; snapshot re-hydration in flight |

**Why three.** The combinations are all real and all must render differently:

| Market | Data | Quality | Real situation | Display |
| --- | --- | --- | --- | --- |
| OPEN | LIVE | FRESH | normal trading | green LIVE + time |
| OPEN | LIVE | DELAYED | slow ticks / thin instrument | amber, show age |
| OPEN | LAST_SESSION | STALE | **today's actual state — ingestion down** | red: "market open, data from 04 Sep" |
| CLOSED | LAST_SESSION | FRESH | normal after-hours | neutral: "LAST SESSION 04 Sep" |
| HOLIDAY | LAST_SESSION | FRESH | holiday | neutral: "HOLIDAY — Diwali" |
| OPEN | LIVE | RECOVERING | reconnect | amber spinner, values retained and marked |
| OPEN | LIVE | PARTIAL | ingestion restarted mid-session | green + PARTIAL badge on charts |
| CLOSED | NO_DATA | — | new instrument, no history | typed empty state |

A single enum could not express row 3 — the state the product is in right now — which is exactly why
the split is mandatory.

### D.8 NSE and derivative constraints — availability audit

| Constraint | Available today | Source when built | Needed for | Phase |
| --- | --- | --- | --- | --- |
| `tick_size` | ✅ in cache, **used nowhere** | Kite master → RM-4 | price rendering (N-13), future order validation | 2 |
| `lot_size` | ✅ in cache, used nowhere | Kite master → RM-4 | quantity display in lots (N-13), margin, order qty | 2 |
| `freeze_quantity` | ❌ | Kite master / NSE circular | order validation only | 13 (future) |
| Price bands / circuit limits | ❌ | NSE band file, or Kite quote `lower_circuit_limit`/`upper_circuit_limit` via REST quote (not in the tick stream) — **UNVERIFIED whether the ticker carries them; treat as a REST-sourced daily reference** | "near band" warnings; explaining a frozen instrument; distinguishing a halt from a data gap | 9 |
| Contract specification | ✅ partially (expiry, strike, type) | RM-4 | chain, futures, rollover | 7 |
| Expiry conventions (weekly/monthly) | ✅ per contract; ❌ as a rule | RM-4 + RM-5 `expiry_kind` | Near/Next/Far, `days_to_expiry`, expiry-day semantics (N-15) | 9 |
| Rollover | ❌ | RM-5 + `expiry_rule` durable identity (N-10) | watchlists and alerts surviving expiry | 7 |
| Contract changes / re-listing | ❌ | RM-4 diffing per refresh | detecting a vanished contract | 7 |
| Settlement price | ❌ (`ohlc_close` substitutes — CO-8) | UNVERIFIED source; NSE bhavcopy is the likely one | correct futures/options day change | 9 |
| Product types (CNC/MIS/NRML) | ❌ | Kite | order ticket, margin | 13 |
| Charges (STT, stamp, exchange, GST, brokerage) | partially — `CostsSettings` exists for backtesting | `config/settings.yaml` `costs` + Kite charges API | P&L net of charges (W) | 13 |
| T+1 settlement semantics | ❌ | reference | holdings vs positions distinction | 13 |
| Corporate actions | ❌ (`ca_suspect` flag only) | NSE announcements | adjusted history (Y) | 14 |

**Rule:** every one of these is REFERENCE data (layer 1), lives in RM-4/RM-5, and is delivered with
instrument meta — never recomputed, never inferred from prices, never held in the frontend as a
constant. The current `INDEX_INSTRUMENTS` hardcoding in `lib/instruments.ts` is the anti-pattern to
delete.

### D.9 Snapshot coherence

Every multi-field snapshot the API returns declares four things:

```
coherence: {
  observation_time:   "2026-09-07T09:47:12.340+05:30",  // the newest layer-2 input
  component_freshness: { ...D.5 groups... },
  max_input_spread_ms: 3240,                            // newest − oldest layer-2 input
  status: "COHERENT" | "MIXED" | "INCOHERENT"
}
```

| Status | Definition | UI |
| --- | --- | --- |
| `COHERENT` | every layer-2 input is from one observation, or all within the tightest group tolerance | no marker |
| `MIXED` | inputs span multiple observations but each is within its own group tolerance | a subtle marker; the inspector shows the spread |
| `INCOHERENT` | at least one input exceeds its tolerance; every derived metric depending on it is `null` | explicit marker + reasons |

Applied concretely: an option-chain response spans 84 contracts observed over a window, so it is
`MIXED` by nature and its `max_input_spread_ms` is meaningful — 500 ms is fine, 45 s means half the
chain is not trading and PCR is misleading. A single-instrument quote from one tick is `COHERENT`. A
futures panel with a 12 s spot/future spread is `INCOHERENT` for `basis` specifically, which is why
`basis` is null and `basis_status` says so — behaviour that already exists and is now named.

### D.10 Historical data layers

**The single most important structural change in this plan.** Today there is exactly one historical
tier (raw ticks, compacted) and every chart query aggregates it live (R-2, BL-3).

| Tier | Content | Path | Written by | Retention (N-1) | Serves |
| --- | --- | --- | --- | --- | --- |
| **T0 raw** | every observation as received, all depth columns | `data/raw/{date}/…` | ingestion, flushed continuously | **7 sessions** | compaction; forensic replay; nothing user-facing |
| **T1 compacted ticks** | one file per symbol per session, all columns incl. depth arrays | `data/compacted/{date}/{symbol}/ticks.parquet` | `nse-compact` 15:45 | **90 sessions** | Time & Sales history; depth-event analysis; feature/label jobs; unusual-activity backfill |
| **T2 1-minute bars** | `open/high/low/close/volume/oi/vwap/trades_observed/bar_complete` | `data/bars/{date}/{symbol}/1m.parquet` | **NEW** — `nse-compact`, from T1 | **400 sessions** | every intraday chart interval, by rollup |
| **T3 daily bars** | one row per symbol per session + `previous_close`, `settlement_price` | `data/bars/daily/{symbol}.parquet` (append) | **NEW** — `nse-compact` | **forever** | daily charts; multi-year history; RM-2 seeding |
| **T4 session reference** | `previous_close`, `today_open`, `official_close`, `settlement_price` per instrument per session | SQLite `session_reference` (RM-2) | **NEW** — `nse-compact` | forever | canonical change (ADR-15); the Marketwatch join |

**Authoritative source per chart interval — no ambiguity:**

| Interval | Source | Method |
| --- | --- | --- |
| `1m` | T2 | direct read |
| `3m`, `5m`, `10m`, `15m`, `30m`, `60m` | T2 | server-side rollup of 1m bars, bucketed on NSE session boundaries via the existing `nse_session_bucket` |
| `1D` | T3 | direct read |
| current forming bar (all intervals) | `latest_quotes` + the stream | the browser builds only the **current, incomplete** bar from live observations, and marks it `bar_complete: false` (L.4) |
| pre-T2 history (the 4 existing sessions) | T1 | one-time backfill job producing T2/T3 — not a request-time path |

**Rules.**
1. No request-time path may read T0 or T1 for a chart. Ever. Charts read T2/T3 only.
2. T1 remains readable at request time only for bounded, non-chart queries: Time & Sales for one
   symbol for one session, and the unusual-activity backfill (which becomes RM-3 anyway).
3 Bars are never fabricated. A bucket with no observations produces **no row** — the existing
  "no empty-bar fill" rule (`ohlc.py:147`) is retained and becomes visible as a gap (N-12).
4. `bar_complete` distinguishes a finished bar from the forming one, so the chart never persists a
   partial bar as history.
5. `trades_observed` (the count of source observations in the bucket) is stored, because it is the
   honest measure of how well a bar represents the interval, and it is what makes `volume`
   trustworthy — which today it is not, being always null (CO-6).

**Why the bar store fixes the latency, arithmetically.** A 5m chart over 10 sessions needs
750 bars ≈ 750 rows × ~9 columns. Reading that from a columnar file with a date+symbol path prune is
a single-digit-millisecond read. Today the same request parses and groups on the order of 10⁵–10⁶
tick rows in pandas (`NIFTY 50` alone is 301 KB/session compressed, 115 485 observations were
reported by a prior chart response). Three to four orders of magnitude of work removed — this is why
the target of p95 < 400 ms warm is realistic rather than aspirational.

---

## E. Live-data architecture

### E.1 The complete path (non-negotiable requirement #1)

```
Kite ──► ONE ingestion WebSocket (broker/websocket_listener.py, unchanged)
          │   MODE_FULL: 190 tokens · MODE_QUOTE: 399 tokens
          ▼
     NormalizedTick  (exchange_timestamp, ltp, ohlc, volume, oi, depth[5×2], ltt)
          ▼
     LatestQuoteTracker.observe()   in-process, monotonic guard (latest.py:102-105)
          ├──► raw Parquet (T0)                        [unchanged]
          ├──► activity_samples every ≥60 s            [unchanged]
          └──► latest_quotes every ≥2 s  + NEW bid_levels/ask_levels (ADR-12)
                      │
                      ▼
     ┌──────────────────────────────────────────────────────────────┐
     │ FastAPI  api/stream.py            [NEW]                      │
     │  one shared 1 s poll of latest_quotes  (ADR-2)               │
     │  diff vs last emitted → per-token change frames              │
     │  fan out to all SSE subscribers, filtered by their sub set   │
     │  MUST NOT touch DuckDB or Parquet                            │
     └──────────────────────────────────────────────────────────────┘
                      │  text/event-stream (ADR-1)
                      ▼
     Next.js server proxy  /api/v1/stream   (streaming passthrough, no buffering)
                      │
                      ▼
     Browser SharedWorker (N-5)  ── one EventSource per browser profile
                      │  postMessage
                      ▼
     MarketCache (ADR-3)  Map<token, InstrumentState> + subscriber registry
                      │  notify only subscribers of changed tokens
                      ▼
     useInstrument(token, selector)  via useSyncExternalStore
                      ▼
     Only the components displaying that instrument re-render
```

**Guarantees.** Exactly one Kite WebSocket, in the ingest process, forever. The browser never
connects to Kite and never holds a Kite credential (AF). No second market-data process (N-64/AI.4).

**What updates without any user action:** LTP, change, change%, volume, OI, depth ladder, bid/ask,
spread, imbalance, Time & Sales, futures, option chain, activity metrics, unusual-activity events,
and the intelligence inputs that derive from them. **Depth, volume and OI changes are visible even
when LTP does not change**, because the stream diffs *fields*, not prices — an observation whose
only change is `bid_levels` produces a frame containing `bid_levels`, and the depth panel's
subscriber fires while the price cell does not.

### E.2 Subscription management (ADR-9)

Formal pipeline: **UNIVERSE → ELIGIBILITY → SUBSCRIPTION → MODE → CAPABILITY → CONSUMER.**

**UNIVERSE** — every instrument that exists. RM-4, the full Kite master. Searchable (J), not
subscribed.

**ELIGIBILITY** — what may be subscribed, and with what priority:

| Tier | Members | Priority | Kite mode | Static/dynamic |
| --- | --- | --- | --- | --- |
| E0 indices | `NIFTY 50`, `NIFTY BANK` (+ India VIX, sector indices when added) | highest — never evicted | FULL | STATIC |
| E1 index derivatives | NIFTY/BANKNIFTY futures (Near/Next/Far) + ATM±10 options for the configured expiries | high | FULL | STATIC core, DYNAMIC wings |
| E2 depth equities | NIFTY100 ∩ listed | high | FULL | STATIC |
| E3 quote equities | NIFTY500 − NIFTY100 | medium | QUOTE | STATIC |
| E4 stock derivatives | NIFTY100 ∩ F&O-eligible; futures + ATM±5 | low | FULL | **DYNAMIC** (configured, never materialised — R-4) |
| E5 focus set | anything the user is actively looking at that is not in E0–E4 | on demand | FULL | **DYNAMIC**, TTL-evicted |

**SUBSCRIPTION** — the subscription manager, a new component **inside the existing ingest process**
(never a second connection). It owns:

- the static set, rebuilt at session start from RM-4 + eligibility rules;
- the dynamic set, mutated at runtime by focus requests;
- a **token budget** enforced against `broker.limits` (N-8) and `stock_derivatives.max_total_tokens`;
- **reference counting** on dynamic subscriptions: a token subscribed because three viewers are
  watching it is released when the third releases it;
- **LRU eviction with a TTL** (default 300 s idle) for dynamic tokens under budget pressure, and
  static tokens are never evictable;
- batched `set_mode` calls (`mode_change_batch_size`) because per-token mode changes are the
  expensive operation;
- an `ingestion_meta` audit event for every subscribe/unsubscribe/evict, so subscription churn is
  observable (AH).

**How a focus request reaches it.** The API cannot call into the ingest process today. The control
path is: `POST /api/v1/subscriptions` (the first non-GET route in the system) → a
`subscription_requests` SQLite table with `(token, requester, requested_at, ttl)` → the ingest
process polls it on the same cadence it already flushes (≤2 s) and reconciles. Chosen over a socket
or a queue because it adds no new process, no new port and no new dependency, is crash-safe (the
table survives a restart, so subscriptions are restored), and its latency (≤2 s to first tick) is
acceptable for "user opened a new instrument". This is the one place the plan accepts a
database-as-IPC, and it is deliberate.

**MODE** — the minimum mode that satisfies the consumer:

| Consumer need | Required mode |
| --- | --- |
| Marketwatch row (LTP, change, volume) | QUOTE |
| Instrument snapshot with depth, Time & Sales, depth events | **FULL** |
| Option chain with OI | **FULL** |
| Futures basis and OI | **FULL** |
| Chart (any interval) | QUOTE — bars need no depth |
| Index ticker | QUOTE (indices have no depth anyway — D.4) |

Rule: a consumer requests a *capability*, and the manager resolves the mode. Panels never name Kite
modes. Upgrading an instrument from QUOTE to FULL because the user opened its depth panel is a mode
change, not a new subscription, and is reference-counted independently.

**CAPABILITY** → D.4. **CONSUMER** → the browser's reference-counted subscription set, unioned
across tabs by the SharedWorker (N-5).

**Per-requirement subscription analysis** (the requirement's own checklist):

| UI need | Instrument(s) | Must subscribe? | Mode | Static/dynamic | Shares existing? | Removable when | Capacity impact |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Index ticker | 2 indices | yes | FULL (E0) | static | — | never | 2 |
| Marketwatch (default) | ≤250 of E2/E3 | yes | QUOTE/FULL | static | ✅ already subscribed | never | 0 marginal |
| Marketwatch (custom, off-universe) | user's picks | yes | QUOTE | dynamic | maybe | on watchlist removal | +1 each |
| Instrument snapshot + depth | 1 | yes | **FULL** | dynamic upgrade | ✅ if in E2 | on deselect + TTL | 0 or a mode change |
| Chart | 1 | **no** for history; yes for the forming bar | QUOTE | shares the snapshot's | ✅ | with the snapshot | 0 marginal |
| Time & Sales | 1 | yes | FULL | shares the snapshot's | ✅ | with the snapshot | 0 |
| Option chain (NIFTY/BANKNIFTY) | 42 contracts | yes | FULL | static core + dynamic wings | ✅ already | never for core | 0 marginal; wings + ~10 on an ATM shift (N-7) |
| Option chain (stock) | ~11 contracts | yes | FULL | **dynamic** | ❌ not subscribed today | on close + TTL | +11 each; budget-capped |
| Futures panel | 2–3 + spot | yes | FULL | static | ✅ (Near/Next exist — R-3) | never | +1 for Far |
| Unusual activity (market-wide) | all subscribed | no new | — | — | ✅ | — | 0 |
| Screener | universe-wide | no — reads RM-2/T3 and `latest_quotes` | — | — | ✅ | — | 0 |
| Alerts on an unsubscribed instrument | 1 each | yes | QUOTE | dynamic, **not TTL-evictable while the alert is armed** | maybe | on alert deletion | +1 each; separately capped |

The last row is a real design trap: an alert is a *background* consumer with no visible panel, so
TTL eviction would silently disarm it. Alerts therefore hold a distinct, non-evictable reference and
are counted against their own budget (V.4).

### E.3 Current subscription reality vs target

589 tokens today (A.3) against a self-imposed 2 500 cap. Target steady state: E0–E3 unchanged (505),
E1 options 84 → ~126 with Far futures and wing headroom, E4 stock derivatives ~600 when enabled,
E5 focus ≤100. Total ~1 330, comfortably inside the cap and roughly half of what the previous pass
assumed (R-4). Capacity is not the binding constraint; **CPU is** (AI).

### E.4 Browser transport decision (ADR-1)

**Decision: Server-Sent Events from FastAPI, proxied by the Next.js server, with REST for
hydration, history, metadata and analytics.**

| Criterion | SSE (chosen) | Browser WebSocket | Coordinated polling | Hybrid WS+poll |
| --- | --- | --- | --- | --- |
| Latency floor | poll interval + persist interval (~1–3 s today, ADR-2) | same — the bottleneck is upstream, not the socket | 3–8 s + no coalescing | same as WS |
| Direction needed | server→client only, which is exactly what we need | bidirectional, unused | request/response | mixed |
| Reconnect | **built into `EventSource`**, automatic, with `Last-Event-ID` | manual: backoff, heartbeat, state machine, all hand-written | n/a | manual |
| Proxy compatibility | plain HTTP through the existing Next.js route | needs an HTTP upgrade the current proxy does not do; would require a separate path or direct exposure | trivial | complex |
| CORS | none — same-origin via the proxy | would need the `:8501`-only CORS config fixed and an origin policy | none | needs the fix |
| Server cost on 1 vCPU | one shared poll + N cheap writes | same, plus per-connection frame machinery | **N × full recomputation** — the current 113 req/min problem, multiplied | worst of both |
| Head-of-line / multiplexing | HTTP/2 multiplexes; over HTTP/1.1 it consumes 1 of ~6 connections per origin | one dedicated socket | consumes the connection pool constantly | — |
| Binary payloads | text only (JSON) — fine at our volume | binary possible | text | — |
| Implementation size | ~150 lines server, ~120 client | ~400 lines server, ~350 client | ~0 new, but the cost problem is unsolved | largest |
| Failure visibility | HTTP status codes, standard logs, `curl`-testable | opaque handshake failures | standard | mixed |

**Why not a browser WebSocket.** We would pay for bidirectionality we do not use (the one control
path — subscriptions — is a REST POST, E.2), hand-write reconnection that `EventSource` gives free,
and either fix CORS to expose the API to the browser directly or teach the Next.js proxy to upgrade
connections. The only genuine WebSocket advantages here — binary frames and client→server streaming
— have no consumer in this design. If order placement later needs a low-latency bidirectional
channel, it will be a *separate* channel in a *separate* security domain (N-14, AF), not a reason to
put market data on a socket now.

**Why not coordinated polling.** It cannot meet requirement #1. Even perfectly coordinated — one
timer, one bulk request, shared cache — a 2 s poll of a 250-row Marketwatch is 30 full requests per
minute per tab, each recomputing derived fields server-side, on one core. And it structurally cannot
deliver "depth changed but LTP did not" without either polling depth for every visible instrument or
sending full state every time. It is the current architecture, and it is why the current
architecture is being replaced.

**Why not hybrid.** A hybrid means two live paths, two reconnection stories, two freshness models
and two sets of bugs, in exchange for nothing that SSE alone does not do.

**Endpoint contract:**

```
GET /api/v1/stream?tokens=408065,260105,…&groups=price,depth,oi
Accept: text/event-stream
Cache-Control: no-store            X-Accel-Buffering: no

event: hello
data: {"connection_id":"…","server_time":"…","session":{…},"heartbeat_ms":5000,
       "max_instruments":250,"subscribed":[…],"rejected":[…]}

event: tick                        # coalesced, 1 s, changed fields only
data: {"t":408065,"ts":"2026-09-07T09:47:12.340+05:30","seq":8842,
       "ltp":1712.4,"chg":7.15,"chgp":0.42,"vol":4823901,"vd":1200,
       "bid":[[1712.35,450,3],…],"ask":[[1712.45,300,2],…]}

event: session                     # market/data/quality state changed
data: {"market_state":"OPEN","data_status":"LIVE","quality":"FRESH","as_of":"…"}

event: heartbeat                   # every 5 s, so silence is distinguishable from a dead link
data: {"server_time":"…","subscribed":589,"lag_ms":740}

event: resync                      # server lost continuity; client must re-hydrate via REST
data: {"reason":"ingestion_restart","from_seq":8840}
```

Field names are abbreviated on the stream only (`ltp`, `chg`, `vol`) and expanded to canonical names
by the client adapter. Rationale: at 250 instruments × 1 Hz, key names are a material share of
bytes; the abbreviation is confined to one adapter function and never leaks into panels.

**Proxy requirement:** the existing `src/app/api/v1/[...path]/route.ts` buffers with
`await response.text()` and imposes a 20 s timeout — both fatal for a stream. `/stream` needs a
dedicated route that pipes `response.body` through unbuffered, with no timeout, and
`X-Accel-Buffering: no`. This is a small, additive frontend change and is a Phase 4 deliverable.

### E.5 Ingest → API live handoff (ADR-2)

**Decision: the API polls `latest_quotes` on one shared 1-second timer and fans the diff out to all
SSE subscribers. The UNIX-domain-socket upgrade is designed now and built only if measurement
demands it.**

The ingest process and the API are separate OS processes (`nse-ingest` and `nse-api`). Something
must cross that boundary.

| Option | Latency added | Cost | Complexity | Verdict |
| --- | --- | --- | --- | --- |
| **Shared 1 s poll of `latest_quotes`** | ≤1 s, plus the ≤2 s persist interval | one indexed `SELECT` over ≤600 rows per second, **independent of client count** | lowest — no new process, no new dependency | **Chosen** |
| Per-client polling | same | ×N clients | low | Rejected — reintroduces the N× problem SSE exists to remove |
| UNIX-domain-socket fan-out from ingest | ≤50 ms | a publisher in ingest, a subscriber task in the API | moderate; needs framing, backpressure, reconnect | **Designed, deferred** |
| Redis pub/sub | ≤20 ms | +1 process, ~10 MB, a new dependency | moderate | Rejected — a whole daemon on a 2 GB box for one channel |
| Shared memory | ≤5 ms | fragile, needs locking | high | Rejected |
| Second Kite WS in the API | — | violates the hard constraint | — | Rejected outright |

**Honest latency statement.** End-to-end floor today is `persist interval (≤2 s) + poll (1 s)` ≈
**1–3 s**, worst case ~3 s. That is genuinely good enough for position monitoring, option-chain
observation, OI and flow work, and everything Trade Intelligence does. It is **not** good enough for
scalping the top of book, and the document must not pretend otherwise. Two levers exist, in order:

1. Lower the persist interval from 2 s toward 500 ms. Cost: 4× the SQLite write rate on ~600 rows,
   which is small but must be measured against N-9.
2. Then the UDS fan-out, which bypasses SQLite for the live path entirely and brings the floor under
   200 ms.

The upgrade is a swap behind one interface (`LiveSource.subscribe() -> AsyncIterator[Frame]`), with
`SqlitePollSource` and `UdsSource` as implementations. Nothing above that interface changes. This is
the boundary the self-critique (Appendix 2) demands be designed now.

**The status bar shows the real number.** `lag_ms` on the heartbeat is
`now − max(exchange_timestamp)` across the subscribed set, so the user sees the true end-to-end age
rather than an implied "real-time".

### E.6 Session state machine (ADR-14 consumer)

One `SessionState`, computed **only** in the backend, delivered on hydration and on every change via
the `session` stream event, held in exactly one frontend store slot.

```
MarketState:  PRE_OPEN → OPEN → CLOSING → POST_CLOSE → CLOSED
              WEEKEND | HOLIDAY  (from RM-5)
              HALTED             (manual/never inferred — D.7)
DataStatus:   LIVE | LAST_SESSION | NO_DATA        (MarketDataPolicy, unchanged)
Quality:      FRESH | DELAYED | STALE | PARTIAL | DEGRADED | RECOVERING
Coverage:     FULL_SESSION | PARTIAL_SESSION       (session_coverage + N-3 warm starts)
```

Wire shape:

```
session: {
  market_state: "OPEN", session_date: "2026-09-07",
  windows: { pre_open: ["09:00","09:08"], open: "09:15", close: "15:30", post_close: "16:00" },
  is_expiry: false, expiry_kind: null, session_type: "NORMAL",
  data_status: "LIVE", quality: "FRESH", coverage: "FULL_SESSION",
  previous_trading_day: "2026-09-04",
  as_of: "2026-09-07T09:47:12.340+05:30",
  clock_skew_seconds: 0.2,
  reason: null
}
```

**What it drives:** live vs fallback labelling (AD); chart session boundaries and the forming bar
(L.4); which reference price applies (AB); whether volume and OI deltas are session-cumulative or
carried over; whether alerts may fire (V); whether intelligence runs; and every status indicator.

**What must be deleted:** `lib/freshness.ts:31-86` (`sessionHoursIst`, `marketStatus`) — browser-side
session derivation from a hardcoded clock. The frontend keeps `classifyFreshness` but feeds it
market timestamps from the D.5 groups instead of `envelope.as_of`.

### E.7 Timestamp model

| Name | Meaning | Source | Timezone on the wire | Used for |
| --- | --- | --- | --- | --- |
| `exchange_timestamp` | **the authoritative market observation time** | Kite tick | ISO 8601 with `+05:30` | **all freshness, all ordering, all bar bucketing** |
| `last_trade_time` | exchange's time of the last actual trade | Kite tick (FULL/some QUOTE) | same | tape; distinguishing "no trades" from "no data" |
| `event_time` | alias of `exchange_timestamp` in stream frames (`ts`) | — | same | client-side ordering |
| `receive_time` | when the ingest process received the packet | ingestion `ingested_at` | UTC internally, IST on the wire | clock-skew estimation (N-4); ingestion lag |
| `persist_time` | when the row was written to `latest_quotes` | `updated_at` | UTC | persistence-lag diagnostics only |
| `data_state.as_of` | **the market time of the served data** | `session_iso(observation)` | IST | the only timestamp shown as "as of" to a user |
| `response.as_of` | when the HTTP response was created | `_as_of()` | IST | logging, cache diagnostics. **Never displayed as data time.** |
| `browser_received_at` | when the frame reached the browser | client `Date.now()` | local | round-trip diagnostics only |
| `display_time` | a formatted rendering of one of the above | `formatTimestamp` | **always rendered `Asia/Kolkata`** | display |

**Rules.**
1. Storage and transport are timezone-aware ISO 8601. Display is always IST, regardless of browser
   locale — a trader in Singapore reads NSE times (`lib/format.ts::formatTimestamp` already does
   this correctly; the hardcoded 5.5 h offsets in `lib/candles.ts` must be replaced by it).
2. **Response creation time is never market-data time.** This is the CO-3 bug; `_envelope`'s own
   docstring already states the rule, and the frontend violates it. Fixed by having
   `DataFreshnessBadge` accept a D.5 freshness group and by lint-banning `envelope.as_of` in
   presentation components.
3. Freshness is always `now − exchange_timestamp`, with `now` trusted only while the clock skew
   estimate is within ±30 s (N-4).
4. Milliseconds are preserved end-to-end. Depth freshness of 900 ms is not expressible in seconds.

### E.8 Event ordering, duplication and recovery

| Concern | Rule | Where enforced | Status |
| --- | --- | --- | --- |
| Out-of-order ticks | drop any observation older than the instrument's current state | `LatestQuoteTracker.observe` (`latest.py:102-105`) | ✅ exists |
| Out-of-order at the SQL layer | the upsert has no monotonic guard, so a late writer could regress a row | add `WHERE excluded.timestamp > latest_quotes.timestamp` to the `ON CONFLICT` clause | ❌ gap; single-writer today so latent |
| Duplicate observations | identical `(token, exchange_timestamp)` is a no-op; deltas must not be recomputed | tracker + a `seq` on stream frames | partially — add the guard |
| Out-of-order stream frames | client keeps `lastSeq` per token and drops lower `seq` | `MarketCache.applyFrame` | ❌ to build |
| Duplicate stream frames | idempotent by `(token, seq)` | same | ❌ to build |
| Stale observation on reconnect | after `resync`, discard the cache for affected tokens and re-hydrate from REST before applying new frames | `StreamClient` + `queryClient` | ❌ to build |
| Session reset (new trading day) | on `session_date` change, clear all session-cumulative state (volume, OI deltas, tape, session OHLC) and re-hydrate | `MarketCache.onSessionChange` | ❌ to build |
| Ingestion restart mid-session | server emits `resync` with `reason: "ingestion_restart"`; first post-restart deltas are `null` (N-3); session marked `PARTIAL_SESSION` | ingest + stream | partially (tracker is safe) |
| Late packets after reconnect | `Last-Event-ID` lets the server say "cannot replay" → `resync` rather than silently gapping | `api/stream.py` | ❌ to build |
| Chart ordering | bars keyed and sorted by bucket start; the forming bar is replaced, never appended twice | chart controller (L.4) | ❌ to build |
| Tape ordering | append-only by `exchange_timestamp`; an out-of-order print is inserted in position, not appended, and the ring buffer is bounded | tape panel (N.4) | ❌ to build |
| **No fabricated identifiers** | the system never invents an exchange trade ID, order ID or participant identity. `seq` is explicitly a *transport* sequence number, documented as such, and never displayed as a trade count. | ADR-8, N.3 | rule |

**Recovery sequence** (the acceptance test in AN.21 exercises exactly this): connection lost →
`ConnectionState: RECONNECTING`, quality `DEGRADED`, values retained and visibly marked stale, never
blanked → `EventSource` auto-reconnects with backoff → server cannot replay from `Last-Event-ID` →
`resync` → client re-hydrates subscribed instruments via one bulk REST snapshot → quality
`RECOVERING` → first frames applied → `FRESH`. Total target under 5 s on a transient drop. Values
are never cleared to zero or to em-dashes during a reconnect; a known-stale price is far more useful
to a trader than a blank cell, provided it is labelled.

## F. API architecture

### F.1 Classification

Every endpoint belongs to exactly one class, which fixes its caching, its latency budget, its
failure mode and its security domain.

| Class | Characteristics | Cache | Budget (p95) | Failure mode |
| --- | --- | --- | --- | --- |
| **STREAM** | long-lived, server-push, layer 2 | none | first byte < 500 ms | degrade to snapshot polling |
| **SNAPSHOT** | point-in-time layer 2+3, hydration and on-selection | 1 s server, none client (the stream supersedes it) | < 250 ms | typed empty state per panel |
| **HISTORICAL** | immutable past, layer 2+3 | **long** — a closed session never changes | < 400 ms warm / < 900 ms cold | typed empty state |
| **METADATA** | layer 1 | hours, versioned, `ETag` | < 100 ms | fatal for the affected panel — no instrument meta means no correct rendering (N-13) |
| **ANALYTICS** | layer 3, market-wide, expensive | 30–120 s | < 300 ms | `subsystems.*.status = UNAVAILABLE` (N-18) |
| **MODEL** | layer 4 | 60 s | < 300 ms | `NOT_MATURE` / `ERROR`, never a substituted number |
| **ACCOUNT** | separate security domain (N-14) | **never shared**, 5 s private | < 500 ms | isolated boundary |
| **CONTROL** | the only non-GET routes: subscriptions, watchlist writes, workspace sync, alerts | none | < 100 ms | typed error, optimistic UI rollback |

### F.2 Existing 26 routes — disposition

| Route | Class | Disposition |
| --- | --- | --- |
| `/health` | METADATA | **extend**: `ingestion_status`, `kite_token_state` (N-2), `clock_skew` (N-4), `disk_free_gb` + `sessions_of_headroom` (N-1), stream connections (N-6), `sqlite_busy_retries` (N-9) |
| `/overview` | SNAPSHOT | keep; becomes the hydration call for the top bar |
| `/quotes/{symbol}` | SNAPSHOT | **fix `change` semantics** (ADR-15); add D.5 `freshness`, D.9 `coherence`, depth ladder (ADR-12) |
| `/watchlists`, `/watchlists/quotes` | SNAPSHOT | keep; generalise into `/quotes/bulk` (F.4) |
| `/charts/{symbol}` | HISTORICAL | **split** into `/candles` + `/series` (F.4); back with RM-1 |
| `/options/{u}`, `/options/{u}/{e}`, `/options/{u}/{e}/activity`, `/options/{u}/{e}/oi` | SNAPSHOT + ANALYTICS | keep contracts; add `atm_coverage` (N-7), Greeks (D-1); **fix the 1.29 s latency** (PE-2) |
| `/futures/{u}` | SNAPSHOT | keep; add Far month, `settlement_price` reference (N-15) |
| `/market-activity/{symbol}` | ANALYTICS | keep |
| `/unusual-activity` | ANALYTICS | keep contract; back the last-session path with RM-3 |
| `/cross-market` | ANALYTICS | keep; extend for relative strength |
| `/maturity` | MODEL | keep verbatim — immutable |
| `/signals`, `/decisions` | MODEL | keep; superseded for the UI by `/intelligence` |
| `/account` | ACCOUNT | **replace** with typed `/account/**` (N-14, W.3) |
| legacy `/v1/*` | — | deprecate after cutover |

### F.3 New endpoints

| Endpoint | Class | Purpose | Phase |
| --- | --- | --- | --- |
| `GET /api/v1/stream` | STREAM | SSE live data (E.4) | 4 |
| `POST /api/v1/subscriptions` | CONTROL | dynamic focus set (E.2) | 4 |
| `GET /api/v1/instruments?q=&segment=&limit=` | METADATA | search + capabilities + contract spec (J) | 7 |
| `GET /api/v1/instruments/{key}` | METADATA | one instrument's full meta + capabilities | 7 |
| `GET /api/v1/calendar?from=&to=` | METADATA | trading calendar (D.6) | 2 |
| `GET /api/v1/quotes/bulk?tokens=` | SNAPSHOT | ≤250 canonical quotes in one call (I.3) | 7 |
| `GET /api/v1/candles/{key}?interval=&from=&to=` | HISTORICAL | bars from RM-1/T3, with `coverage` (N-12) | 3 |
| `GET /api/v1/series/{key}?fields=&from=&to=` | HISTORICAL | volume/OI/activity series, separately from candles | 3 |
| `GET /api/v1/tape/{key}?limit=&from=` | HISTORICAL | Time & Sales observation events (N) | 8 |
| `GET /api/v1/depth/{key}` | SNAPSHOT | ladder + recent depth events (M) | 8 |
| `GET /api/v1/intelligence/{key}` | MODEL | all algorithm outputs, generically (S, T) | 11 |
| `GET /api/v1/algorithms` | METADATA | algorithm registry for dynamic UI (T.3) | 11 |
| `GET /api/v1/breadth` | ANALYTICS | advance/decline, above-VWAP, 52w counts (Q.4) | 10 |
| `GET /api/v1/screener` + `POST /api/v1/screener/run` | ANALYTICS | screener (U) | 12 |
| `/api/v1/watchlists` (POST/PUT/DELETE) | CONTROL | watchlist writes | 7 |
| `/api/v1/alerts` (CRUD) | CONTROL | alerts (V) | 12 |
| `/api/v1/workspaces` (CRUD) | CONTROL | workspace sync, later | 14 |
| `/api/v1/account/{positions,holdings,orders,trades,margins,funds}` | ACCOUNT | typed account (W) | 13 |

**CORS and the proxy.** `allow_origins` currently lists only `:8501`. The design keeps the browser
talking exclusively to the Next.js server (same-origin), so the API stays localhost-bound and CORS
stays restrictive — a security property (AF), not an oversight. The `/stream` proxy route is the one
addition.

### F.4 Two contract splits worth calling out

**`/charts` → `/candles` + `/series`.** Today one response carries `candles[]`, `points[]`,
`volume`, `oi` and activity samples — 125 KB, and the reason the endpoint reads Parquet twice. They
have different sources, different cache lifetimes and different consumers: candles come from RM-1 and
are immutable once a session closes; activity series come from `activity_samples`; the forming bar
comes from the stream. Splitting them lets `/candles` be aggressively cached and lets a timeframe
change avoid refetching activity data entirely.

**`/watchlists/quotes` → `/quotes/bulk?tokens=`.** The current endpoint serves the server-configured
watchlist. Marketwatch needs arbitrary instrument sets of up to 250, and so do screener results,
positions marking and alert evaluation. One bulk endpoint with an explicit token list serves all of
them; `/watchlists/quotes` becomes a thin wrapper for backward compatibility.

### F.5 Canonical response envelope

```
{
  "maturity":   {…},                      // existing, immutable, required — envelope.ts enforces it
  "as_of":      "…",                      // response creation time. NEVER data time (E.7)
  "data_state": { market_state, data_status, as_of, session_date, source, reason },
  "session":    {…},                      // E.6, on hydration endpoints
  "freshness":  { price:{…}, depth:{…}, oi:{…}, reference:{…} },   // D.5, per group
  "coherence":  { observation_time, max_input_spread_ms, status },  // D.9
  "subsystems": { options:{status,reason}, futures:{…}, activity:{…}, intelligence:{…} }, // N-18
  "<payload>":  …
}
```

`maturity`, `as_of` and `data_state` exist today. `freshness`, `coherence`, `subsystems` and
`session` are additive, so `parseEnvelope` in the frontend keeps working unchanged during migration.

### F.6 Server-side read models — the correct set

**The previous pass recommended "five new read models". That recommendation is rejected.** Two of
the five were unnecessary, one was mis-scoped, and the highest-leverage one was missing entirely.
Each candidate below was evaluated against three questions: is the computation actually expensive at
request time; can an existing store serve it; and does materialising it introduce a staleness class
we would then have to manage.

| Candidate | Verdict | Reasoning |
| --- | --- | --- |
| Bar/candle store | **REQUIRED — RM-1** | The 32 s chart. No existing store can serve it: T1 has no bars, and building them per request is O(observations). Three to four orders of magnitude of work removed (D.10). |
| **Session reference prices** | **REQUIRED — RM-2. Was missing from the previous five.** | Canonical day change (ADR-15) needs `previous_close` per instrument. Without it, every quote and every Marketwatch row needs a per-symbol historical lookup for the previous *trading* day — the single change that would otherwise make a 250-row Marketwatch expensive. This is the highest-leverage read model in the plan and the previous pass omitted it. |
| Marketwatch bulk read model | **REJECTED** | `latest_quotes` **is already the live Marketwatch read model**: one row per token, primary-keyed on token, indexed on symbol. A bulk `SELECT … WHERE instrument_token IN (…)` over ≤250 of ≤600 rows, joined to RM-2, is a sub-10-ms query. Materialising a second copy would add a staleness tier and an invalidation problem for zero gain. What was actually missing was an *endpoint* (`/quotes/bulk`), not a read model. |
| Option-chain read model | **REJECTED** | A chain is 42 rows from `latest_quotes` plus aggregates over them. The measured 1.29 s (PE-2) is not a storage problem — it is per-request IV solving and analytics recomputation. Fix it with a 1 s memoisation of chain aggregates keyed on `(underlying, expiry, max(exchange_timestamp))` and by not re-solving IV when no input changed. Materialising the chain would make it stale precisely when it matters most. |
| Unusual-activity ranking | **REQUIRED but DEFERRED — RM-3** | The last-session path scans a day-wide partition. Already optimised 11.55 s → 2.93 s cold, 1.03 s warm behind a 120 s cache (R-10). Precompute the ranking at compaction into SQLite. Genuinely needed, but no longer urgent — it moves from Phase 3 to Phase 10. |
| Instrument master + search | **REQUIRED — RM-4** | Not an analytics read model but a *reference* store, which is why it was mis-scoped before. Needed for search (J), capabilities (D.4), tick/lot rendering (N-13), durable identity (N-10) and expiry semantics (N-15). |
| Market calendar | **REQUIRED — RM-5** | ADR-14. Small, but foundational: `previous_close` cannot be defined without it. |
| Screener/breadth precompute | **REJECTED for now** | Buildable on RM-1 + RM-2 + `latest_quotes` when the screener ships (Phase 12). Precomputing filter results before the filter language exists would be designing storage for unknown queries. |
| Tape/print store | **REQUIRED, LATE — RM-6** | Time & Sales at observation granularity for a focus set. Deferred to Phase 8; T1 serves historical tape until then. |

**Final set: four now (RM-1, RM-2, RM-4, RM-5), two later (RM-3, RM-6), plus one schema extension
(ADR-12), plus one memoisation fix (options).** Not five, and not the same five.

---

#### RM-1 — Bar store

- **Purpose.** Serve every chart interval without touching tick data at request time.
- **Source.** T1 compacted ticks (`data/compacted/{date}/{symbol}/ticks.parquet`).
- **Storage (ADR-11).** Parquet. `data/bars/{date}/{symbol}/1m.parquet` for intraday;
  `data/bars/daily/{symbol}.parquet` appended for daily. Parquet because it is columnar, DuckDB
  reads it natively with path pruning, it needs no write concurrency (one writer, after close), and
  it keeps SQLite small.
- **Schema.** `bucket_start, open, high, low, close, volume, oi, vwap, trades_observed, bar_complete`.
- **Refresh.** In `nse-compact` at 15:45 IST, immediately after T1 is written, per symbol. Plus a
  one-time backfill for the four existing sessions.
- **Latency target.** 5m/10 sessions: p95 < 120 ms cold, < 40 ms warm. 1D/2 years: < 80 ms.
- **Consumers.** `/candles`, `/series`, the screener, relative strength, VWAP, chart-based alerts.
- **Invalidation.** A closed session's bars are immutable. Re-running compaction for a date
  overwrites that date's partition atomically (write to a temp path, then rename).
- **Fallback.** No bars for a date → `candles_status: "unavailable"` with
  `candles_reason: "bars_not_built"`, and the endpoint **does not** silently fall back to
  tick aggregation — that fallback is what makes latency unpredictable today. A daily backfill-gap
  alarm in `/health` catches it instead.
- **Cost.** ~750 bars/symbol/session × 589 symbols ≈ 440 k rows/session, a few MB compressed.
  Well inside the T2 400-session retention budget (N-1).

#### RM-2 — Session reference

- **Purpose.** The canonical reference price for day change (ADR-15), for every instrument, for
  every session, resolved against the trading calendar.
- **Source.** T1/T3 for `official_close` and `today_open`; RM-5 for "previous trading day";
  bhavcopy or an equivalent for `settlement_price` (source UNVERIFIED — N-15).
- **Storage.** SQLite `session_reference(session_date, instrument_token, symbol, previous_close,
  today_open, official_close, settlement_price, reference_price, reference_type, source,
  PRIMARY KEY(session_date, instrument_token))`. SQLite because access is a keyed point lookup
  joined to `latest_quotes` in the same database — Parquet would force a cross-engine join on the
  hottest path in the system.
- **Refresh.** At compaction for the closing session; and at session start, the row for *today* is
  created by copying the previous trading day's `official_close`/`settlement_price` into
  `previous_close`, so the reference exists from the first tick of the day rather than being
  computed lazily.
- **Latency target.** Join with a 250-row bulk quote: < 10 ms.
- **Consumers.** `/quotes`, `/quotes/bulk`, the stream (change is computed server-side per frame),
  options, futures, screener, alerts.
- **Invalidation.** Immutable once written, except on a corporate action, which rewrites the
  affected instrument's history and is an explicit, audited operation (Y).
- **Fallback.** No reference → `change_absolute: null`, `change_percent: null`,
  `reference_type: null`, `reason: "no_reference"`. The UI shows a dash and the inspector explains
  it. **A missing reference never produces a change of zero** — a flat quote and an unknown
  reference must not look identical.

#### RM-3 — Unusual-activity rank (deferred, Phase 10)

- **Purpose.** Replace the day-wide Parquet scan on the last-session path.
- **Source.** T1 + `activity_samples`, using the existing `market/unusual.py` scoring unchanged.
- **Storage.** SQLite `unusual_rank(session_date, symbol, activity_score, activity_level,
  reasons_json, rank, PRIMARY KEY(session_date, symbol))`.
- **Refresh.** In `nse-compact`, once per session.
- **Latency target.** < 50 ms for `limit=50`.
- **Consumers.** `/unusual-activity` last-session path only. The live path keeps computing from
  `latest_quotes`, which is already cheap.
- **Invalidation.** Immutable per session.
- **Fallback.** No precomputed row → the current bounded scan
  (`unusual_historical_max_candidates: 100`), which stays as the safety net.

#### RM-4 — Instrument master

- **Purpose.** Reference data, search, capabilities, contract specification, durable identity.
- **Source.** Kite instrument master (full dump, not the 589-token subscription cache) + the
  membership CSVs for sector and index membership.
- **Storage.** SQLite `instruments(instrument_token PK, exchange, segment, tradingsymbol, name,
  instrument_type, expiry, strike, option_type, lot_size, tick_size, sector, indices_json,
  isin, is_active, first_seen, last_seen, search_rank, search_text)` plus
  `symbol_alias(old, new, effective_date)`. Indexes on `tradingsymbol`,
  `(exchange, tradingsymbol)`, `(instrument_type, expiry)`, and `search_text`.
- **Refresh.** Daily, before market open, with a **schema-version check** (R-9). Diffed against the
  previous load to emit new/expired/changed contracts (E.8, N-10). `last_seen` makes delisting
  detectable rather than silent.
- **Latency target.** Search < 30 ms; single lookup < 5 ms.
- **Consumers.** search, capabilities, subscription eligibility, price/quantity rendering, expiry
  semantics, durable identity resolution.
- **Invalidation.** Daily reload; `is_active` marks contracts absent from the latest master.
- **Fallback.** Stale master → serve it and report `reference_as_of` plus a `/health` WARNING. Never
  block market data on a stale instrument master.

#### RM-5 — Market calendar

- **Purpose.** ADR-14 / D.6.
- **Source.** Published NSE holiday list, seeded by script, `source_version` stamped.
- **Storage.** SQLite `market_calendar` (schema in D.6).
- **Refresh.** Annual seed; manual amendments for special sessions.
- **Latency target.** < 2 ms (point lookup, cached in-process for the current date).
- **Consumers.** `market_state`, `session_coverage`, RM-2, bar gap detection, expiry semantics,
  Near/Next/Far resolution, alerts, the frontend `SessionState`.
- **Invalidation.** On seed.
- **Fallback.** Unknown date → **not a trading day**, `data_status` falls to `last_session`. The
  conservative direction: better to under-claim live than over-claim it.

#### RM-6 — Print/tape store (deferred, Phase 8)

- **Purpose.** Time & Sales at observation granularity for the focus set.
- **Source.** The ingest stream, for instruments in E5 + E0/E1 cores only — not all 589.
- **Storage.** SQLite ring table `prints(instrument_token, exchange_timestamp, last_price,
  last_quantity, volume, volume_delta, oi_delta, best_bid, best_ask)` capped per instrument per
  session, plus T1 for anything older.
- **Refresh.** Continuous during the session; pruned by the retention job.
- **Latency target.** < 50 ms for the last 500 prints.
- **Consumers.** `/tape`, depth-event analysis, aggressive-side proxy.
- **Invalidation.** Session-scoped.
- **Fallback.** T1 for historical sessions; a typed empty state for an unsubscribed instrument.

### F.7 The options latency fix (PE-2) — not a read model

`/options/NIFTY` at 1.29 s for 84 contracts is ~15 ms per contract, which is IV solving plus
analytics, not I/O. Three bounded changes:

1. **Memoise chain aggregates** on `(underlying, expiry, max(exchange_timestamp) across the chain)`.
   With a ≤2 s persist interval the key changes at most every 2 s, so successive requests within a
   tick window are free. Correct by construction: the key includes the newest observation, so a new
   tick invalidates it.
2. **Cache solved IV** per `(token, ltp, spot, days_to_expiry_bucket)`. Black-Scholes root-finding
   is the expensive part and its inputs are unchanged between most requests.
3. **Do not recompute Max Pain and PCR per request** when no contract has changed — they are pure
   functions of the memoised chain.

Target p95 < 250 ms. Measurable before and after with the same `curl` used in A.2.

---

## G. Frontend architecture

### G.1 Directory structure and enforced boundaries

```
src/
  shell/       TerminalShell TopBar StatusBar PanelHost WorkspaceManager CommandPalette
               → imports: stores, canonical. MUST NOT import data/ or api/.
  panels/      marketwatch/ chart/ depth/ tape/ options/ futures/ activity/ unusual/
               intelligence/ marketContext/ screener/ orders/ positions/ holdings/ notes/
               → imports: stores, canonical, ui, data hooks. MUST NOT call apiGet or hold market state.
  stores/      marketCache selectionStore workspaceStore userStore
               → marketCache is written ONLY by transport/ and data/ hydration.
  transport/   StreamClient SharedWorkerBridge UpdateQueue ConnectionMonitor LiveSource
               → the sole writer of live market state.
  data/        queryClient + domain hooks (useInstrumentMeta useCandles useChain useTape …)
               → the sole owner of HTTP. Returns canonical types, never wire shapes.
  api/         types.ts (wire shapes) envelope.ts paths.ts mock.ts fixtures/
               → pure, no React.
  canonical/   quote.ts change.ts freshness.ts session.ts capability.ts units.ts
               provenance.ts instrument.ts status.ts
               → the ONLY place freshness/capability/units/provenance are resolved.
                 change/change% are NOT computed here — they arrive from the backend (ADR-15).
  ui/          Table VirtualList Panel Badge MarketField ErrorBoundary ContextMenu Inspector
               → presentational only. No store imports.
  lib/         format.ts search.ts (pure helpers)
```

Enforced by ESLint `no-restricted-imports`:

1. `panels/**` may not import `api/client` or call `fetch`.
2. Only `transport/**` and `data/**` may import `marketCache`'s write API.
3. Only `canonical/**` may resolve freshness, capability, units or provenance.
4. `shell/**` may not import `panels/**` concretely — panels register into a registry (H.2).
5. `ui/**` may not import `stores/**`.
6. No component may format a price without `InstrumentMeta` (N-13).
7. `envelope.as_of` may not be referenced in `panels/**` or `ui/**` (CO-3).

### G.2 State ownership

| Store | Owns | Written by | Persisted | Never contains |
| --- | --- | --- | --- | --- |
| `marketCache` | per-instrument observed + derived state, depth, tape ring, freshness groups, session, connection, subscriptions | `transport/` and `data/` hydration only | **no** | user data, layout, account data |
| `selectionStore` | activeWorkspace, activePanel, activeInstrument, linked context (expiry, strike, timeframe), selected watchlist, selected algorithm | UI intent, via named actions | session (URL-reflected) | market data |
| `workspaceStore` | workspaces, panel instances, geometry, density, theme | UI | `localStorage`, versioned (ADR-10) | market data |
| `userStore` | watchlists, columns, chart prefs, notes, journal, alerts, preferences | UI + backend sync | `localStorage` + backend later | market data |

Anti-patterns to eliminate, all present today: market data in component `useState` (AR-2); five live
queries in the shell (AR-3); session state derived in a component (CO-4); two components owning the
same field with different answers (CO-3).

`selectionStore` mutations are intent-named actions — `selectInstrument(key, { source: "marketwatch" })`
— never raw setters, so one panel cannot implicitly clobber another's selection, and the source is
available for prefetch heuristics (AG.5).

### G.3 Market state container (ADR-3)

**Decision: Zustand for `selectionStore` / `workspaceStore` / `userStore`; a plain, non-React
`MarketCache` for market data, exposed to React through `useSyncExternalStore`.**

| Option | Verdict |
| --- | --- |
| Everything in Zustand | **Rejected for market data.** Every set notifies every subscriber, which then runs its selector. At 250 instruments × 1 Hz that is 250 notifications/s × every mounted selector — the fan-out cost is O(subscribers) per tick regardless of relevance. Fine for the ~10 low-frequency selection/layout subscribers, which is why Zustand is kept for those. |
| Redux Toolkit | Rejected. Same fan-out issue, plus action/reducer ceremony for 1 Hz market frames, plus bundle weight. |
| Jotai / atom-per-instrument | Considered seriously — atom granularity is the right shape. Rejected because atoms would be created dynamically per instrument per field group, and the lifecycle (garbage-collecting atoms for instruments no longer displayed) becomes a manual problem we would solve by writing… a registry. So write the registry directly. |
| React Context | Rejected outright. Any context value change re-renders every consumer. |
| **`MarketCache`: `Map<token, InstrumentState>` + `Map<token, Set<callback>>`, `useSyncExternalStore` per subscription** | **Chosen.** O(1) notification of exactly the components displaying that instrument. React 19 built-in, zero dependencies, tearing-safe. Non-React data (the tape ring buffer, chart series) lives outside React entirely and is read imperatively by the panels that own a canvas. |

```ts
class MarketCache {
  private state = new Map<number, InstrumentState>();
  private subs  = new Map<number, Set<() => void>>();
  private globalSubs = new Set<() => void>();          // session, connection

  applyFrame(f: Frame): void                            // transport only; seq-guarded (E.8)
  hydrate(token: number, snapshot: CanonicalQuote): void // data/ only
  subscribe(token: number, cb: () => void): () => void   // ref-counted → subscription manager
  getSnapshot(token: number): InstrumentState | undefined
  evict(token: number): void                             // on refcount 0 + TTL (AG.4)
}
```

`useInstrument(token, selector)` wraps `useSyncExternalStore` with a memoised selector and an
equality check, so a volume-only frame does not re-render a price cell.

### G.4 Update pipeline

```
SSE frame → SharedWorker → structural decode → seq/duplicate guard (E.8)
  → UpdateQueue: coalesce per token per animation frame
  → MarketCache.applyFrame
  → notify only that token's subscribers
  → React renders only those components
```

- **Coalescing:** at most one visible update per token per frame (~16 ms). Two frames arriving in one
  frame window collapse to the newer, per field group.
- **Batching:** all frames in one queue flush are applied inside a single `flushSync`-free batch, so
  React coalesces the renders.
- **Backpressure:** if the queue exceeds 5 000 pending frames (a tab resumed after sleep), drop
  everything except the newest frame per token, set quality `RECOVERING`, and re-hydrate. Never
  replay a backlog into the UI.
- **Hidden tabs:** `document.hidden` → stop applying frames to the cache for non-subscribed-critical
  instruments, keep the connection alive with heartbeats only, and re-hydrate on visibility. The
  current `useApiQuery` visibility handling is the right instinct and this generalises it.
- **Lifecycle cleanup:** every `subscribe` returns an unsubscribe; unmount decrements the refcount;
  zero refcount plus a 300 s TTL evicts the instrument from the cache and releases the server-side
  dynamic subscription (E.2).

**The requirement's specific test:** a single HDFCBANK update must not re-render every Marketwatch
row, chart, chain or panel. It does not, because notification is keyed by token, Marketwatch rows
subscribe individually, and the chain subscribes per contract. Verified by the render-count test in
AM.4.

### G.5 Query layer (ADR-4)

**Decision: TanStack Query v5** for SNAPSHOT, HISTORICAL, METADATA, ANALYTICS, MODEL and ACCOUNT
classes. The stream is not a query.

| Option | Verdict |
| --- | --- |
| Keep `useApiQuery` | **Rejected.** It is the source of AR-2/AR-3/AR-5: no shared cache, no dedupe across components, no stale-while-revalidate, one timer per widget. It is technical debt T1. |
| SWR | Viable. Rejected for weaker cache-key introspection, no built-in mutation/optimistic model (needed for CONTROL routes), and no query cancellation. |
| Hand-rolled cache | Rejected. We already have one (`apiGet`'s 1.5 s TTL) and it is insufficient in exactly the ways a library solves. |
| **TanStack Query v5** | **Chosen.** Request dedupe, per-class `staleTime`, structural sharing (fewer re-renders), cancellation on unmount (pairs with N-17), optimistic mutations for watchlists/alerts, and a devtools story. |

`staleTime` by class: METADATA 4 h; HISTORICAL closed sessions `Infinity`, current session 60 s;
SNAPSHOT 1 s (the stream supersedes it); ANALYTICS 30 s; MODEL 60 s; ACCOUNT 5 s and
`gcTime` 0 (never persisted — N-14).

`refetchOnWindowFocus` is **off** globally. With a live stream, focus refetching is a request storm
with no benefit; freshness is the stream's job.

### G.6 Rendering discipline

- Virtualize any list that can exceed 50 rows: Marketwatch, option chain, Time & Sales, screener
  results, orders, positions, unusual-activity feed (AG.3).
- Column definitions live in module scope or `useMemo` with stable deps — the current
  `FinancialTable` rebuilds them per render (AR-6).
- Charts are imperative: `series.update()` for the forming bar, `setData()` only on symbol or
  timeframe change. Never recreate the series (AR-7).
- Number formatting goes through `canonical/units.ts` with `InstrumentMeta` (N-13).
- Every panel is wrapped in an error boundary (AE.2).
- `reactStrictMode` stays on; every effect must be idempotent under double invocation — which the
  current chart host is not (AR-7).

## H. Workspace architecture

### H.1 Model

```
Workspace { id, name, icon, layout: LayoutNode, panels: PanelInstance[], schemaVersion }
LayoutNode = Split { dir: "row"|"col", sizes: number[], children: LayoutNode[] }
           | Leaf  { panelId }
PanelInstance { id, type: PanelType, title?, config: PanelConfig,
                instrumentBinding: "LINKED" | { pinned: DurableInstrumentKey } }
```

`instrumentBinding` is the mechanism that makes the terminal instrument-centric rather than
page-centric. A `LINKED` panel follows `selectionStore.activeInstrument`; a `pinned` panel holds its
own instrument regardless of selection. That single field is what lets a trader watch RELIANCE's
depth while charting HDFCBANK — the defining behaviour of a terminal versus a dashboard, and it must
exist from the first workspace implementation because retrofitting it means touching every panel.

Panel geometry is a nested split tree, not free-floating windows. Rationale: a split tree
guarantees no overlap and no lost panels, resizes deterministically, serializes compactly, and
matches how Kite Terminal and every professional terminal actually behave. Free-floating windows
require z-order management, collision handling and off-screen recovery for no workflow gain inside a
browser tab.

### H.2 Panel registry

Panels self-register so the shell never imports them concretely (G.1 rule 4):

```ts
registerPanel({
  type: "chart",
  title: "Chart",
  minSize: { w: 320, h: 240 },
  defaultConfig: { interval: "5m", indicators: [] },
  capabilities: ["ltp"],              // D.4 — hidden for instruments that cannot support it
  component: lazy(() => import("@/panels/chart/ChartPanel")),
  configSchema: chartConfigSchema,    // validates persisted config on restore
});
```

`capabilities` is what stops the terminal offering a Depth panel for `NIFTY 50` (D.4) — the panel is
not offered rather than offered and then empty. Lazy loading keeps the initial bundle to the shell
plus Marketwatch.

### H.3 Screen and panel inventory

| Surface | Kind | Required data | Live requirement | Historical requirement | Phase |
| --- | --- | --- | --- | --- | --- |
| Terminal Home | default workspace | overview, indices, breadth | index ticker, breadth | — | 6 |
| Marketwatch | panel | bulk quotes + RM-2 + RM-4 meta | **yes**, per row | — | 7 |
| Instrument Snapshot | panel | quote + freshness + capabilities | **yes** | — | 8 |
| Chart | panel | RM-1 bars + forming bar | **yes**, current bar only | yes | 8 |
| Depth | panel | ladder (ADR-12) + depth events | **yes** | recent events | 8 |
| Time & Sales | panel | RM-6 / T1 prints | **yes**, append | yes | 8 |
| Options chain | panel or full view | chain + aggregates + Greeks | **yes**, per contract | OI history | 9 |
| Futures | panel | Near/Next/Far + spot + basis | **yes** | OI, basis history | 9 |
| Activity / flow | panel | `activity_samples` + derived | yes | yes | 10 |
| Unusual activity | panel + full view | scored events (RM-3 for history) | yes | yes | 10 |
| Market context | panel | indices, VIX, sector, breadth | yes | — | 10 |
| Trade Intelligence | panel (primary) | `/intelligence` | on input change | signal history | 11 |
| Screener | full view | RM-1 + RM-2 + `latest_quotes` | on run | yes | 12 |
| Alerts | full view + panel | user alerts + `marketCache` | evaluation | — | 12 |
| Notification / event center | shell overlay | all event sources | yes | recent | 12 |
| Orders / Trades | panel | `/account/orders`, `/trades` | yes when live | yes | 13 |
| Positions / Holdings | panel | `/account/*` + live marking | **yes**, for P&L | yes | 13 |
| Portfolio / Risk | full view | positions + Greeks + exposure | yes | yes | 13 |
| Notes / Journal | panel | `userStore` + backend later | — | — | 14 |
| Settings | full view | preferences | — | — | 6 |
| System Health | full view | `/health` | yes | — | 6 |

"Full view" means it takes the whole panel area of a dedicated workspace; it is still a panel, so it
can also be docked. There are no non-panel routes except Settings and System Health.

### H.4 Default workspaces

| Workspace | Layout |
| --- | --- |
| **Trade** (default) | left Marketwatch · centre Chart · right Snapshot + Depth · bottom tabs: Intelligence / Options / Futures / Activity / Positions |
| **Scalper** | left Marketwatch (compact) · centre Depth (large) + Time & Sales · right Chart 1m · bottom Orders |
| **Options** | left Marketwatch (options preset) · centre Option chain (full) · right Futures + Snapshot · bottom Intelligence + OI charts |
| **Research** | left Screener · centre Chart (large, daily) · right Intelligence · bottom Unusual + Activity |
| **Portfolio** | centre Positions + Holdings · right Risk · bottom Orders + Trades |
| **General** | centre Marketwatch (wide) · right Market context |

Operations: add, remove, move, resize, maximize (temporary full-panel), minimize (collapse to a
tab), clone workspace, rename, delete, reset to default, and reorder. Maximize is a view state, not
a layout mutation, so it does not dirty persistence.

### H.5 Persistence (ADR-10)

**Decision: `localStorage`, versioned schema, forward migrations, with the backend sync path
designed behind the same interface.**

| Option | Verdict |
| --- | --- |
| **`localStorage` + version + migrations** | **Chosen.** Synchronous read on boot (no layout flash), no backend dependency, no auth requirement, adequate for the layout/watchlist/preference payload (a few tens of KB). |
| Backend-only | Rejected for now: adds an auth and identity requirement the product does not have yet, and makes boot depend on a network round trip against a 1-vCPU box. |
| IndexedDB | Rejected for layout: asynchronous, so the first paint would have no layout. Reserved for large artefacts if drawings or long tape exports ever need it. |
| URL-only | Rejected as the store, kept as a *projection* (AG.7 deep links). A URL cannot carry a full workspace tree without becoming unusable. |

```
PersistedState { schemaVersion: number, workspaces, activeWorkspaceId, watchlists,
                 columnPresets, chartPrefs, density, theme, notes, alerts, preferences }
```

`schemaVersion` with a migration chain (`migrations[from] → to`). An unmigratable version resets to
defaults **and keeps a backup copy under `…:backup:<version>`** — losing a trader's carefully built
workspace silently is unacceptable; telling them it was reset and offering the backup is not.
`configSchema` validation per panel on restore means one corrupt panel config resets that panel, not
the workspace.

**Persisted (requirement #31 in full):** workspace tree, panel positions and sizes, panel configs, watchlists
and their order, column selections and widths, sort order, chart timeframe, indicators and drawings,
selected expiry, selected strike, density, theme, notes, journal, alerts, preferences.
**Never persisted:** any market data, any account data, connection state, session state.

**Instrument references in persisted state use durable identity (N-10)**, never
`instrument_token` — otherwise every monthly expiry wipes saved layouts.

### H.6 Command palette and keyboard model

`⌘K` / `Ctrl+K` opens a single palette over instruments (RM-4 search), commands, workspaces and
panels. Commands: `Open <symbol>`, `Open <symbol> options`, `Open <symbol> future`,
`Chart <symbol> <interval>`, `Depth <symbol>`, `Switch to <workspace>`, `Add <symbol> to <watchlist>`,
`Set alert on <symbol>`, `Add panel <type>`, `Show unusual activity`, `Show intelligence`,
`Toggle density`, `Export <panel>`.

| Key | Action |
| --- | --- |
| `⌘K` | palette |
| `/` | focus search |
| `↑ ↓` | move Marketwatch selection (drives linked panels immediately) |
| `Enter` | open the selected instrument in the active workspace |
| `⌘1…9` | switch workspace |
| `⌥1…9` | focus panel n |
| `F` | maximize/restore the focused panel |
| `C` `D` `T` `O` `U` `I` | jump to Chart / Depth / Tape / Options / Unusual / Intelligence for the current instrument |
| `1 2 3 4 5` | chart interval presets |
| `A` | set alert on the current instrument |
| `N` | add a note on the current instrument |
| `Esc` | close overlay / restore panel |
| `?` | shortcut reference |

The terminal must be fully operable without a mouse (AN.28). Keyboard focus is panel-scoped: arrow
keys move within the focused panel, `⌥n` moves between panels. No global shortcut may shadow a
browser shortcut a trader relies on (`⌘T`, `⌘W`, `⌘L`).

---

## I. Marketwatch

**WHAT.** A virtualized, multi-watchlist, column-configurable live quote list that is the primary
instrument-selection surface and drives all linked panels.

**WHY.** It is where a trader spends most of their attention. Today's equivalent is a
non-virtualized `FinancialTable` fed by a 5-symbol server-configured watchlist with a wrong day
change (BL-4) and per-widget polling (AR-5).

**WHERE.** `panels/marketwatch/*`. Left column of most workspaces.

**HOW.**
1. Watchlists come from `userStore` (local) synced to `/api/v1/watchlists`; the server-configured
   `watchlist_default` seeds the first one.
2. On mount, the panel subscribes its **visible** rows plus a small overscan to `marketCache`, which
   reference-counts to the server subscription set (E.2). Scrolling changes the subscribed window;
   a 2 000-symbol watchlist never subscribes 2 000 instruments.
3. Hydration is one `/quotes/bulk?tokens=` call for the visible window; the stream keeps it current.
4. Each row subscribes individually, so a HDFCBANK tick re-renders one row (G.4).
5. `change` and `change_percent` are rendered exactly as received (ADR-15). No arithmetic in the row.

**DATA SOURCE.** `latest_quotes` ⋈ RM-2 (reference) ⋈ RM-4 (meta), live via the stream.

**API CONTRACT.**
```
GET /api/v1/quotes/bulk?tokens=408065,260105,…      // ≤250, N-16
→ { …envelope, quotes: [ CanonicalQuote ] }          // AA.2
```

**Column presets** (per requirement #11), all built from canonical fields:

| Preset | Columns |
| --- | --- |
| COMPACT | symbol, LTP, change% |
| TRADING | symbol, LTP, change, change%, bid, ask, spread, volume |
| OPTIONS | symbol, LTP, change%, OI, OI chg%, IV, volume |
| FLOW | symbol, LTP, change%, volume, vol Δ, `bid_depth_5`, `ask_depth_5`, imbalance, notional |
| INVESTING | symbol, LTP, change%, day range, 52w range, sector, index membership |

Custom column sets are user-defined and persisted. Sorting is client-side over the loaded window,
with an explicit indicator that sorting applies to loaded rows when the watchlist exceeds the
window — a silent partial sort would be a correctness lie.

**Row interaction.** Click selects (drives linked panels). Right-click opens the standard instrument
context menu (per requirement #33): Open chart / depth / options / futures / Time & Sales /
intelligence / activity, Add to watchlist, Set alert, Add note, Copy symbol, Export — plus Buy /
Sell / Basket rendered **disabled with an explanatory tooltip** until execution is explicitly
enabled (W.5). Keyboard: `↑ ↓` moves selection and updates linked panels immediately.

**FRONTEND OWNER.** `panels/marketwatch/`. **BACKEND OWNER.** `api/read_model.py` +
`market/assemble.py` for `/quotes/bulk`.

**PERFORMANCE.** 250 visible rows at 60 fps with 250 instruments ticking at 1 Hz; row re-render
< 1 ms; hydration < 250 ms; zero requests per row; memory < 5 MB for 1 000 watchlist entries.

**ACCEPTANCE TEST.** (1) 250 rows, 1 Hz each: frame time stays under 16 ms (measured with the
Performance panel). (2) A single-token frame increments exactly one row's render counter. (3) A
2 000-entry watchlist subscribes ≤ (visible + overscan) instruments, asserted against the server's
subscription count. (4) `change%` equals `/quotes/{symbol}`'s value byte-for-byte for the same
instrument at the same `as_of`. (5) Scrolling to the bottom and back leaves the subscription count
unchanged (no leak).

---

## J. Search

**WHAT.** Server-side universal instrument search over the full NSE master, returning equities,
indices, futures and options with live LTP and change%.

**WHY.** Search is the primary navigation verb in a terminal. Today it covers only watchlist symbols
plus two hardcoded indices (AR-8) and cannot find a single option contract.

**WHERE.** Top bar + `⌘K` palette. `panels/../shell/GlobalSearch` + `data/useInstrumentSearch`.

**HOW (ADR-7).** SQLite `instruments` (RM-4) with a precomputed `search_text` and `search_rank`.

| Option | Verdict |
| --- | --- |
| Client-side over watchlists (today) | Rejected — cannot cover the master. |
| Ship the master to the browser | Rejected — tens of MB (N-16). |
| SQLite FTS5 | Considered. Rejected: an extension dependency plus index rebuild cost, for query patterns that are overwhelmingly *prefix* matches on short symbols, which a plain index serves. |
| In-memory trie in the API process | Rejected: ~10–15 MB per worker on a 2 GB box, rebuilt on every reload, for a gain over an indexed `LIKE 'x%'` that is unmeasurable at this scale. |
| **SQLite table + prefix indexes + `search_rank`** | **Chosen.** No new dependency, < 30 ms, and ranking is data rather than code. |

**Query grammar** — parsed server-side, so every client behaves identically:

| Input | Interpretation |
| --- | --- |
| `HDFCBANK` | prefix on `tradingsymbol` and `name`, all segments |
| `NSE:HDFCBANK` | exchange-qualified exact |
| `HDFCBANK FUT` | futures for that underlying, Near first |
| `HDFCBANK 1800 CE` | that strike and side, nearest expiry first |
| `NIFTY 24000` | NIFTY options at 24000, both sides |
| `BANKNIFTY OCT FUT` | month-qualified future |
| `banking` | sector match (RM-4 `sector`) |

**Ranking** (`search_rank`, precomputed daily, then adjusted by the live query): exact symbol >
symbol prefix > name prefix > name contains; within ties, index > equity > future > option; F&O
eligible above non-eligible; nearer expiry above further; nearer-ATM strikes above far strikes;
`is_active` only.

**API CONTRACT.**
```
GET /api/v1/instruments?q=hdfc&segment=&types=EQ,FUT,CE,PE&limit=20
→ { …envelope, results: [ {
      durable_key, instrument_token, exchange, segment, tradingsymbol, name,
      instrument_type, expiry, expiry_kind, days_to_expiry, strike, option_type,
      lot_size, tick_size, sector, indices, capabilities,
      last_price, change_absolute, change_percent, reference_type,   // from latest_quotes ⋈ RM-2
      subscription: { mode, kind }                                    // so the UI can say "not subscribed"
  } ] }
```

Returning LTP and change% with the search result is deliberate: it makes search a micro-quote
surface, which is how traders actually use it, and it comes free from a join to a table already in
memory. Instruments that are not subscribed return `last_price: null` with
`subscription.mode: "NONE"`, which the UI renders as "not subscribed" — never as a zero, and never
as a blank that implies a data failure (D.4).

**Behaviour.** 150 ms debounce; results grouped by type; `↑ ↓` navigate, `Enter` selects and sets
`activeInstrument`, `⌘Enter` opens pinned in a new panel; the last 10 selections are kept in
`userStore` and shown on empty focus.

**PERFORMANCE.** p95 < 30 ms server, < 120 ms perceived. **FRONTEND OWNER.** `shell/GlobalSearch` +
`shell/CommandPalette`. **BACKEND OWNER.** RM-4 + `api/instruments.py`.

**ACCEPTANCE TEST.** (1) `HDFCBANK 1800 CE` returns that contract as the first result.
(2) A three-character query returns in < 30 ms server-side on the VM. (3) An unsubscribed
instrument shows "not subscribed", not `0.00`. (4) Selecting a result updates every LINKED panel and
the URL (AG.7).

---

## K. Instrument workspace and linked context

**WHAT.** The set of panels that follow the active instrument, and the rules linking related
instruments.

**HOW.** `selectionStore.activeInstrument` holds a `DurableInstrumentKey` (N-10). A
`contextResolver` derives the linked set:

```
resolveContext(key) → {
  instrument, underlying?, spot?, futures: [Near, Next, Far],
  optionExpiries: [...], selectedExpiry, atmStrike, selectedStrike,
  sector, sectorIndex, benchmarkIndex, indexMemberships
}
```

**Relationships and what each change propagates:**

| Change | Propagates to | Does **not** touch |
| --- | --- | --- |
| Instrument changes | every LINKED panel; snapshot, chart, depth, tape, activity, intelligence re-target; futures and options resolve the new underlying; expiry resets to nearest; strike resets to ATM | pinned panels; workspace layout; watchlists |
| Expiry changes | option chain, ATM, `days_to_expiry`, chain aggregates, OI charts | chart, depth, tape, quote |
| Strike changes | selected-contract snapshot, that contract's chart/OI | the rest of the chain |
| Timeframe changes | chart only (and its indicators) | everything else |
| Underlying spot changes (ticks) | ATM recentering **only when the ATM strike actually changes** | selection |

The last row matters: recentering the chain on every spot tick would make the chain unreadable.
Recentre only when `nearest_listed` ATM changes, and never scroll the user's viewport if they have
scrolled manually — a "recentre" affordance appears instead.

**Equity ↔ derivative linking** uses RM-4: equity → its F&O contracts via `underlying`; option →
its underlying and spot; future → its underlying and spot; stock → sector (RM-4 `sector`) and index
memberships (RM-4 `indices`). Where a stock has no derivatives, the Options and Futures panels
render `NOT_APPLICABLE`, not empty (D.4). Where it has derivatives that are **not subscribed** (all
stock F&O today — R-4), they render `UNAVAILABLE` with the reason "not subscribed", which is a
different and honest statement.

**ACCEPTANCE TEST.** Selecting HDFCBANK updates snapshot, chart, depth, tape, activity, intelligence,
futures and options within one frame, leaves a pinned RELIANCE depth panel untouched, and updates the
URL. Switching expiry re-renders only the chain.

---

## L. Charts

**WHAT.** A candlestick/line chart with volume and OI subcharts, indicators, drawings, multiple
intervals, historical depth and a live forming bar.

**WHY.** It is the most-used panel and it is currently unusable: 32.03 s to load (BL-3), series
recreated every 8 s (AR-7), volume always null (CO-6), gaps rendered as moves (CO-10), disposal
errors on unmount.

### L.1 Backend (RM-1, D.10)

Authoritative source per interval is fixed in D.10 — no runtime ambiguity, no fallback to tick
aggregation.

```
GET /api/v1/candles/{key}?interval=5m&from=&to=&max_points=500
→ { …envelope,
    interval: "5m", source: "bars_1m_rollup" | "bars_daily",
    candles: [ {t, o, h, l, c, v, oi, trades_observed, complete} ],
    coverage: { expected_bars, observed_bars, completeness, gaps: [{from,to}] },   // N-12
    candles_status: "ok"|"partial"|"unavailable", candles_reason }

GET /api/v1/series/{key}?fields=volume,oi,activity&interval=5m&from=&to=
→ { …envelope, series: { volume: [...], oi: [...], activity: [...] } }
```

`source` is always truthful and never says `compacted_ticks` for a chart again, because charts no
longer read ticks.

### L.2 Frontend controller

A `ChartController` owns the `lightweight-charts` instance imperatively, outside React state:

| Event | Action |
| --- | --- |
| mount | create chart + series once; attach a resize observer |
| symbol or interval change | `series.setData(bars)` once |
| live frame for the current instrument | `series.update(formingBar)` — one call, no reallocation |
| bar rollover | `series.update()` closes the previous bar and starts the next, driven by the bucket boundary from RM-5's session windows, **not** by a local timer |
| unmount | remove series **then** remove chart (fixes the current disposal-order bug), disconnect observers, cancel in-flight queries |
| indicator toggle | recompute from the loaded bars; never refetch |
| gap in `coverage.gaps` | render a hatched discontinuity (N-12) |

React re-renders the panel chrome; it never re-renders the canvas. This is why the current
recreate-per-poll pattern (AR-7) disappears rather than being optimised.

### L.3 Live chart architecture (ADR-5, requirement #17)

Historical bars and the forming bar are strictly separated:

- **History** comes from `/candles`, is immutable, and is fetched once per (symbol, interval, range).
- **The forming bar** is built in the browser from stream observations for the current bucket only:
  `open` = first observed price in the bucket, `high`/`low` = running extremes, `close` = latest,
  `volume` = latest cumulative session volume minus the session volume at bucket start,
  `complete: false`.
- **On rollover**, the forming bar is finalised locally for display continuity and **replaced** by
  the authoritative bar when the next `/candles` fetch covers it. The locally-built bar is never
  persisted and never treated as history.
- **On symbol change**, the forming bar is discarded.
- **On session boundary**, all forming state resets (E.8).
- **On reconnect**, the forming bar is discarded and rebuilt after re-hydration, because frames were
  missed and a bar built across a gap would be wrong.
- **On a live→stale transition**, the forming bar stops updating and is visually marked; it is not
  extended with a flat line.

**Never fabricate candles.** The existing `lib/candles.ts` rule ("never synthesise candles from
`last_price` observations") is preserved for *history*. The forming bar is the single, explicit,
labelled exception, and it is labelled `complete: false` in the data model — not merely styled
differently.

**DEPENDENCIES.** RM-1, RM-5 (bucket boundaries), the stream, `InstrumentMeta` (tick-size
rendering, N-13).

**PERFORMANCE.** Cold `/candles` p95 < 900 ms, warm < 400 ms, payload < 64 KB for 500 bars;
`series.update()` < 2 ms; timeframe switch < 300 ms; symbol switch < 400 ms; chart memory < 8 MB per
instance; no memory growth over 6 h.

**ACCEPTANCE TEST.** (1) `/candles/NIFTY 50?interval=5m` p95 < 900 ms cold on the VM — from
32.03 s. (2) 60 minutes of live ticks produce zero `setData` calls after the initial load. (3) A
synthetic 60-minute gap renders as a discontinuity and reports `completeness ≈ 0.84`. (4) Symbol
switch × 100 leaks no chart instances (heap snapshot). (5) Unmount during an in-flight fetch throws
nothing. (6) Volume bars are populated (regression test for CO-6).

---

## M. Depth

**WHAT.** A 5-level bid/ask ladder with per-level price, quantity and order count, live deltas,
spread, imbalance, total buy/sell quantity, and consecutive-state depth events.

**WHY.** Depth is the requirement's canonical proof of liveness ("a change in depth must be visible
even when LTP does not change") and today it is the least available data in the system.

### M.1 The blocker (BL-5)

MODE_FULL delivers five levels per side. Raw and compacted Parquet keep them. **`latest_quotes`
does not store them at all** (A.6) — its depth footprint is aggregate quantities and best bid/ask
only. So the live ladder cannot be served from live state no matter what the API does.

### M.2 Storage decision (ADR-12)

| Option | Verdict |
| --- | --- |
| **Two JSON text columns on `latest_quotes`: `bid_levels`, `ask_levels`** | **Chosen.** ~200 bytes per row, no join on the hottest read path, one migration, trivially serialisable straight into a stream frame. |
| A separate `latest_depth` table | Rejected: adds a join to the bulk-quote query, which is the query that must stay under 10 ms for 250 rows. |
| msgpack/BLOB | Rejected: unreadable in `sqlite3` during debugging, for a saving that is noise at 600 rows. |
| Keep discarding depth | Rejected: makes requirement #1's depth clause unimplementable. |

Migration: `ALTER TABLE latest_quotes ADD COLUMN bid_levels TEXT` / `ask_levels TEXT`;
`snapshot_from_tick` serialises `[[price, quantity, orders] × 5]`; `upsert_latest_quotes` carries
them. Additive and backward-compatible — existing consumers ignore the new columns.

### M.3 Depth events (requirement #15)

Snapshot → comparison → event → aggregate → intelligence, computed by diffing consecutive depth
observations:

| Event | Definition | Class |
| --- | --- | --- |
| `depth_added` / `depth_removed` | per-level quantity increase/decrease at an unchanged price | DERIVED |
| `liquidity_added` / `liquidity_withdrawn` | aggregate 5-level quantity change beyond `depth_shock_pct: 40` (existing config) | DERIVED |
| `best_level_moved` | best bid or ask price changed | DERIVED |
| `spread_widened` / `spread_narrowed` | beyond `spread_widen_pct: 50` (existing) | DERIVED |
| `imbalance_shift` | `depth_imbalance` crossed a configured band | DERIVED |
| `aggressive_buy_proxy` / `aggressive_sell_proxy` | a trade printed at/through the ask/bid within `aggressor_tolerance_bps: 2.0` (existing) | **INFERRED** — labelled as such, never called "buyer initiated" as fact |

The last row is the honesty line: we can observe that a print occurred at the ask, and we may infer
aggression, but we cannot observe the aggressor. `market/unusual.py` already models these as
`liquidity_events` and `aggressive_proxy`, and `ActivityPanel.tsx` is already disciplined about not
implying participant identity. That discipline is preserved and made structural via the
`provenance` field (D.1).

**API CONTRACT.**
```
GET /api/v1/depth/{key}
→ { …envelope,
    ladder: { bid: [{price, quantity, orders}×5], ask: [...] },
    totals: { buy_quantity, sell_quantity, bid_depth_5, ask_depth_5 },
    metrics: { spread, spread_bps, mid_price, depth_imbalance },
    events: [ {t, kind, level, delta, provenance} ],
    freshness: { depth: {as_of, age_ms, state} },
    coherence: { status }                              // bid and ask MUST be same-observation (D.5)
  }
```

Stream frames carry `bid`/`ask` arrays; the depth panel subscribes only the `depth` group, so a
price-only frame does not re-render the ladder and vice versa.

**UI.** Aligned ladder with quantity bars; per-level flash on change (green/red, 150 ms, respecting
`prefers-reduced-motion`); cumulative quantity column; spread in ticks and bps; imbalance gauge;
quantities in lots for derivatives (N-13). For `equity_quote` instruments and indices the panel
renders `UNAVAILABLE (not subscribed at FULL)` and `NOT_APPLICABLE` respectively (D.4).

**DEPENDENCIES.** ADR-12 schema change, ingestion change, stream, RM-4 capabilities.

**PERFORMANCE.** Ladder update < 1 ms; `/depth` p95 < 100 ms; no re-render on price-only frames.

**ACCEPTANCE TEST.** (1) With LTP unchanged for 30 s while depth changes, the ladder updates and the
price cell's render counter does not increment. **This is the definitive liveness test.** (2) Bid and
ask always come from the same observation — asserted by a coherence test. (3) An index renders
`NOT_APPLICABLE`, not "no data".

---

## N. Time & Sales / market tape

**WHAT.** A live, virtualized, append-oriented feed of trade-related observations for the active
instrument.

**WHY.** Required by the brief; no endpoint exists (verified). It is the primary surface for reading
order flow and the input to the aggressive-side proxy.

### N.1 The honesty constraint (ADR-8)

Kite market-data observations do **not** guarantee a unique exchange trade identifier or any
participant identity. At MODE_FULL we receive periodic snapshots containing `last_price`,
`last_quantity`, cumulative `volume` and `last_trade_time` — not a trade-by-trade feed. Therefore:

- The panel shows **observation events**, not exchange trades. One row = one observation in which
  something trade-related changed.
- `last_quantity` is the exchange's last traded quantity: **OBSERVED**.
- `volume_delta` between observations is the volume traded *since the previous observation*, which
  may cover multiple trades: **DERIVED**, and labelled so.
- Aggressive side is **INFERRED**.
- **No transaction ID is ever fabricated.** No HFT, FII, DII, proprietary or client identity is ever
  claimed or hinted. There is no field for it, so there is nothing to accidentally populate.
- The panel header states plainly: "Observation tape — derived from market-data snapshots, not a
  trade-by-trade feed." A professional user reads that and knows exactly what they are looking at;
  hiding it would be the amateur choice.

### N.2 Columns

| Column | Provenance |
| --- | --- |
| time (`exchange_timestamp`, ms) | OBSERVED |
| `last_price` | OBSERVED |
| `last_quantity` (+ lots) | OBSERVED |
| price Δ vs previous observation | DERIVED |
| `volume_delta` | DERIVED |
| estimated notional (`last_quantity × last_price`) | DERIVED |
| `oi_delta` (derivatives) | DERIVED |
| spread at observation | DERIVED |
| depth imbalance at observation | DERIVED |
| side proxy (at bid / at ask / between) | **INFERRED** |
| size class (large / very large / extreme, from the existing 95/99/99.9 percentile config) | DERIVED |

**API CONTRACT.**
```
GET /api/v1/tape/{key}?limit=200&from=&session=
→ { …envelope, prints: [ {t, price, quantity, price_delta, volume_delta, notional,
                          oi_delta, spread, imbalance, side_proxy, size_class} ],
    truncated: bool, source: "prints"|"compacted_ticks" }
```

**FRONTEND.** Virtualized, newest-first, bounded ring buffer of 1 000 rows per instrument in
`marketCache` (AG.4). Appended from stream frames; out-of-order events are inserted by timestamp,
not appended (E.8). Filters: minimum size, side proxy, size class. Row flash on arrival. Discarded
on instrument change.

**PERFORMANCE.** Append < 0.5 ms; 1 000 rows at 60 fps; ≤ 2 MB per instrument;
`/tape` p95 < 150 ms.

**ACCEPTANCE TEST.** (1) No response field can carry a trade identifier — asserted by a schema test.
(2) At 5 observations/s the panel stays at 60 fps and the buffer stays at 1 000 rows.
(3) `side_proxy` is labelled INFERRED in the UI and in the payload. (4) Switching instruments frees
the previous buffer (heap snapshot).

---

## O. Options

**WHAT.** A first-class option chain: underlying, spot, expiry selector, days to expiry, ATM
marker, CE | strike | PE layout, per-contract live fields, chain aggregates, and explicit coverage
and completeness disclosure.

**WHY.** The chain is the primary derivatives surface. Today it exists, is 1.29 s per request
(PE-2), is not virtualized, guesses IV units (CO-11), has no Greeks, refetches wholly on every poll,
and can silently stop bracketing ATM (CO-7 / N-7).

### O.1 What exists, verified

`/options/{u}`, `/options/{u}/{e}`, `/options/{u}/{e}/activity`, `/options/{u}/{e}/oi` serve
per-contract LTP, volume, OI, OI change, `oi_change_pct` (a **ratio** — A.7), `iv`, `iv_pct`,
`iv_source`, plus chain aggregates PCR and Max Pain with `max_pain_min_strikes: 11` and
`max_pain_min_completeness: 0.70`, and `chain_status` with selected/eligible/missing counts.
`analytics.atm_method: nearest_listed`, `pcr_atm_strikes: 5`.

Coverage is **NIFTY and BANKNIFTY only**, 84 contracts, 42 strikes each (A.3). Stock options are
configured but were never materialised (R-4) and must render `UNAVAILABLE (not subscribed)`.

### O.2 What is missing and how each is resolved

| Missing | Resolution | Fabrication risk |
| --- | --- | --- |
| Greeks (δ, γ, θ, ν) | Compute from the **already-solved IV** using the documented `option_risk_free_rate: 0.06` and `days_to_expiry` from RM-5. Emit **only** where `iv_source` indicates a genuine solve; where IV is unavailable, Greeks are `null`. | High if done carelessly — hence the hard rule: **no Greek is ever emitted without a real IV.** |
| Bid/ask per contract | Available at MODE_FULL, already ingested. Expose in the chain. | none |
| IV unit ambiguity (CO-11) | Ship `iv` as a **percentage** under ADR-13, drop the frontend's `iv <= 3` guess. | none |
| ATM coverage (N-7) | `atm_coverage` block + PCR/Max-Pain suppression when `ATM_OUTSIDE_WINDOW`. | none |
| IV percentile, ATM IV, straddle price, expected move | Derived from the chain + a short IV history; require ≥30 sessions of history (matching the existing `baseline_min_observations: 30` convention) and return `null` with a reason below that. **Only 4 sessions exist today**, so these render `UNAVAILABLE (insufficient history)` at launch — honestly. | High — gated by the history requirement. |
| IV skew, smile, term structure | Phase 15+. Needs multi-expiry subscription and IV history. Contract designed now, not built. | — |
| Incremental updates | Per-contract stream subscriptions; aggregates recomputed server-side on a 1 s memo (F.7). | none |

### O.3 Contract

```
GET /api/v1/options/{underlying}/{expiry}
→ { …envelope,
    underlying: {symbol, spot, change_absolute, change_percent, reference_type, freshness},
    expiry: {date, kind: WEEKLY|MONTHLY, days_to_expiry, expiry_phase},   // N-15
    atm: {strike, method: "nearest_listed"},
    atm_coverage: {subscribed_min_strike, subscribed_max_strike,
                   strikes_below_atm, strikes_above_atm,
                   coverage_status: BALANCED|SKEWED|ATM_OUTSIDE_WINDOW},   // N-7
    rows: [ {strike,
             ce: {token, last_price, change_absolute, change_percent, volume, oi,
                  oi_change, oi_change_percent, iv, iv_source,
                  greeks: {delta,gamma,theta,vega}|null, bid, ask, freshness},
             pe: {…} } ],
    aggregates: {pcr_oi, pcr_volume, max_pain, atm_iv, atm_straddle, expected_move,
                 iv_percentile, reasons: {pcr: null|"...", max_pain: null|"..."}},
    chain_status, selected_count, eligible_count, missing_count,
    coherence: {max_input_spread_ms, status} }
```

Every aggregate has a paired `reasons` entry, so a `null` is always explained rather than merely
absent.

**FRONTEND.** Virtualized rows (42 today, hundreds when stock options and multiple expiries arrive).
ATM row pinned and highlighted; recentre only on an actual ATM change and never against a manual
scroll (K). OI bars in-cell; OI-change colouring; heat-mapped IV column. Per-contract subscription
so one contract's tick re-renders one row. Expiry tabs from RM-4. `NOT_APPLICABLE` for
non-derivative instruments; `UNAVAILABLE (not subscribed)` for stock options.

**PERFORMANCE.** `/options/{u}/{e}` p95 < 250 ms (from 1.29 s); a per-contract frame re-renders one
row; 200 rows at 60 fps; payload < 64 KB.

**ACCEPTANCE TEST.** (1) 60 s of live ticks produce zero full-chain refetches. (2) A Greek is
present if and only if `iv_source` is a real solve — property test over the whole chain. (3) A 3%
index move yields `ATM_OUTSIDE_WINDOW` with `pcr: null` and a reason. (4) With 4 sessions of
history, `iv_percentile` is `null` with `reason: "insufficient_history"`. (5) A stock underlying
renders `UNAVAILABLE (not subscribed)`, never an empty chain.

---

## P. Futures

**WHAT.** Spot, Near, Next and Far contracts with LTP, change, volume, OI, OI change, basis,
basis%, days to expiry, premium/discount, and derived buildup classification.

**WHY.** Futures are the cleanest read on directional positioning and the input to buildup
inference.

**Verified state.** `/futures/{underlying}` exists. **Near and Next are already subscribed**
(R-3: `futures.contract_count: 2`, and `NIFTY26SEPFUT`/`NIFTY26OCTFUT`/`BANKNIFTY26SEPFUT`/
`BANKNIFTY26OCTFUT` are in the compacted data). `basis`, `basis_pct` (a **percentage**) and
`basis_freshness` exist and are exemplary — `futures_basis_max_age_seconds: 10` and a null return
with `basis_status` rather than a wrong number. `price_change_pct` and `oi_change_pct` are
**ratios** (A.7, fixed by ADR-13). `price_oi` provides buildup classification. Stock futures are not
subscribed (R-4).

**Gaps.** Far month (a one-line config change: `contract_count: 2 → 3`); `settlement_price` as the
day-change reference instead of `ohlc_close` (CO-8, N-15); expiry-day OI guards (CO-9, N-15);
rollover-aware "Near" resolution (N-10).

**Buildup classification** — `(price Δ, OI Δ)` → `long buildup | short buildup | short covering |
long unwinding`. This is **INFERRED**, not observed: rising price with rising OI is *consistent with*
long buildup, it does not prove it. Labelled INFERRED in the payload and rendered with that
semantic (D.1). Suppressed on `EXPIRY_DAY` for the expiring series with a stated reason (N-15).

```
GET /api/v1/futures/{underlying}
→ { …envelope,
    spot: {symbol, last_price, change_absolute, change_percent, reference_type, as_of},
    contracts: [ {durable_key, tradingsymbol, expiry, expiry_kind, days_to_expiry, expiry_phase,
                  contract_slot: NEAR|NEXT|FAR,
                  last_price, change_absolute, change_percent, reference_type,
                  volume, oi, oi_change, oi_change_percent,
                  basis, basis_percent, basis_status, spot_as_of, futures_as_of,
                  data_age_seconds, premium_discount,
                  buildup: {classification, provenance: "INFERRED", reason}} ] }
```

**PERFORMANCE.** p95 < 200 ms. **ACCEPTANCE TEST.** (1) Far month appears after the config change.
(2) A 12 s spot/future spread yields `basis: null` with `basis_status` — regression test for existing
behaviour. (3) Buildup is labelled INFERRED everywhere it renders. (4) On a replayed expiry day the
expiring series emits no buildup classification and states why.

---

## Q. Activity, market context and relative strength

### Q.1 Activity / flow

**Verified state.** `/market-activity/{symbol}` serves volume and activity ratios against
time-of-day baselines (`tod_bucket_minutes: 15`, `baseline_min_observations: 30`), liquidity events,
aggressive proxy, and trade size classes at the 95/99/99.9 percentiles. `activity_samples` is
written every ≥60 s — so activity is a **60-second-resolution** series, not a tick series, and the UI
must say so rather than implying continuous data. `ActivityPanel.tsx` is already careful about not
implying participant identity.

**Target.** Restructure the panel **by provenance** rather than by metric type: OBSERVED (volume,
last quantity, notional), DERIVED (deltas, ratios, imbalance), INFERRED (aggressive proxy, liquidity
classification). Same data, honest hierarchy. Add the 60 s resolution disclosure. Live updates come
from the stream's `volume`/`oi` groups; the baseline comparison refreshes on the sample cadence.

### Q.2 Market context

| Tile | Available today | Source | Gap |
| --- | --- | --- | --- |
| NIFTY, BANKNIFTY | ✅ subscribed | `latest_quotes` | — |
| SENSEX | ❌ | BSE index — **out of scope**: the pipeline is NSE-only; render `NOT_APPLICABLE` rather than pretending it is coming |
| India VIX | ❌ | needs subscription (E0 tier) | one instrument, one config line |
| Sector indices | ❌ | needs subscription | ~11 instruments, E0 tier |
| Advance / decline | ❌ | computable **now** from `latest_quotes` + RM-2 over the 499 subscribed equities | `/breadth` |
| 52-week highs / lows | ❌ | needs T3 daily bars over a year; **only 4 sessions exist** → `UNAVAILABLE (insufficient history)` until history accumulates | RM-1/T3 + time |
| Stocks above VWAP | ❌ | computable from `latest_quotes.average_price` vs `last_price` | `/breadth` |
| Market-wide unusual activity | ✅ | `/unusual-activity` | — |

`GET /api/v1/breadth` → advances, declines, unchanged, above-VWAP count, and the counts' universe
size, each with its own `status` so one unavailable tile does not blank the panel (AE.3). The
52-week tiles must render `UNAVAILABLE (insufficient history)` — a terminal that shows a 52-week
high computed from four sessions is worse than one that admits it cannot.

### Q.3 Relative strength

Stock vs NIFTY, stock vs sector index, stock vs peer, future vs spot (already `basis`), option vs
underlying. Definition: normalised return over a window, computed from RM-1 bars, requiring the same
bar buckets for both legs (D.5 tolerance). Sector mapping comes from RM-4, sourced from the
`Industry` column of `config/membership/ind_nifty100list.csv` / `ind_nifty500list.csv` — **verified
present and currently unused**. Sector *indices* need subscription (Q.2) before stock-vs-sector-index
works; stock-vs-sector-*aggregate* (mean of sector constituents) works without it and is the Phase 10
deliverable. All of it is DERIVED and labelled.

---

## R. Unusual activity

**Verified categories** from `market/unusual.py` scoring, `reasons[]`, `liquidity_events[]` and
`aggressive_proxy`: Large Trade, Large Notional, Volume Burst, OI Burst, Depth Imbalance, Liquidity
Withdrawal, Liquidity Addition, Aggressive Buy Proxy, Aggressive Sell Proxy. Price/Volume Divergence
and Cross-Market Divergence exist descriptively through `/cross-market`'s `relationships[]`. Options
Concentration is derivable from `/options/{u}/{e}/activity`. Futures Buildup is `price_oi`.

Thresholds are all existing config: `volume_elevated_ratio: 2.0`, `volume_burst_ratio: 3.0`,
`volume_extreme_ratio: 5.0`, `activity_*_ratio`, `depth_shock_pct: 40`, `spread_widen_pct: 50`,
`large/very_large/extreme_trade_percentile: 95/99/99.9`, `unusual_high_score: 70`.

**Every event renders:** WHAT (category) · WHEN (`exchange_timestamp`) · SYMBOL · MAGNITUDE (the
ratio or percentile that triggered it) · WHY FLAGGED (`reasons[]`) · STRENGTH (`activity_score`,
`activity_level`) · PROVENANCE (OBSERVED / DERIVED / INFERRED, already carried as `kind`).

**No participant identity, ever.** Aggressive buy/sell proxies are INFERRED and named "proxy" in the
data model itself, so the UI cannot accidentally present them as fact.

**Expiry guard (N-15):** OI Burst is suppressed on the expiring series on `EXPIRY_DAY`, with a stated
reason. Without this, every weekly expiry produces the system's highest-scoring false event.

**Performance.** 1.03 s warm / 2.93 s cold today (R-10), behind a 120 s cache. RM-3 precomputes the
last-session ranking to < 50 ms in Phase 10; the live path already computes from `latest_quotes`
cheaply. Contract and ordering semantics are preserved.

**Acceptance test.** (1) Every event has non-empty `reasons[]` and a provenance label. (2) No
response field can carry participant identity — schema test. (3) A replayed expiry day emits no
`oi_burst` on the expiring series. (4) With RM-3, p95 < 300 ms.

---

## S. Trade Intelligence

**WHAT.** The differentiating surface. It answers, for the active instrument: what is happening, why,
how strong, what could happen next, and what would invalidate the read.

**WHERE.** Bottom-centre panel of the Trade workspace, primary by position. `panels/intelligence/`.

### S.1 Inputs

Price, volume and volume delta, VWAP (`average_price`), OI and OI delta, futures basis and buildup,
option chain aggregates (PCR, Max Pain, ATM IV), depth and imbalance, depth events, activity ratios
vs time-of-day baselines, unusual-activity events, market context (indices, breadth), sector context,
relative strength, and algorithm outputs from `signal_log`.

Every input arrives with its own freshness and provenance (D.5, D.1), and the panel **displays which
inputs were missing** rather than silently computing with fewer.

### S.2 Output contract

```
GET /api/v1/intelligence/{key}
→ { …envelope,
    instrument, as_of, session,
    state: READY | PARTIAL_INPUT | NOT_MATURE | ERROR | DISABLED,     // requirement #26
    bias: {direction: LONG|SHORT|NEUTRAL|UNCLEAR, strength: 0..1|null, provenance: "MODEL"},
    market_state_read: {phase, description},
    confidence: 0..1|null,
    probability: number|null,                    // null unless maturity permits — IMMUTABLE
    maturity: {pooled_live_days, required: 60, tier, probability_permitted},
    reasons:        [ {text, inputs[], weight, provenance} ],
    contradictions: [ {text, inputs[], provenance} ],
    evidence:       [ {field, value, as_of, provenance} ],
    invalidation:   [ {condition, level, description} ],
    levels: {entry_area: [lo,hi]|null, target_area: [lo,hi]|null, risk_area: [lo,hi]|null,
             basis: "OBSERVED_STRUCTURE"|"DERIVED"|null},
    algorithms: [ AlgorithmOutput ],             // T.2
    missing_inputs: [ {name, reason} ],
    input_freshness: {…},
    latency_ms }
```

**`contradictions` is as important as `reasons`.** A read that only lists supporting evidence is
marketing, not intelligence. If OI is rising while price falls and the depth is bid-heavy, the panel
says so.

**`invalidation` is mandatory and non-empty whenever `bias.direction` is not `UNCLEAR`.** A trade
idea without an invalidation condition is not actionable, and a terminal that offers one is
dangerous.

**`levels` are only ever derived from observed structure** (session OHLC, VWAP, prior-session levels,
Max Pain, depth clusters) with an explicit `basis`. They are **never** model-generated price
predictions.

### S.3 The maturity gate — IMMUTABLE

Below 60 pooled live days: `probability = null`, `score = null`, and the display is
**"insufficient data, N/60 pooled days"**. No substitute confidence, no "preliminary" probability, no
rescaled score. `signals/maturity.py`, `maturity_gate.suppress_below_days` and
`probability_permitted` are unchanged, and `api/envelope.ts::assertNoFabricatedProbability` remains
the frontend's enforcement — it stays **verbatim** through the rebuild.

With ingestion down since 2026-09-04 and 4 stored sessions, the honest launch state is
`NOT_MATURE`. The panel must be excellent at communicating that: it shows the full qualitative
read — bias, reasons, contradictions, evidence, invalidation — while probability and score display
as unavailable with the day count. That is a genuinely useful panel that makes no false claim, and
designing for it is the difference between a system that survives contact with its own maturity rules
and one that gets quietly "fixed" by someone under pressure to show a number.

### S.4 Rendering

Bias and strength band; the market-state read in one sentence; reasons and contradictions as
two clearly separated columns; evidence with per-field values, timestamps and provenance chips;
invalidation as a checklist; levels on the chart when present; per-algorithm cards (T); a
`missing_inputs` strip; and the maturity strip (the existing `MaturityStrip` component, preserved).

**PERFORMANCE.** p95 < 300 ms; refresh on input change (throttled to 5 s), not on every tick.
**ACCEPTANCE TEST.** (1) No probability appears below 60 pooled days, under any code path — property
test. (2) `state: PARTIAL_INPUT` lists every missing input. (3) A non-`UNCLEAR` bias always carries
at least one invalidation condition. (4) Every evidence row has a provenance label and a market
timestamp.

---

## T. Multi-algorithm architecture

**WHAT.** Algorithms are pluggable. Adding one must require **zero frontend changes**.

**WHY.** The research pipeline exists to produce new algorithms. If each one requires UI work, the
research loop is throttled by frontend capacity — and six months from now that is a redesign
(Appendix 2).

**Verified state.** `contracts/algorithms.py` defines an `Algorithm` protocol and an
`AlgorithmResult` dataclass — a real interface, already correct in shape. `algorithms/logistic.py`
and `algorithms/unavailable.py` implement it. But `signals/engine.py` **hard-wires algorithms by
class key** (`equity`, `options`, `futures`) rather than resolving a registry (AR-9), so a new
algorithm requires an engine edit.

### T.1 Registry

```python
@dataclass(frozen=True)
class AlgorithmDescriptor:
    algorithm_id: str          # "logistic_v3"
    name: str                  # "Logistic Momentum"
    version: str
    instrument_classes: tuple[str, ...]     # ("equity", "futures")
    required_inputs: tuple[str, ...]
    optional_inputs: tuple[str, ...]
    output_kind: str           # "directional" | "state" | "score_only"
    maturity_class: str        # which pooled-day pool governs it
    status: str                # ACTIVE | EXPERIMENTAL | DISABLED | DEPRECATED
    display: AlgorithmDisplayHints   # units, ranges, preferred chart, colour semantics

ALGORITHM_REGISTRY: dict[str, tuple[AlgorithmDescriptor, Algorithm]]
```

`signals/engine.py` iterates the registry filtered by instrument class instead of switching on a
class key. `GET /api/v1/algorithms` exposes the descriptors so the UI **discovers** algorithms at
runtime.

### T.2 Generic output

```
AlgorithmOutput {
  algorithm_id, algorithm_name, version,
  status: READY|PARTIAL_INPUT|NOT_MATURE|ERROR|DISABLED,
  signal: string|null, direction: LONG|SHORT|NEUTRAL|null,
  score: number|null, probability: number|null, confidence: number|null,
  reasons: [...], contradictions: [...],
  inputs: [ {name, value, as_of, provenance} ], missing_inputs: [ {name, reason} ],
  timestamp, maturity: {...}, data_status, latency_ms
}
```

`display` hints tell the UI how to render an unfamiliar output: `output_kind` selects the card
layout, `display.score_range` scales the gauge, `display.units` labels the value. A new algorithm
therefore renders correctly on first deployment without a frontend release — which is the entire
point.

### T.3 Frontend

`panels/intelligence/AlgorithmCard` renders any `AlgorithmOutput` from its `output_kind` and display
hints. Unknown `output_kind` falls back to a generic key/value card with a "new algorithm" marker —
degraded but never broken, and never a crash. The algorithm list, ordering and enable/disable state
live in `userStore`.

**ACCEPTANCE TEST.** (1) Register a synthetic algorithm in the backend; it appears in the UI with
**no frontend commit**. (2) An algorithm with a missing required input renders `PARTIAL_INPUT` and
names it. (3) An algorithm below maturity renders `NOT_MATURE` with `N/60` and no probability.
(4) An unknown `output_kind` renders the generic card without throwing.

---

## U. Screener

**WHAT.** A filterable, rankable, savable scan over a chosen universe.

**Universes:** NIFTY50, NIFTY100, NIFTY500, F&O, F&O stocks, each watchlist, and "all subscribed".
Membership comes from RM-4 (`indices_json`), sourced from the membership CSVs already on disk.

**Filters** — each declares its data source and its availability, because several are unavailable
until history accumulates:

| Filter | Source | Available at launch |
| --- | --- | --- |
| price, change%, volume | `latest_quotes` ⋈ RM-2 | ✅ |
| volume anomaly (vs time-of-day baseline) | `activity_samples` + baselines | ✅ (needs `baseline_min_observations: 30`) |
| OI, OI change | `latest_quotes` (derivatives) | ✅ |
| VWAP position | `average_price` vs `last_price` | ✅ |
| depth imbalance, spread | `latest_quotes` | ✅ |
| trade notional, size class | `activity_samples` | ✅ |
| unusual activity score | `/unusual-activity` / RM-3 | ✅ |
| PCR, futures basis | options/futures analytics | ✅ (index only — R-4) |
| Trade Intelligence bias/score | `signal_log` | ✅ (subject to maturity) |
| RSI, breakout, gap, volatility, 52w position | RM-1 bars | ❌ **needs history** — 4 sessions exist; renders `UNAVAILABLE (insufficient history)` |

**Logic:** filter groups combined with AND, groups internally OR, each filter with an operator and
value; rank by any numeric column; save named screens to `userStore`; run on demand and optionally
re-run on a timer.

```
POST /api/v1/screener/run
{ universe, filters: [{field, op, value}], groups: [...], rank_by, limit }
→ { …envelope, results: [ CanonicalQuote + matched_filters[] ],
    evaluated_count, matched_count, truncated,
    unavailable_filters: [{field, reason}] }
```

`unavailable_filters` is the honest answer to a filter that cannot be evaluated — the run proceeds
with the rest and says which were skipped, rather than silently returning results that ignore a
filter the user set. Silently ignoring a filter is the worst possible screener behaviour.

**PERFORMANCE.** 500 instruments, 10 filters, p95 < 2 s; results virtualized.
**ACCEPTANCE TEST.** (1) A filter requiring absent history is reported in `unavailable_filters` and
not silently dropped. (2) 500 × 10 completes under 2 s on the VM. (3) A saved screen restores after
reload.

---

## V. Alerts

**WHAT.** User-defined conditions evaluated against live data, firing into the notification centre.

**Types:** price crossing, change% threshold, OHLC breach, volume threshold, volume anomaly, OI
threshold, OI change, VWAP cross, spread threshold, depth imbalance, option (IV, OI, PCR), futures
(basis), unusual-activity category, Trade Intelligence bias change.

### V.1 Where evaluation happens

Client-side against `marketCache` for Phase 12 — the data is already there, evaluation is free, and
it requires no server-side per-user state. **Its limitation is stated plainly: alerts only fire while
a terminal tab is open.** Server-side evaluation (Phase 15+) is designed behind the same rule schema
so migration needs no rule rewriting; only then do alerts fire with the browser closed.

### V.2 Contract

```
Alert { id, durable_instrument_key, type, operator, value, value2?,
        cooldown_seconds, expiry, active, note?, created_at,
        trigger_mode: ONCE | EVERY_TIME | ONCE_PER_SESSION,
        require_session_state: [OPEN] }
```

`require_session_state` prevents the classic bug of an alert firing on a last-session price at
21:00. `cooldown_seconds` and `trigger_mode` prevent an oscillating price from firing 400 times.
Both are mandatory fields, not options.

### V.3 Subscription implication (E.2)

An armed alert on an unsubscribed instrument requires a subscription with **no visible panel**. It
therefore holds a distinct, **non-TTL-evictable** reference, counted against its own budget
(`alerts.max_subscribed_instruments`, default 50). Exceeding it refuses the alert with a clear
message rather than accepting one that will never fire. This is the trap identified in E.2 and it is
closed here.

**ACCEPTANCE TEST.** (1) A price alert fires within 2 s of the crossing frame, with no refresh.
(2) An oscillating price fires once per cooldown. (3) No alert fires while `market_state != OPEN` if
so configured. (4) An alert on an unsubscribed instrument causes exactly one new subscription, and
deleting it releases it.

---

## W. Portfolio, orders and account

**Verified state (R-8).** `positions_snapshot` 3 826 rows whose latest payload is an **empty list**;
`margin_snapshot` 1 913; `holdings_snapshot` **1**; `order_events` **0**; `trade_fills` **0**. All
opaque `payload_json`. `order_events` does have typed columns (`status`, `transaction_type`,
`quantity`, `price`, `filled_quantity`, `average_price`, `order_timestamp`) but zero rows. The
capture service is the one crash-looping on the invalid token (N-2). `settings/page.tsx` renders
account JSON via `JSON.stringify`.

### W.1 Order lifecycle semantics

Canonical states, mapped from the broker's status strings rather than passed through raw:

```
CREATED → OPEN → { TRIGGER_PENDING | PARTIAL } → COMPLETE
                                                → CANCEL_PENDING → CANCELLED
        → REJECTED
        → EXPIRED
```

| State | Meaning |
| --- | --- |
| `CREATED` | accepted by us, not yet acknowledged by the exchange |
| `OPEN` | live at the exchange, unfilled |
| `TRIGGER_PENDING` | SL/SL-M awaiting its trigger |
| `PARTIAL` | partly filled, remainder live |
| `COMPLETE` | fully filled |
| `CANCEL_PENDING` | cancellation requested, unconfirmed |
| `CANCELLED` | cancelled, with `filled_quantity` possibly > 0 |
| `REJECTED` | rejected, with the broker's reason preserved verbatim |
| `EXPIRED` | expired unfilled at session end |

A `broker_status_map` table makes the mapping explicit and auditable; an unrecognised broker status
maps to `UNKNOWN` and is **surfaced**, never silently coerced into a neighbouring state.

### W.2 P&L decomposition — never one field

| Field | Definition |
| --- | --- |
| `realized_pnl` | closed-quantity P&L for the period |
| `unrealized_pnl` | open-quantity P&L at the current mark |
| `day_pnl` | P&L attributable to today, from the day's opening mark |
| `net_pnl` | `realized + unrealized`, gross of charges |
| `mtm` | mark-to-market against the day's reference mark |
| `charges` | brokerage, STT, exchange fees, stamp duty, GST, SEBI — itemised |
| `net_pnl_after_charges` | `net_pnl − charges` |
| `average_price` | position-weighted entry |
| `mark_price` | the price used, **with its own `as_of` and provenance** |
| `settlement_status` | for T+1 equity settlement |

**The mark matters as much as the number.** A P&L computed against a last-session mark during a live
market is misleading, so `mark_price.as_of` and the D.5 freshness group travel with every P&L value,
and the UI shows the mark's state. Positions are marked against the **live stream**, so P&L updates
continuously without a refresh.

### W.3 Typed projection (N-14)

A projection job parses the captured broker payloads into typed tables — `positions`, `holdings`,
`orders`, `trades`, `margins`, `funds` — with explicit columns. Reasons: the P&L decomposition above
is unqueryable from a JSON blob; the UI must never receive raw broker JSON; and field-level
allow-listing requires knowing the fields.

```
GET /api/v1/account/positions → { …envelope, day: [...], net: [...], totals: {…W.2 fields…} }
GET /api/v1/account/holdings  → { …envelope, holdings: [...], totals: {…} }
GET /api/v1/account/orders?status=&from=  → { …envelope, orders: [ {…W.1 canonical state…} ] }
GET /api/v1/account/trades    · /margins  · /funds
```

Positions and holdings are **distinct concepts** and are never merged: positions are intraday/F&O
exposure that nets off; holdings are delivered equity subject to T+1 settlement.

### W.4 Panels

Orders (filterable by status, with the canonical state and the broker's raw status available in the
inspector), Trades, Positions (live-marked, with the full W.2 breakdown), Holdings, Funds and
Margins, and a Portfolio summary. All virtualized. All behind their own error boundary (AE) so an
account failure — today's actual state — leaves the market side untouched.

### W.5 Execution readiness (designed, disabled)

No live execution in this plan. Designed now so it does not force a redesign later (Appendix 2):

```
OrderRequest { durable_instrument_key, transaction_type: BUY|SELL,
               order_type: MARKET|LIMIT|SL|SL_M,
               quantity_lots, quantity_units,     // both, derived via lot_size (N-13)
               price?, trigger_price?,
               product: CNC|MIS|NRML, validity: DAY|IOC|TTL, validity_ttl_minutes?,
               disclosed_quantity?, tag? }
```

Validation the gateway must perform **before** any network call: tick-size conformance, lot-size
multiple, freeze-quantity limit, price band, margin availability. Every one of those is a reference
field the system does not have today (D.8) — which is precisely why they are listed now.

`OrderGateway` ships as `DisabledOrderGateway`, which validates fully and then refuses with
`EXECUTION_DISABLED`. The order ticket UI is fully built and renders margin and charge estimates, so
the execution path is exercised end-to-end minus the final call. Enabling execution is a deliberate,
separately-reviewed change with its own auth boundary (AF).

**ACCEPTANCE TEST.** (1) No raw broker JSON reaches the browser. (2) Killing account capture leaves
every market panel functional. (3) P&L updates from the live stream with no refresh, and shows the
mark's freshness. (4) The order ticket validates tick and lot size and then refuses with
`EXECUTION_DISABLED`. (5) An unrecognised broker order status surfaces as `UNKNOWN`, not as a guess.

---

## X. Risk

Future capability; contracts defined now because retrofitting portfolio-level aggregation into
per-instrument panels is a rebuild.

| Metric | Definition | Requires |
| --- | --- | --- |
| gross exposure | Σ abs(position value) | positions + live marks |
| net exposure | Σ signed position value | same |
| sector exposure | grouped by RM-4 `sector` | RM-4 sector (available) |
| concentration | largest position / net worth; top-5 share | positions |
| margin utilisation | used / available | `/margins` |
| portfolio delta, gamma, vega, theta | Σ (position × contract Greek × lot size) | **O.2 Greeks** |
| beta-adjusted exposure | Σ (exposure × beta vs NIFTY) | RM-1 history for beta — insufficient today |
| drawdown | peak-to-trough of realised + unrealised | P&L history |
| scenario / stress | portfolio value at ±n% underlying and ±n vol | Greeks + a pricing model |

Portfolio Greeks are the reason O.2 forbids fabricated Greeks: a fabricated per-contract delta
becomes a fabricated *portfolio* delta, which is a number someone might size a position against.
Where a leg's Greek is unavailable, the portfolio aggregate is `null` with the count of
unpriceable legs — never a partial sum presented as a total.

---

## Y. Events and corporate actions

### Y.1 Corporate actions and historical price semantics (requirement #40)

Three distinct price series, never mixed:

| Series | Meaning | Use |
| --- | --- | --- |
| `raw` | as observed, unadjusted | intraday charts; tape; anything within one session |
| `official_reference` | exchange official close/settlement | day change (AB); reconciliation |
| `adjusted` | back-adjusted for splits, bonuses, rights, dividends | multi-session charts; returns; relative strength; any percentage across an ex-date |

`corporate_actions(instrument, action_type, ex_date, ratio_from, ratio_to, dividend_amount, source)`
plus an `adjustment_factor` applied cumulatively. A chart crossing an ex-date **must** use `adjusted`
or it shows a fictitious gap; a day-change calculation on the ex-date **must** use the adjusted
previous close or it reports a fictitious −50% on a 1:2 split. Every response carries
`price_basis: "raw" | "adjusted"` so a consumer can never be unsure which it received.

Current state: only a `ca_suspect` quality flag exists. Until the action data does, the terminal
must **display the `ca_suspect` flag** on affected instruments rather than silently showing a wrong
day change — the honest interim behaviour.

### Y.2 Event calendar

Architecture only, no implementation: `economic_events(date, time, event, region, importance,
actual, forecast, previous)` for RBI, Fed, CPI, GDP; `instrument_events(instrument, date, kind,
detail)` for earnings, dividends, bonuses, splits, rights, expiry. Consumed as chart markers, a
Marketwatch "event today" column, an intelligence input (an earnings date is a genuine invalidation
condition), and notification-centre entries. Sourcing is external and out of scope; the schema and
the UI hooks are defined so adding a source is a data task rather than an architecture task.

---

## Z. Notes and journal

Two separate things, both in the user domain, never mixed with market data:

**Instrument notes** — free text per durable instrument key, shown in a panel, indicated by a marker
in Marketwatch, searchable.

**Trade journal** — `JournalEntry { id, durable_key, created_at, thesis, expected_outcome,
invalidation, entry_area, target_area, risk_area, intelligence_snapshot_ref, outcome?,
review?, closed_at? }`.

`intelligence_snapshot_ref` is the feature worth building: it captures what Trade Intelligence said
at the moment the idea was formed, so the post-trade review compares the thesis against what was
actually known then — not against a later, hindsight-contaminated view. It joins to `signal_log`
(N-20), which already stores the features and attribution, so the reference is cheap and the
reconstruction is exact.

Storage: `userStore` → `localStorage` initially, backend later. Never in `marketCache`. Never sent
to any analytics or model path — journal text is user-private (AF).

---

## AA. Canonical data dictionary

### AA.1 Definitions

One definition per concept, for the whole system. Where the current implementation disagrees, the
"today" column says so.

| Field | Definition | Layer | Provenance | Unit | Today |
| --- | --- | --- | --- | --- | --- |
| `last_price` | most recent traded price observed | 2 | OBSERVED | ₹ | ✅ |
| `previous_close` | official close of the **previous trading day** per RM-5 | 2/T4 | OBSERVED | ₹ | ❌ RM-2 |
| `today_open` | first traded price of the current session | 2 | OBSERVED | ₹ | ✅ session `ohlc_open` |
| `official_close` | exchange official close of a session | 2/T4 | OBSERVED | ₹ | ❌ RM-2 |
| `settlement_price` | official settlement for a derivative | 2/T4 | OBSERVED | ₹ | ❌ (`ohlc_close` substitutes — CO-8) |
| `reference_price` | the price day change is measured from, chosen per AB.2 | 3 | DERIVED | ₹ | ❌ |
| `reference_type` | which reference was used (AB.1) | 3 | DERIVED | enum | ❌ |
| `reference_timestamp` | the reference's own observation time | 3 | DERIVED | ISO | ❌ |
| `change_absolute` | `last_price − reference_price` | 3 | DERIVED | ₹ | ❌ **is a tick delta (BL-4)** |
| `change_percent` | `change_absolute / reference_price × 100` | 3 | DERIVED | **%** | ❌ divides a tick delta |
| `price_delta` | change since the **previous observation** — a tick delta, kept, renamed, and never called "change" | 3 | DERIVED | ₹ | ✅ exists, mislabelled |
| `open`/`high`/`low`/`close` (session) | session range so far | 2 | OBSERVED | ₹ | ✅ `ohlc_*` |
| bar `o/h/l/c` | bar-scoped OHLC for an interval | 2/T2 | OBSERVED | ₹ | ❌ RM-1 |
| `volume` | cumulative traded quantity for the session | 2 | OBSERVED | units | ✅ |
| `volume_delta` | `volume` change between consecutive observations | 3 | DERIVED | units | ✅ (null-guarded on gaps) |
| `average_price` | exchange-supplied session average price | 2 | OBSERVED | ₹ | ✅ |
| `vwap` | Σ(price×qty)/Σqty over a window | 3 | DERIVED | ₹ | ❌ (`average_price` is used as the session proxy — labelled as such) |
| `last_quantity` | quantity of the last trade | 2 | OBSERVED | units | ✅ |
| `trade_notional` | `last_quantity × last_price`, same observation | 3 | DERIVED | ₹ | ✅ |
| `oi` | open interest | 2 | OBSERVED | contracts | ✅ |
| `oi_delta` | OI change between consecutive observations | 3 | DERIVED | contracts | ✅ |
| `oi_change_percent` | `oi_delta / previous_oi × 100` | 3 | DERIVED | **%** | ⚠️ **is a ratio (CO-2)** |
| `bid` / `ask` | best bid / ask price | 2 | OBSERVED | ₹ | ✅ |
| `bid_quantity` / `ask_quantity` | quantity at best bid / ask | 2 | OBSERVED | units | ✅ |
| `bid_levels` / `ask_levels` | 5 levels of `[price, quantity, orders]` | 2 | OBSERVED | — | ❌ **not stored (BL-5)** |
| `bid_depth_5` / `ask_depth_5` | Σ quantity over 5 levels | 3 | DERIVED | units | ✅ |
| `spread` | `ask − bid`, same observation | 3 | DERIVED | ₹ | ✅ |
| `spread_bps` | `spread / mid_price × 10 000` | 3 | DERIVED | bps | ❌ |
| `mid_price` | `(bid + ask) / 2`, same observation | 3 | DERIVED | ₹ | ✅ |
| `depth_imbalance` | `(bid_depth_5 − ask_depth_5) / (bid_depth_5 + ask_depth_5)` | 3 | DERIVED | ratio −1..1 | ✅ |
| `basis` | `future_price − spot_price`, within 10 s | 3 | DERIVED | ₹ | ✅ |
| `basis_percent` | `basis / spot_price × 100` | 3 | DERIVED | **%** | ✅ already percent |
| `premium_discount` | sign of `basis` as a label | 3 | DERIVED | enum | ✅ |
| `iv` | implied volatility, annualised | 3 | DERIVED | **%** | ⚠️ unit ambiguous (CO-11) |
| `delta`/`gamma`/`theta`/`vega` | Black-Scholes Greeks from a solved IV | 3 | DERIVED | per convention | ❌ |
| `pcr_oi` / `pcr_volume` | Σ PE / Σ CE over the qualifying strikes | 3 | DERIVED | ratio | ✅ |
| `max_pain` | strike minimising total writer payout | 3 | DERIVED | ₹ | ✅ |
| `activity_score` / `activity_level` | composite anomaly score and band | 3 | DERIVED | 0-100 / enum | ✅ |
| `aggressive_buy_proxy` / `aggressive_sell_proxy` | proxy for aggressor side | 3 | **INFERRED** | ratio | ✅ correctly named |
| `score` / `probability` / `confidence` | model outputs | 4 | MODEL | — | ✅ maturity-gated |

### AA.2 `CanonicalQuote` — the single frontend quote shape

```ts
interface CanonicalQuote {
  // identity (layer 1, from RM-4)
  durable_key: string; instrument_token: number; tradingsymbol: string; exchange: string;

  // observed (layer 2)
  last_price: number | null;  last_quantity: number | null;
  volume: number | null;      average_price: number | null;   oi: number | null;
  session: { open, high, low, close } | null;
  bid: number | null; bid_quantity: number | null;
  ask: number | null; ask_quantity: number | null;
  bid_levels: DepthLevel[] | null;  ask_levels: DepthLevel[] | null;
  total_buy_quantity: number | null; total_sell_quantity: number | null;
  exchange_timestamp: string | null; last_trade_time: string | null;

  // reference (layer 2, RM-2)
  reference_price: number | null;
  reference_type: ReferenceType | null;
  reference_timestamp: string | null;

  // derived (layer 3, computed BACKEND-side — ADR-15)
  change_absolute: number | null; change_percent: number | null;
  price_delta: number | null;                      // tick delta, explicitly NOT "change"
  volume_delta: number | null; oi_delta: number | null; oi_change_percent: number | null;
  spread: number | null; spread_bps: number | null; mid_price: number | null;
  depth_imbalance: number | null;

  // state
  freshness: Record<FieldGroup, GroupFreshness>;   // D.5
  capabilities: InstrumentCapabilities;            // D.4
  coherence: Coherence;                            // D.9
  data_state: DataState;                           // AD
}
```

**Every field is nullable, and `null` means "not observed", never zero.** A price of `0` and an
absent price must be distinguishable at the type level, because a chart, a P&L and an alert all
behave catastrophically differently between the two.

### AA.3 Unit convention (ADR-13)

**Decision: every field named `*_pct` or `*_percent` is a percentage on a 0–100 scale. Every field
named `*_ratio` is a 0–1 ratio. Fixed at the source, in the backend.**

| Option | Verdict |
| --- | --- |
| Keep the mixed convention, compensate in the frontend (today) | **Rejected.** `formatPct` vs `formatRatioPct` per call site is a convention held in human memory across two repos, one mistake away from a 100× error on a number a trader acts on. |
| Rename ratio fields to `*_ratio` and keep their values | Considered; the honest option, but it changes more field names. |
| **Normalise all `*_pct` to percent, at the source** | **Chosen.** Fewest renames, one rule, testable by a schema-wide assertion. |
| Ship both `_pct` and `_ratio` for one release | Adopted as the **migration mechanism** only, then the ratio forms are removed. |

Fields to change: `futures.price_change_pct`, `futures.oi_change_pct`, `options.oi_change_pct`
(all currently ratios via `_ratio()`). Already correct: `quotes.change_pct`, `futures.basis_pct`.
`iv` becomes an unambiguous percentage and `iv_pct` is removed as redundant.

**Enforcement:** a contract test asserting that every `*_pct` field across every endpoint, over a
fixture corpus (N-19), falls in a plausible percentage range and that no endpoint emits both forms
after the migration release. Plus a frontend lint rule banning `formatRatioPct` on any field whose
name ends in `_pct`.

---

## AB. Canonical change semantics (ADR-15)

### AB.1 The current defect

```
latest.py:54   price_delta = numeric_delta(tick.last_price, prev.last_price)   # tick-to-tick
latest.py:126  "change": row.get("price_delta")                               # exposed as `change`
```

`change` is the movement since the **previous observation**, typically seconds. `change_pct` then
divides that by the previous close, producing a number that is neither a tick change percentage nor
a day change percentage. Marketwatch, the index ticker and the instrument header all display it.
This is the most consequential single defect in the system: it is the first number a trader reads,
on every surface, and it is wrong.

### AB.2 The canonical rule

**Computed once, in the backend, for every instrument, from RM-2.**

```
reference_price, reference_type = resolve_reference(instrument, session)
change_absolute = last_price − reference_price
change_percent  = change_absolute / reference_price × 100
```

`resolve_reference` per instrument class — audited separately as required:

| Class | `reference_type` | Reference price | Notes |
| --- | --- | --- | --- |
| Equity | `PREVIOUS_CLOSE` | previous trading day's `official_close` from RM-2 | "Previous trading day" comes from RM-5, never "yesterday" (CO-5) |
| Index | `PREVIOUS_CLOSE` | previous trading day's official close | indices have no volume/OI/depth (D.4) |
| Futures | `SETTLEMENT` when available, else `OFFICIAL_CLOSE` | previous day's settlement | `reference_type` states which — no silent substitution (CO-8) |
| Options | `OFFICIAL_CLOSE` | previous day's close of that contract | thin contracts may have no previous close → `null` + reason, **never zero** |
| Pre-open | `PREVIOUS_CLOSE` | previous close | there is no `today_open` yet |
| New listing / new contract | `TODAY_OPEN` | today's open | with `reference_type: TODAY_OPEN` shown in the UI so the user knows it is not a day change |
| No reference available | `null` | `null` | `change_absolute: null`, `reason: "no_reference"` |

`ReferenceType = PREVIOUS_CLOSE | TODAY_OPEN | OFFICIAL_CLOSE | SETTLEMENT | INSTRUMENT_SPECIFIC`.

### AB.3 Display semantics (requirement #10)

Five distinct quantities. No screen may show one while labelling it simply "Change":

| Quantity | Formula | Label | Where |
| --- | --- | --- | --- |
| **Day change** | `last_price − reference_price` | "Chg" / "Chg %" — the default everywhere | Marketwatch, ticker, header, chain, futures |
| Open change | `last_price − today_open` | "From open" | snapshot detail |
| Previous close → LTP | identical to day change for `PREVIOUS_CLOSE` instruments | "vs prev close" | inspector |
| Open → LTP | same as open change | "vs open" | inspector |
| Intraday move | `high − low`, and `last_price` position within it | "Day range" | snapshot, Marketwatch INVESTING preset |
| Tick change | `price_delta` | "Tick" — only in Time & Sales | tape |

**Unqualified "Change" always means day change.** Everything else is explicitly labelled.

### AB.4 Why the backend and not React

Three reasons, each sufficient. (1) The reference requires the trading calendar and the previous
session's official close — data the browser does not and should not have. (2) Consistency: with a
stream, a bulk quote, a chain and a chart all showing change, a frontend computation gives four
opportunities to diverge. (3) It is the number users act on; it belongs in the tested, versioned,
single-implementation tier. `envelope.ts`'s existing refusal to let the frontend invent
probabilities is the same principle applied to a different field.

**Migration.** ADR-13 and ADR-15 change displayed numbers, so backend and frontend must ship
together. The sequence: RM-2 and RM-5 land → the API emits `reference_price`, `reference_type`,
`change_absolute`, `change_percent` **alongside** the legacy `change`/`change_pct` for one release →
the frontend switches to the canonical fields → the legacy fields are removed. Contract tests assert
`change_absolute == last_price − reference_price` for every instrument in the fixture corpus.

---

## AC. End-to-end data lineage

Format: **Kite field → ingestion → storage/read model → calculation → API field → frontend store →
component.**

| Datum | Lineage |
| --- | --- |
| **LTP** | tick `last_price` → `NormalizedTick.last_price` → `LatestQuoteTracker` → `latest_quotes.last_price` / T1 → none → `quotes.last_price` → `marketCache[token].last_price` → `PriceHeader`, Marketwatch row, chain cell, depth header |
| **change / change%** | tick `last_price` **+** RM-2 `previous_close` (from T1/T3 at compaction, resolved through RM-5) → `resolve_reference` + subtraction/division → `change_absolute`, `change_percent`, `reference_price`, `reference_type` → `marketCache` → Marketwatch, ticker, header. **Today:** `price_delta` → `change`, which is the defect (AB.1) |
| **volume** | tick `volume` (cumulative) → `latest_quotes.volume` → none → `quotes.volume` → `marketCache` → snapshot, Marketwatch FLOW preset |
| **volume_delta** | consecutive `volume` observations → `numeric_delta` with a negative clamp (`latest.py:50-52`) and a gap guard (D.5) → `latest_quotes.volume_delta` → `quotes.volume_delta` → tape, activity, unusual |
| **OI** | tick `oi` (derivatives only) → `latest_quotes.oi` → none → chain/futures `oi` → `marketCache` → chain, futures, OI charts |
| **OI change %** | consecutive `oi` → `_ratio(oi_delta, prev_oi)` **× 100 after ADR-13** → `oi_change_percent` → chain, futures |
| **depth** | tick `depth.buy[5]` / `depth.sell[5]` → `NormalizedTick.bid_prices/quantities` → **T1 Parquet only today; `latest_quotes.bid_levels/ask_levels` after ADR-12** → aggregation into `bid_depth_5`, `spread`, `mid_price`, `depth_imbalance` → `/depth.ladder` → `marketCache[token].depth` → Depth panel |
| **VWAP** | tick `average_price` (exchange-supplied session average) → `latest_quotes.average_price` → labelled as the session proxy, not a recomputed VWAP → `quotes.average_price` → snapshot, breadth (above-VWAP), screener |
| **basis** | future `last_price` **+** spot `last_price`, within `futures_basis_max_age_seconds: 10` → `futures_analytics.basis` / `basis_pct` / `basis_freshness` → `futures.basis`, `basis_status`, `spot_as_of`, `futures_as_of` → futures panel with freshness |
| **options PCR** | Σ CE/PE `oi` and `volume` over `pcr_atm_strikes: 5`, gated by `atm_coverage` (N-7) → `options_analytics` → `aggregates.pcr_oi`, `pcr_volume` + `reasons` → chain header |
| **Max Pain** | all chain `oi`, gated by `max_pain_min_strikes: 11` and `max_pain_min_completeness: 0.70` → `options_analytics` → `aggregates.max_pain` → chain header, chart marker |
| **IV** | option `last_price` + spot + `days_to_expiry` + `option_risk_free_rate: 0.06` → Black-Scholes solve → `iv`, `iv_source` → chain column; **Greeks derive from this same solved IV or are `null`** |
| **activity** | tick `volume`/`last_quantity`/depth → `activity_samples` **every ≥60 s** → ratios vs time-of-day baselines (`tod_bucket_minutes: 15`, `baseline_min_observations: 30`) → `market-activity` → activity panel, at 60 s resolution and labelled so |
| **unusual activity** | `activity_samples` + `latest_quotes` (live) or T1/RM-3 (historical) → `unusual.py` scoring against the percentile and ratio config → `unusual-activity.events[]` with `reasons[]`, `activity_score`, `kind` → unusual panel and notification centre |
| **Trade Intelligence** | features from `feature_log` (built by `nse-features` from T1) → `algorithms/*` via the registry → `signal_log` with `features_json` and `attribution_json` → `/intelligence` with maturity gating → intelligence panel |
| **chart OHLC** | T1 ticks → **RM-1 1m bars at compaction** → server-side rollup to the requested interval (or T3 for daily) → `/candles.candles[]` + `coverage` → `ChartController.setData` → chart canvas. **The forming bar** comes from the stream and is marked `complete: false`. **Today:** T1 → per-request `session_aggregate_ohlc(mode="last_price")` → 32 s, with `volume: null` |

Each row is also the audit trail for the runtime inspector (N-11): the inspector shows this chain
for the specific value on screen.

---

## AD. Live, last-session and no-data state

### AD.1 Rules (existing behaviour, preserved)

| `data_status` | Condition | `as_of` |
| --- | --- | --- |
| `LIVE` | `market_state == OPEN` **and** the `latest_quotes` observation is within `live_quote_max_age_seconds: 120` | the observation's `exchange_timestamp` |
| `LAST_SESSION` | market closed, **or** open with no sufficiently fresh observation, **and** a real historical observation exists | the **historical** observation's timestamp, never rewritten to now |
| `NO_DATA` | neither exists | `null` |

`market/read_policy.py` is the single decision point; `market/data_state.py` is the single
vocabulary. Row existence in `latest_quotes` never implies live. `merge_states` ranks
live > last_session > no_data for multi-symbol payloads. **This logic is correct and is not being
changed** — it is extended with market state (D.7), quality (D.7), coverage (N-12) and per-group
freshness (D.5).

### AD.2 What the UI must render

| Situation | Badge | Detail |
| --- | --- | --- |
| Live and fresh | **● LIVE** green | `09:47:12 IST`, per-group ages in the status bar |
| Live but delayed | **● DELAYED** amber | "LTP 45s" |
| Market open, data stale (**today's actual state**) | **▲ STALE** red | "Market open · data from 04 Sep 15:30 · ingestion down" |
| Market closed, last session | **LAST SESSION** neutral | "04 Sep 2026" |
| Holiday / weekend | **HOLIDAY** / **WEEKEND** neutral | the holiday reason from RM-5 |
| Pre-open | **PRE-OPEN** | "opens 09:15" |
| Partial session | **PARTIAL** on the affected panels | "87% coverage · gap 10:00–11:00" |
| Reconnecting | **RECOVERING** amber | values retained and marked, never blanked |
| No data | typed empty state | the reason, never a zero |
| Token invalid (N-2) | **CRITICAL** banner | "broker session expired — no data will become live" |

**Two absolute rules.** (1) Historical data is never presented as live: the badge, the timestamp and
the session date are always visible on any panel showing fallback data. (2) A missing value is never
rendered as `0`, `0.00`, `—` without a reason, or an empty cell that looks like a layout bug. Every
absence is typed (D.4) and explainable (N-11).

### AD.3 Per-panel independence

`data_state` is per payload, not global. A live quote can coexist with a last-session chart (bars are
only built at compaction, so today's bars do not exist until 15:45) and with unavailable options
analytics. Each panel renders its own state, and the status bar shows the aggregate. Collapsing this
to one global indicator would be a lie in the common case — and the chart case above is not an edge
case, it is every trading day before 15:45.

---

## AE. Error handling and graceful degradation

### AE.1 Failure isolation matrix

| Failure | Must keep working | Rendering |
| --- | --- | --- |
| Options analytics throws | **spot, quote, chart, depth, tape, futures, activity, intelligence** | `subsystems.options.status = UNAVAILABLE` (N-18); the chain panel shows a typed error with a retry |
| Trade Intelligence unavailable | **Marketwatch, chart, all market data** | intelligence panel shows `ERROR` with the reason; other panels untouched |
| Chart endpoint fails | everything else | chart panel error boundary + retry; the instrument stays selected |
| Stream disconnects | all panels, with stale values retained and marked | `RECOVERING`; snapshot polling resumes as a temporary fallback at a low rate |
| One instrument has no data | every other instrument | that row/panel shows its typed empty state |
| Account capture down (**today's state**) | **all market panels** | account panels show `UNAVAILABLE`; no market impact |
| Ingestion down (**today's state**) | last-session data everywhere | global `STALE` banner; panels show last-session badges |
| **Token invalid (today's state)** | last-session data everywhere | global `CRITICAL` banner: no data will become live |
| SQLite locked | reads retry within `busy_timeout` (N-9) | transient; `sqlite_busy_retries` in `/health` |
| Disk full (N-1) | nothing — this is total | `CRITICAL`; ingestion stops writing; the health page names the cause |
| A panel component throws | every other panel | that panel's error boundary; the shell never unmounts |
| Instrument master stale/unavailable | market data continues | `reference_as_of` shown; `/health` WARNING; tick/lot formatting falls back to a safe default with a marker |

### AE.2 Frontend boundaries

Three levels. **Shell** — catches catastrophic failures and renders a recovery screen with a
"reset workspace" action (which preserves the backup, H.5); it must never itself depend on market
data. **Panel** — every panel is wrapped; a throw shows the panel's error state with retry and
"remove panel", and the rest of the workspace is unaffected. **Widget** — chart canvas, ladder, table
body: a throw degrades to a message inside the panel.

Boundaries must not swallow errors: every catch reports to the event centre (V/AH) with the panel
type, the instrument and the error, so a repeatedly failing panel is visible rather than merely
annoying.

### AE.3 Backend rules

No endpoint returns 500 for missing *data* — missing data is a typed 200 with `data_status` and a
reason. 500 is reserved for genuine faults. An analytics exception inside an assembly function is
caught at the domain boundary and turned into `subsystems.<domain>.status = UNAVAILABLE`, so one
domain's bug cannot blank a payload that also contains a perfectly good quote. Deadlines return a
typed `deadline_exceeded` (N-17), not a hang.

---

## AF. Security

### AF.1 Boundaries

| Asset | Where it lives | Browser access |
| --- | --- | --- |
| Kite API key / secret | VM config only | **never** |
| Kite access token | VM, `broker/session.py` (N-2) | **never** |
| SQLite / Parquet | VM filesystem | **never** — no path is ever exposed in a response |
| Market data | VM → API → Next.js server → browser | read-only, via the proxy |
| Account data | VM → API `/account/**` → Next.js server → browser | read-only, **allow-listed fields only** (N-14) |
| Order placement | not implemented; `DisabledOrderGateway` | none |
| User workspaces / notes / journal | browser `localStorage`, backend later | own data only |

### AF.2 Network topology

The FastAPI process binds `127.0.0.1:8080` and is never exposed. The browser talks only to the
Next.js server, which proxies server-side. Consequences: no CORS surface for the API (the current
`:8501`-only config is therefore correct and stays restrictive); no API credentials in browser code;
one place to add authentication when the product leaves single-user development. Today access is via
an SSH tunnel; production browser access terminates TLS at the Next.js server (AI.4).

### AF.3 Rules

1. No secret in any `NEXT_PUBLIC_*` variable, ever.
2. No raw broker payload reaches the browser (N-14) — today's `JSON.stringify(account)` in
   `settings/page.tsx` violates this and is removed.
3. Account responses are never cached in a shared layer, never persisted in `localStorage`, never
   logged, and have `gcTime: 0` in the query client.
4. Market data and account data never share an endpoint, a read model or a stream channel.
5. Enabling execution requires a separate, explicit authorisation boundary — it must not be a config
   flag that a deploy could flip.
6. Logs never contain tokens, account numbers or holdings. A log-scrubbing test asserts this.
7. The stream authorises per connection; when auth exists, `/stream` requires it and subscription
   requests are validated against the caller.
8. No user-supplied string reaches a SQL string, a file path or a shell. `duckdb_store._safe_symbol`
   already sanitises symbols for path construction — that pattern is mandatory for every new path
   built from a symbol (RM-1 and RM-6 both construct paths from symbols).

---

## AG. Performance

### AG.1 Backend budgets (p95, measured on the VM)

| Endpoint | Budget | Today | Mechanism |
| --- | --- | --- | --- |
| `/health` | 50 ms | fast | — |
| `/quotes/{symbol}` | 100 ms | 0.22 s class | SQLite point lookup ⋈ RM-2 |
| `/quotes/bulk` (250) | 150 ms | n/a | one indexed `IN` query ⋈ RM-2 |
| `/instruments?q=` | 30 ms | n/a | RM-4 prefix index |
| `/calendar` | 20 ms | n/a | RM-5 point lookup |
| `/candles` (500 bars) | **400 ms warm / 900 ms cold** | **32 030 ms** | **RM-1** |
| `/series` | 300 ms | — | RM-1 + `activity_samples` |
| `/options/{u}/{e}` | **250 ms** | **1 290 ms** | memoised aggregates + cached IV (F.7) |
| `/futures/{u}` | 200 ms | fast | — |
| `/depth/{key}` | 100 ms | n/a | `latest_quotes` + ADR-12 |
| `/tape/{key}` (200) | 150 ms | n/a | RM-6 / T1 |
| `/market-activity` | 250 ms | fast | — |
| `/unusual-activity` | **300 ms** | 1 030 ms warm / 2 930 ms cold | RM-3 |
| `/breadth` | 200 ms | n/a | `latest_quotes` aggregate |
| `/intelligence` | 300 ms | n/a | `signal_log` read |
| `/screener/run` | 2 000 ms | n/a | RM-1 + RM-2 |
| `/account/*` | 500 ms | — | typed projections |
| `/stream` first byte | 500 ms | n/a | shared poll |
| **Any endpoint, hard deadline** | **5 000 ms** → typed `deadline_exceeded` | unbounded | N-17 |

### AG.2 Frontend budgets

| Metric | Budget |
| --- | --- |
| Shell first paint | < 1.5 s |
| Time to interactive terminal | < 2.5 s |
| Panel mount | < 200 ms |
| Marketwatch row update | < 1 ms |
| Depth ladder update | < 1 ms |
| Chart `series.update()` | < 2 ms |
| Frame budget under full load (250 instruments @ 1 Hz) | < 16 ms |
| Idle HTTP requests | < 10/min (from ~113/min) |
| Stream connections per browser | **exactly 1** (N-5) |
| Heap after 1 h | < 250 MB |
| Heap after 6 h | < 350 MB, **no monotonic growth** |
| Bundle (shell + Marketwatch) | < 250 KB gzipped |

### AG.3 Virtualization

Mandatory above 50 rows: Marketwatch, option chain, Time & Sales, screener results, orders, trades,
positions, holdings, unusual feed, search results. Fixed row heights (variable heights force
measurement passes that break the frame budget at 1 Hz). Overscan 10. Only visible rows subscribe to
`marketCache`, so the subscription set follows the viewport (I).

### AG.4 Browser memory

| Structure | Bound |
| --- | --- |
| `marketCache` instruments | 500 max, LRU-evicted at refcount 0 + 300 s TTL |
| Tape ring per instrument | 1 000 rows, 3 instruments retained max |
| Depth events per instrument | 200 events |
| Chart bars per panel | 2 000 bars |
| Chart instances | 1 per panel, ≤6 panels |
| Query cache | `gcTime` 5 min; account `gcTime` 0 |
| Notification centre | 500 events |
| Fixtures/dev only | excluded from the production bundle |

Cleanup on: panel unmount, instrument deselect + TTL, workspace switch (non-visible panels' caches
released), session change (all session-cumulative state cleared), tab hidden > 15 min (non-essential
buffers trimmed).

### AG.5 Prefetching

Prefetch, on a bounded basis: instrument meta for the visible Marketwatch window plus overscan
(cheap, long TTL, RM-4); the nearest ±3 option strikes around ATM when the chain is open; the
current instrument's `/candles` at the *default* interval when a chart panel exists in the workspace
but is not yet visible; and instrument meta for the top 5 search results as the user types.

**Never** prefetch: whole option chains for unopened underlyings, full historical series for
non-visible timeframes, market-wide analytics, or anything account-related. On a 1-vCPU box a wrong
prefetch is not merely wasteful, it directly steals latency from the visible panel — which is the
same failure mode as the account crash loop stealing CPU from the chart (A.2).

### AG.6 Responsive strategy

Primary targets: large desktop (≥1920), desktop (≥1440), laptop (≥1280). Below 1280, panels reduce
to a two-column layout; below 1024, a single-column stack with a panel switcher; below 768, a
read-only "market view" (ticker, Marketwatch, chart, snapshot) with workspace editing disabled.
**Desktop density is never compromised to serve small screens** — the professional desktop case is
the product.

### AG.7 URL and deep links

```
/terminal/:workspace?i=NSE:HDFCBANK&p=chart&iv=5m&exp=2026-09-24&k=1800&s=CE&v=chain
```

The URL is a **projection** of `selectionStore`, not the store itself (H.5). It carries workspace,
active instrument, focused panel, chart interval, expiry, strike, side and view. A refresh restores
workspace layout from `localStorage` and selection from the URL, so context survives reload, and a
link shares a meaningful view. URL writes are debounced (300 ms) and use `replaceState` for
selection changes so arrow-key navigation through Marketwatch does not create 200 history entries —
a real bug in URL-as-state designs.

---

## AH. Observability

### AH.1 Exists today

`ingestion_meta` (connect/disconnect/flush/shutdown events), `quality_log`, `feature_log`,
`signal_log`, `/health` with pipeline freshness, and systemd journals.

### AH.2 Required additions

| Signal | Why | Where |
| --- | --- | --- |
| `ingestion_status` (running/stopped/degraded) + uptime | BL-2 was invisible for three days | `/health` |
| **`kite_token_state`, `kite_token_age`, `last_auth_at`** | BL-1 / N-2 — the single most impactful blind spot | `/health` |
| `clock_skew_estimate_seconds` | N-4 — silent inversion of live/stale | `/health` |
| `disk_free_gb`, `sessions_of_headroom`, oldest partition per tier | N-1 — the system's most likely unattended death | `/health` |
| subscription count by mode + churn rate | E.2 capacity and eviction behaviour | `/health` + `ingestion_meta` |
| tick rate, processing lag (`receive_time − exchange_timestamp`) | detect degraded ingestion before users do | `/health` |
| persist lag (`persist_time − receive_time`) | ADR-2's latency floor | `/health` |
| dropped/late/out-of-order tick counters | E.8 correctness | `/health` |
| API latency histogram + error rate per endpoint | AG.1 enforcement; PE-1/PE-2 were found by hand | middleware |
| `deadline_exceeded` count per endpoint | N-17 | middleware |
| chart latency specifically | the top offender | middleware |
| stream connections current/peak, admission rejections | N-6 | `/health` |
| `sqlite_busy_retries`, WAL size | N-9 | `/health` |
| compaction and bar-build success + duration + row counts | RM-1 correctness | `nse-compact` → `ingestion_meta` |
| bar-coverage gaps per session | N-12 | `/health` |
| model run status, latency, missing inputs | requirement #26 | `/intelligence` + `/health` |
| **browser** stream state, reconnects, frames/s, dropped frames, queue depth, heap | AG.2 enforcement in the field | status bar + a debug overlay |

### AH.3 Alarms

CRITICAL: token invalid; ingestion stopped during market hours; disk headroom < 5 sessions; clock
skew > 30 s; API error rate > 5%; `data_status != LIVE` for > 5 min while `market_state == OPEN`.
WARNING: processing lag > 5 s; chart p95 over budget; bar build failed; instrument master stale
> 36 h; disk headroom < 10 sessions; stream rejections > 0.

The System Health screen (H.3) renders all of it, and the status bar surfaces CRITICAL states
without requiring the user to go looking. Today's three concurrent problems — invalid token, stopped
ingestion, crash-looping service — would all have been visible on day one with this in place.

---

## AI. Resource budgets

### AI.1 What 1 vCPU / 2 GB must carry

| Process | CPU (market hours) | RSS |
| --- | --- | --- |
| `nse-ingest` | 10–20% at ~300 updates/s (589 tokens — R-4) | 250–400 MB |
| `nse-api` (1 uvicorn worker) | 20–40% including the stream poll | 300–500 MB |
| `nse-live-signals` | 5–10% | 150–250 MB |
| `nse-account-capture` | < 1% (**currently ~100% in a crash loop** — N-2) | 100 MB |
| OS + SQLite page cache | 5% | 200–300 MB |
| **Total** | **40–75%** | **1.0–1.5 GB** |

After-hours: `nse-compact`, `nse-features`, `nse-labels` and the new bar build and retention jobs run
sequentially at 15:45–17:00 and may use the full core — no user-facing traffic competes.

### AI.2 Update volume, recomputed

589 tokens (not ~2 500 as previously assumed — R-4). Kite delivers roughly 1 update/s/token at
MODE_FULL and less at MODE_QUOTE, so ~300–600 updates/s peak. Each is a dict construction, a delta
computation and a dirty-set insert — the expensive part is the batched SQLite write every ≥2 s over
≤589 rows, which is a few milliseconds.

Stream cost is **independent of client count**, which is ADR-2's whole point: one `SELECT` per
second over ≤600 rows (< 5 ms), one diff pass, then N cheap serialised writes. At 16 connections
(N-6) with 250 instruments each, worst case is 4 000 frame-writes/s of small JSON — the measurable
risk, and the reason for the cap and the coalescing.

### AI.3 Storage budget (N-1)

| Tier | Per session | Retention | Total |
| --- | --- | --- | --- |
| T0 raw | **2.3 GB** (measured) | 7 sessions | ~16 GB |
| T1 compacted | **98 MB** (measured: 390 MB / 4) | 90 sessions | ~9 GB |
| T2 1m bars | ~5 MB (estimated: 440 k rows) | 400 sessions | ~2 GB |
| T3 daily bars | < 1 MB | forever | < 1 GB |
| SQLite | grows with `feature_log`/`quality_log`/`signal_log` (367 MB today) | pruned per N-1 | ~1 GB |
| **Total steady state** | | | **~29 GB of 48 GB** |

Without retention: 2.3 GB/session against 32 GB free = **disk full in ~14 sessions**. With it, steady
state fits with headroom. Raw retention is the dominant term and 7 sessions is the tunable knob; it
exists to allow re-compaction after a bug, not for long-term storage.

### AI.4 Deployment topology

```
                    ┌──────────── VM: 1 vCPU / 2 GB / 48 GB (blr1) ────────────┐
  Kite ──WS──────►  │ nse-ingest.service      ONE WebSocket, 589→~1330 tokens   │
  Kite ──REST────►  │ nse-account-capture     orders/positions/margins           │
                    │ nse-api.service         FastAPI 127.0.0.1:8080, 1 worker   │
                    │ nse-live-signals        model inference                    │
                    │ timers: compact 15:45 · features 16:00 · labels 16:30      │
                    │         bars (with compact) · retention 17:00 [NEW]        │
                    │ storage: SQLite (WAL) + Parquet T0/T1/T2/T3                │
                    └────────────────────────────┬─────────────────────────────┘
                                                 │ localhost only
                    ┌────────────────────────────▼─────────────────────────────┐
                    │ Next.js server: proxy /api/v1/** + streaming /stream      │
                    │ development: SSH tunnel · production: TLS on this host     │
                    └────────────────────────────┬─────────────────────────────┘
                                                 ▼  browser: 1 SSE + REST
```

**Exactly one market-data process.** The stream lives inside `nse-api`, not in a new service — a
second process would need its own SQLite reader, its own lifecycle and its own memory on a 2 GB box,
for no benefit.

### AI.5 When to scale, and why

Do not scale pre-emptively. Scale on evidence, in this order:

1. **After RM-1, RM-2, RM-3 and F.7 land.** Three of the four measured performance problems are
   missing read models, not missing CPU. Buying CPU first would hide the architectural defect and
   still leave a 32 s chart at 8 s.
2. **Trigger for 2 vCPU:** sustained CPU > 70% during market hours *after* the read models, or
   stream p95 lag > 3 s at 8 connections, or the compaction window overrunning into the next
   session.
3. **Trigger for 4 GB:** RSS > 1.7 GB sustained, or SQLite page-cache thrash visible as rising read
   latency.
4. **Trigger for a second API worker:** only after 2 vCPU, since two workers on one core add
   contention and double the memory. Note that two workers also double any in-process cache and each
   would run its own stream poll — so the shared-poll design (ADR-2) must move to a single dedicated
   poller before workers are added. **This is designed now** (the `LiveSource` interface, E.5) so
   scaling out does not require a redesign.

---

## AJ. Technical debt register

### AJ.1 Frontend

| # | Debt | Location | Disposition | Phase |
| --- | --- | --- | --- | --- |
| T1 | `useApiQuery` polling as the only data primitive | `hooks/useApiQuery.ts` | **delete** — replaced by stream + TanStack Query | 6 |
| T2 | Five live queries in the shell | `AppShell.tsx:20-24` | **delete** — the shell holds no queries | 6 |
| T3 | Duplicate fetching on the overview | `app/page.tsx` | **delete** with the page-centric model | 6 |
| T4 | No global state | repo-wide | **replace** — ADR-3 | 6 |
| T5 | Page-per-domain routing | 9 routes | **replace** — workspaces | 6 |
| T6 | `apiGet`'s ad-hoc 1.5 s TTL cache | `api/client.ts` | **replace** — ADR-4 | 6 |
| T7 | No virtualization | `FinancialTable`, `OptionChain` | **rebuild** — AG.3 | 7 |
| T8 | Column defs rebuilt per render | `FinancialTable.tsx` | **fix** in the rebuild | 7 |
| T9 | Series recreated per update | `CandlestickChart.tsx`, histogram wrappers | **rebuild** — L.2 | 8 |
| T10 | Chart disposal order bug | `chartTheme.ts::useChartHost` | **delete** — replaced by `ChartController` | 8 |
| T11 | Hardcoded IST offsets | `lib/candles.ts` | **fix** — use `formatTimestamp` / RM-5 | 8 |
| T12 | IV unit guessing (`iv <= 3`) | `OptionChain.tsx` | **fix** — ADR-13 | 9 |
| T13 | Browser-derived session state | `lib/freshness.ts:31-86` | **delete** — E.6 | 6 |
| T14 | Freshness from `envelope.as_of` | `Badges.tsx` | **fix** — D.5 groups | 6 |
| T15 | Hardcoded index instruments | `lib/instruments.ts` | **replace** — RM-4 | 7 |
| T16 | Search over watchlists only | `lib/search.ts` | **replace** — J; keep the scoring | 7 |
| T17 | Account JSON dumped via `JSON.stringify` | `settings/page.tsx` | **delete** — N-14 | 13 |
| T18 | No error boundaries | repo-wide | **add** — AE.2 | 6 |
| T19 | `next.config.ts` has no rewrites despite UI claims | `next.config.ts` | **fix** the claim; keep the proxy route | 6 |

**Preserve verbatim or nearly so:** `api/envelope.ts` (the maturity guard — the most valuable file
in the frontend), `lib/format.ts` (`formatTimestamp`'s IST handling is correct),
`api/types.ts` (extend, do not replace — it encodes real domain knowledge),
`lib/search.ts`'s scoring function, `components/data/PriceHeader.tsx` (it uses the *right*
timestamp), `DepthChart.tsx`'s honesty about unavailable per-level counts, `MaturityStrip.tsx`, and
`lib/candles.ts`'s no-synthesis rule (keep the rule, delete the timeframe filtering).

### AJ.2 Backend

| # | Debt | Location | Disposition | Phase |
| --- | --- | --- | --- | --- |
| B1 | No bar layer; charts aggregate ticks per request | `market/ohlc.py`, `duckdb_store` | **RM-1** | 3 |
| B2 | `/charts` reads Parquet twice | `market/assemble.py::assemble_charts` | **fix** with the F.4 split | 3 |
| B3 | `change` = `price_delta` | `market/latest.py:126` | **fix** — ADR-15 | 2 |
| B4 | Mixed `_pct` conventions | `_ratio` call sites | **fix** — ADR-13 | 2 |
| B5 | No per-level depth in live state | `sqlite_store.py:251-283` | **fix** — ADR-12 | 8 |
| B6 | Candle `volume` always null | `ohlc.py:176` | resolved by RM-1 | 3 |
| B7 | Algorithms hard-wired by class key | `signals/engine.py` | **fix** — T.1 registry | 11 |
| B8 | No monotonic guard in the SQL upsert | `sqlite_store.upsert_latest_quotes` | **add** — E.8 | 4 |
| B9 | No `busy_timeout`; API opens read-write | `sqlite_store.py` | **fix** — N-9 | 4 |
| B10 | No request deadline | `api/app.py` | **add** — N-17 | 3 |
| B11 | No response-size governance | `api/` | **add** — N-16 | 0 |
| B12 | Instrument cache refresh unsupervised **and schema-stale** | `broker/instruments.py` | **fix** — R-9, RM-4 | 0/7 |
| B13 | No retention | none | **add** — N-1 | 0 |
| B14 | Account data opaque | `sqlite_store`, `/account` | **fix** — W.3 | 13 |
| B15 | No holiday calendar | none | **add** — RM-5 | 2 |
| B16 | Per-request IV solving | `options_analytics.py` | **fix** — F.7 | 9 |
| B17 | Unusual last-session path scans Parquet | `read_policy`, `unusual.py` | **RM-3** | 10 |
| B18 | No sector mapping despite the data being on disk | `nse_membership.py` | **fix** — RM-4 | 7 |
| B19 | Only near+next futures | `settings.yaml:56` | **config** — `contract_count: 3` | 9 |
| B20 | Stock derivatives configured, never materialised | `settings.yaml:60`, cache | **fix** with the cache refresh | 7 |
| B21 | No settlement price | none | **source** — N-15 | 9 |
| B22 | Static option strike window | `settings.yaml:36-48` | **fix** — N-7 | 9 |
| B23 | No expiry-phase guards | `futures_analytics`, `unusual` | **add** — N-15 | 9 |

---

## AK. Backend gap audit

Consolidated, prioritised by the phase that needs them. "Gap" means the terminal cannot be built
correctly without it.

| Gap | Need | Phase |
| --- | --- | --- |
| **G-0a** | Valid Kite session + token lifecycle (N-2) | **0** |
| **G-0b** | Supervised `nse-ingest.service` (N-3) | **0** |
| **G-0c** | Retention job (N-1) | **0** |
| **G-0d** | Fixture capture for offline development (N-19) | **0** |
| **G-0e** | Response-size contracts (N-16), subsystem status contract (N-18), canonical type contracts | **0** |
| **G-0f** | Instrument cache refresh with a schema-version check (R-9) | **0** |
| **G-1** | RM-5 market calendar (ADR-14) | 2 |
| **G-2** | RM-2 session reference (ADR-15) | 2 |
| **G-3** | Canonical change fields + `_pct` normalisation (ADR-13/15) | 2 |
| **G-4** | Clock-skew estimate (N-4) | 1 |
| **G-5** | RM-1 bar store + `/candles` + `/series` + coverage (BL-3, N-12) | **3** |
| **G-6** | Request deadlines (N-17) | 3 |
| **G-7** | SSE `/stream` + heartbeats + resync (ADR-1) | **4** |
| **G-8** | `LiveSource` abstraction + shared poll (ADR-2) | 4 |
| **G-9** | Subscription manager + `POST /subscriptions` + capacity limits (ADR-9, N-8) | 4 |
| **G-10** | SQLite concurrency policy + upsert monotonic guard (N-9, E.8) | 4 |
| **G-11** | Stream admission control (N-6) | 4 |
| **G-12** | RM-4 instrument master + `/instruments` search + capabilities + durable identity (D.4, J, N-10) | **7** |
| **G-13** | `/quotes/bulk` (I) | 7 |
| **G-14** | Watchlist write endpoints | 7 |
| **G-15** | Sector + index membership from the CSVs (B18) | 7 |
| **G-16** | Stock-derivative materialisation (B20) | 7 |
| **G-17** | ADR-12 depth ladder: schema + ingestion + `/depth` + events (BL-5, M) | **8** |
| **G-18** | RM-6 print store + `/tape` (N) | 8 |
| **G-19** | Greeks from solved IV (O.2) | 9 |
| **G-20** | Options latency fix (F.7) | 9 |
| **G-21** | `atm_coverage` + dynamic strike window (N-7) | 9 |
| **G-22** | Settlement price + expiry phase + expiry guards (N-15) | 9 |
| **G-23** | Far-month futures (B19) | 9 |
| **G-24** | `/breadth` + India VIX + sector index subscription (Q.2) | 10 |
| **G-25** | RM-3 unusual rank (R) | 10 |
| **G-26** | Relative strength (Q.3) | 10 |
| **G-27** | `/intelligence` + algorithm registry + `/algorithms` (S, T) | **11** |
| **G-28** | `/screener/run` (U) | 12 |
| **G-29** | Alert persistence (V) | 12 |
| **G-30** | Typed account projections + `/account/**` (W, N-14) | 13 |
| **G-31** | Workspace sync endpoints (H.5) | 14 |
| **G-32** | Corporate actions + adjusted series (Y.1) | 14 |
| **G-33** | Server-side alert evaluation (V.1) | 15 |
| **G-34** | UDS `LiveSource` for sub-second latency (E.5) | 15 (conditional) |

---

## AL. Migration strategy

### AL.1 Final decision (ADR-16)

# **HYBRID: keep and extend the backend. Rebuild the frontend shell, routing, state and transport. Salvage the frontend domain layer.**

### What is preserved

**Backend — kept essentially whole.** Ingestion, normalization, the monotonic latest-quote tracker,
raw and compacted storage, compaction, features, labels, the model pipeline, the maturity gate, the
research separation, `MarketDataPolicy`, `data_state`, `basis_freshness`, the options and futures
analytics, the unusual-activity scoring, and `signal_log`'s reproducibility. Nothing here is
rewritten. The work is **additive**: 4 read models now (RM-1, RM-2, RM-4, RM-5) and 2 later
(RM-3, RM-6), one SSE endpoint, one subscription manager, one schema extension, two contract
corrections, and the operational gaps (token, ingestion, retention).

**Frontend — roughly 40% preserved:** `api/types.ts`, `api/envelope.ts` (verbatim),
`lib/format.ts`, `lib/search.ts`'s scoring, `lib/instruments.ts`'s normalization helpers (generalized
against RM-4), `PriceHeader`, `MaturityStrip`, `Badges` (extended), `DepthChart`'s honesty pattern,
and the no-fabrication rules in `lib/candles.ts`.

### What is rebuilt

Shell, routing model, state model, transport, panel composition, tables, charts, and every page
component — roughly 60% of `src/`.

### Why not REFACTOR

An incremental path would have to change, in one coordinated step: the transport (poll → stream),
the state model (component-local → normalized cache with per-instrument subscriptions), the render
model (unmemoized → subscription-based), the routing model (page-per-domain → panels in workspaces),
and the freshness model (browser clock → market timestamps from backend groups). Every existing
component depends on at least three of those. Concretely: `AppShell` holds five live queries and
wraps `{children}` at `layout.tsx:30`, so it cannot be de-stated without rewriting it, and rewriting
it invalidates all nine pages beneath it. That is a rebuild with extra steps and a longer period of
instability.

### Why not REBUILD BACKEND

Nothing in the audit justifies it. Every measured latency except two is fine, and both exceptions
have identified, localized, *architectural* causes — a missing bar layer and per-request IV solving —
not systemic ones. The data model, the fallback policy, the maturity discipline, the provenance
naming (`aggressive_*_proxy`), the null-rather-than-wrong conventions (`basis_freshness`,
`chain_status`, `candles_status`) and the research separation are all things you would want to
arrive at, not things to escape. The three worst problems in production right now — an invalid
token, a missing systemd unit, and no retention policy — are operational, and a rewrite would not
have prevented any of them.

### Migration mechanics — strangler, inside the existing repos

1. **Add `/terminal` as a new route group** in the existing dashboard repo with the new shell,
   stores and transport. The nine existing pages keep working untouched throughout.
2. **Port one panel at a time.** A ported panel reads only from `marketCache` and `data/` hooks; it
   may not call `apiGet`. Lint enforces this (G.1).
3. **Backend changes ship independently and additively.** New endpoints are added; only ADR-13 and
   ADR-15 change existing contracts, and each ships with both field forms for one release (AB.4).
4. **Cutover** when the AN suites pass on `/terminal`: `/terminal` becomes the root, old routes
   become redirects for one release, then are deleted.
5. **Freeze the legacy pages** — no new features outside `/terminal`, so they cannot rot in a way
   that costs anything.

### Risk, cost, benefit

| | |
| --- | --- |
| **Risk** | Highest risk is that Phase 0 stalls on the broker session (BL-1), because everything real-time is unverifiable until it clears. Mitigated by N-19 fixtures, which unblock all frontend phases offline. Second risk is that 1 vCPU cannot carry the stream plus REST; mitigated by measuring after Phase 4 and by the AI.5 scaling triggers. |
| **Cost** | Backend: additive, well-bounded, and three items (token, unit, retention) are operational rather than development work. Frontend: a genuine rebuild of the shell and panels, executed incrementally with a working app at every step. |
| **Benefit** | Requirement #1 becomes achievable (it is not today, at any effort, without a transport). Chart latency drops by ~2 orders of magnitude. The first number a trader reads becomes correct. Depth, tape, search and instrument coverage become possible at all. And the terminal stops being a set of pages that each refetch the market. |

### The three things that matter most, in order

1. **Re-establish the Kite session and make token lifecycle observable.** The token is invalid right
   now, the account service is crash-looping on it, and ingestion cannot start without it. Every
   real-time requirement is blocked behind this one credential. *(Changed from the previous pass,
   which had "start the ingest service" first — that would have failed on first contact.)*
2. **Build the bar store.** 32 seconds against a 15-second client timeout means the chart can never
   work, however good the chart component is. It is not a tuning problem; the artefact does not
   exist.
3. **Fix what `change` means.** It is a tick delta presented as a day change, on every surface, and
   it must be corrected before a Marketwatch is built on top of it.

---

## AM. Testing strategy

### AM.1 Backend

| Layer | Tests |
| --- | --- |
| Unit | `data_state.resolve` across all live/historical/clock permutations (exists); `market_state` with holidays (new, RM-5); `resolve_reference` per instrument class; unit normalisation (ADR-13); bar rollup bucketing on session boundaries; gap detection; delta guards on gaps; capability derivation from `(instrument_type, mode)` |
| Contract | every endpoint against a JSON schema; **every `*_pct` field within a plausible percentage range**; `change_absolute == last_price − reference_price`; no `null` where a typed reason is required; response size within its N-16 budget; **no field anywhere can carry a trade identifier or participant identity** |
| Fallback | the existing suite (live / last_session / no_data, `latest_quotes` untouched, historical `as_of` preserved, coherent single-session option chains, maturity unchanged) — **kept and extended** with market state, quality, coverage and per-group freshness |
| Read model | RM-1 bars reproduce a known session from T1; RM-2 references match a known close; RM-3 preserves the pre-existing ranking order; RM-4 diffing detects new/expired contracts; RM-5 answers previous-trading-day across a holiday |
| Performance | every AG.1 budget asserted against the VM using real data, in CI where possible and as a scripted probe otherwise |
| Concurrency | the N-9 soak: ingestion writing + 16 stream readers + the signal engine, zero `SQLITE_BUSY` |
| Stream | frame ordering, dedupe, seq gaps, resync, heartbeats, admission control, subscription reference counting |
| Safety | maturity suppression cannot be bypassed by any code path (property test); the retention job never touches `signal_log`; no write occurs on any read path |

### AM.2 Frontend

Fixture-driven (N-19), so the whole suite runs with no VM and no network.

| Layer | Tests |
| --- | --- |
| Unit | canonical adapters; capability resolution across all five states; freshness-group classification; unit formatting with `InstrumentMeta`; `MarketCache.applyFrame` seq/dupe guards; `UpdateQueue` coalescing and backpressure; workspace migrations |
| Component | every panel × every state (loading, live, delayed, stale, last-session, partial, unavailable, not-applicable, not-mature, error) |
| **Render count** | **a single-token frame increments exactly one Marketwatch row's counter and no chart, chain or unrelated panel** |
| Integration | select instrument → all LINKED panels update, pinned panels do not; expiry change re-renders only the chain; reconnect → resync → re-hydrate; session change clears cumulative state |
| Memory | 6 h simulated session under a synthetic frame generator; heap growth within AG.2; subscription count returns to baseline after closing panels |
| Accessibility & keyboard | every workflow completable by keyboard; focus order; `prefers-reduced-motion` respected by the flash animations |
| Visual | density snapshots at 1280/1440/1920 |

### AM.3 End-to-end

The AN.1 real-time scenario executed against a live market open, plus a **recorded-session replay
harness**: feed captured ticks through the ingest path at accelerated speed into a throwaway
database, then drive the terminal against it. This is what makes expiry day, ATM drift, partial
sessions, reconnects and stale transitions testable without waiting for the calendar — and each of
those is a state the system currently gets wrong.

---

## AN. Acceptance criteria

### AN.1 Real-time acceptance test (requirement #55) — the definitive suite

Preconditions: valid Kite token; `nse-ingest` running and supervised; RM-1/RM-2/RM-4/RM-5 built; the
stream deployed; the terminal open in a browser before market open, and **not touched thereafter**.

| # | Criterion | Pass measurement |
| --- | --- | --- |
| 1 | Terminal opens pre-market and shows `PRE_OPEN` | badge reads PRE-OPEN with the open time from RM-5 |
| 2 | Session transitions to `OPEN` without user action | `MarketState` flips within 5 s of 09:15 |
| 3 | Marketwatch updates automatically | every visible row's `exchange_timestamp` advances within 5 s; **zero** user actions |
| 4 | Indices update | both index tickers advance within 5 s |
| 5 | A specific instrument (HDFCBANK) updates | LTP, change, change% advance; freshness < 5 s |
| 6 | **Depth updates while LTP is unchanged** | over a 60 s window with a static LTP, ≥1 ladder change is rendered; the price cell's render count does not increase |
| 7 | Volume updates | `volume` monotonically increases; `volume_delta` non-null |
| 8 | OI updates | derivative OI changes are rendered; `oi` freshness < 60 s |
| 9 | Futures update | LTP, OI, basis advance; `basis_status` honest |
| 10 | Options update | chain contracts advance **without a full-chain refetch** — 0 `/options` requests in 60 s of ticking |
| 11 | Chart updates | the forming bar advances via `series.update()`; 0 `setData` calls after load |
| 12 | Bar rollover | at the interval boundary the forming bar closes and a new one opens; no duplicate or missing bar |
| 13 | Time & Sales updates | rows append in timestamp order; the ring stays at its bound |
| 14 | Activity updates | activity metrics refresh on the sample cadence |
| 15 | Unusual activity appears | events arrive without a refresh, each with `reasons[]` and provenance |
| 16 | Intelligence inputs update | `input_freshness` advances; `state` is accurate |
| 17 | **No page refresh required** | 0 reloads over the session |
| 18 | **No symbol reopen required** | 0 re-selections |
| 19 | **No duplicate request storm** | < 10 HTTP req/min at idle (from ~113); exactly 1 stream connection **across 4 open tabs** (N-5) |
| 20 | **Exactly one Kite WebSocket** | `ingestion_meta` shows one connection; `ss -tp` shows one upstream socket |
| 21 | Browser stays responsive | frame time < 16 ms p95 over the session; no long task > 200 ms |
| 22 | Disconnect/reconnect recovers | kill the stream: `RECOVERING` within 2 s, values retained and marked, full recovery < 5 s, no duplicate or lost frames after resync |
| 23 | Stale state is visible | stop ingestion: `STALE` within 120 s with the correct last observation time |
| 24 | Historical fallback is honest | after close: `LAST SESSION` with the real session date; nothing labelled live |
| 25 | **Canonical change is consistent everywhere** | Marketwatch, ticker, header, chain, futures and search all show the identical `change_percent` for the same instrument at the same `as_of` |
| 26 | Laptop sleep/wake recovers | close the lid 10 min: on wake, resync + re-hydrate, no backlog replay, heap unchanged |
| 27 | Backend restart recovers | restart `nse-api`: the client reconnects and re-hydrates without a reload |
| 28 | Fully keyboard operable | every AN.1 observation reachable without a mouse |
| 29 | Clock integrity holds | `clock_skew_estimate` < 1 s; no spurious stale states |
| 30 | 6-hour stability | heap < 350 MB with no monotonic growth; 1 connection; no duplicate timers; subscription count stable |

### AN.2 Correctness acceptance

| Criterion | Measurement |
| --- | --- |
| Day change is a day change | `change_absolute == last_price − reference_price` for every instrument in the corpus; `reference_type` present |
| No unit ambiguity | every `*_pct` is a percentage; no endpoint emits both forms post-migration |
| Freshness from market time | no presentation component references `envelope.as_of` (lint) |
| Session state has one source | no frontend module computes session hours (lint) |
| Holiday handling | on a seeded holiday, `market_state == HOLIDAY` and `previous_trading_day` skips it |
| Gaps render as gaps | a synthetic 60-min hole produces a discontinuity and `completeness ≈ 0.84` |
| No fabrication | no Greek without a real IV; no probability below 60 pooled days; no trade ID; no participant identity; no synthesised candle — each a property test |
| `NOT_APPLICABLE ≠ UNAVAILABLE` | an index renders no volume row; an unsubscribed stock option renders "not subscribed" |
| Coherence enforced | bid and ask always same-observation; basis null beyond 10 s |
| Expiry safety | a replayed expiry day emits no OI-burst or buildup on the expiring series |

### AN.3 Performance acceptance

Every budget in AG.1 and AG.2, measured on the VM and in the browser. Headline targets:
`/candles` p95 < 900 ms cold (from 32 030 ms); `/options/{u}/{e}` p95 < 250 ms (from 1 290 ms);
`/unusual-activity` p95 < 300 ms; idle requests < 10/min (from ~113/min); 250 Marketwatch rows at
60 fps; 6 h heap < 350 MB.

### AN.4 Operational acceptance

Token state visible and no crash loops; ingestion supervised and auto-restarting; disk headroom
> 10 sessions with retention active; every AH.3 alarm firing correctly in a fault injection test;
bar build succeeding daily with coverage reported.

---

## AO. Exact implementation phases

The order below follows the mandated dependency logic. Three deviations are marked **[DEVIATION]**
and each is justified by repository or VM evidence, as required.

### PHASE 0 — Unblock, contract, and stop the bleeding

| Item | Gap | Deliverable |
| --- | --- | --- |
| **Re-establish the Kite session; token lifecycle + backoff + `/health` state** | G-0a / N-2 | no service crash-loops on `TokenException`; token state visible |
| **Supervised `nse-ingest.service`** | G-0b / N-3 | installed, enabled, restart-on-failure, `warm_start` events |
| **Retention job + `retention:` config + `nse-retention.timer`** | G-0c / N-1 | disk headroom reported; raw bounded to 7 sessions |
| **Fixture capture + frontend mock mode** | G-0d / N-19 | `npm test` and local dev run with no VM |
| Contracts written and reviewed | G-0e | `CanonicalQuote`, `ReferenceType`, `Provenance`, `DataState`, `SessionState`, `Quality`, `Coverage`, `InstrumentCapabilities`, `GroupFreshness`, `Coherence`, `subsystems`, `AlgorithmOutput`, `AlgorithmDescriptor`, `DurableInstrumentKey`, `OrderRequest`, response-size budgets |
| Instrument-cache refresh with a schema-version check | G-0f / R-9 | the stale, schema-stale cache can no longer go unnoticed |
| NTP verification in `deploy.sh` | N-4 | deploy fails if time sync is inactive |

**[DEVIATION 1]** The token, the ingest unit and retention were suggested for Phase 1. They are
pulled into Phase 0 because: the token is invalid *right now* and its crash loop is actively
consuming CPU that the API needs (A.2); no real-time work in any later phase is verifiable until
ingestion runs; and retention has a hard ~14-session deadline against 32 GB of free disk (R-5). All
three are operational tasks measured in hours, not development phases.

**[DEVIATION 2]** Fixture capture is added to Phase 0. It is not in the suggested order, but it is
what allows Phases 6–15 to proceed in parallel with (and independently of) the broker-session
resolution. Without it, a single expired credential blocks the entire frontend programme.

**Exit:** ingestion supervised and visible; no crash loops; disk bounded; contracts frozen;
frontend testable offline.

### PHASE 1 — Live-data correctness and observability

Clock-skew estimation (G-4); processing/persist lag, tick rate, dropped/late counters; subscription
counts; `subsystems` populated for existing domains (N-18); API latency histograms; the AH.3 alarm
set; the System Health screen fed by the extended `/health`.

**Exit:** with ingestion live, `data_status: LIVE` is observable and every AH.2 signal is reported.
The three problems found by hand in this audit would now be visible automatically.

### PHASE 2 — Canonical market semantics

RM-5 market calendar (G-1); RM-2 session reference (G-2); `resolve_reference` per instrument class;
`reference_price` / `reference_type` / `change_absolute` / `change_percent` emitted alongside the
legacy fields; `_pct` normalisation with both forms for one release (G-3); provenance attached to
canonical fields (N-11 data model); full `SessionState` including holidays and pre-open.

**Exit:** day change is correct and identical across every endpoint; contract tests assert units and
the change identity; `previous_trading_day` is correct across a holiday.

### PHASE 3 — Historical read models and chart performance

RM-1 1m + daily bars built at compaction; a backfill for the four existing sessions; `/candles` and
`/series` split with `coverage` and gaps (G-5, N-12); request deadlines (G-6, N-17); the
`assemble_charts` double read removed.

**Exit:** `/candles` p95 < 900 ms cold and < 400 ms warm on the VM — from 32.03 s. Candle volume is
populated. Gaps are reported.

### PHASE 4 — Backend live read and stream layer

`LiveSource` abstraction + the shared 1 s poll (G-8); SSE `/stream` with heartbeats, `seq`, resync
and `Last-Event-ID` (G-7); the streaming proxy route in Next.js; subscription manager +
`POST /subscriptions` + capacity limits (G-9, N-8); admission control (G-11, N-6); SQLite
concurrency policy + the upsert monotonic guard (G-10, N-9).

**Exit:** `curl` on `/stream` shows coalesced frames at 1 Hz with heartbeats; 16 connections hold
without latency regression; the 17th is refused cleanly; the N-9 soak passes.

### PHASE 5 — End-to-end real-time verification (backend only)

Before any UI work: a scripted consumer subscribes 250 instruments, records inter-frame latency,
kills the connection, verifies resync, restarts ingestion mid-session and verifies `warm_start`
suppression and `PARTIAL_SESSION`. Plus the replay harness (AM.3).

**Exit:** measured end-to-end latency published; recovery verified; the stream contract is stable
enough to build on. **This phase exists to avoid discovering a transport flaw after eight UI phases
depend on it.**

### PHASE 6 — Frontend terminal shell, stores and transport

`TerminalShell`, `TopBar`, `StatusBar`, `PanelHost`, panel registry; the four stores (ADR-3);
`MarketCache` + subscriber registry; `StreamClient` + `SharedWorker` bridge + leader-election
fallback (N-5); `UpdateQueue` with coalescing and backpressure; TanStack Query setup (ADR-4);
`/terminal` route with deep-link parsing (AG.7); error boundaries (AE.2); deletion of T1, T2, T3,
T13, T14.

**Exit:** the shell renders with **zero market fetches of its own**; the stream connects, reconnects
and reports every connection state; `data_state` and per-group freshness are visible in the status
bar; the render-count test passes; 4 tabs share 1 connection.

### PHASE 7 — Marketwatch and search

RM-4 instrument master with capabilities, sector, index membership, durable identity and search
(G-12, G-15, N-10); `/quotes/bulk` (G-13); watchlist writes (G-14); stock-derivative
materialisation (G-16); tick/lot-aware rendering (N-13); virtualized Marketwatch with presets,
keyboard navigation and context menus; universal search and the command palette.

**Exit:** 250 rows at 60 fps; viewport-scoped subscriptions; `HDFCBANK 1800 CE` is findable; one
canonical change everywhere; idle requests < 10/min.

### PHASE 8 — Instrument workspace: chart, depth, Time & Sales

ADR-12 depth ladder — schema, ingestion, `/depth`, depth events (G-17); RM-6 print store and
`/tape` (G-18); `ChartController` with `series.update()` and the forming bar (L); the depth ladder
panel; virtualized Time & Sales; the linked-context resolver (K).

**Exit:** the AN.1 #6 test passes — depth updates with LTP unchanged and the price cell does not
re-render. Charts load under budget and update without `setData`. No lifecycle errors on symbol
switch or unmount.

### PHASE 9 — Options and futures

Greeks from solved IV (G-19); the options latency fix (G-20, F.7); `atm_coverage` + dynamic strike
window (G-21, N-7); settlement price, `expiry_phase` and the expiry guards (G-22, N-15); far-month
futures (G-23); virtualized chain with per-contract streaming; futures panel with basis freshness
and INFERRED buildup labels.

**Exit:** the chain updates without refetching; no Greek exists without a real IV; a 3% index move
is disclosed; a replayed expiry day produces no false OI events.

### PHASE 10 — Activity, unusual activity and market context

RM-3 unusual rank (G-25); `/breadth` + India VIX + sector index subscription (G-24); relative
strength (G-26); the activity panel restructured by provenance with its 60 s resolution disclosed;
the unusual feed with WHAT/WHY/STRENGTH/PROVENANCE; market context with per-tile availability.

**Exit:** `/unusual-activity` p95 < 300 ms; market context degrades per tile; 52-week tiles honestly
report insufficient history.

### PHASE 11 — Trade Intelligence and the algorithm registry

`/intelligence` (G-27); the algorithm registry replacing the hard-wired engine keys (B7, T.1);
`/algorithms`; generic `AlgorithmOutput` rendering; model observability; reasons, contradictions,
evidence, invalidation and levels; the maturity strip.

**Exit:** a synthetic algorithm registered in the backend appears in the UI with **no frontend
commit**; no probability below 60 pooled days by any path; every non-`UNCLEAR` bias carries an
invalidation condition.

### PHASE 12 — Screener, alerts and the notification centre

`/screener/run` with `unavailable_filters` (G-28); saved screens; client-side alert evaluation with
cooldowns, trigger modes and session gating (G-29, V); non-evictable alert subscriptions; the event
centre with severity and dedupe.

**Exit:** 500 × 10 filters under 2 s; alerts fire without a refresh and never fire on last-session
prices; an unevaluable filter is reported, never silently dropped.

### PHASE 13 — Portfolio, orders, positions and risk

Typed account projections and `/account/**` as a separate domain (G-30, N-14); canonical order
lifecycle mapping (W.1); the full P&L decomposition with mark provenance (W.2); positions, holdings,
orders, trades, funds and margin panels; live position marking; the risk panel; the order ticket
behind `DisabledOrderGateway` (W.5); T17 deleted.

**Exit:** no raw broker JSON reaches the browser; killing account capture leaves every market panel
working; the order ticket validates tick and lot size and then refuses with `EXECUTION_DISABLED`.

### PHASE 14 — Persistence, keyboard, notes and events

Workspace persistence with versioned migrations and backups (H.5); workspace sync endpoints (G-31);
the full keyboard model and command set (H.6); instrument notes and the trade journal with
`intelligence_snapshot_ref` (Z); corporate actions and adjusted series (G-32, Y.1); the event
calendar scaffolding.

**Exit:** a full reload restores the workspace exactly; the terminal is fully operable without a
mouse; a saved watchlist survives a simulated expiry rollover; a chart crossing an ex-date uses the
adjusted series.

### PHASE 15 — Hardening and production validation

Memory budgets verified by heap snapshots over a 6 h session; every AG budget enforced in CI; the
full AN.1 suite executed on a live market open; server-side alert evaluation (G-33); the UDS
`LiveSource` **only if** Phase 5's measured latency proves insufficient (G-34); the runtime
provenance inspector (N-11 UI); export capabilities (chart CSV, activity, trades, watchlists,
screener results — all client-side from data already loaded, and therefore genuinely a Phase 15
item rather than an architectural one).

**[DEVIATION 3]** The UDS live source is conditional rather than scheduled. Building it before
measuring would be optimising a latency we have not observed to be inadequate; the interface is in
place from Phase 4 so the decision stays cheap either way.

**Exit:** all 30 real-time criteria pass on a real session.

### Dependency graph

```
PHASE 0  token ─┬─► PHASE 1 observability ──┐
         ingest ┤                            │
      retention┤                            │
       fixtures┼──────────────────────► PHASE 6 shell/stores/transport (unblocked by fixtures)
      contracts┘                            │        ▲
                                            │        │
         PHASE 2 canonical (RM-2, RM-5) ────┼────────┤
                    │                       │        │
         PHASE 3 RM-1 bars ─────────────────┤        │
                    │                       │        │
         PHASE 4 stream ─────────────────────┤        │
                    │                       │        │
         PHASE 5 backend real-time verify ───┴────────┘
                                                     │
                              PHASE 7 MW + search ◄──┤ needs RM-4, /quotes/bulk
                                        │            │
                              PHASE 8 workspace ◄────┘ needs RM-1, depth ladder, tape
                                        │
                       ┌────────────────┼────────────────┐
                 PHASE 9 opt/fut   PHASE 10 activity   PHASE 13 portfolio (independent)
                       └────────────────┼────────────────┘
                                  PHASE 11 intelligence
                                        │
                                  PHASE 12 screener/alerts
                                        │
                                  PHASE 14 persistence/notes
                                        │
                                  PHASE 15 hardening
```

**Hard orderings, each with its evidence:**

- **Phase 0 token before anything real-time.** `TokenException` at 13:41:41 today; no live data
  otherwise.
- **Phase 2 before Phase 7.** Building a Marketwatch on a tick-delta `change` means rebuilding it
  (BL-4).
- **Phase 3 before Phase 8.** A 32 s chart makes the instrument workspace untestable (BL-3).
- **Phase 4 before Phase 7.** Marketwatch is the first live surface; retrofitting a transport under
  a built panel means rewriting its subscription model.
- **Phase 5 before Phase 6.** Verify the transport before eight phases depend on it.
- **Phase 7 before Phase 14.** Persistence must store durable identity, which needs RM-4 (N-10), or
  every expiry wipes saved state.
- **Phases 8–10 before Phase 11.** Intelligence consumes depth, tape, options, futures, activity and
  context as inputs.
- **Phase 13 is independent of 9–12** and can be parallelised if capacity allows.

---

## Appendix 1 — Professional-trader workflow completeness check

Measured against a full trading workflow rather than against "dashboard viewing".

| Stage | Required UI | Required data | Required backend | Available today | Missing | Phase |
| --- | --- | --- | --- | --- | --- | --- |
| **DISCOVER** | universal search; screener; unusual-activity feed; breadth; sector performance | instrument master; bulk quotes; anomaly scores; breadth | RM-4, `/instruments`, `/screener`, `/unusual-activity`, `/breadth` | unusual activity ✅; search ❌ (watchlists only); screener ❌; breadth ❌ | search, screener, breadth, sector | 7, 10, 12 |
| **SELECT** | search → active instrument; Marketwatch selection; command palette; deep links | instrument identity + capabilities | RM-4, durable identity | route-param navigation only | shared instrument context, capabilities, durable identity | 6, 7 |
| **OBSERVE** | snapshot; chart; **depth**; **Time & Sales**; volume; OI; live updates | live observed data, per-group freshness | stream, ADR-12 ladder, RM-6, RM-1 | quote ✅ (polled); chart ❌ (32 s); depth ❌ **not stored**; tape ❌ | continuous updates, depth, tape, usable charts | 3, 4, 8 |
| **ANALYSE** | option chain with Greeks; futures basis and buildup; activity vs baseline; VWAP; OI analytics; IV | derived analytics with declared tolerances | options/futures analytics, Greeks, F.7 | chain ✅ (slow, no Greeks); futures ✅; activity ✅; IV ✅ | Greeks, IV surface, ATM coverage, latency | 9 |
| **COMPARE** | multi-instrument view; relative strength; sector vs stock; future vs spot; peer comparison; multi-chart workspace | multiple instruments simultaneously; aligned bars | RM-1, `/cross-market`, sector mapping | `/cross-market` descriptive only; no multi-panel | relative strength, sector mapping, multi-chart workspaces | 8, 10 |
| **FORM A TRADE IDEA** | Trade Intelligence with bias, reasons, contradictions, evidence, invalidation, levels; notes | model output + all inputs with provenance | `/intelligence`, algorithm registry, `signal_log` | `/signals` + `/decisions` raw; no synthesis surface | the intelligence panel, contradictions, invalidation, levels | 11 |
| **VALIDATE** | contradiction display; data-quality indicators; maturity state; multi-timeframe confirmation; **the provenance inspector** | freshness, coherence, maturity, provenance | D.5, D.9, maturity, N-11 | maturity ✅ (well done); freshness ❌ (wrong timestamp); coherence ❌ | honest freshness, coherence, inspector | 2, 6, 15 |
| **ENTER** | order ticket with product, validity, qty in lots, price, trigger, margin and charge estimates; basket | reference constraints (tick, lot, freeze, band); margin | RM-4 constraints, `/margins`, order gateway | nothing; no reference constraints exist | the whole entry path, disabled by design | 13 |
| **MONITOR** | positions with live P&L decomposition; orders with canonical states; alerts; risk; notifications | live marks + account data | `/account/**`, alerts, stream | account data opaque and stale; capture crash-looping | typed account, live marking, alerts, risk | 12, 13 |
| **EXIT** | position-level exit action; SL/target management; P&L impact preview | live marks, order gateway | order gateway | nothing | designed, disabled | 13 |
| **REVIEW** | trade journal with entry thesis and the intelligence snapshot; realised P&L; outcome vs expectation; algorithm attribution | journal + `signal_log` reproducibility | Z, `signal_log` (already stores features and attribution) | `signal_log` ✅ (excellent) — no UI | journal UI, snapshot references, outcome tracking | 14 |

**Conclusion.** Of eleven stages, the current system meaningfully serves **two** (ANALYSE, partially;
and the model-audit half of REVIEW). OBSERVE — the stage a terminal exists for — is the weakest,
because depth is not stored, the tape does not exist, charts do not load and nothing updates
continuously. The plan's phase order matches this: OBSERVE is fixed first (Phases 3, 4, 8), then
DISCOVER and SELECT (6, 7), then ANALYSE and COMPARE (9, 10), then the idea → monitor → review loop
(11, 12, 13, 14). The system is currently optimised for dashboard viewing, and the phase order is
the correction.

---

## Appendix 2 — Final self-critique

*Reviewed as a senior engineer inheriting this project tomorrow, asking: what requirement would
force another major redesign six months from now, and what boundary must therefore exist today?*

| # | Redesign risk | Why it would force a rebuild | Boundary designed **now** | Built now? |
| --- | --- | --- | --- | --- |
| 1 | **Sub-second latency is demanded** | If the live path is hard-wired to a SQLite poll, moving to push means rewriting the stream, the frame model and possibly the store | `LiveSource.subscribe() -> AsyncIterator[Frame]` with `SqlitePollSource` today and `UdsSource` later; the frame contract is source-agnostic (E.5) | interface yes, UDS no |
| 2 | **Multi-user / auth arrives** | Per-user subscriptions, per-user watchlists, per-user alerts and account isolation touch everything | subscriptions are already reference-counted per *requester*; account is already a separate domain (N-14); watchlists/alerts/workspaces are already keyed to a user-scoped store with a backend sync interface (H.5) | boundaries yes, auth no |
| 3 | **Order execution is enabled** | Retrofitting validation, product types, margins and an auth boundary into a read-only system is a rebuild | `OrderRequest` + `OrderGateway` + `DisabledOrderGateway` + the canonical order lifecycle + a separate security domain, all specified (W.1, W.5, AF) | contracts yes, execution no |
| 4 | **A new algorithm class arrives** (e.g. a sequence model with per-bar outputs) | If the UI knows algorithm shapes, every new algorithm is a frontend release | registry + descriptors + display hints + generic `AlgorithmOutput` + a generic fallback card (T) | yes |
| 5 | **Corporate actions break history** | Adjusted vs raw is not retrofittable — every stored series and every percentage becomes suspect | three explicit series, `price_basis` on every response, an `adjustment_factor` model (Y.1) | contract yes, data no |
| 6 | **Instrument identity changes** (rollover, rename, token reuse) | Persisted state keyed on tokens dies at every expiry | durable vs transient identity, with a resolver and an alias table (N-10) | yes, before persistence ships |
| 7 | **More instruments** (full F&O, 2 500+ tokens) | A design assuming ~600 rows fits in one query and one frame breaks | bulk caps (250), viewport-scoped subscriptions, LRU eviction, capacity governance (N-8), virtualization everywhere | yes |
| 8 | **A second data source** (another broker, an index feed) | Ingestion coupled to Kite's tick shape means a parallel pipeline | `NormalizedTick` already abstracts the broker; `LiveSource` abstracts the transport; provenance carries `source` | partially — `NormalizedTick` exists, a second adapter does not |
| 9 | **Field-level freshness is demanded per field, not per group** | Group-level freshness baked into the wire format would need a schema change | groups are a map keyed by group name, extensible to finer keys without a breaking change (D.5) | yes |
| 10 | **The chart needs 5 years of history** | Aggregating from 1m bars across 5 years is 500 k+ bars | tiered T2/T3 with per-interval authoritative sources; daily is a separate materialised tier (D.10) | yes |
| 11 | **Server-side alerts / notifications while the browser is closed** | Client-side evaluation baked into components means rewriting the rule engine | the alert rule schema is storage- and evaluator-agnostic; the evaluator is a separate module (V.1) | schema yes, server evaluator no |
| 12 | **Panels must be shareable or embeddable** | Panels coupled to the shell cannot be mounted standalone | panels receive their instrument via `instrumentBinding` and read only from stores; they never own transport (G.1, H.1) | yes |
| 13 | **Two API workers become necessary** | Two workers each running a stream poll would double DB load and split in-process caches | the shared-poll design is explicitly documented as single-poller, with a dedicated poller as the scaling step (AI.5) | designed, flagged |
| 14 | **Regulatory or disclosure requirements on model output** | Retrofitting audit trails onto model displays is painful | `signal_log` already stores features, attribution, version and maturity tier; provenance and the inspector expose them (N-20, N-11) | yes |
| 15 | **Depth becomes 20-level** | Fixed 5-level columns would need a migration | `bid_levels`/`ask_levels` are JSON arrays of arbitrary length, and `bid_depth_5` is named for what it is (ADR-12) | yes |

### What this plan deliberately does **not** build, and why that is safe

Server-side alerting, order execution, IV surfaces, scenario analysis, corporate-action data,
economic-event data, a second broker adapter, multi-user auth, and the UDS live source. Each is
either genuinely not needed yet or blocked on external data. In every case the **contract and the
boundary exist**, so building it later is additive work behind an interface rather than a redesign.

### The three honest weaknesses of this plan

1. **Latency is 1–3 s, not sub-second, at Phase 15** unless the UDS path is built. That is stated
   plainly in E.5 rather than glossed, and the status bar shows the real `lag_ms` rather than
   implying "real-time". A scalping workflow would need item 1 of the risk table.
2. **Several features will launch as `UNAVAILABLE (insufficient history)`** — IV percentile, 52-week
   levels, beta, RSI and breakout screens — because only four sessions of data exist and ingestion
   has been down since 2026-09-04. The plan makes this honest rather than hiding it, but a user
   will see genuinely empty capability for weeks. The alternative — computing a 52-week high from
   four sessions — is worse.
3. **`HALTED` market state and settlement prices have no confirmed source.** Both are marked
   UNVERIFIED with a sourcing task rather than assumed into existence. If neither is sourceable,
   `HALTED` stays manual-only and futures day change keeps reporting
   `reference_type: OFFICIAL_CLOSE` — technically imperfect and, critically, *labelled* as such.

---

## Appendix 3 — FINAL GAP CHECK

Every mandated requirement, its status in this plan, where it lives, what it depends on, and the
phase that implements it.

**Legend:** COMPLETE = fully specified and implementable · PARTIAL = specified, with a named
external dependency or a deliberate deferral · MISSING = not specified (there are none).

| Requirement | Status | Location in plan | Dependency | Phase |
| --- | --- | --- | --- | --- |
| #1 True continuous live market data | COMPLETE | E.1, E.4, E.5, AN.1 | valid token (N-2) | 4 |
| #2 Real-time transport decision | COMPLETE | ADR-1, E.4 | — | 4 |
| #3 Real-time live store | COMPLETE | ADR-3, G.3, G.4 | — | 6 |
| #4 Session state machine | COMPLETE | D.7, E.6, ADR-14 | RM-5 | 2 |
| #5 Timestamp model | COMPLETE | E.7, AA.1 | — | 2 |
| #6 Event ordering / duplication / recovery | COMPLETE | E.8 | stream | 4 |
| #7 Instrument master / subscription | COMPLETE | E.2, D.4, RM-4 | Kite master | 4, 7 |
| #8 Instrument lifecycle | COMPLETE | N-10, D.8, RM-4 | RM-4 | 7 |
| #9 Canonical market-data semantics | COMPLETE | AA, AB, ADR-13, ADR-15 | RM-2, RM-5 | 2 |
| #10 Display semantics | COMPLETE | AB.3 | RM-2 | 2 |
| #11 Marketwatch read model | COMPLETE | I, ADR-6, F.6 (rejected as a read model, delivered as `/quotes/bulk`) | RM-2, RM-4 | 7 |
| #12 Universal search | COMPLETE | J, ADR-7, RM-4 | RM-4 | 7 |
| #13 Active instrument / linked context | COMPLETE | K, H.1 `instrumentBinding` | RM-4 | 6, 8 |
| #14 Time & Sales | COMPLETE | N, ADR-8, RM-6 | ADR-12, RM-6 | 8 |
| #15 Order-book event analysis | COMPLETE | M.3 | **ADR-12 (depth not stored today)** | 8 |
| #16 Chart read model | COMPLETE | L.1, D.10, RM-1, ADR-5 | — | 3 |
| #17 Live chart architecture | COMPLETE | L.3 | RM-1, stream | 8 |
| #18 Options live architecture | PARTIAL | O | Greeks need IV solves (have); IV surface needs ≥30 sessions of history (**do not have**) | 9 / 15 |
| #19 Futures | COMPLETE | P | settlement price source UNVERIFIED (N-15) | 9 |
| #20 Market context | PARTIAL | Q.2 | VIX + sector indices need subscription; 52w needs history; SENSEX is out of scope (BSE) | 10 |
| #21 Relative strength | COMPLETE | Q.3 | RM-1; sector mapping (verified available) | 10 |
| #22 Unusual activity | COMPLETE | R | expiry guards (N-15) | 10 |
| #23 Trade Intelligence | COMPLETE | S | phases 8–10 inputs | 11 |
| #24 Multi-algorithm architecture | COMPLETE | T, T.1, T.2 | registry replaces engine keys | 11 |
| #25 Model maturity | COMPLETE — **immutable, unchanged** | S.3 | — | preserved |
| #26 Model observability | COMPLETE | S.2 `state`, T.2 | `/intelligence` | 11 |
| #27 Screener | PARTIAL | U | history-dependent filters unavailable until data accumulates, and reported as such | 12 |
| #28 Alerts | COMPLETE | V | non-evictable subscriptions (V.3) | 12 |
| #29 Notification / event center | COMPLETE | V, AE.2, H.3 | — | 12 |
| #30 Workspace system | COMPLETE | H.1–H.4 | — | 6, 14 |
| #31 Persistent user state | COMPLETE | H.5, ADR-10 | durable identity (N-10) | 14 |
| #32 Command palette / keyboard-first | COMPLETE | H.6 | RM-4 search | 7, 14 |
| #33 Context menus / quick actions | COMPLETE | I (row interaction), W.5 (disabled execution) | — | 7 |
| #34 Portfolio / account | COMPLETE | W, N-14 | valid token; typed projections | 13 |
| #35 Order execution readiness | COMPLETE — designed, disabled | W.5 | reference constraints (D.8) | 13 |
| #36 Options strategy workbench | PARTIAL — architecture only | X, W.5 basket | Greeks (O.2) | 15+ |
| #37 Portfolio risk | PARTIAL — contracts only | X | Greeks; beta needs history | 13, 15+ |
| #38 Economic / corporate event context | PARTIAL — schema + UI hooks only | Y.1, Y.2 | external data source | 14 |
| #39 Notes / journal | COMPLETE | Z | `signal_log` (exists) | 14 |
| #40 Corporate action / historical price semantics | COMPLETE — contract; PARTIAL — data | Y.1 | external CA data | 14 |
| #41 Data quality / capability indicators | COMPLETE | D.4, D.5, D.7, AD.2 | — | 2, 6 |
| #42 Graceful degradation | COMPLETE | AE.1, N-18 | — | 0 (contract), per domain |
| #43 Frontend memory / performance | COMPLETE | AG.2, AG.3, AG.4 | — | 6, 15 |
| #44 Prefetching | COMPLETE | AG.5 | — | 7 |
| #45 API architecture | COMPLETE | F.1, F.2, F.3, N-16 | — | 0 |
| #46 Server-side read models | COMPLETE — **"five" rejected; four now, two later, with reasoning** | F.6 | — | 3, 2, 7, 2 / 10, 8 |
| #47 Production observability | COMPLETE | AH | — | 1 |
| #48 Security | COMPLETE | AF, N-14 | — | 0, 13 |
| #49 Deep links / URL state | COMPLETE | AG.7 | — | 6 |
| #50 Exports | COMPLETE — client-side from loaded data | AO Phase 15 | — | 15 |
| #51 Research / production separation | COMPLETE — **already correct, preserved** | A.4, AJ.2 | — | preserved |
| #52 User action telemetry / journal data | PARTIAL — schema only, deliberately | Z | — | 14 |
| #53 Observed / derived / inferred / model | COMPLETE | D.1, AA.1, and enforced per surface | — | 2 |
| #54 Live vs historical separation | COMPLETE — **already correct, extended** | AD | — | preserved + 2 |
| #55 Real-time acceptance testing | COMPLETE — 30 measurable criteria | AN.1 | live market + token | 5, 15 |
| #56 End-to-end data lineage | COMPLETE | AC, plus the runtime inspector (N-11) | — | 2, 15 |
| #57 Duplicate data / logic audit | COMPLETE | B.3, AJ.1, AC | — | 6 |
| #58 Detailed screen inventory | COMPLETE | H.3 | — | per phase |
| #59 Target terminal shell | COMPLETE | C.2, H.4 | — | 6 |
| #60 Visual system | COMPLETE | AG.6 + C.2 density rules | — | 6 |
| #61 Responsive strategy | COMPLETE | AG.6 | — | 6 |
| #62 Error boundaries | COMPLETE | AE.2 | — | 6 |
| #63 Long-run session stability | COMPLETE | AG.4, AN.1 #26/#30, AM.2 | — | 6, 15 |
| #64 Deployment topology | COMPLETE | AI.4 | — | 0 |
| #65 Resource budget | COMPLETE — recomputed against 589 tokens | AI.1–AI.5 | — | 0 |
| #66 Migration strategy | COMPLETE | AL | — | — |
| Four data layers | COMPLETE | D.1, D.2 | — | 2 |
| Subscription management pipeline | COMPLETE | E.2 (universe→eligibility→subscription→mode→capability→consumer) | — | 4 |
| Data capability negotiation | COMPLETE | D.4, five states, `NOT_APPLICABLE` distinct | RM-4 | 7 |
| Market / data / quality separation | COMPLETE | D.7, three enums, never collapsed | RM-5 | 2 |
| Market calendar ownership | COMPLETE | D.6, ADR-14, RM-5 | NSE holiday list | 2 |
| Temporal consistency | COMPLETE | D.5 tolerance table | — | 2 |
| Field-level freshness | COMPLETE | D.5 groups | — | 2 |
| Reference vs market data ownership | COMPLETE | D.2 | RM-4, RM-5 | 7 |
| NSE / derivative constraints | PARTIAL — audited; several unavailable | D.8 (tick/lot available and unused; freeze/band/settlement absent) | external sources | 7, 9, 13 |
| Historical data layers | COMPLETE | D.10, T0–T4 with per-interval authority | — | 3 |
| Snapshot coherence | COMPLETE | D.9 | — | 2 |
| Order / account semantics | COMPLETE | W.1 lifecycle, W.2 P&L decomposition | typed projections | 13 |
| Solution neutrality on every major decision | COMPLETE | 16 ADRs with alternatives and reasoning (§0.2) | — | — |
| Professional-terminal completeness check | COMPLETE | Appendix 1, 11 stages | — | — |
| Final self-critique | COMPLETE | Appendix 2, 15 redesign risks + 3 honest weaknesses | — | — |
| Existing audit findings 1–18 verified | COMPLETE — **6 corrected** | §0.3 (R-1…R-10), A | — | — |
| Independent gap discovery | COMPLETE — **20 new requirements** | §0.4 (N-1…N-20) | — | 0–15 |

### Unresolved items — complete list

There are no `TBD` foundational architectural decisions. Every architectural choice is made in
§0.2 with alternatives and reasoning. Four **data-sourcing** questions remain open, each with a
named owner, a phase and an explicit fallback:

| # | Open question | Fallback if unresolved | Phase |
| --- | --- | --- | --- |
| 1 | Source for official derivative **settlement prices** | `reference_type: OFFICIAL_CLOSE`, labelled truthfully; day change technically imperfect and disclosed | 9 |
| 2 | Whether Kite exposes **price bands / circuit limits** on any endpoint | no band warnings; a frozen instrument is reported as `STALE` rather than "at band" | 9 |
| 3 | Source for **freeze quantity** | order validation omits the freeze check, which blocks nothing until execution is enabled | 13 |
| 4 | Whether a market-wide **HALT** is detectable from market data | `HALTED` stays manual-only; silence is reported as `STALE`, which is the honest reading | 9 |

Two **capability** items are gated on time rather than design: features needing ≥30 sessions
(IV percentile, volatility screens) and ≥1 year (52-week levels, beta) will render
`UNAVAILABLE (insufficient history)` until the data accumulates, because only four sessions exist
and ingestion has been down since 2026-09-04.

---

**END OF DOCUMENT.** This is the authoritative architecture. There are no competing plans in this
repository. The first action is Phase 0, item 1: re-establish the Kite session and stop the
`nse-account-capture` crash loop.

