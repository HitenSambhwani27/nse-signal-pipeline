# Phase 1 implementation plan

Phase 0 remains the baseline. This adds backend session/change/freshness/health only.
No SSE, no frontend, no ingest restart, no holiday invention, no maturity/research edits.

## Already satisfied by Phase 0 (do not duplicate)

- `kite_token_state`, disk free / sessions of headroom
- envelope `subsystems` + maturity `NOT_MATURE`
- `data_status`: live | last_session | no_data from `MarketDataPolicy`
- `observation_age_seconds` against real timestamps
- cache schema health, account status, ingest-fresh inference
- first-tick null volume/OI deltas; `warm_start` coverage hook
- response-size budgets

## Gaps this phase fills

| Gap | Approach |
| --- | --- |
| `market_state` is only open/closed; weekends look like closed; 09:14 is closed | Rich clock: `pre_open`, `open`, `post_close`, `closed`, `weekend`. `LIVE` still requires **open** (09:15–15:30). |
| No holiday calendar | `market_calendar` table seeded with weekday/weekend rows only. `source=weekday_clock`, `holiday_list_seeded=false`. Never claim `holiday` without an NSE list. |
| `change` is tick `price_delta` | Keep legacy `change` / `change_pct`. Add canonical `reference_price`, `reference_type`, `change_absolute`, `change_percent` (0–100). Missing/zero ref → nulls + `no_reference`, never a fake 0. |
| Reference store | Compute from **observed** Kite `ohlc_close` (prev close) / `ohlc_open`. Do not invent settlement. Futures/options: `OFFICIAL_CLOSE` from ohlc_close, never silent `SETTLEMENT`. |
| Freshness | `freshness` object from observation timestamp, not `envelope.as_of`. |
| Health | Distinct components: ingest, latest_quotes, activity_samples, cache, calendar, account, api, disk, maturity. Lightweight queries only. Clock-skew is `ingested_at − canonical observed_at`. Stream/tick-drop counters: `not_applicable`. |
| Retention | Expose dry-run mode; **do not apply**. |

## LIVE rule (unchanged)

`data_status=live` only when cash session is **open** and the observation is within `live_quote_max_age_seconds`. Pre-open / post-close / weekend → `last_session` if an observation exists.

## Completion-pass audit (timestamp + ingest lifecycle)

### Timestamp lifecycle (proven)

Kite binary packet stores Unix epoch seconds. `kiteconnect.ticker` does
`datetime.fromtimestamp(epoch)` → **naive local wall-clock** of the ingest host.
Production VM TZ is `Asia/Kolkata`, so that naive value is IST (e.g. 15:53:59).

`normalize._as_datetime` previously did `replace(tzinfo=UTC)` on naive datetimes.
That was the mislabel: `2026-09-07T15:53:59+00:00` meaning 15:53 IST.
`ingested_at` is `datetime.now(UTC)` and is correct.

Do **not** subtract 5.5h from every timestamp. Relabel naive Kite times as
`Asia/Kolkata` at one ingest boundary (`parse_kite_datetime`). On read, repair the
stored `+00:00` + ~19800s-ahead-of-`ingested_at` pattern only. Ambiguous 09:00–10:00
UTC-labeled hours without `ingested_at` stay explicit `ambiguous_utc_hour`, not guessed.

| Field | Meaning | Source | After canonicalization |
| --- | --- | --- | --- |
| `exchange_timestamp` / observation `timestamp` | market observation instant | Kite epoch via naive IST wall-clock | timezone-aware ISO |
| `ingested_at` | receive/process time | ingest host UTC now | UTC ISO, unchanged |
| `freshness.observed_at` | same instant as observation, IST display | canonical observation | `+05:30` |
| `envelope.as_of` | HTTP response time | API now | never used as observation time |
| `updated_at` | SQLite persist time | writer UTC | persistence lag only |

Clock skew = `ingested_at − observed_at` (canonical). Compared clocks are
receive vs exchange observation, not VM vs envelope.

### Ingest lifecycle (proven)

`nse-ingest.service`: `Restart=on-failure`, `WantedBy=multi-user.target`.
Clean SIGTERM → exit 0 → systemd does not restart. Boot would start it; a
long-lived VM after a clean stop will not. Compact/features/labels/retention
timers existed; ingest had none before this pass.

Mechanism: `nse-ingest.timer` `OnCalendar=Mon..Fri 08:45:00` (machine TZ =
IST, same as existing after-close timers). `Persistent=false` so enabling the
timer after 08:45 does not immediately start a second/off-hours WebSocket.
`systemctl start nse-ingest` remains idempotent (no second process).
`Restart=on-failure` unchanged. No holiday awareness. Install/deploy enable the
timer only; they do not start ingest. Cutover still starts ingest after close.

## Out of this commit

SSE, RM-1 bars, NSE holiday CSV seed, settlement prices, retention `--apply`.
