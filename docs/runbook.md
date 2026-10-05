# Runbook

Use only these two daily commands on the VPS.

## Start Live

```bash
cd ~/var-rh-eth
source .venv/bin/activate
python tools/live.py --asset SOL
```

Replace `SOL` with `BTC` or `ETH` when needed.

The command refuses to start when another `python main.py` is running, when `log/live_inventory_state.json` is not flat, or when it contains `open_lots` or `pending_actions`.

Before running it, manually confirm both exchanges have no positions and no open orders.

Startup parameters are read once from `live_config.json`. They are not hot-reloaded into the trading loop.

To inspect the full `main.py` command without starting live:

```bash
python tools/live.py --asset SOL --dry-run --verbose
```

## Reset Local State Without Starting Live

After manually flattening an asset, use this one-shot command when the local
journal still shows old lots:

```bash
python tools/live.py --asset ETH --reset-local-state-only
```

The command asks for `FLAT:ETH`, then independently checks two consecutive
snapshots from Variational and Lighter. It refuses to reset if either venue has
a non-zero position, an active order, an API error, or an unrecognized local
state. On success it backs up `log/live_inventory_state.json`, marks it flat,
and exits before starting any trading loop. Run log maintenance only after this
command exits and all other log writers are stopped.

## Analyze Live Data

```bash
cd ~/var-rh-eth
source .venv/bin/activate
python tools/analyze.py
```

This reads `log/order_metrics.jsonl`, `log/runtime.log`, and `log/live_inventory_state.json`. It does not start live, stop live, submit orders, or modify state.

## Profit And Account Equity Report

The live process records a non-blocking account snapshot at startup and two
seconds after every confirmed entry and exit. The snapshot uses Variational
`balance + upnl` and Lighter `collateral`; failures are logged as partial
snapshots and never stop trading.

Telegram reports only operationally important state changes: the startup-flat
account snapshot, confirmed entries/add-ons, confirmed tier exits, account-risk
or venue failures and recovery, and the fully-flat PnL summary. Routine tier
arming, normal entry/exit guards, percentile changes, and Robinhood collector
heartbeats stay in local metrics instead of producing chat noise. Entry and
tier-exit messages include the current tier, open child count, venue equities,
and available margin-risk context.

Show confirmed pair-fill PnL, both venue balances, account-equity change, and
simple annualized return:

```bash
python tools/pnl_report.py --asset ETH --capital-usd 35
```

`--capital-usd` is the amount of capital allocated to this strategy across
both venues, not one leg's order notional. It can be saved outside the command
as `PNL_REPORT_CAPITAL_USD` in `.env`. If neither is provided, the report uses
the first complete flat account snapshot. Use `--since YYYY-MM-DD` to limit the
reporting period.

The confirmed pair-fill PnL is calculated from both venues' final fill prices.
It does not separately deduct fees or funding. Account-equity change includes
fees, funding, deposits, withdrawals, and any other account activity, so the
report keeps these two figures separate.

PnL reporting uses Beijing natural days (`Asia/Shanghai`, UTC+8). A reporting
day runs from Beijing 00:00 through 24:00. Telegram shows both the current
Beijing day's confirmed close PnL and the persistent tracking-period total.
The total simple annualized return is the tracking-period return divided by its
covered Beijing calendar days and multiplied by 365; it is not extrapolated
from a partial day. Multi-day command-line reports use the same calendar-day
denominator rather than fractional UTC elapsed time. Date-only `--since` values
mean Beijing midnight.

Query the latest 30 or 90 Beijing natural days. Each command prints the period
total and annualized return followed by every day's confirmed PnL, including
zero-trade days:

```bash
python tools/pnl_report.py --asset ETH --period 1m
python tools/pnl_report.py --asset ETH --period 3m
```

Resend the latest confirmed close to the configured Telegram chat in the
Chinese PnL-summary format:

```bash
python tools/pnl_report.py --asset ETH --telegram-latest
```

This command only reads logs and sends a message. It never submits orders.

The live process also maintains a small atomic daily ledger at
`log/pnl_reporting_baseline.json`. Each confirmed two-venue close records PnL,
closed child lots, and exact four-leg notional once. The ledger is keyed by
run id, asset, and lot id, so restarts and repeated fill callbacks do not count
the same close twice.

After the first upgrade from the schema-v1 ledger, backfill historical volume
once. This streams the audit log and updates only close keys already counted in
the baseline; it cannot submit orders or double-count PnL:

```bash
python tools/backfill_pnl_volume.py
```

Preview or manually send one Beijing-day report:

```bash
python tools/daily_pnl_report.py --asset ETH --day today --dry-run
python tools/daily_pnl_report.py --asset ETH --day today --force
```

Install the independent systemd timer. It runs every five minutes, sends the
previous completed Beijing day once, retries failed Telegram delivery, and
catches up missed days after a VPS outage. In account-equity mode, valid equity
samples require a fresh risk-health file from the running strategy; if the
strategy is stopped or that data is stale, the reporter skips the sample and
does not invent a daily return:

```bash
sudo cp deploy/systemd/var-rh-daily-pnl.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now var-rh-daily-pnl.timer
systemctl list-timers var-rh-daily-pnl.timer
```

Start a new persistent PnL reporting period, excluding all older trades:

```bash
python tools/pnl_report.py --asset ETH --reset-baseline
```

Run the reset only while the strategy is stopped and local state is flat. The
next startup records the initial combined account equity. Confirmed PnL,
completed cycles, return, and simple annualized return then accumulate from
that checkpoint across later strategy restarts. Historical logs are retained
for audit but excluded from the default report.

For an RH ETH strategy that began without a reporting baseline and now has an
open position, do not use `--reset-baseline`. Stop the RH strategy first, keep
the position untouched, and preview a historical rebuild instead:

```bash
cd /home/ubuntu/var-rh-eth
source .venv/bin/activate
python tools/bootstrap_rh_pnl_baseline.py
```

The preview prints the RH start date, starting two-venue capital, latest
available two-venue equity, account-equity PnL estimate, and a history digest.
It uses a complete startup-flat account snapshot or, when that is unavailable,
two matching fresh account-risk records after a verified flat startup and
before the first entry. Verify that there were no unrecorded deposits or
withdrawals and that the capital agrees with the start-day records. If the
preview is correct, create the baseline with the printed digest:

```bash
python tools/bootstrap_rh_pnl_baseline.py --apply --expect-digest '<history_digest>' --i-confirm-no-unrecorded-transfers
python tools/daily_pnl_report.py --asset ETH --day today --dry-run
```

The bootstrap refuses an existing baseline, a running strategy, unverified
account records, or a last-flat unexplained account change over 1 USD. It does
not trade or send a notification. The account-equity daily report samples the
strategy's fresh two-venue risk health every five minutes. Cumulative return
starts at the baseline equity; daily return starts at the first valid sample
of each Beijing day. Older daily splits without equity samples are reported as
unavailable. Stale or incomplete venue data is skipped. Before enabling the RH
daily timer, disable the old mainnet timer to prevent mixed or duplicate
reports:

```bash
sudo systemctl disable --now var-lighter-daily-pnl.timer
sudo cp deploy/systemd/var-rh-daily-pnl.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now var-rh-daily-pnl.timer
```

## V4 Exit Confirmation

V4 uses the latest executable quote plus two qualifying observations in the
latest three-sample window. A guarded strong-single exit is allowed only when
its mode-specific shortfall reserve, short-window stability, quote latency, and
Lighter depth checks all pass. If a strong-single exit is estimated profitable
but its confirmed final PnL is negative, strong-single is disabled for the rest
of that run and exits fall back to the latest-and-two-of-three policy.

Inspect the active policy and automatic fallback state with:

```bash
python tools/analyze.py --tail 30000 --asset ETH | grep -E \
'^(process=|state=|exit_confirmation_policy=|exit_submit_mode=|exit_block_reasons=|operational_readiness=)'
```

V4 trades continuously on UTC weekends. The same multi-window threshold logic
applies, with a conservative six-hour boundary transition at weekend start and
end. The legacy weekend flag is not needed in new commands.

## Notes

`main.py` remains the trading engine. `tools/live.py` is only a safety wrapper around the existing live command, and `tools/analyze.py` is only an offline live-log analyzer.

## Log Maintenance

`runtime.log` and `basis_collector.log` rotate automatically. Do not use a
generic rotation command on `order_metrics.jsonl`: the strategy reads retained
execution events from that file for calibration.

Run the unified log maintenance preview regularly:

```bash
python tools/log_maintenance.py
```

Execution requires all trading and collector writers to be stopped and the
local live inventory state to be flat with no pending actions. It compresses
`order_metrics.jsonl` once it reaches 512 MiB, archives oversized raw capture
logs, compresses closed mainnet/RH basis days, and removes sample days older
than 45 days. Raw capture archives expire after 14 days; market and paper log
archives expire after 45 days. The exact full order-metrics archives remain
protected. Use:

```bash
python tools/log_maintenance.py --execute
```

The live strategy also records repeated basis-state snapshots less often and
blocks new entries/add-ons below 3 GiB free disk while continuing to manage
existing positions. Closed-day sample retention preserves at least 30 days.
The 4 GiB log-directory budget is a monitoring target; if protected full order
archives push usage over it, keep them and add disk or move verified archives
off-host rather than deleting execution history.

Other stopped-process logs can be previewed and archived separately:

```bash
python tools/archive_legacy_logs.py
python tools/archive_legacy_logs.py --execute
```

## Robinhood Chain Lighter Research Sidecar

The Robinhood Chain Lighter basis collector can run alongside live V4 because
it tails the live process's persisted Variational samples and opens only a
public Robinhood Lighter market-data connection. It does not use the extension
command port or any trading credentials. See
`docs/robinhood_lighter_basis_collector.md` for startup and verification.
### V4 parallel shadow-gradient research

`--v4-shadow-gradient` does not submit a second real order. During each real
one-lot episode it independently simulates alternative USD 20 second-tranche
entries after basis improvement of `+0.5`, `+1.0`, `+1.5`, and `+2.0` bps.
Each alternative has its own entry price, exit confirmation state, MFE/MAE, and
PnL. The alternatives are not summed into an USD 80 position.

Use the following analyzer line to compare the alternatives after several real
cycles:

```bash
python tools/analyze.py --tail 50000 --asset ETH | grep '^shadow_gradient_levels='
```

### Five-tier real gradient and account risk

`--v4-real-gradient` is an explicit real-order mode. It uses five dynamic
historical-percentile tiers. Each order remains fixed at USD 20. The smaller
fresh venue equity determines integer child slots: each tier receives one fifth
of the total 5x slot budget cumulatively. For example, USD 98.64 supports
`5/10/15/20/24` children across tiers 1-5, USD 100 supports
`5/10/15/20/25`, and USD 120 supports `6/12/18/24/30`. This keeps the hard
limit at 5x per venue without a static USD 500 ceiling. Every child order
requires a fresh quote and confirmation of the previous pair of fills. There is
no fixed delay between children. Each venue uses a rolling 60-second request
window with normal capacity reserved below the hard capacity so reduce-only
exits remain available. It is mutually exclusive with `--v4-shadow-gradient`.
The optional `--v4-elastic-capacity` flag allows one additional USD 20 child
order after cumulative tier 1 is full. That reserve slot is available only for
the same locked direction, after the refreshed exact RFQ remains at least
`0.25 bps` above the first-tier exact threshold and all normal fill and risk
checks pass.

Tier activation requires the latest observation and at least two of the latest
three observations to reach that tier. The spacing between adjacent tiers is
the maximum of the historical percentile difference, observed market noise,
and incremental depth cost. The total paired-execution error is already included
in the first-tier execution reserve and is not charged again at every tier.
After a tier closes,
that tier must first fall below its threshold and then satisfy activation again
before it can add new children.

Every candidate entry is checked against fresh account data. The hard leverage
limit is 5x on each venue independently, not 5x on combined gross notional.
Operational actions use maintenance-margin usage: 40% warns, 50% blocks new
entries, 60% reduces one child lot, and 75% requests emergency exit. An equity
imbalance near 20% warns and near 30% blocks entries because the smaller venue
limits paired capacity. After balances recover, fresh account data restores
entry permission automatically. Account risk is refreshed every 5 seconds
while a position is open and every 15 seconds while flat. If a venue omits
maintenance-margin data, the engine uses a conservative venue-specific fallback
rate instead of disabling the risk ladder.

Exit accounting is independent by entry tier. The engine checks higher tiers
first, quantity-weights only that tier's confirmed child fills, and closes the
whole tier only when that tier alone reaches its executable net target. A
partially filled tier may close under the same rule. Profit from a higher tier
never subsidizes a loss in a lower tier; lower tiers may remain open for their
own later profitable reversion. Risk-forced reduction and emergency exit remain
allowed to override profit exits. Funding is excluded from live exit decisions
and is included only in post-close account-equity reporting.

V4 has no time-driven exit and does not relax its profit target based on holding
time. Executable profit, the independent unrealized-loss fuse, and account-risk
actions are the only exit triggers. The V4 launcher uses a wider 50 bps
unrealized-loss fuse; this is a last-resort strategy anomaly guard, not the
normal exit condition.

Variational account data must have a valid `published_at` no older than 60
seconds. A stale snapshot is logged as partial, is excluded from account PnL and
capital baselines, and pauses new entries/add-ons without forcing an open
position to close. Every live startup and every recovery from stale or incomplete
account data requires three consecutive complete account checks. Entry and
gradient confirmation windows are cleared during recovery, so a position cannot
open immediately from a signal observed while the browser link was unavailable.

### Persistent Variational browser

The Chrome extension stores the enabled tab, wakes itself every 30 seconds,
restores its debugger attachment and local sockets after a Manifest V3 service
worker restart, and keeps the Variational page lifecycle active. After updating
the repository, reload the unpacked extension once and click `Start` once; later
service-worker restarts recover automatically.

Chrome itself must be launched with background throttling disabled. Do this only
while the strategy is stopped and both venues are confirmed flat. Close every
Chrome window, open a terminal inside the VPS remote desktop, and run:

```bash
cd ~/var-rh-eth
source .venv/bin/activate
python tools/launch_variational_chrome.py
```

Unlock the wallet, open Variational, reload the unpacked extension if Chrome asks,
and click the forwarder's `Start` button once. The launcher reuses the existing
Chrome profile; it never reads or stores wallet secrets. Verify that the required
anti-throttling flags are active:

```bash
python tools/launch_variational_chrome.py --check
```

Test the fix by disconnecting the remote desktop for at least five minutes. After
reconnecting, `variational_snapshot_age_seconds` should remain below 60 and no
stale-account alert should appear. If the page or wallet genuinely logs out, the
strategy remains fail-closed and requires fresh account checks after login.

External deposits and withdrawals are detected again after every complete,
confirmed flat exit snapshot, not only at startup. The current cycle PnL is
recorded before unexplained equity change is classified as external cash flow.

V4 trades continuously across UTC weekends. A simple six-hour boundary marker
keeps the multi-window threshold conservative around weekend start and end.
The old weekend test flag remains accepted only for command compatibility.

Before increasing either venue to USD 100, stop the strategy and confirm both
venues are flat. Make both deposits, then reset the PnL baseline once so the
cash inflow is not counted as strategy profit:

```bash
python tools/pnl_report.py --asset ETH --reset-baseline
```

Start the five-tier mode only after the new startup account snapshot shows
both venue equities near USD 100:

```bash
python tools/live.py --asset ETH --v4-live --v4-real-gradient \
  --v4-test-skip-recent-health --v4-test-max-cycles 3 \
  --reset-state-after-manual-flat
```

The command above is a bounded three-cycle acceptance run. For normal
continuous operation after acceptance, use the explicit continuous mode:

```bash
python tools/live.py --asset ETH --v4-live --v4-real-gradient \
  --v4-continuous --v4-elastic-capacity --v4-test-skip-recent-health \
  --reset-state-after-manual-flat
```

`--reset-state-after-manual-flat` is an explicit operator confirmation that both
venues were manually checked flat. When an old local journal still contains
lots or pending actions, startup backs it up, resets it to flat, and then runs
the strict two-venue reconciliation before enabling entries. If either venue
is not flat or cannot be queried, startup remains fail-closed. This option
continues into the selected live strategy; for state cleanup without starting
trading, use `--reset-local-state-only` instead.

Continuous mode records a checkpoint every time the whole portfolio returns to
flat, then rearms and starts another episode. `max_cycles=0` is accepted only
with this explicit real-gradient mode. The cumulative run-loss fuse, account
risk actions, exchange reconciliation, and maintenance drain remain active.

To end a healthy V4 test position before its normal profit exit, stop the old
process without changing `live_inventory_state.json`, then run the reconciled
one-shot exit:

```bash
python tools/live.py --asset ETH --v4-live --close-open-position
```

This command accepts only `status=open` with at least one recorded lot and no
pending action. Startup fetches both venue positions and requires their
quantities and directions to match the saved lots. It then disables entries,
submits the existing concurrent reduce-only exit path, waits for both final
fills and the PnL report, and stops. It never resets an open state. Do not use
manual sequential closes unless startup reconciliation has refused and the
venues require manual recovery.

If a reduce-only exit stopped at
`basis_exit_lighter_final_fill_not_confirmed`, manually re-align the two venue
positions to the saved open lots, then use the normal open-state takeover:

```bash
python tools/live.py --asset ETH --v4-live --v4-real-gradient \
  --v4-continuous --v4-test-skip-recent-health \
  --resume-open-position
```

This reason is accepted only as an admission to strict startup reconciliation;
the process still refuses to trade unless both live quantities and directions
match every saved lot. It never clears the manual-review state merely because
the operator supplied the flag.

For routine deployment, request a maintenance drain instead of waiting for a
multi-cycle batch to finish:

```bash
python tools/live.py --asset ETH --drain-after-flat
```

The request is bound to the currently running PID and run id. The live process
immediately blocks every new entry and add-on, but keeps each existing gradient
tier under its normal independent profit-exit rule. Once there are no local
lots or pending submissions, it reads both exchanges again. It stops only when
both venue positions are confirmed flat, then sends a Chinese Telegram message
that the deployment can proceed. If either venue cannot be checked or still has
a position, entries stay blocked and the process retries without forcing a
loss-making exit. `tools/analyze.py` reports the request as
`maintenance_drain_status=`.
# Automatic margin rebalance

The implementation contract and safety state machine are documented in
`docs/automatic_margin_rebalance.md`. Execution remains disabled until both
venue adapters have verified official transfer and finality paths.

Validate the five percentile tiers on a chronological holdout:

```bash
python tools/validate_gradient_tiers.py --asset ETH
```

The first 70% of samples fits thresholds; the final 30% reports crossings and
six-hour 1/2/4 bps reversion hit rates. The tool is offline and never feeds its
output into the live process.

# Independent night risk wakeup

The independent Pushover Emergency and Tencent Cloud voice escalation service
is documented in `docs/risk_wakeup_watchdog.md`. It reads local state and the
main runtime's small risk heartbeat only; it never enters the quote or order
path.
