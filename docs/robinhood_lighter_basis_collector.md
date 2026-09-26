# Robinhood Chain Lighter basis sidecar

This sidecar measures executable ETH basis between Variational and the Lighter
deployment on Robinhood Chain. It never submits trades or modifies inventory;
validated baseline history can be consumed read-only by the V4 anchor:

- It tails Variational quotes already written by the live V4 process under
  `log/basis_samples/ETH/`.
- It also tails `log/order_metrics.jsonl` and takes a fresh Robinhood public
  book snapshot at entry-candidate, entry-confirmed, exit-confirmed, and final
  PnL events. Event rows use `sample_kind=trade_event`.
- It connects only to the public Robinhood Lighter REST market-data API.
- It does not bind the Variational forwarder ports, request extra Variational
  quotes, import private keys, submit orders, or modify live inventory state.
- It writes separate daily samples under
  `log/robinhood_basis_samples/ETH/` and rotates closed days to gzip.
- Runtime diagnostics rotate in `log/robinhood_basis_collector.log`; the compact
  health snapshot is `log/robinhood_basis_health.json`.

The default executable depth ladder is USD 20, 40, and 60. Each sample records
both trade directions, normalized Variational prices when present, source and
book ages, nonce continuity, and depth prices.

Trade-event snapshots preserve the source event, run, episode, lot, direction,
and available Variational price/PnL fields. Cross-venue edge fields remain null
when an event does not contain enough Variational price data; the collector does
not substitute a later quote. Event snapshots remain research-only. Validated
baseline snapshots may contribute only to the V4 rolling entry anchor; they do
not replace live quotes, satisfy the recent-health window, or change execution
sizing. V4 checks market ID, lot notional, source quote age, and Robinhood book
freshness/continuity before accepting them.

The default source is `log/robinhood_basis_samples/`. An existing sidecar
history elsewhere can be included by setting
`LIVE_INVENTORY_BASIS_V4_ROBINHOOD_HISTORY_DIR` for the live process. Point it
only at a Robinhood sidecar sample root, never the old mainnet
`basis_samples` directory.

The official browser WebSocket currently rejects an unauthenticated bare
Python handshake at its WAF boundary. The sidecar therefore requests one full
public REST order-book snapshot per new Variational baseline sample, plus one
forced snapshot for each selected trade event. It does not poll continuously or
reuse browser cookies. This is sufficient for the
first-stage baseline comparison; any later execution integration must obtain a
supported direct streaming connection before real orders are considered.

## Start alongside live V4

Start the live process first. Then start the sidecar in a separate tmux session:

```bash
cd ~/var-rh-eth
source .venv/bin/activate

SESSION="eth-rh-basis-$(date -u +%m%d%H%M)"
OUT="log/${SESSION}.startup.log"

tmux new-session -d -s "$SESSION" \
"cd ~/var-rh-eth && source .venv/bin/activate && exec python tools/robinhood_basis_collector.py >> '$OUT' 2>&1"

echo "session=$SESSION"
echo "startup_log=$OUT"
```

The default startup follows only samples appended after the sidecar starts.
This avoids replaying old Variational quotes against a current Robinhood book.

## Verify

```bash
sleep 90

pgrep -af "python.*robinhood_basis_collector.py" || echo sidecar_stopped

python - <<'PY'
import json
from pathlib import Path

path = Path("log/robinhood_basis_health.json")
print(path.read_text() if path.exists() else "health_missing")
PY

tail -n 30 log/robinhood_basis_collector.log
```

Do not use these samples to enable Robinhood Lighter order submission. Collect
at least seven days, preferably fourteen days spanning weekdays and weekends,
then compare executable opportunity count, duration, depth, continuity, and
funding against the current Lighter deployment.
