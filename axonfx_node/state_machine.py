"""
This is the replicated ledger. The only thing that makes it replicated is that raft.py calls its single public entry point, 
apply(), once per committed log entry, in the same order, on every node in the cluster (see raft.py's _apply_loop).

Note: apply() must be a pure function of (current ledger state, command bytes) - no clock or no external API call. If three 
nodes each independently asked "what's the forex rate right now?" while applying the same command, they could get three
different answers and their ledgers would silently disagree - Raft only guarantees replicas see the same command bytes, not that they'd
compute the same result from different live inputs.

Our solution: take_snapshot() below is called exactly once, by grpc_service.py, before a command is ever built and handed to 
raft_node.propose(). Its result travels inside the pickled command bytes that become the log entry. 
apply() just unpacks and replays already decided numbers. It never calls take_snapshot() itself.
"""

import hashlib
import pickle
import time

from axonfx_node.routing import plan_fills

# ---------------------------------------------------------------------
# Currencies & the simulated market. This is the ONLY place that ever
# generates a "live" number - see the module docstring above.
# ---------------------------------------------------------------------

# Mid-market rate of 1 unit of currency, expressed in USD. Illustrative
# constants, not live data.
BASE_USD_RATE = {
    "USD": 1.0,
    "INR": 1.0 / 83.0,
    "GBP": 1.27,
    "EUR": 1.09,
    "JPY": 1.0 / 149.0,
}
CURRENCIES = tuple(BASE_USD_RATE.keys())

# Starting liquidity pools - fixed constants so every node boots into an
# identical initial state.
STARTING_POOLS = {
    "USD": 50_000.0,
    "INR": 2_000_000.0,
    "GBP": 20_000.0,
    "EUR": 25_000.0,
    "JPY": 3_000_000.0,
}

CUSTOMER_FEE_BPS = 80       # the visible fee quoted to the customer ("under 1%")
EXECUTION_SPREAD_BPS = 25   # the smaller spread AxonFx pays executing conversions itself


class RateSnapshot:
    """An immutable set of numbers, safe to pickle into a command and
    replay identically on every node."""

    def __init__(self, taken_at, usd_rates):
        self.taken_at = taken_at
        self.usd_rates = usd_rates

    def mid_cross(self, a, b):
        return self.usd_rates[a] / self.usd_rates[b]

    def locked_rate(self, a, b):
        """What we quote the customer - mid rate minus our visible fee."""
        return self.mid_cross(a, b) * (1 - CUSTOMER_FEE_BPS / 10_000)

    def execution_rate(self, a, b):
        """What we net when we actually execute a conversion ourselves."""
        return self.mid_cross(a, b) * (1 - EXECUTION_SPREAD_BPS / 10_000)

    def cross_rate_table(self):
        """Every ordered pair's execution rate, for the routing engine."""
        return {
            (a, b): self.execution_rate(a, b)
            for a in self.usd_rates for b in self.usd_rates if a != b
        }


def take_snapshot():
    """Simulate 'the current market' - deterministic-looking drift keyed
    off wall-clock second, so it behaves like a live feed without needing
    a real one. Call this exactly once per client request, in
    grpc_service.py, BEFORE building a command - never from inside
    AxonFxState.apply()."""
    now = time.time()
    bucket = int(now)
    drifted = {}
    for ccy, base in BASE_USD_RATE.items():
        h = hashlib.sha256(f"{ccy}:{bucket}".encode()).hexdigest()
        wobble = (int(h[:8], 16) / 0xFFFFFFFF - 0.5) * 0.003  # +/- 0.15%
        drifted[ccy] = base * (1 + wobble)
    return RateSnapshot(taken_at=now, usd_rates=drifted)


def encode_submit_transfer(request_id, source_currency, dest_currency, source_amount,
                            locked_rate, mid_rate, cross_rates, recipient_name,
                            recipient_account, decided_at):
    """Builds the opaque command bytes that become a Raft log entry.
    Everything here is a plain, already-decided value - see the module
    docstring. Called by grpc_service.py, never by AxonFxState itself."""
    return pickle.dumps({
        "op": "submit_transfer",
        "args": {
            "request_id": request_id, "source_currency": source_currency,
            "dest_currency": dest_currency, "source_amount": source_amount,
            "locked_rate": locked_rate, "mid_rate": mid_rate, "cross_rates": cross_rates,
            "recipient_name": recipient_name, "recipient_account": recipient_account,
            "decided_at": decided_at,
        },
    })


# ---------------------------------------------------------------------
# The state machine itself
# ---------------------------------------------------------------------

MAX_HISTORY = 500


class AxonFxState:
    """Plain class - no base class, no decorators. raft.py holds one
    instance of this per node and calls .apply(command_bytes) once per
    committed log entry, in order. Every method below is a normal Python
    method; nothing here talks to Raft, gRPC, or the network."""

    def __init__(self):
        self.balances = dict(STARTING_POOLS)
        self.fx_pnl = {ccy: 0.0 for ccy in STARTING_POOLS}
        self.transfer_log = []
        # Idempotency cache: request_id -> previous result. A real system
        # would bound/evict this; kept as a plain dict for simplicity.
        self.seen_requests = {}

    def apply(self, command_bytes):
        """The single entry point raft.py calls. Unpacks the command and
        dispatches to the right handler. Only one command type exists in
        this project (submit_transfer), but the {op, args} envelope
        leaves room for more without changing this dispatch shape."""
        command = pickle.loads(command_bytes)
        op = command["op"]
        if op == "submit_transfer":
            return self._apply_submit_transfer(**command["args"])
        raise ValueError(f"unknown command op: {op}")

    def _apply_submit_transfer(self, request_id, source_currency, dest_currency,
                                source_amount, locked_rate, mid_rate, cross_rates,
                                recipient_name, recipient_account, decided_at):
        cached = self.seen_requests.get(request_id)
        if cached is not None:
            replay = dict(cached)
            replay["idempotent_replay"] = True
            return replay

        # Step 1 (always happens): sender's funds land in our pool.
        self.balances[source_currency] = self.balances.get(source_currency, 0.0) + source_amount

        dest_amount_needed = round(source_amount * locked_rate, 2)

        # Step 2: use whatever is already free in the destination pool.
        pool_available = max(self.balances.get(dest_currency, 0.0), 0.0)
        pool_covered = round(min(pool_available, dest_amount_needed), 2)
        shortfall = round(dest_amount_needed - pool_covered, 6)

        # Step 3: matching engine covers any remaining shortfall (routing.py).
        fills = []
        if shortfall > 1e-9:
            fills, _ = plan_fills(dest_currency, shortfall, source_currency, self.balances, cross_rates)
            for f in fills:
                self.balances[f.via_currency] = self.balances.get(f.via_currency, 0.0) - f.drained
                self.balances[dest_currency] = self.balances.get(dest_currency, 0.0) + f.produced

        # Step 4: pay out - debit the destination pool by the full amount owed.
        self.balances[dest_currency] = self.balances.get(dest_currency, 0.0) - dest_amount_needed

        # "Local bank rails" - simulated, so just a receipt, no external call.
        rail = {"INR": "UPI/NEFT", "USD": "ACH", "GBP": "Faster Payments",
                "EUR": "SEPA", "JPY": "Zengin"}.get(dest_currency, "local rail")

        # Company P&L: mid-market value of what arrived, minus what we
        # promised the customer at the fee-inclusive locked rate.
        pnl = round(source_amount * mid_rate - dest_amount_needed, 6)
        self.fx_pnl[dest_currency] = self.fx_pnl.get(dest_currency, 0.0) + pnl

        if shortfall <= 1e-9:
            scenario = "pool_covered"
        elif all(f.used_pool_surplus for f in fills):
            scenario = "routed_intermediate"
        elif any(f.used_pool_surplus for f in fills):
            scenario = "routed_intermediate+forex_fallback"
        else:
            scenario = "forex_direct"

        result = {
            "success": True,
            "request_id": request_id,
            "dest_amount_paid": dest_amount_needed,
            "locked_rate": locked_rate,
            "pool_covered_amount": pool_covered,
            "fills": [f.__dict__ for f in fills],
            "scenario": scenario,
            "idempotent_replay": False,
            "rail": rail,
        }

        self.transfer_log.append({
            "request_id": request_id, "source_currency": source_currency,
            "dest_currency": dest_currency, "source_amount": source_amount,
            "dest_amount_paid": dest_amount_needed, "locked_rate": locked_rate,
            "scenario": scenario, "timestamp": decided_at,
        })
        if len(self.transfer_log) > MAX_HISTORY:
            self.transfer_log.pop(0)

        self.seen_requests[request_id] = result
        return result

    # Reads - served locally, no Raft round trip. On a follower this can be
    # a few milliseconds stale vs. the leader - a deliberate trade-off:
    # reads are cheap and don't need a majority, writes do.
    def read_balances(self):
        return dict(self.balances)

    def read_fx_pnl(self):
        return dict(self.fx_pnl)

    def read_history(self, limit=20):
        return list(self.transfer_log[-limit:])
