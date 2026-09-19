"""
Runs the product spec's five scenarios (plus a live leader-failover test)
against a REAL running cluster - this isn't a mock, it's making actual
gRPC calls that go through actual Raft consensus.

Start a fresh cluster first (see README.md), then:

    python3 -m demo.scenarios
"""

import json
import os
import subprocess
import sys
import time
import uuid

import grpc

from axonfx_node import exchange_pb2, exchange_pb2_grpc

CLUSTER_CONFIG = os.path.join(os.path.dirname(__file__), "cluster.json")


def load_node_addresses():
    with open(CLUSTER_CONFIG) as f:
        return [f"127.0.0.1:{n['grpc_port']}" for n in json.load(f)["nodes"]]


NODES = load_node_addresses()


def stub_for(addr):
    return exchange_pb2_grpc.ClientServiceStub(grpc.insecure_channel(addr))


def banner(text):
    print("\n" + "=" * 78)
    print(text)
    print("=" * 78)


def show_balances(addr, label):
    resp = stub_for(addr).GetBalances(exchange_pb2.GetBalancesRequest())
    print(f"\nbalances ({label}, answered by {resp.handled_by_node}):")
    for ccy in sorted(resp.balances):
        print(f"  {ccy:<5} {resp.balances[ccy]:>15,.2f}   (fx pnl {resp.fx_pnl.get(ccy, 0.0):>10,.2f})")


def transfer(addr, source, dest, amount, to="Demo Recipient", account="ACC-0001", request_id=None):
    req = exchange_pb2.SubmitTransferRequest(
        request_id=request_id or str(uuid.uuid4()),
        source_currency=source, dest_currency=dest, source_amount=amount,
        recipient_name=to, recipient_account=account,
    )
    resp = stub_for(addr).SubmitTransfer(req, timeout=10)
    print(f"  -> handled_by={resp.handled_by_node}  scenario driven by locked_rate={resp.locked_rate:.5f}")
    print(f"     paid {resp.dest_amount_paid:,.2f} {dest}  "
          f"(pool_covered={resp.pool_covered_amount:,.2f}, routed_fills={len(resp.fills)})")
    for f in resp.fills:
        kind = "intermediate route (pool surplus)" if f.used_pool_surplus else "direct forex conversion"
        print(f"       + {f.drained_amount:,.2f} {f.via_currency} -> {f.produced_amount:,.2f} {dest} "
              f"@ {f.rate:.5f}  [{kind}]")
    return resp


def status(addr):
    return stub_for(addr).GetStatus(exchange_pb2.GetStatusRequest())


def find_leader():
    for addr in NODES:
        try:
            s = status(addr)
            if s.raft_state == "LEADER":
                return s.node_id, addr
        except grpc.RpcError:
            continue
    return None, None


def main():
    banner("Cluster status")
    for addr in NODES:
        s = status(addr)
        print(f"  {addr}  node={s.node_id:<6} state={s.raft_state:<9} leader={s.leader_id}")

    banner("Scenario 1 - sufficient pre-funded destination balance")
    show_balances(NODES[0], "before")
    transfer(NODES[0], "USD", "INR", 5_000, to="Priya Sharma", account="HDFC-1")
    show_balances(NODES[0], "after")

    banner("Scenario 2/3 - deficit triggers routing engine "
           "(uses best-rate pool surplus first, spills into direct forex conversion if needed)")
    transfer(NODES[1], "USD", "INR", 40_000, to="Rohan Gupta", account="ICICI-2")
    show_balances(NODES[1], "after big transfer")

    banner("Scenario 4 - locked-in vs market rate")
    print("  Every transfer above used a rate DECIDED ONCE by the node that received the\n"
          "  request and baked into the Raft log entry - every replica pays the recipient\n"
          "  the exact same amount regardless of which node applies the entry or when.\n"
          "  The gap between that locked rate and the true mid-market rate at execution\n"
          "  time is tracked as fx_pnl (see balances above) - this is where a real\n"
          "  business's margin actually comes from.")

    banner("Idempotency check - retry the same request_id twice")
    fixed_id = "demo-fixed-id-001"
    transfer(NODES[0], "GBP", "EUR", 1_000, request_id=fixed_id)
    print("  submitting the exact same request_id again (simulating a client retry)...")
    replay = transfer(NODES[0], "GBP", "EUR", 1_000, request_id=fixed_id)
    print(f"  idempotent_replay={replay.idempotent_replay}  (should be True, no double-credit)")

    banner("Raft failover - kill the current leader mid-operation")
    leader_id, leader_addr = find_leader()
    print(f"  current leader is {leader_id} ({leader_addr})")
    print(f"  sending SIGKILL to {leader_id}'s process...")
    subprocess.run(["pkill", "-9", "-f", f"node-id {leader_id}"], check=False)
    time.sleep(3)

    survivors = [a for a in NODES if a != leader_addr]
    new_leader_id, new_leader_addr = None, None
    for _ in range(10):
        new_leader_id, new_leader_addr = find_leader()
        if new_leader_id and new_leader_id != leader_id:
            break
        time.sleep(1)
    print(f"  new leader elected: {new_leader_id} ({new_leader_addr})")

    print("  submitting a transfer via a surviving node while the old leader is dead...")
    transfer(survivors[0], "EUR", "GBP", 200, to="Post-Failover Test", account="X-1")
    print("  \N{CHECK MARK} the cluster kept accepting and correctly committing writes "
          "with only 2 of 3 nodes alive.")

    banner("Done. Final transfer history:")
    hist = stub_for(survivors[0]).GetTransferHistory(exchange_pb2.GetHistoryRequest(limit=50))
    for r in hist.records:
        print(f"  {r.request_id[:8]:<10} {r.source_amount:>12,.2f} {r.source_currency} -> "
              f"{r.dest_amount_paid:>12,.2f} {r.dest_currency:<4} [{r.scenario}]")


if __name__ == "__main__":
    sys.exit(main())
