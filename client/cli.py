"""
Minimal CLI client for the AxonFx demo cluster.

Usage:
    python3 -m client.cli --node 127.0.0.1:7002 status
    python3 -m client.cli --node 127.0.0.1:7002 balances
    python3 -m client.cli --node 127.0.0.1:7002 transfer USD INR 10000 --to "Priya Sharma" --account "HDFC-x1234"
    python3 -m client.cli --node 127.0.0.1:7002 history

We can point --node at ANY node in the cluster, not just the leader - the node we contact forwards writes to the leader internally.
"""

import argparse
import sys
import uuid

import grpc

from axonfx_node import exchange_pb2, exchange_pb2_grpc


def connect(node_addr):
    channel = grpc.insecure_channel(node_addr)
    return exchange_pb2_grpc.ClientServiceStub(channel)


def cmd_status(stub, args):
    resp = stub.GetStatus(exchange_pb2.GetStatusRequest())
    print(f"node:        {resp.node_id}")
    print(f"raft state:  {resp.raft_state}")
    print(f"leader:      {resp.leader_id or '(none yet)'}")
    print(f"has quorum:  {resp.has_quorum}")
    print(f"raft term:   {resp.raft_term}")
    print(f"commit idx:  {resp.commit_index}")
    print(f"log length:  {resp.log_len}")


def cmd_balances(stub, args):
    resp = stub.GetBalances(exchange_pb2.GetBalancesRequest())
    print(f"(as seen by {resp.handled_by_node})")
    print(f"{'currency':<10}{'pool balance':>18}{'fx pnl':>16}")
    for ccy in sorted(resp.balances):
        print(f"{ccy:<10}{resp.balances[ccy]:>18,.2f}{resp.fx_pnl.get(ccy, 0.0):>16,.2f}")


def cmd_transfer(stub, args):
    req = exchange_pb2.SubmitTransferRequest(
        request_id=args.request_id or str(uuid.uuid4()),
        source_currency=args.source_currency.upper(),
        dest_currency=args.dest_currency.upper(),
        source_amount=args.amount,
        recipient_name=args.to,
        recipient_account=args.account,
    )
    try:
        resp = stub.SubmitTransfer(req, timeout=10)
    except grpc.RpcError as e:
        print(f"ERROR: {e.code()}: {e.details()}", file=sys.stderr)
        sys.exit(1)

    if not resp.success:
        print(f"FAILED: {resp.error}", file=sys.stderr)
        sys.exit(1)

    print(f"handled by:        {resp.handled_by_node}"
          f"{' (idempotent replay)' if resp.idempotent_replay else ''}")
    print(f"request id:        {resp.request_id}")
    print(f"locked rate:       {resp.locked_rate:.6f}")
    print(f"paid recipient:    {resp.dest_amount_paid:,.2f} {args.dest_currency.upper()}")
    print(f"covered from pool: {resp.pool_covered_amount:,.2f} {args.dest_currency.upper()}")
    if resp.fills:
        print("routed/forex fills:")
        for f in resp.fills:
            kind = "intermediate pool surplus" if f.used_pool_surplus else "direct forex conversion"
            print(f"  - {f.drained_amount:,.2f} {f.via_currency} -> "
                  f"{f.produced_amount:,.2f} {args.dest_currency.upper()} "
                  f"@ {f.rate:.6f}  ({kind})")


def cmd_history(stub, args):
    resp = stub.GetTransferHistory(exchange_pb2.GetHistoryRequest(limit=args.limit))
    for r in resp.records:
        print(f"{r.request_id[:8]}  {r.source_amount:>12,.2f} {r.source_currency} -> "
              f"{r.dest_amount_paid:>12,.2f} {r.dest_currency:<4} "
              f"[{r.scenario}]")


def main():
    parser = argparse.ArgumentParser(description="AxonFx demo CLI")
    parser.add_argument("--node", default="127.0.0.1:7002", help="host:grpc_port of any cluster node")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status")
    sub.add_parser("balances")

    p_transfer = sub.add_parser("transfer")
    p_transfer.add_argument("source_currency")
    p_transfer.add_argument("dest_currency")
    p_transfer.add_argument("amount", type=float)
    p_transfer.add_argument("--to", default="Test Recipient")
    p_transfer.add_argument("--account", default="ACC-0000")
    p_transfer.add_argument("--request-id", default=None)

    p_history = sub.add_parser("history")
    p_history.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()
    stub = connect(args.node)

    {
        "status": cmd_status,
        "balances": cmd_balances,
        "transfer": cmd_transfer,
        "history": cmd_history,
    }[args.command](stub, args)


if __name__ == "__main__":
    main()

