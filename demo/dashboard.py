"""
Live dashboard for the AxonFx cluster.
It's just an observability tool for demos. Run it in a second terminal/pane
while running the actual commands elsewhere, and we can watch leadership move, a node go down and come
back, and the ledger stay identical across every live node - all in
real time, without having to type `status`/`balances` over and over.

    python3 -m demo.dashboard

Polls every node once a second over the same public gRPC ClientService
API any client uses - it has no special access and doesn't touch Raft
internals directly.
"""

import json
import os
import socket
import sys
import time

import grpc

from axonfx_node import exchange_pb2, exchange_pb2_grpc

CLUSTER_CONFIG = os.path.join(os.path.dirname(__file__), "cluster.json")
LLM_PORT = 7100
REFRESH_SECONDS = 1.0
RPC_TIMEOUT = 0.8

GREEN = "\033[92m"
BLUE = "\033[94m"
YELLOW = "\033[93m"
RED = "\033[91m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"

ROLE_COLOR = {"LEADER": GREEN, "FOLLOWER": BLUE, "CANDIDATE": YELLOW}


def load_nodes():
    with open(CLUSTER_CONFIG) as f:
        return json.load(f)["nodes"]


def port_open(host, port, timeout=0.4):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def fetch_status(addr):
    try:
        stub = exchange_pb2_grpc.ClientServiceStub(grpc.insecure_channel(addr))
        return stub.GetStatus(exchange_pb2.GetStatusRequest(), timeout=RPC_TIMEOUT)
    except grpc.RpcError:
        return None


def fetch_balances(addr):
    try:
        stub = exchange_pb2_grpc.ClientServiceStub(grpc.insecure_channel(addr))
        return stub.GetBalances(exchange_pb2.GetBalancesRequest(), timeout=RPC_TIMEOUT)
    except grpc.RpcError:
        return None


def fetch_last_transfer(addr):
    try:
        stub = exchange_pb2_grpc.ClientServiceStub(grpc.insecure_channel(addr))
        resp = stub.GetTransferHistory(exchange_pb2.GetHistoryRequest(limit=1), timeout=RPC_TIMEOUT)
        return resp.records[0] if resp.records else None
    except grpc.RpcError:
        return None


def render(nodes):
    lines = [
        f"{BOLD}AxonFx Live Dashboard{RESET}  "
        f"{DIM}(refreshing every {REFRESH_SECONDS:.0f}s - Ctrl+C to exit){RESET}",
        "=" * 70,
        "",
        f"{BOLD}RAFT CLUSTER (Nodes 2-4){RESET}",
    ]

    statuses = {}
    for n in nodes:
        addr = f"127.0.0.1:{n['grpc_port']}"
        status = fetch_status(addr)
        statuses[n["id"]] = status
        if status is None:
            lines.append(f"  {n['id']:<8} {addr:<18} {RED}DOWN{RESET}")
        else:
            role = status.raft_state
            color = ROLE_COLOR.get(role, "")
            lines.append(
                f"  {n['id']:<8} {addr:<18} {color}{role:<9}{RESET} "
                f"term={status.raft_term:<4} commit={status.commit_index:<4} log={status.log_len:<4}"
            )

    lines.append("")
    lines.append(f"{BOLD}NODE 1: LLM SERVER (independent){RESET}")
    llm_up = port_open("127.0.0.1", LLM_PORT)
    llm_line = f"  llm_node  127.0.0.1:{LLM_PORT}  "
    llm_line += f"{GREEN}UP{RESET}" if llm_up else f"{RED}DOWN{RESET}"
    lines.append(llm_line)

    lines.append("")
    lines.append(f"{BOLD}LEDGER - as seen by each live node (should always match){RESET}")

    balances_by_node = {}
    last_transfer = None
    for n in nodes:
        if statuses.get(n["id"]) is not None:
            addr = f"127.0.0.1:{n['grpc_port']}"
            bal = fetch_balances(addr)
            if bal is not None:
                balances_by_node[n["id"]] = dict(bal.balances)
            if last_transfer is None:
                last_transfer = fetch_last_transfer(addr)

    if not balances_by_node:
        lines.append(f"  {RED}no reachable node - cluster has no quorum{RESET}")
    else:
        alive_ids = list(balances_by_node.keys())
        lines.append("  " + "Currency".ljust(9) + "".join(nid.ljust(16) for nid in alive_ids))
        currencies = sorted(next(iter(balances_by_node.values())).keys())
        all_match = True
        for ccy in currencies:
            values = [balances_by_node[nid].get(ccy, 0.0) for nid in alive_ids]
            match = len(set(round(v, 2) for v in values)) == 1
            all_match = all_match and match
            row = "  " + ccy.ljust(9) + "".join(f"{v:>14,.2f}  " for v in values)
            lines.append(row)
        lines.append("")
        if all_match:
            lines.append(f"  {GREEN}\u2713 all live nodes agree{RESET}")
        else:
            lines.append(f"  {RED}\u2717 MISMATCH DETECTED{RESET}")

    lines.append("")
    lines.append(f"{BOLD}LAST TRANSFER{RESET}")
    if last_transfer:
        lines.append(
            f"  {last_transfer.request_id[:8]}  {last_transfer.source_amount:,.2f} "
            f"{last_transfer.source_currency} -> {last_transfer.dest_amount_paid:,.2f} "
            f"{last_transfer.dest_currency}  [{last_transfer.scenario}]"
        )
    else:
        lines.append(f"  {DIM}none yet{RESET}")

    return "\n".join(lines)


def main():
    nodes = load_nodes()
    try:
        while True:
            output = render(nodes)
            sys.stdout.write("\033[2J\033[H")  # clear screen, cursor home - in-place refresh
            sys.stdout.write(output + "\n")
            sys.stdout.flush()
            time.sleep(REFRESH_SECONDS)
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    main()

