"""
Entrypoint for a single AxonFx cluster node.

Each node process runs TWO gRPC servers on two different ports:
  - RaftService, on `raft_address` (e.g. 127.0.0.1:6002) - internal
    consensus traffic (RequestVote/AppendEntries) between nodes 2-4 only.
  - ClientService, on `grpc_port` (e.g. 7002) - the public API clients,
    the LLM node, and the CLI talk to.

    python3 -m axonfx_node.server --node-id node2
    python3 -m axonfx_node.server --node-id node3
    python3 -m axonfx_node.server --node-id node4
"""

import argparse
import json
import os
import signal
import socket
import sys
import time
from concurrent import futures

import grpc

from axonfx_node import exchange_pb2_grpc, raft_pb2_grpc
from axonfx_node.state_machine import AxonFxState
from axonfx_node.grpc_service import ClientServiceServicer
from axonfx_node.raft import RaftNode, RaftServiceServicer

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "demo", "cluster.json")


def _require_port_free(host, port, label):
    """
    Raise a clear, immediate error if something is already listening on
    port, instead of silently sharing it.

    gRPC's own add_insecure_port() sets SO_REUSEPORT on Linux, so binding
    the SAME port twice, both servers end up genuinely listening, and the
    kernel splits incoming connections between them arbitrarily.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        probe.bind((host, port))
    except OSError:
        sys.exit(
            f"{label} port {port} is already in use. This --node-id " is
            f"already running? Stop the existing process first.")
    finally:
        probe.close()


def load_cluster(config_path):
    with open(config_path) as f:
        return json.load(f)["nodes"]


def main():
    parser = argparse.ArgumentParser(description="Run one AxonFx Raft cluster node")
    parser.add_argument("--node-id", required=True, help="must match an id in cluster.json")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--data-dir", default=None, help="defaults to data/<node-id>")
    args = parser.parse_args()

    nodes = load_cluster(args.config)
    by_id = {n["id"]: n for n in nodes}
    if args.node_id not in by_id:
        sys.exit(f"unknown --node-id '{args.node_id}', expected one of {list(by_id)}")

    me = by_id[args.node_id]
    raft_peers = {n["id"]: n["raft_address"] for n in nodes if n["id"] != args.node_id}
    client_peers = {n["id"]: f"127.0.0.1:{n['grpc_port']}" for n in nodes if n["id"] != args.node_id}
    data_dir = args.data_dir or os.path.join(
        os.path.dirname(__file__), "..", "data", args.node_id
    )
    os.makedirs(data_dir, exist_ok=True)

    # Fail loudly, immediately, before touching Raft state at all, if
    # either port this node needs is already taken - see
    # _require_port_free()'s docstring for why this can't be left to
    # add_insecure_port() itself to catch. Probed on 0.0.0.0, matching
    # exactly what add_insecure_port() below actually binds to.
    raft_port = int(me["raft_address"].split(":")[1])
    _require_port_free("0.0.0.0", raft_port, "raft_address")
    _require_port_free("0.0.0.0", me["grpc_port"], "grpc_port")

    def log(msg):
        print(f"[{args.node_id}] {msg}", flush=True)

    log(f"starting Raft node at {me['raft_address']}, peers={list(raft_peers)}")
    state_machine = AxonFxState()
    raft_node = RaftNode(args.node_id, raft_peers, data_dir, state_machine, log=log)

    raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServiceServicer(raft_node), raft_server)
    raft_server.add_insecure_port(f"0.0.0.0:{me['raft_address'].split(':')[1]}")
    raft_server.start()
    log(f"RaftService listening on {me['raft_address']}")

    client_server = grpc.server(futures.ThreadPoolExecutor(max_workers=24))
    exchange_pb2_grpc.add_ClientServiceServicer_to_server(
        ClientServiceServicer(raft_node, args.node_id, client_peers, log=log), client_server
    )
    client_addr = f"0.0.0.0:{me['grpc_port']}"
    client_server.add_insecure_port(client_addr)
    client_server.start()
    log(f"gRPC ClientService listening on {client_addr}")

    stopping = {"flag": False}

    def handle_stop(*_):
        if stopping["flag"]:
            return
        stopping["flag"] = True
        log("shutting down...")
        raft_node.stop()
        client_server.stop(grace=1).wait(timeout=2)
        raft_server.stop(grace=1).wait(timeout=2)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_stop()


if __name__ == "__main__":
    main()

