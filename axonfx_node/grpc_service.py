"""
The gRPC-facing layer for client traffic (transfers, balance/history
reads, status). Two jobs, same as before:

1. Take the ONE rate snapshot per request (see state_machine.py's module
   docstring) before building a command - this file is the only caller
   of take_snapshot() in the whole project.
2. If this node isn't the current Raft leader, transparently forward the
   write to whichever node is, so a client can hit ANY of the three
   cluster nodes and still get a correct answer.
"""

import uuid

import grpc

from axonfx_node import exchange_pb2, exchange_pb2_grpc
from axonfx_node.raft import peer_channel
from axonfx_node.state_machine import take_snapshot, encode_submit_transfer, CURRENCIES


class ClientServiceServicer(exchange_pb2_grpc.ClientServiceServicer):
    def __init__(self, raft_node, node_id, peer_client_addresses, log=print):
        """
        raft_node: this node's RaftNode instance
        peer_client_addresses: dict {node_id: "host:grpc_port"} for every
            OTHER node - used only to forward a write to the leader.
        """
        self.raft_node = raft_node
        self.state = raft_node.state_machine
        self.node_id = node_id
        self.peer_client_addresses = peer_client_addresses
        self.log = log
        self._forward_stubs = {}

    def _forward_stub(self, peer_node_id):
        stub = self._forward_stubs.get(peer_node_id)
        if stub is None:
            addr = self.peer_client_addresses[peer_node_id]
            stub = exchange_pb2_grpc.ClientServiceStub(peer_channel(addr))
            self._forward_stubs[peer_node_id] = stub
        return stub

    def SubmitTransfer(self, request, context):
        if request.source_currency not in CURRENCIES or request.dest_currency not in CURRENCIES:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"unsupported currency; supported: {sorted(CURRENCIES)}")
            return exchange_pb2.SubmitTransferResponse(success=False, error="unsupported currency")
        if request.source_currency == request.dest_currency:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("source and destination currency must differ")
            return exchange_pb2.SubmitTransferResponse(success=False, error="same currency")
        if request.source_amount <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("source_amount must be positive")
            return exchange_pb2.SubmitTransferResponse(success=False, error="invalid amount")

        request_id = request.request_id or str(uuid.uuid4())

        # The one and only place "the market" gets consulted for this
        # request - everything downstream is a deterministic replay of
        # these exact numbers. See state_machine.py.
        snapshot = take_snapshot()
        locked_rate = snapshot.locked_rate(request.source_currency, request.dest_currency)
        mid_rate = snapshot.mid_cross(request.source_currency, request.dest_currency)
        cross_rates = snapshot.cross_rate_table()

        command = encode_submit_transfer(
            request_id, request.source_currency, request.dest_currency, request.source_amount,
            locked_rate, mid_rate, cross_rates, request.recipient_name, request.recipient_account,
            snapshot.taken_at)

        outcome = self.raft_node.propose(command)

        if not outcome.ok and outcome.error == "not leader" and outcome.leader_hint:
            return self._forward(outcome.leader_hint, request, context)
        if not outcome.ok:
            self.log(f"[{self.node_id}] submit_transfer failed: {outcome.error}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"cluster could not commit this write: {outcome.error}")
            return exchange_pb2.SubmitTransferResponse(success=False, error=outcome.error, handled_by_node=self.node_id)

        result = outcome.result
        fills_pb = [
            exchange_pb2.FillDetail(
                via_currency=f["via_currency"], drained_amount=f["drained"],
                produced_amount=f["produced"], rate=f["rate"],
                used_pool_surplus=f["used_pool_surplus"],
            )
            for f in result["fills"]
        ]
        return exchange_pb2.SubmitTransferResponse(
            success=result["success"],
            request_id=result["request_id"],
            dest_amount_paid=result["dest_amount_paid"],
            locked_rate=result["locked_rate"],
            pool_covered_amount=result["pool_covered_amount"],
            fills=fills_pb,
            handled_by_node=self.node_id,
            idempotent_replay=result["idempotent_replay"],
        )

    def _forward(self, leader_node_id, request, context):
        if leader_node_id not in self.peer_client_addresses:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("not leader, and leader id is unknown to this node")
            return exchange_pb2.SubmitTransferResponse(success=False, error="no known leader", handled_by_node=self.node_id)
        try:
            return self._forward_stub(leader_node_id).SubmitTransfer(request, timeout=6.0)
        except grpc.RpcError as e:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f"could not reach leader {leader_node_id}: {e}")
            return exchange_pb2.SubmitTransferResponse(success=False, error=str(e), handled_by_node=self.node_id)

    def GetBalances(self, request, context):
        return exchange_pb2.GetBalancesResponse(
            balances=self.state.read_balances(),
            fx_pnl=self.state.read_fx_pnl(),
            handled_by_node=self.node_id,
        )

    def GetStatus(self, request, context):
        status = self.raft_node.get_status()
        RAFT_STATE_NAMES = {0: "FOLLOWER", 1: "CANDIDATE", 2: "LEADER"}
        return exchange_pb2.GetStatusResponse(
            node_id=self.node_id,
            raft_state=RAFT_STATE_NAMES.get(status.get("state"), "UNKNOWN"),
            leader_id=status.get("leader") or "",
            has_quorum=bool(status.get("has_quorum")),
            raft_term=status.get("raft_term", 0),
            commit_index=status.get("commit_idx", 0),
            log_len=status.get("log_len", 0),
        )

    def GetTransferHistory(self, request, context):
        limit = request.limit or 20
        records = self.state.read_history(limit)
        return exchange_pb2.GetHistoryResponse(records=[
            exchange_pb2.TransferRecord(
                request_id=r["request_id"], source_currency=r["source_currency"],
                dest_currency=r["dest_currency"], source_amount=r["source_amount"],
                dest_amount_paid=r["dest_amount_paid"], locked_rate=r["locked_rate"],
                scenario=r["scenario"], timestamp=r["timestamp"],
            )
            for r in records
        ])
