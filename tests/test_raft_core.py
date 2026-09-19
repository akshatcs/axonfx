"""
Tests the four required RPC handlers' decision logic directly - term
comparisons, the log-matching consistency check, vote-granting rules -
by calling handle_request_vote()/handle_append_entries() with hand-built
protobuf messages. No gRPC server, no peers, no real networking: peers={}
so the background replication loops have nothing to do, which is what
makes this safe to run as a fast, isolated unit test.
"""
import shutil
import sys
import tempfile
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from axonfx_node import raft_pb2
from axonfx_node.raft import RaftNode, Role, LEADER_LEASE_TIMEOUT


class FakeStateMachine:
    def __init__(self):
        self.applied = []

    def apply(self, command):
        self.applied.append(command)
        return {"ok": True, "n": len(self.applied)}


def make_node():
    tmp = tempfile.mkdtemp()
    node = RaftNode("node1", peers={}, data_dir=tmp, state_machine=FakeStateMachine())
    return node, tmp


def make_node_with_unreachable_peers():
    # Fake addresses with nothing listening - real gRPC calls to them will
    # always fail, which is exactly what the lease test needs: guaranteed
    # zero acks from "peers" that exist on paper but can never respond.
    tmp = tempfile.mkdtemp()
    peers = {"peerA": "127.0.0.1:1", "peerB": "127.0.0.1:2"}
    node = RaftNode("node1", peers=peers, data_dir=tmp, state_machine=FakeStateMachine())
    return node, tmp


def cleanup(node, tmp):
    node.stop()
    shutil.rmtree(tmp, ignore_errors=True)


def rv_request(candidate_id, term, last_log_index=0, last_log_term=0):
    return raft_pb2.RequestVoteRequest(
        candidate_id=candidate_id, term=term,
        last_log_index=last_log_index, last_log_term=last_log_term)


def ae_request(leader_id, term, prev_index, prev_term, commit_index, entries=()):
    return raft_pb2.AppendEntriesRequest(
        leader_id=leader_id, term=term, prev_index=prev_index,
        prev_term=prev_term, commit_index=commit_index, entries=list(entries))


def entry(index, term, command=b""):
    return raft_pb2.LogEntry(index=index, term=term, command=command)


def test_grants_vote_for_higher_term_with_up_to_date_log():
    node, tmp = make_node()
    try:
        reply = node.handle_request_vote(rv_request("node2", term=1))
        assert reply.vote_granted is True
        assert reply.term == 1
    finally:
        cleanup(node, tmp)


def test_rejects_vote_for_stale_term():
    node, tmp = make_node()
    try:
        node.handle_request_vote(rv_request("node2", term=5))  # bumps us to term 5
        reply = node.handle_request_vote(rv_request("node3", term=2))  # stale
        assert reply.vote_granted is False
        assert reply.term == 5
    finally:
        cleanup(node, tmp)


def test_rejects_second_vote_in_same_term_for_different_candidate():
    node, tmp = make_node()
    try:
        first = node.handle_request_vote(rv_request("node2", term=1))
        second = node.handle_request_vote(rv_request("node3", term=1))
        assert first.vote_granted is True
        assert second.vote_granted is False  # already voted for node2 this term
    finally:
        cleanup(node, tmp)


def test_grants_repeat_vote_to_same_candidate_same_term():
    # A retried RequestVote RPC from the same candidate must still succeed.
    node, tmp = make_node()
    try:
        first = node.handle_request_vote(rv_request("node2", term=1))
        second = node.handle_request_vote(rv_request("node2", term=1))
        assert first.vote_granted is True
        assert second.vote_granted is True
    finally:
        cleanup(node, tmp)


def test_rejects_vote_when_candidate_log_is_behind():
    node, tmp = make_node()
    try:
        # Give node1 a log entry at term 3 first, via a leader AppendEntries.
        node.handle_append_entries(ae_request("leaderX", term=3, prev_index=0, prev_term=0,
                                               commit_index=0, entries=[entry(1, 3, b"cmd")]))
        # A candidate whose log is older (term 2) should be rejected even
        # though its RPC term (4) is higher than ours.
        reply = node.handle_request_vote(rv_request("node2", term=4, last_log_index=1, last_log_term=2))
        assert reply.vote_granted is False
    finally:
        cleanup(node, tmp)


def test_append_entries_rejects_stale_term():
    node, tmp = make_node()
    try:
        node.handle_append_entries(ae_request("leaderX", term=5, prev_index=0, prev_term=0, commit_index=0))
        reply = node.handle_append_entries(ae_request("oldLeader", term=2, prev_index=0, prev_term=0, commit_index=0))
        assert reply.entry_appended is False
        assert reply.term == 5
    finally:
        cleanup(node, tmp)


def test_append_entries_consistency_check_rejects_mismatched_prev_term():
    node, tmp = make_node()
    try:
        node.handle_append_entries(ae_request("leaderX", term=1, prev_index=0, prev_term=0,
                                               commit_index=0, entries=[entry(1, 1, b"a")]))
        # Claim prev_index=1 was from term 2, but we actually stored term 1 there.
        reply = node.handle_append_entries(ae_request("leaderX", term=2, prev_index=1, prev_term=2, commit_index=0))
        assert reply.entry_appended is False
    finally:
        cleanup(node, tmp)


def test_append_entries_appends_and_advances_commit_index_and_applies():
    node, tmp = make_node()
    try:
        reply = node.handle_append_entries(ae_request(
            "leaderX", term=1, prev_index=0, prev_term=0, commit_index=1,
            entries=[entry(1, 1, b"cmd-a")]))
        assert reply.entry_appended is True
        assert reply.match_index == 1

        # Give the apply loop a moment to pick up the new commit_index.
        import time
        for _ in range(50):
            if node.get_status()["commit_idx"] >= 1 and node.state_machine.applied:
                break
            time.sleep(0.02)
        assert node.get_status()["commit_idx"] == 1
        assert node.state_machine.applied == [b"cmd-a"]
    finally:
        cleanup(node, tmp)


def test_append_entries_truncates_conflicting_suffix():
    node, tmp = make_node()
    try:
        # Term 1 leader writes two entries.
        node.handle_append_entries(ae_request(
            "leader1", term=1, prev_index=0, prev_term=0, commit_index=0,
            entries=[entry(1, 1, b"a"), entry(2, 1, b"b")]))
        assert node.get_status()["log_len"] == 2

        # A term-2 leader overwrites index 2 with a different entry -
        # the old index-2 entry must be discarded, not kept alongside it.
        reply = node.handle_append_entries(ae_request(
            "leader2", term=2, prev_index=1, prev_term=1, commit_index=0,
            entries=[entry(2, 2, b"c")]))
        assert reply.entry_appended is True
        assert node.get_status()["log_len"] == 2  # still 2 entries, not 3
    finally:
        cleanup(node, tmp)


def test_leader_id_clears_when_becoming_candidate():
    # Regression test: a node must not keep reporting a stale leader (and
    # has_quorum=True) once it's cut off and starts its own election -
    # that's actively misleading, not just harmlessly out of date.
    import time
    node, tmp = make_node()
    try:
        node.handle_append_entries(ae_request("leaderX", term=1, prev_index=0, prev_term=0, commit_index=0))
        assert node.get_status()["leader"] == "leaderX"

        # peers={} means this node will never win an election and will
        # keep retrying - wait for it to become a candidate on its own.
        deadline = time.monotonic() + 1.5
        status = node.get_status()
        while time.monotonic() < deadline and status["state"] != int(Role.CANDIDATE):
            time.sleep(0.02)
            status = node.get_status()

        assert status["state"] == int(Role.CANDIDATE)
        assert status["leader"] is None
        assert status["has_quorum"] is False
    finally:
        cleanup(node, tmp)


def test_leader_steps_down_after_losing_majority_contact():
    # Regression test for a real bug found while testing a 4-node cluster:
    # a leader that keeps a minority of the cluster happy (e.g. itself
    # plus one follower, out of four total) has no way to know it's lost
    # its majority unless it actively checks. Without this check, writes
    # would hang until PROPOSE_TIMEOUT (8s) instead of failing fast, and
    # has_quorum would misleadingly stay True the whole time.
    import time
    node, tmp = make_node_with_unreachable_peers()
    try:
        with node._lock:
            node._role = Role.LEADER
            node._leader_id = node.node_id
            node._last_ack_time = {}  # no acks at all - peers are unreachable

        deadline = time.monotonic() + LEADER_LEASE_TIMEOUT + 1.0
        status = node.get_status()
        while time.monotonic() < deadline and status["state"] == int(Role.LEADER):
            time.sleep(0.05)
            status = node.get_status()

        assert status["state"] == int(Role.FOLLOWER), "leader should have stepped down"
        assert status["leader"] is None
        assert status["has_quorum"] is False
    finally:
        cleanup(node, tmp)


if __name__ == "__main__":
    test_grants_vote_for_higher_term_with_up_to_date_log()
    test_rejects_vote_for_stale_term()
    test_rejects_second_vote_in_same_term_for_different_candidate()
    test_grants_repeat_vote_to_same_candidate_same_term()
    test_rejects_vote_when_candidate_log_is_behind()
    test_append_entries_rejects_stale_term()
    test_append_entries_consistency_check_rejects_mismatched_prev_term()
    test_append_entries_appends_and_advances_commit_index_and_applies()
    test_append_entries_truncates_conflicting_suffix()
    test_leader_id_clears_when_becoming_candidate()
    test_leader_steps_down_after_losing_majority_contact()
    print("all raft core tests passed")
