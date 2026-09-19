"""
A from-scratch Raft implementation (Diego Ongaro & John Ousterhout's consensus algorithm - https://raft.github.io/raft.pdf).
The two required RPCs - RequestVote and AppendEntries - are real gRPC calls between the raft cluster's
nodes, defined in proto/raft.proto.

WHAT THIS FILE OWNS: leader election, log replication, and turning a sequence of committed log entries into calls on a
state machine's apply() method, in order, on every node. It knows nothing about transfers, currencies, or matching
engines - it only moves opaque `command` bytes around and guarantees every node applies them in the
same order. That separation is the entire point of a replicated state machine (see state_machine.py's
module docstring for the other documentation - why the *contents* of those commands must be decided once, outside this
file, before they ever reach propose()).

THREADING MODEL: one RLock (self._lock, wrapped in a Condition so propose() can block until its entry is applied) guards
every mutation of Raft state. Three kinds of background threads touch it: one election-timeout timer, one apply loop, and
one replication loop per peer. All network calls (the actual gRPC RequestVote/AppendEntries) are made WITHOUT holding
the lock - only the state needed to build the request is read under the lock, and only the reply is processed under
the lock. Holding a lock across a network call would let one slow or dead peer stall the entire node.
"""

import enum
import os
import pickle
import random
import threading
import time

import grpc

from axonfx_node import raft_pb2, raft_pb2_grpc

ELECTION_TIMEOUT_MIN = 0.3   # seconds
ELECTION_TIMEOUT_MAX = 0.6
HEARTBEAT_INTERVAL = 0.1     # leader -> follower, when caught up
RPC_TIMEOUT = 0.3            # a single RequestVote/AppendEntries call
PROPOSE_TIMEOUT = 8.0        # how long propose() waits for its entry to commit+apply
LEADER_LEASE_TIMEOUT = 1.5   # a leader that hasn't heard back from a majority
                              # within this long steps down on its own - see
                              # _check_leader_lease_locked()

# Default gRPC reconnect backoff grows with repeated failures (can reach
# several seconds). A peer that's briefly down and comes back should be
# rediscovered almost immediately - a slow reconnect here means a
# recovering node keeps missing heartbeats and needlessly starts its own
# election. Shared by every long-lived channel this node opens to a peer.
FAST_RECONNECT_OPTIONS = [
    ("grpc.min_reconnect_backoff_ms", 20),
    ("grpc.max_reconnect_backoff_ms", 100),
    ("grpc.initial_reconnect_backoff_ms", 20),
]


def peer_channel(address):
    return grpc.insecure_channel(address, options=FAST_RECONNECT_OPTIONS)


class Role(enum.IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class LogEntry:
    __slots__ = ("index", "term", "command")

    def __init__(self, index, term, command):
        self.index = index
        self.term = term
        self.command = command  # opaque bytes - see state_machine.py


class ProposeResult:
    def __init__(self, ok, result=None, error=None, leader_hint=None):
        self.ok = ok
        self.result = result
        self.error = error
        self.leader_hint = leader_hint


class RaftNode:
    def __init__(self, node_id, peers, data_dir, state_machine, log=print):
        """
        node_id: this node's id, e.g. "node1"
        peers: dict {node_id: raft_address} for every OTHER node
        data_dir: where to persist raft_state.pkl
        state_machine: object with an .apply(command_bytes) -> result method
        """
        self.node_id = node_id
        self.peers = dict(peers)
        self.data_dir = data_dir
        self.state_machine = state_machine
        self.log_fn = log

        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)

        # Persistent state (Figure 2 of the Raft paper), plus commit_index
        # which the paper treats as volatile - we persist it too so a
        # restarted node can replay and be immediately correct without
        # waiting to re-learn commit_index from the network first. This
        # is a deliberate, documented deviation in the safe direction:
        # commit_index only ever moves forward and only ever reflects
        # genuinely majority-committed entries, so persisting it early
        # cannot violate any safety property.
        self._current_term = 0
        self._voted_for = None
        self._log = [LogEntry(0, 0, None)]  # index 0 is a sentinel, 1-indexed real entries
        self._commit_index = 0

        # Volatile state
        self._role = Role.FOLLOWER
        self._leader_id = None
        self._last_applied = 0
        self._applied_results = {}   # log index -> result of apply()

        # Leader-only volatile state
        self._next_index = {}
        self._match_index = {}
        # last time each peer successfully answered an AppendEntries (accept
        # OR reject both count - either way proves the peer is alive and
        # reachable). Used only by _check_leader_lease_locked() below.
        self._last_ack_time = {}

        self._election_deadline = 0.0
        self._stopped = False

        self._stubs = {}  # peer_id -> RaftServiceStub, lazily created

        self._load_persisted()
        self._replay_committed_locked_at_startup()

        self._threads = [threading.Thread(target=self._election_timer_loop, daemon=True),
                          threading.Thread(target=self._apply_loop, daemon=True)]
        for peer_id in self.peers:
            self._threads.append(threading.Thread(target=self._replication_loop, args=(peer_id,), daemon=True))
        self._reset_election_timer_locked()
        for t in self._threads:
            t.start()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _state_file(self):
        return os.path.join(self.data_dir, "raft_state.pkl")

    def _load_persisted(self):
        path = self._state_file()
        if not os.path.exists(path):
            return
        try:
            with open(path, "rb") as f:
                saved = pickle.load(f)
            self._current_term = saved["current_term"]
            self._voted_for = saved["voted_for"]
            self._log = saved["log"]
            self._commit_index = saved["commit_index"]
            self.log_fn(f"raft: recovered term={self._current_term}, "
                        f"log_len={len(self._log) - 1}, commit_index={self._commit_index} from disk")
        except Exception as exc:
            self.log_fn(f"raft: could not load persisted state ({exc}); starting fresh")

    def _persist_locked(self):
        """Must be called with self._lock held. Atomic write (temp file +
        rename) so a crash mid-write can never corrupt the state file -
        this matters here specifically because the demo deliberately
        kills nodes with SIGKILL at arbitrary moments."""
        os.makedirs(self.data_dir, exist_ok=True)
        path = self._state_file()
        tmp_path = path + ".tmp"
        payload = {
            "current_term": self._current_term,
            "voted_for": self._voted_for,
            "log": self._log,
            "commit_index": self._commit_index,
        }
        with open(tmp_path, "wb") as f:
            pickle.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)  # atomic on POSIX

    def _replay_committed_locked_at_startup(self):
        """Rebuild the state machine from our own persisted log, up to our
        own persisted commit_index, before we've even talked to a peer.
        This is what makes a restarted node's reads immediately correct
        instead of momentarily appearing empty until it re-syncs."""
        for idx in range(1, self._commit_index + 1):
            if idx < len(self._log):
                result = self.state_machine.apply(self._log[idx].command)
                self._applied_results[idx] = result
        self._last_applied = self._commit_index

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------

    def _last_log_index_locked(self):
        return len(self._log) - 1

    def _last_log_term_locked(self):
        return self._log[-1].term

    def _reset_election_timer_locked(self):
        self._election_deadline = time.monotonic() + random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _stub_for(self, peer_id):
        stub = self._stubs.get(peer_id)
        if stub is None:
            stub = raft_pb2_grpc.RaftServiceStub(peer_channel(self.peers[peer_id]))
            self._stubs[peer_id] = stub
        return stub

    def _become_follower_locked(self, term):
        self._current_term = term
        self._voted_for = None
        self._role = Role.FOLLOWER
        self._persist_locked()
        self._reset_election_timer_locked()

    def is_leader(self):
        with self._lock:
            return self._role == Role.LEADER

    def get_status(self):
        with self._lock:
            return {
                "state": int(self._role),
                "leader": self._leader_id,
                "has_quorum": self._leader_id is not None,
                "raft_term": self._current_term,
                "commit_idx": self._commit_index,
                "log_len": len(self._log) - 1,
            }

    def stop(self):
        with self._lock:
            self._stopped = True
            self._cv.notify_all()

    # ------------------------------------------------------------------
    # requestVote(term, candidateId, lastLogIndex, lastLogTerm)
    #   -> requestVoteReply(term, voteGranted)
    # ------------------------------------------------------------------

    def handle_request_vote(self, req):
        with self._lock:
            candidate = req.candidate_id
            if req.term < self._current_term:
                return raft_pb2.RequestVoteReply(term=self._current_term, vote_granted=False)

            if req.term > self._current_term:
                self._become_follower_locked(req.term)

            my_last_index = self._last_log_index_locked()
            my_last_term = self._last_log_term_locked()
            # Section 5.4.1: candidate's log must be at least as up-to-date
            # as ours - compare by term first, then by length.
            log_ok = (req.last_log_term > my_last_term or
                      (req.last_log_term == my_last_term and req.last_log_index >= my_last_index))

            can_vote = self._voted_for in (None, candidate)
            if can_vote and log_ok:
                self._voted_for = candidate
                self._persist_locked()
                self._reset_election_timer_locked()
                return raft_pb2.RequestVoteReply(term=self._current_term, vote_granted=True)

            return raft_pb2.RequestVoteReply(term=self._current_term, vote_granted=False)

    # ------------------------------------------------------------------
    # appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)
    #   -> appendEntriesReply(term, entryAppended, matchIndex)
    # ------------------------------------------------------------------

    def handle_append_entries(self, req):
        with self._lock:
            leader = req.leader_id
            if req.term < self._current_term:
                return raft_pb2.AppendEntriesReply(
                    term=self._current_term, entry_appended=False,
                    match_index=self._last_log_index_locked())

            if req.term > self._current_term:
                self._become_follower_locked(req.term)

            # A valid AppendEntries from the current term's leader is
            # exactly what tells a candidate/follower "there IS a leader" -
            # accept it and reset our own election clock.
            self._role = Role.FOLLOWER
            self._leader_id = leader
            self._reset_election_timer_locked()

            # Consistency check (Section 5.3).
            if req.prev_index > self._last_log_index_locked():
                return raft_pb2.AppendEntriesReply(
                    term=self._current_term, entry_appended=False,
                    match_index=self._last_log_index_locked())
            if req.prev_index > 0 and self._log[req.prev_index].term != req.prev_term:
                conflict_term = self._log[req.prev_index].term
                first_of_term = req.prev_index
                while first_of_term > 1 and self._log[first_of_term - 1].term == conflict_term:
                    first_of_term -= 1
                return raft_pb2.AppendEntriesReply(
                    term=self._current_term, entry_appended=False, match_index=first_of_term - 1)

            # Passed the check - append/overwrite entries, truncating any
            # conflicting suffix (Section 5.3, "then delete the existing
            # entry and all that follow it").
            changed = False
            insert_at = req.prev_index + 1
            for i, e in enumerate(req.entries):
                idx = insert_at + i
                if idx < len(self._log):
                    if self._log[idx].term != e.term:
                        self._log = self._log[:idx]
                        self._log.append(LogEntry(idx, e.term, e.command))
                        changed = True
                    # else identical entry already present - no-op (this
                    # AppendEntries is itself a retried/duplicate RPC)
                else:
                    self._log.append(LogEntry(idx, e.term, e.command))
                    changed = True

            if req.commit_index > self._commit_index:
                self._commit_index = min(req.commit_index, self._last_log_index_locked())
                changed = True
                self._cv.notify_all()

            if changed:
                self._persist_locked()

            return raft_pb2.AppendEntriesReply(
                term=self._current_term, entry_appended=True,
                match_index=self._last_log_index_locked())

    # ------------------------------------------------------------------
    # Writes: propose() is what grpc_service.py calls on the leader.
    # ------------------------------------------------------------------

    def propose(self, command_bytes):
        with self._lock:
            if self._role != Role.LEADER:
                return ProposeResult(ok=False, error="not leader", leader_hint=self._leader_id)
            term = self._current_term
            index = self._last_log_index_locked() + 1
            self._log.append(LogEntry(index, term, command_bytes))
            self._persist_locked()
            self._match_index[self.node_id] = index  # counts toward our own majority

        deadline = time.monotonic() + PROPOSE_TIMEOUT
        with self._cv:
            while True:
                if index in self._applied_results:
                    if self._log[index].term == term:
                        return ProposeResult(ok=True, result=self._applied_results[index])
                    return ProposeResult(ok=False, error="entry was overwritten before committing "
                                                          "(leadership changed mid-flight)")
                if self._role != Role.LEADER or self._current_term != term:
                    return ProposeResult(ok=False, error="lost leadership before this entry committed",
                                          leader_hint=self._leader_id)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ProposeResult(ok=False, error="timed out waiting for commit")
                self._cv.wait(timeout=min(remaining, 0.2))

    # ------------------------------------------------------------------
    # Elections
    # ------------------------------------------------------------------

    def _election_timer_loop(self):
        while True:
            time.sleep(0.01)
            with self._lock:
                if self._stopped:
                    return
                if self._role == Role.LEADER:
                    self._check_leader_lease_locked()
                    continue
                if time.monotonic() < self._election_deadline:
                    continue
                self._current_term += 1
                self._voted_for = self.node_id
                self._role = Role.CANDIDATE
                # Starting our own election means we no longer have a
                # legitimate belief that the previous leader is still
                # valid - clear it, rather than leaving get_status() and
                # propose()'s forwarding hint pointing at a leader we no
                # longer have any basis for trusting (it may well be
                # dead, which is exactly why we're here).
                self._leader_id = None
                self._persist_locked()
                self._reset_election_timer_locked()
                term = self._current_term
                last_index = self._last_log_index_locked()
                last_term = self._last_log_term_locked()
            self.log_fn(f"raft: election timeout, starting election for term {term}")
            self._run_election(term, last_index, last_term)

    def _run_election(self, term, last_index, last_term):
        votes = {self.node_id}
        votes_lock = threading.Lock()

        def request_from(peer_id):
            req = raft_pb2.RequestVoteRequest(
                term=term, candidate_id=self.node_id,
                last_log_index=last_index, last_log_term=last_term)
            reply = self._safe_call(lambda: self._stub_for(peer_id).RequestVote(req, timeout=RPC_TIMEOUT))
            if reply is None:
                return
            with self._lock:
                if reply.term > self._current_term:
                    self._become_follower_locked(reply.term)
                    return
                if self._role != Role.CANDIDATE or self._current_term != term:
                    return
            if reply.vote_granted:
                with votes_lock:
                    votes.add(peer_id)
                    won = len(votes) > (len(self.peers) + 1) // 2
                if won:
                    self._become_leader_if_still_candidate(term)

        threads = [threading.Thread(target=request_from, args=(p,), daemon=True) for p in self.peers]
        for t in threads:
            t.start()

    def _become_leader_if_still_candidate(self, term):
        with self._lock:
            if self._role != Role.CANDIDATE or self._current_term != term:
                return
            self._role = Role.LEADER
            self._leader_id = self.node_id
            last_index = self._last_log_index_locked()
            now = time.monotonic()
            for peer_id in self.peers:
                self._next_index[peer_id] = last_index + 1
                self._match_index[peer_id] = 0
                # Grace period: don't let the lease check fire before the
                # replication loops have even had a chance to try - treat
                # "just became leader" as "just heard from everyone".
                self._last_ack_time[peer_id] = now
            self._match_index[self.node_id] = last_index
        self.log_fn(f"raft: elected leader for term {term}")

    def _check_leader_lease_locked(self):
        """Called only while we're LEADER, on every election-timer tick.
        Being able to append to our own log and still call ourselves
        LEADER doesn't mean anything if we can no longer reach a majority
        of the cluster - e.g. a network partition, or simply losing
        contact with enough peers, can leave a leader happily talking to
        a minority of followers while genuinely unable to commit anything
        new. Rather than let writes silently hang until PROPOSE_TIMEOUT
        (8s) before failing, check directly: has a majority of the
        cluster (peers + self) acknowledged us - accept OR reject, either
        proves they're reachable - within the last LEADER_LEASE_TIMEOUT?
        If not, step down now, so status/propose() fail fast and honestly
        instead of pretending everything's fine."""
        now = time.monotonic()
        recent = 1  # ourselves - always "in contact" with our own log
        for peer_id in self.peers:
            last = self._last_ack_time.get(peer_id, 0.0)
            if now - last < LEADER_LEASE_TIMEOUT:
                recent += 1
        n_nodes = len(self.peers) + 1
        if recent <= n_nodes // 2:
            self.log_fn(f"raft: lost contact with a majority ({recent}/{n_nodes} recently "
                        f"reachable) - stepping down from leader")
            self._role = Role.FOLLOWER
            self._leader_id = None
            self._reset_election_timer_locked()

    # ------------------------------------------------------------------
    # Replication (also serves as heartbeats when a peer is caught up)
    # ------------------------------------------------------------------

    def _replication_loop(self, peer_id):
        while True:
            with self._lock:
                if self._stopped:
                    return
                if self._role != Role.LEADER:
                    am_leader = False
                else:
                    am_leader = True
                    term = self._current_term
                    next_idx = self._next_index.get(peer_id, self._last_log_index_locked() + 1)
                    prev_index = next_idx - 1
                    prev_term = self._log[prev_index].term if prev_index < len(self._log) else 0
                    entries = self._log[next_idx:] if next_idx < len(self._log) else []
                    commit_index = self._commit_index
            if not am_leader:
                time.sleep(0.05)
                continue

            req = raft_pb2.AppendEntriesRequest(
                term=term, leader_id=self.node_id, prev_index=prev_index, prev_term=prev_term,
                commit_index=commit_index,
                entries=[raft_pb2.LogEntry(index=e.index, term=e.term, command=e.command) for e in entries])
            reply = self._safe_call(lambda: self._stub_for(peer_id).AppendEntries(req, timeout=RPC_TIMEOUT))

            caught_up = True
            if reply is not None:
                with self._lock:
                    self._last_ack_time[peer_id] = time.monotonic()
                    if reply.term > self._current_term:
                        self._become_follower_locked(reply.term)
                    elif self._role == Role.LEADER and self._current_term == term:
                        if reply.entry_appended:
                            new_match = prev_index + len(entries)
                            if new_match > self._match_index.get(peer_id, 0):
                                self._match_index[peer_id] = new_match
                            self._next_index[peer_id] = self._match_index[peer_id] + 1
                            self._maybe_advance_commit_index_locked()
                            caught_up = self._next_index[peer_id] > self._last_log_index_locked()
                        else:
                            self._next_index[peer_id] = max(1, reply.match_index + 1)
                            caught_up = False
            else:
                caught_up = False  # RPC failed/timed out - retry soon, don't wait a full heartbeat

            time.sleep(HEARTBEAT_INTERVAL if caught_up else 0.02)

    def _maybe_advance_commit_index_locked(self):
        """Section 5.3/5.4.2: a leader may only advance commitIndex to N if
        a majority of matchIndex[] >= N AND log[N] is from the leader's
        OWN current term - committing an older term's entry directly
        would risk it later being overwritten by a future leader who
        never saw it, which is exactly the safety violation the paper warns about."""
        n_nodes = len(self.peers) + 1
        for n in range(self._last_log_index_locked(), self._commit_index, -1):
            if self._log[n].term != self._current_term:
                continue
            count = sum(1 for m in self._match_index.values() if m >= n)
            if count > n_nodes // 2:
                self._commit_index = n
                self._cv.notify_all()
                return

    # ------------------------------------------------------------------
    # Applying committed entries to the state machine
    # ------------------------------------------------------------------

    def _apply_loop(self):
        while True:
            with self._cv:
                while not self._stopped and self._last_applied >= self._commit_index:
                    self._cv.wait(timeout=0.5)
                if self._stopped:
                    return
                to_apply = list(range(self._last_applied + 1, self._commit_index + 1))
            for idx in to_apply:
                with self._lock:
                    if idx >= len(self._log):
                        break
                    command = self._log[idx].command
                result = self.state_machine.apply(command)
                with self._cv:
                    self._applied_results[idx] = result
                    self._last_applied = idx
                    self._persist_locked()
                    self._cv.notify_all()

    # ------------------------------------------------------------------
    def _safe_call(self, fn):
        try:
            return fn()
        except grpc.RpcError:
            return None


class RaftServiceServicer(raft_pb2_grpc.RaftServiceServicer):
    """Thin gRPC-facing wrapper - all real logic lives on RaftNode above."""

    def __init__(self, node: RaftNode):
        self.node = node

    def RequestVote(self, request, context):
        return self.node.handle_request_vote(request)

    def AppendEntries(self, request, context):
        return self.node.handle_append_entries(request)
