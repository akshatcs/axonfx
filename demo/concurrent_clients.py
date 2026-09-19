"""
Node 5: simulates several concurrent users hitting the cluster at once.

Two things this proves that firing requests one at a time can't:
1. Many distinct transfers submitted at the same moment, spread across
   all three nodes, all succeed without any crashes or errors.
2. The SAME request_id, when submitted from several threads at the exact same
   instant (a real race, not a sequential retry), still results in
   exactly ONE real execution - proving idempotency holds under actual
   concurrency, not just when requests happen to arrive one after another.

    python3 -m demo.concurrent_clients
"""

import json
import os
import random
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

from axonfx_node import exchange_pb2, exchange_pb2_grpc
from axonfx_node.state_machine import CURRENCIES

CLUSTER_CONFIG = os.path.join(os.path.dirname(__file__), "cluster.json")


def load_node_addresses():
    with open(CLUSTER_CONFIG) as f:
        return [f"127.0.0.1:{n['grpc_port']}" for n in json.load(f)["nodes"]]


NODES = load_node_addresses()
NUM_CONCURRENT_USERS = 20
NUM_RETRY_RACES = 5
RACE_FANOUT = 4  # this many threads submit the identical request_id at once


def stub_for(addr):
    return exchange_pb2_grpc.ClientServiceStub(grpc.insecure_channel(addr))


def submit(request_id, source, dest, amount, label):
    node = random.choice(NODES)
    req = exchange_pb2.SubmitTransferRequest(
        request_id=request_id, source_currency=source, dest_currency=dest,
        source_amount=amount, recipient_name=label, recipient_account=f"ACC-{request_id[:8]}",
    )
    try:
        return stub_for(node).SubmitTransfer(req, timeout=15)
    except grpc.RpcError as e:
        return e


def simulated_user(i):
    source, dest = random.sample(CURRENCIES, 2)
    amount = round(random.uniform(50, 2_000), 2)
    return submit(f"user-{i}-{uuid.uuid4().hex[:6]}", source, dest, amount, f"Sim User {i}")


def retry_race(i):
    """Threads submit the identical request_id simultaneously."""
    source, dest = random.sample(CURRENCIES, 2)
    amount = round(random.uniform(50, 2_000), 2)
    request_id = f"race-{i}-{uuid.uuid4().hex[:6]}"
    barrier = threading.Barrier(RACE_FANOUT)
    results = [None] * RACE_FANOUT

    def worker(idx):
        barrier.wait()  # line everyone up to submit as simultaneously as possible
        results[idx] = submit(request_id, source, dest, amount, f"Race Sim {i}")

    threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(RACE_FANOUT)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return request_id, results


def main():
    print(f"Firing {NUM_CONCURRENT_USERS} concurrent users + {NUM_RETRY_RACES} idempotency "
          f"races ({RACE_FANOUT} simultaneous identical submissions each) across {len(NODES)} nodes...\n")

    with ThreadPoolExecutor(max_workers=NUM_CONCURRENT_USERS + NUM_RETRY_RACES) as pool:
        user_futs = [pool.submit(simulated_user, i) for i in range(NUM_CONCURRENT_USERS)]
        race_futs = [pool.submit(retry_race, i) for i in range(NUM_RETRY_RACES)]

        failures = 0
        for f in as_completed(user_futs):
            resp = f.result()
            if isinstance(resp, Exception) or not getattr(resp, "success", False):
                failures += 1
                print(f"  concurrent user transfer FAILED: {resp}")

        print(f"\n{NUM_CONCURRENT_USERS - failures}/{NUM_CONCURRENT_USERS} concurrent transfers "
              f"succeeded with no crashes.\n")

        print("Idempotency races (proving no double-application under real concurrency):")
        race_bugs = 0
        for f in as_completed(race_futs):
            request_id, results = f.result()
            successes = [r for r in results if not isinstance(r, Exception) and r.success]
            real = [r for r in successes if not r.idempotent_replay]
            replays = [r for r in successes if r.idempotent_replay]
            ok = len(real) == 1
            if not ok:
                race_bugs += 1
            print(f"  {request_id:<20} {RACE_FANOUT} simultaneous submits -> "
                  f"{len(real)} real execution(s), {len(replays)} replay(s)  "
                  f"[{'OK' if ok else '*** BUG ***'}]")

    print()
    if failures or race_bugs:
        print(f"RESULT: FAIL - {failures} failed transfer(s), {race_bugs} idempotency bug(s)")
        return 1
    print(f"RESULT: PASS - {NUM_CONCURRENT_USERS} concurrent users all succeeded, "
          f"{NUM_RETRY_RACES}/{NUM_RETRY_RACES} idempotency races resolved to exactly one "
          f"real execution each.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

