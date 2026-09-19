# AxonFx
*Distributed multi-currency exchange infrastructure*

A lightweight, simulation of a cross-border currency settlement network, built to demonstrate a Raft consensus implementation, gRPC, and a replicated state machine.

## Prerequisites

This project uses [`uv`](https://docs.astral.sh/uv/) to manage the Python environment and dependencies. All commands below should be run through it:

- Python commands: `uv run -m <module>`
- Shell scripts: `uv run bash demo/<script>.sh`, or `uv run ./demo/<script>.sh` if the file already has execute permissions

## Quickstart

```bash
uv sync                                          # install dependencies

uv run bash ./demo/run_cluster.sh                # the Raft cluster (3 processes, nodes 2-4)

# For local LLM:
curl -fsSL https://ollama.com/install.sh | sh   # Run once to install ollama. If installed, proceed to next step.
ollama serve &                                  # if it's not already running as a service. Run this in wsl
ollama pull qwen2.5:1.5b                        # the default model this expects. Run only once and if already installed, proceed to next step.
export LLM_PROVIDER=ollama
uv run -m llm_node.server

uv run bash ./demo/run_llm_node.sh               # LLM/NLP server (node 1)

uv run -m demo.dashboard                  # optional: live view of roles + ledger, in a second terminal

uv run -m demo.concurrent_clients         # many concurrent clients + idempotency races (node 5)

uv run bash ./demo/stop_cluster.sh
```

Drive the cluster manually:

```bash
uv run -m client.cli --node 127.0.0.1:7002 status
uv run -m client.cli --node 127.0.0.1:7002 balances
uv run -m axonfx_node.server --node-id node3    # start a node with node-id
uv run -m client.cli --node 127.0.0.1:7002 transfer USD INR 10000 --to "Elon Musk" --account HDFC-1
uv run -m client.cli --node 127.0.0.1:7002 history
```

LLM node FAQ.md queries:
```bash
uv run -m llm_node.ask_cli "what currencies does AxonFx support?"
uv run -m llm_node.ask_cli "how does a transfer actually work?"
uv run -m llm_node.ask_cli "does AxonFx charge a fee?"
```

We can point `--node` at *any* of the three cluster nodes - whichever one receives the request forwards writes to the current leader internally.

---

## Appendix - command reference

| What | Command |
|---|---|
| Start cluster | `uv run bash ./demo/run_cluster.sh` |
| Stop cluster | `uv run bash ./demo/stop_cluster.sh` |
| Status of any node | `uv run -m client.cli --node 127.0.0.1:<port> status` |
| Stop a node | `pkill -9 -f "node-id node3"` |
| Start a node | `uv run -m axonfx_node.server --node-id node3` |
| Balances | `uv run -m client.cli --node 127.0.0.1:<port> balances` |
| Transfer | `uv run -m client.cli --node 127.0.0.1:<port> transfer <SRC> <DST> <AMOUNT> --to "<name>" --account <acct>` |
| Transfer with fixed id (for idempotency demo) | add `--request-id <id>` |
| History | `uv run -m client.cli --node 127.0.0.1:<port> history` |
| Live dashboard | `uv run -m demo.dashboard` |
| Concurrency/idempotency stress test | `uv run -m demo.concurrent_clients` |
| Scripted one-shot version of all scenarios | `uv run -m demo.scenarios` |
| Start LLM/FAQ node | `uv run bash ./demo/run_llm_node.sh` |
| Ask the LLM/FAQ node | `uv run -m llm_node.ask_cli "<question>"` |

Cluster nodes and ports are defined in (`demo/cluster.json`):

The LLM/FAQ node is a separate, single process on port **7100** - it is
not part of the Raft cluster and is not listed in `cluster.json`.

### Troubleshooting

- `Connection refused` from the CLI - the cluster hasn't finished
  electing a leader yet (wait 1-2s after starting), or you're pointing
  at a port that isn't one that's defined in cluster.json.
- A transfer hangs for several seconds then fails - this is
  the timeout or the leader-lease step-down (1.5s) doing its
  job - i.e., the cluster genuinely lost quorum. Check `status` on a couple of
  nodes to confirm.
- Stale state between demo runs - `demo/run_cluster.sh` wipes
  `data/<node>/` on every start, so each run begins from the same fixed
  starting pools in `state_machine.py`.

