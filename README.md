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

uv run -m demo.scenarios                  # scripted walkthrough - deprecated
uv run -m demo.concurrent_clients         # many concurrent clients + idempotency races (node 5)

uv run bash ./demo/stop_cluster.sh
```

Or drive the cluster manually:

```bash
uv run -m client.cli --node 127.0.0.1:7002 status
uv run -m client.cli --node 127.0.0.1:7002 balances
uv run -m axonfx_node.server --node-id node3    # start a node with node-id
uv run -m client.cli --node 127.0.0.1:7002 transfer USD INR 10000 --to "Priya Sharma" --account HDFC-1
uv run -m client.cli --node 127.0.0.1:7002 history
```

LLM node FAQ.md queries:
```bash
uv run -m llm_node.ask_cli "what currencies does AxonFx support?"
uv run -m llm_node.ask_cli "how does a transfer actually work?"
uv run -m llm_node.ask_cli "does AxonFx charge a fee?"
```

We can point `--node` at *any* of the three cluster nodes - whichever one receives the request forwards writes to the current leader internally.
