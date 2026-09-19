#!/usr/bin/env bash
# Launches the standalone LLM/FAQ node (Node 1). Genuinely independent -
# holds no connection to the Raft cluster at all; answers questions
# from llm_node/FAQ.md only.
set -e
cd "$(dirname "$0")/.."
mkdir -p logs
nohup python3 -m llm_node.server "$@" > logs/llm_node.log 2>&1 &
echo "started llm_node (pid $!) - logs/llm_node.log"
