#!/usr/bin/env bash
# Stops any running AxonFx node processes started by run_cluster.sh -
# reads the node list from cluster.json, same as run_cluster.sh does.
cd "$(dirname "$0")/.."
NODE_IDS=$(python -c "import json; print(' '.join(n['id'] for n in json.load(open('demo/cluster.json'))['nodes']))")
for n in $NODE_IDS; do
  pids=$(ps aux | grep "node-id $n" | grep -v grep | awk '{print $2}')
  if [ -n "$pids" ]; then
    echo "stopping $n (pid $pids)"
    kill -9 $pids
  fi
done

