#!/usr/bin/env bash
# Launches every node listed in demo/cluster.json in the background.
# To change cluster size, edit cluster.json - this script doesn't
# hardcode a node count or set of ids.
# Run ./demo/stop_cluster.sh to tear it down.
set -e
cd "$(dirname "$0")/.."

NODE_IDS=$(python -c "import json; print(' '.join(n['id'] for n in json.load(open('demo/cluster.json'))['nodes']))")
FIRST_PORT=$(python -c "import json; print(json.load(open('demo/cluster.json'))['nodes'][0]['grpc_port'])")

mkdir -p logs
for n in $NODE_IDS; do
  mkdir -p "data/$n"
  rm -f "data/$n"/* "logs/$n.log"
done

for n in $NODE_IDS; do
  nohup python -m axonfx_node.server --node-id "$n" > "logs/$n.log" 2>&1 &
  echo "started $n (pid $!)"
done

echo ""
echo "waiting a few seconds for leader election..."
sleep 4
python -m client.cli --node "127.0.0.1:$FIRST_PORT" status || true
echo ""
echo "cluster is up ($(echo $NODE_IDS | wc -w) nodes). Try:"
echo "  python -m client.cli --node 127.0.0.1:$FIRST_PORT balances"
echo "  python -m client.cli --node 127.0.0.1:$FIRST_PORT transfer USD INR 10000 --to \"Priya Sharma\" --account HDFC-1"
echo "  python -m demo.scenarios"
