#!/usr/bin/env bash
set -euo pipefail

LEADER=${1:-astra-node1}
FOLLOWER_A=${2:-astra-node2}
FOLLOWER_B=${3:-astra-node3}

# Isolate leader from other nodes.
docker network disconnect $(docker network ls --format '{{.Name}}' | grep -m1 '_default') "${LEADER}" || true
sleep 5

echo "Leader ${LEADER} isolated. Validate writes fail to isolated leader and succeed on majority side."

echo "Reconnecting leader..."
docker network connect $(docker network ls --format '{{.Name}}' | grep -m1 '_default') "${LEADER}" || true
